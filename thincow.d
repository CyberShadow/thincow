/**
 * Create a deduplicated, COW view of block devices as a FUSE
 * filesystem.
 *
 * License:
 *   This Source Code Form is subject to the terms of
 *   the Mozilla Public License, v. 2.0. If a copy of
 *   the MPL was not distributed with this file, You
 *   can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Authors:
 *   Vladimir Panteleev <vladimir@thecybershadow.net>
 */

import core.bitop;
import core.stdc.errno;
import core.sys.linux.sys.mman : MAP_ANONYMOUS;
import core.sys.posix.fcntl;
import core.sys.posix.sys.ioctl;
import core.sys.posix.sys.mman;
import core.sys.posix.unistd;

import std.algorithm.comparison;
import std.algorithm.iteration;
import std.algorithm.searching;
import std.array;
import std.bitmanip;
import std.conv;
import std.digest.crc;
import std.exception;
import std.file;
import std.format;
import std.math;
import std.path;
import std.range;
import std.range.primitives;
import std.stdio;
import std.string;

import c.fuse.fuse;

import ae.sys.file;
import ae.utils.funopt;
import ae.utils.main;
import ae.utils.math : roundUpToPowerOfTwo;
import ae.utils.meta;

__gshared: // disable TLS

/// Host memory layout (assume typical x86_64 CPU)
enum cacheLineSize = size_t(64);
enum pageSize = size_t(4096);

/// Indicates an index of a block in a virtual (FUSE) device.
alias BlockIndex = ulong;

/// Indicates the index of a B-tree node within the B-tree block storage
/// (similar to a pointer to a B-tree node).
alias BTreeBlockIndex = ulong;

/// Block reference. Can refer to a block on an upstream device, or in the COW store, or nothing.
/// Used in the hash table and block map.
struct BlockRef
{
	enum Type : ubyte
	{
		unknown,  /// in hash table, free cell
		upstream, /// references a data block on an upstream device
		cow,      /// references a data block in our COW store (1-based index)
	}
	enum typeBits = 2;

	ulong value;

	void toString(W)(ref W writer) const
	if (isOutputRange!(W, char))
	{
		writer.formattedWrite!"%s(%d)"(type, offset);
	}

nothrow @nogc:
	this(Type type, ulong offset) { value = (offset << typeBits) | type; assert(this.type == type && this.offset == offset); }
	@property Type type() const { return cast(Type)(value & ((1UL << typeBits) - 1UL)); }
	@property ulong offset() const { return value >> typeBits; }

	@property bool unknown() const { return type == Type.unknown; }
	@property ulong upstream() const { assert(type == Type.upstream); return offset; }
	@property void upstream(ulong i) { this = BlockRef(Type.upstream, i); }
	@property ulong cow() const { assert(type == Type.cow); return offset; }
	@property void cow(ulong i) { this = BlockRef(Type.cow, i); }

	BlockRef opBinary(string op : "+")(ulong o) const { BlockRef r; r.value = value + (o << typeBits); assert(r.type == this.type); return r; }
}

/// Significant bits in a BlockRef.
uint blockRefBits;

/// Block size we're operating with
size_t blockSize;

/// Total number of blocks
BlockIndex totalBlocks;

/// These variables are persistent.
struct Globals
{
	/// Number of allocated B-tree nodes so far.
	BTreeBlockIndex btreeLength;
	/// B-tree node index of the root node.
	BTreeBlockIndex btreeRoot;
}
Globals* globals;

/// Command-line options.
bool retroactiveDeduplication;
bool readOnlyUpstream;

// *****************************************************************************
// Stats

size_t writesTotal, writesDeduplicatedHash, writesDeduplicatedExtend, writesDeduplicatedRetroactive;

template dumpStats(bool full)
{
	void dumpStats(W)(ref W writer)
	if (isOutputRange!(W, char))
	{
		writer.formattedWrite!"Block size: %d\n"(blockSize);
		writer.formattedWrite!"Total blocks: %d (%d bytes)\n"(totalBlocks, totalBlocks * blockSize);
		auto btreeDepth = (&blockMap[globals.btreeRoot])
			.recurrence!((state, i) => state[0] && !state[0].isLeaf ? &blockMap[state[0].elems[0].childIndex] : null)
			.countUntil(null);
		static if (full)
		{
			// Scan intent #1: collect per-device contents
			enum Contents { original, remapped, cow }
			static immutable contentsStr = ["Original", "Remapped", "COW"];
			alias DevContents = BlockIndex[enumLength!Contents];
			auto devContents = new DevContents[devs.length];
			size_t currentDevIndex;

			// Scan intent #2: collect total remapped block count (for disk space saving stats)
			size_t spaceSavedUpstream;

			// Scan intent #3: collect data for B-tree fullness plot
			// Plot: y -> depth, x -> fullness, plot[x,y] -> count with this fullness
			enum btreeFullnessCountBuckets = min(btreeNodeLength, 16);
			auto btreeFullness = new size_t[btreeFullnessCountBuckets][btreeDepth];
			auto btreeDepthUsed  = new size_t[btreeDepth];

			void scan(in ref BTreeNode node, BlockIndex start, BlockIndex end, size_t depth)
			{
				btreeFullness[depth][node.count * btreeFullnessCountBuckets / btreeNodeLength]++;
				btreeDepthUsed[depth] += 1 + node.count;

				foreach (elemIndex, ref elem; node.elems[0 .. node.count + 1])
				{
					auto elemStart = elemIndex ? elem.firstBlockIndex : start;
					auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : end;
					if (node.isLeaf)
					{
						auto elemLen = elemEnd - elemStart;
						Contents contents;
						if (elem.firstBlockRef.type == BlockRef.Type.upstream)
							if (elem.firstBlockRef.upstream == elemStart)
								contents = Contents.original;
							else
							{
								contents = Contents.remapped;
								spaceSavedUpstream += elemLen;
							}
						else
						if (elem.firstBlockRef.type == BlockRef.Type.cow)
							contents = Contents.cow;
						else
							assert(false);

						{
							auto remStart = elemStart;
							while (remStart < elemEnd)
							{
								assert(remStart >= devs[currentDevIndex].firstBlock);
								auto devEnd = currentDevIndex + 1 == devs.length ? totalBlocks : devs[currentDevIndex + 1].firstBlock;
								assert(remStart < devEnd);
								auto segStart = remStart;
								auto segEnd = min(elemEnd, devEnd);
								assert(segStart < segEnd);
								auto segLen = segEnd - segStart;
								devContents[currentDevIndex][contents] += segLen;
								remStart = segEnd;
								assert(remStart <= devEnd);
								if (remStart == devEnd)
									currentDevIndex++;
							}
						}
					}
					else
						scan(blockMap[elem.childIndex], elemStart, elemEnd, depth + 1);
				}
			}
			scan(blockMap[globals.btreeRoot], 0, totalBlocks, 0);
		}
		writer.formattedWrite!"Devices:\n"();
		foreach (i, ref Dev dev; devs)
		{
			auto devEnd = i + 1 == devs.length ? totalBlocks : devs[i + 1].firstBlock;
			auto devBlocks = devEnd - dev.firstBlock;
			writer.formattedWrite!"\tDevice #%d:\n"(i);
			writer.formattedWrite!"\t\tName: %(%s%)\n"(dev.name.only);
			writer.formattedWrite!"\t\tSize: %d bytes (%d blocks)\n"(dev.size, devBlocks);
			writer.formattedWrite!"\t\tFirst block: %d\n"(dev.firstBlock);
			writer.formattedWrite!"\t\tUpstream reads: %d blocks\n"(dev.reads);
			writer.formattedWrite!"\t\tUpstream read errors: %d\n"(dev.readErrors);
			writer.formattedWrite!"\t\tUpstream writes (flushes): %d blocks\n"(dev.writes);
			writer.formattedWrite!"\t\tUpstream write errors: %d\n"(dev.writeErrors);
			writer.formattedWrite!"\t\tFUSE read requests: %d blocks\n"(dev.readRequests);
			writer.formattedWrite!"\t\tFUSE write requests: %d blocks\n"(dev.writeRequests);
			static if (full)
			{
				writer.formattedWrite!"\t\tContents:\n"();
				foreach (c, blocks; devContents[i])
					writer.formattedWrite!"\t\t\t%s: %d blocks (%.0f%%)\n"(contentsStr[c], blocks, blocks * 100.0 / devBlocks);
			}
		}
		writer.formattedWrite!"B-tree nodes: %d (%d bytes)\n"(globals.btreeLength, globals.btreeLength * BTreeNode.sizeof);
		writer.formattedWrite!"B-tree depth: %d\n"(btreeDepth);
		writer.formattedWrite!"Current B-tree root: %d\n"(globals.btreeRoot);
		static if (full)
		{{
			auto btreeTotal = globals.btreeLength * btreeNodeLength;
			auto btreeUsed = btreeDepthUsed.sum;
			writer.formattedWrite!"B-tree occupancy: %d/%d (%d%%)\n"
				(btreeUsed, btreeTotal, btreeUsed * 100 / btreeTotal);
			foreach (depth; 0 .. btreeDepth)
			{
				auto btreeDepthNodes = btreeFullness[depth][].sum; // Total nodes at this depth
				auto btreeDepthTotal = btreeDepthNodes * btreeNodeLength;
				auto maxCount = btreeFullness[depth].reduce!max;
				writer.formattedWrite!"\tLevel %d: [%s] (%d nodes, %d/%d slots used, average: %.0f%%)\n"(
					depth,
					btreeFullness[depth][].map!(f => f ? ".oO@"[f * ($ - 1) / maxCount] : ' '),
					btreeDepthNodes,
					btreeDepthUsed[depth], btreeDepthTotal,
					btreeDepthUsed[depth] * 100.0 / btreeDepthTotal,
				);
			}
		}}
		writer.formattedWrite!"Hash table: %d buckets (%d bytes), %d entries per bucket (%d bytes)\n"
			(hashTable.length, hashTable.length * HashTableBucket.sizeof, hashTableBucketLength, hashTableBucketSize);
		static if (full)
		{{
			size_t totalUsed;
			size_t[hashTableBucketLength + 1] fullnessCounts;
			foreach (ref bucket; hashTable)
			{
				size_t emptyIndex = hashTableBucketLength;
				foreach (i, hbr; bucket)
					if (hbr.empty)
					{
						emptyIndex = i;
						break;
					}
					else
						totalUsed++;
				fullnessCounts[emptyIndex]++;
			}
			auto totalSlots = hashTableBucketLength * hashTable.length;
			writer.formattedWrite!"Hash table occupancy: %d/%d (%d%%)\n"
				(totalUsed, totalSlots, totalUsed * 100 / totalSlots);
			auto maxCount = fullnessCounts[].reduce!max;
			foreach (i, count; fullnessCounts)
			{
				enum maxWidth = 40;
				auto width = count * maxWidth / maxCount;
				writer.formattedWrite!"\t%d slots: %s%s %d buckets\n"(
					i,
					leftJustifier("",            width, '#'),
					leftJustifier("", maxWidth - width, '.'),
					count,
				);
			}
		}}
		auto writesDeduplicated = writesDeduplicatedHash + writesDeduplicatedExtend + writesDeduplicatedRetroactive;
		writer.formattedWrite!"Blocks written: %d total, %d (%d bytes, %.0f%%) deduplicated\n"
			(writesTotal, writesDeduplicated, writesDeduplicated * blockSize, writesDeduplicated * 100.0 / writesTotal);
		writer.formattedWrite!"Duplicate detection method: Hash table: %d (%.0f%%), extent extension: %d (%.0f%%), retroactive: %d (%.0f%%)\n"(
			writesDeduplicatedHash       , writesDeduplicatedHash        * 100.0 / writesDeduplicated,
			writesDeduplicatedExtend     , writesDeduplicatedExtend      * 100.0 / writesDeduplicated,
			writesDeduplicatedRetroactive, writesDeduplicatedRetroactive * 100.0 / writesDeduplicated,
		);
		static if (full)
		{{
			size_t cowFreeListLength = 0;
			size_t cowHead = 0;
			while (cowMap[cowHead].type == COWIndex.Type.nextFree)
			{
				cowFreeListLength++;
				cowHead = cowMap[cowHead].nextFree;
			}
			assert(cowMap[cowHead].type == COWIndex.Type.lastBlock);
			auto cowTotalBlocks = cowMap[cowHead].lastBlock + 1;
			writer.formattedWrite!"COW store: %d blocks (%d bytes), %d free for reuse\n"
				(cowTotalBlocks, cowTotalBlocks * blockSize, cowFreeListLength);
			size_t totalReferenced;
			size_t spaceSavedCOW;
			foreach (ci; cowMap[0 .. cowTotalBlocks])
				if (ci.type == COWIndex.Type.refCount)
				{
					totalReferenced += ci.refCount;
					spaceSavedCOW += ci.refCount - 1; // Don't count the first block, which DOES use up space
				}
			writer.formattedWrite!"Total COW references: %d blocks (%d bytes)\n"
				(totalReferenced, totalReferenced * blockSize);

			auto spaceSavedTotal = spaceSavedUpstream + spaceSavedCOW;
			writer.formattedWrite!"Disk space savings:\n";
			writer.formattedWrite!"\tDeduplicated to upstream: %d blocks (%d bytes)\n"
				(spaceSavedUpstream, spaceSavedUpstream * blockSize);
			writer.formattedWrite!"\tDeduplicated to COW store: %d blocks (%d bytes)\n"
				(spaceSavedCOW, spaceSavedCOW * blockSize);
			writer.formattedWrite!"\tTotal: %d blocks (%d bytes)\n"
				(spaceSavedTotal, spaceSavedTotal * blockSize);
		}}
	}
}

// *****************************************************************************
// Devices

/// Per-device (file) information
struct Dev
{
	/// File descriptor
	int fd;
	/// Size in bytes
	ulong size;
	/// First block index within the global block map
	BlockIndex firstBlock;
	/// File name of the device (in the upstream directory, and in the FUSE filesystem)
	string name;
	/// Stats
	ulong reads, readErrors, writes, writeErrors, readRequests, writeRequests;
}
Dev[] devs;

/// Find device containing the given BlockIndex.
ref Dev findDev(BlockIndex blockIndex) nothrow
{
	// TODO: this could be a binary search
	foreach (i, ref dev; devs)
		if (dev.firstBlock > blockIndex)
			return devs[i-1];
	return devs[$-1];
}

/// Find device by path.
Dev* getDev(const(char)[] path) nothrow
{
	if (!path.startsWith("/devs/"))
		return null;
	path = path["/devs/".length .. $];
	foreach (ref dev; devs)
		if (dev.name == path)
			return &dev;
	return null;
}

// *****************************************************************************
// Hash table

/// 64-bit value packing a BlockRef and using any remaining bits for hashing.
struct HashTableCell
{
	ulong value = 0;

	void toString(W)(ref W writer) const
	if (isOutputRange!(W, char))
	{
		auto hashBitHexDigits = (value.sizeof * 8 - blockRefBits + 3) / 4;
		writer.formattedWrite!"[0x%0*x] %s"(hashBitHexDigits, value >> blockRefBits, blockRef);
	}

nothrow @nogc:
	bool empty() const { return value == 0; }
	private static ulong blockRefMask() { return ((1UL << blockRefBits) - 1UL); }
	this(BlockRef br, ulong hashBits) { assert((br.value & ~blockRefMask) == 0); value = br.value | (hashBits << blockRefBits); }
	@property BlockRef blockRef() const { BlockRef br; br.value = value & blockRefMask; return br; }
	bool isHash(ulong hashBits) const { return (hashBits << blockRefBits) == (value & ~blockRefMask); }
}

/// Hash -> block lookup
enum hashTableBucketSize = cacheLineSize; // in bytes
enum hashTableBucketLength = hashTableBucketSize / BlockRef.sizeof;
alias HashTableBucket = HashTableCell[hashTableBucketLength];
HashTableBucket[] hashTable;

/// Hash some bytes.
alias Hash = ulong;
Hash hashBytes(const(ubyte)[] block) nothrow
{
	CRC64ECMA hash;
	hash.start();
	hash.put(block);
	auto result = hash.finish();
	static assert(Hash.sizeof <= result.length);
	return *cast(Hash*)result.ptr;
}

/// Add a block to the hash table, so that we can find it later.
/// Does nothing if the block is already in the hash table.
/// `br` indicates where the block's data can be found.
/// Returns the BlockRef that was found in or added to the hash table.
BlockRef hashBlock(const(ubyte)[] block, BlockRef br) nothrow
{
	assert(block.length == blockSize);

	auto hash = hashBytes(block);
	auto bucketIndex = hash % hashTable.length;
	auto hashRemainder = hash / hashTable.length;
	auto bucket = hashTable[bucketIndex][];
	auto cell = HashTableCell(br, hashRemainder);

	// First, check if the block ref is already in this bucket
	foreach (i, c; bucket)
		if (c.empty)
			break;
		else
		if (c == cell)
		{
			// Exact hit - move to front
			foreach_reverse (j; 0 .. i)
				bucket[j + 1] = bucket[j];
			bucket[0] = c;
			return c.blockRef;
		}

	// Next, check if one of the blocks has the same contents
	auto end = hashTableBucketLength - 1;
	static ubyte[] blockBuf;
	foreach (i, c; bucket)
		if (c.empty)
		{
			// Free cell - stop here
			end = i;
			break;
		}
		else
		if (c.isHash(hashRemainder) && tryReadBlock(c.blockRef, blockBuf) == block)
		{
			// Cache hit - move to front
			foreach_reverse (j; 0 .. i)
				bucket[j + 1] = bucket[j];
			bucket[0] = c;
			return c.blockRef;
		}

	// Add to front
	foreach_reverse (j; 0 .. end)
		bucket[j + 1] = bucket[j];
	bucket[0] = cell;
	return br;
}

/// Remove a block from the hash table.
void unhashBlock(BlockRef br) nothrow
{
	static ubyte[] blockBuf, blockBuf2;
	auto block = readBlock(br, blockBuf).assertNotThrown;
	assert(block.length == blockSize);

	auto hash = hashBytes(block);
	auto bucketIndex = hash % hashTable.length;
	auto hashRemainder = hash / hashTable.length;
	auto bucket = hashTable[bucketIndex][];
	auto cell = HashTableCell(br, hashRemainder);

	foreach (i, c; bucket)
	{
		if (c.empty)
			return;

		if (c == cell)
		{
			assert(block == tryReadBlock(c.blockRef, blockBuf2), "Matched BlockRef but not data in hash table");
			// Remove
			foreach (j; i + 1 .. hashTableBucketLength)
			{
				bucket[j - 1] = bucket[j];
				if (bucket[j].empty)
					break;
			}
			bucket[hashTableBucketLength - 1] = HashTableCell.init;
			return;
		}
	}
}

void dumpHashTable(W)(ref W writer)
if (isOutputRange!(W, char))
{
	auto bucketIndexHexDigits = (3 + cast(uint)ceil(log2(hashTable.length))) / 4;
	foreach (bucketIndex, ref bucket; hashTable)
	{
		writer.formattedWrite!"Bucket 0x%0*x:\n"(bucketIndexHexDigits, bucketIndex);
		foreach (i, c; bucket)
			writer.formattedWrite!"\tSlot %d: %s\n"(i, c);
	}
}

// *****************************************************************************
// Block allocation B-tree

/// Block map B-tree element, representing one extent
struct BTreeElement
{
	/// Block index of the first block of this element (B-tree key)
	/// Not used for the first element within a node, as this is implicit.
	BlockIndex firstBlockIndex;

	/// Interpretation depends on BTreeNode.isLeaf:
	union
	{
		/// For leaf nodes:
		/// Block index pointing to the first block in the range
		BlockRef firstBlockRef;
		/// For non-leaf nodes:
		/// B-tree node index of child
		BTreeBlockIndex childIndex;
	}
}

/*
 implicit              .              implicit
     |                 .                 |
     |    key   key    .    key   key    |
     |     |     |     .     |     |     |
     | ptr | ptr | ptr . ptr | ptr | ptr |
*/

debug (tiny_btree_nodes)
{
	// Cover more B-tree code (such as splitting)
	// in test suite by forcing tiny nodes
	enum btreeNodeLength = 4;
}
else
{
	enum btreeNodeSize = pageSize;
	enum btreeNodeLength = btreeNodeSize / BTreeElement.sizeof;
}
union BTreeNode
{
	/// A B-tree node with N nodes has N-1 keys.
	/// Take advantage of this and store metadata where the key for
	/// element 0 would be otherwise.
	struct
	{
		uint count;  /// Number of keys in this B-tree node (number of elements - 1)
		bool isLeaf; /// Is this B-tree node a leaf node?
	}
	BTreeElement[btreeNodeLength] elems;

	/// Return the element index (within `this.elems`)
	/// containing the given `blockIndex`.
	size_t find(BlockIndex blockIndex) const nothrow @nogc
	{
		size_t start = 0, end = 1 + count;
		while (start + 1 < end)
		{
			auto mid = (start + end) / 2;
			if (blockIndex < elems[mid].firstBlockIndex)
				end = mid;
			else
				start = mid;
		}
		return start;
	}
}

BTreeNode[/*BTreeBlockIndex*/] blockMap;

void dumpBtree(W)(ref W writer)
if (isOutputRange!(W, char))
{
	BlockIndex cbi = 0;
	void visitBlockIndex(BlockIndex bi)
	{
		assert(cbi < bi, "Out-of-order block index");
		cbi = bi;
	}

	static struct Indent
	{
		uint depth;
		void toString(W)(ref W writer) const
		if (isOutputRange!(W, char))
		{
			foreach (d; 0 .. depth) put(writer, '\t');
		}
	}
	uint depth;
	Indent indent() { return Indent(depth); }

	void dump(BTreeBlockIndex nodeIndex)
	{
		writer.formattedWrite!"%s@%d{\n"(indent, nodeIndex);
		depth++;
		auto node = &blockMap[nodeIndex];
		foreach (i; 0 .. node.count + 1)
		{
			if (i)
			{
				writer.formattedWrite!"%s^%d\n"(indent, node.elems[i].firstBlockIndex);
				visitBlockIndex(node.elems[i].firstBlockIndex);
			}
			if (node.isLeaf)
				writer.formattedWrite!"%s%s\n"(indent, node.elems[i].firstBlockRef);
			else
				dump(node.elems[i].childIndex);
		}
		depth--;
		writer.formattedWrite!"%s}\n"(indent);
	}
	put(writer, "btree:\n" ~ "\t^0\n");
	depth++;
	dump(globals.btreeRoot);
	visitBlockIndex(totalBlocks);
	writer.formattedWrite!"\t^%d\n"(totalBlocks);
}

/// Read the block map B-tree and return the BlockRef corresponding to the given BlockIndex.
BlockRef getBlockRef(BlockIndex blockIndex) nothrow @nogc
{
	static BlockRef search(ref BTreeNode node, BlockIndex blockIndex, BlockIndex start, BlockIndex end) nothrow @nogc
	{
		if (node.count > 0)
		{
			assert(node.elems[1].firstBlockIndex > start);
			assert(node.elems[node.count].firstBlockIndex < end);
		}

		auto elemIndex = node.find(blockIndex);
		auto elem = &node.elems[elemIndex];
		auto elemStart = elemIndex ? elem.firstBlockIndex : start;
		auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : end;
		if (node.isLeaf)
		{
			auto offset = blockIndex - elemStart;
			return elem.firstBlockRef + offset;
		}
		else
			return search(blockMap[elem.childIndex], blockIndex, elemStart, elemEnd);
	}

	return search(blockMap[globals.btreeRoot], blockIndex, 0, totalBlocks);
}

/// Write to the block map B-tree and set the given BlockIndex to the given BlockRef.
void putBlockRef(BlockIndex blockIndex, BlockRef blockRef) nothrow @nogc
{
	debug(btree) dumpToStderr!dumpBtree(">>> putBlockRef before: ");
	static void splitNode(ref BTreeNode parent, size_t childElemIndex)
	{
		assert(!parent.isLeaf);
		assert(parent.count + 1 < btreeNodeLength);
		if (globals.btreeLength >= blockMap.length)
			assert(false, "B-tree full");
		auto leftIndex = parent.elems[childElemIndex].childIndex;
		auto leftNode = &blockMap[leftIndex];
		auto rightIndex = globals.btreeLength++;
		auto rightNode = &blockMap[rightIndex];
		assert(rightNode.count == 0);
		auto pivotElemIndex = (leftNode.count + 1) / 2;
		assert(pivotElemIndex > 0);
		auto pivot = leftNode.elems[pivotElemIndex].firstBlockIndex;
		// Move nodes from left to right
		foreach (i; pivotElemIndex .. leftNode.count + 1)
			rightNode.elems[i - pivotElemIndex] = leftNode.elems[i];
		// Fix right node's metadata
		rightNode.elems[0].firstBlockIndex = 0;
		rightNode.isLeaf = leftNode.isLeaf;
		rightNode.count = leftNode.count - pivotElemIndex;
		// Update left node's metadata
		leftNode.count = pivotElemIndex - 1;
		// Insert new node in parent
		foreach_reverse (i; childElemIndex .. parent.count + 1)
			parent.elems[i + 1] = parent.elems[i];
		parent.count++;
		parent.elems[childElemIndex + 1].firstBlockIndex = pivot;
		parent.elems[childElemIndex + 1].childIndex = rightIndex;
		debug(btree) dumpToStderr!dumpBtree(">>> putBlockRef psplit: ");
	}

	/// Returns false if there was not enough room, and the parent needs splitting.
	bool descend(ref BTreeNode node, BlockIndex start, BlockIndex end)
	{
		if (node.count > 0)
		{
			assert(node.elems[1].firstBlockIndex > start);
			assert(node.elems[node.count].firstBlockIndex < end);
		}

		/// Try to merge the given extent into the previous one.
		void optimize(size_t elemIndex)
		{
			if (elemIndex == 0 || elemIndex > node.count)
				return;

			auto elem = &node.elems[elemIndex];
			auto elemStart = elemIndex ? elem.firstBlockIndex : start;

			auto pelemIndex = elemIndex - 1;
			auto pelem = &node.elems[pelemIndex];
			auto pelemStart = pelemIndex ? pelem.firstBlockIndex : start;
			auto pelemLength = elemStart - pelemStart;
			auto extrapolatedElemBlockRef = pelem.firstBlockRef + pelemLength;

			if (extrapolatedElemBlockRef == elem.firstBlockRef)
			{
				foreach (i; elemIndex .. node.count)
					node.elems[i] = node.elems[i + 1];
				node.count--;
			}
		}

		auto elemIndex = node.find(blockIndex);
	retry:
		auto elem = &node.elems[elemIndex];
		auto elemStart = elemIndex ? elem.firstBlockIndex : start;
		auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : end;
		auto offset = blockIndex - elemStart;
		assert(blockIndex >= elemStart && blockIndex < elemEnd);
		if (node.isLeaf)
		{
			if (elem.firstBlockRef + offset == blockRef)
				return true; // No-op write
			else
			if (blockIndex == elemStart) // Write to the beginning of the extent
			{
				if (elemIndex > 0 &&
					(){
						auto pelemIndex = elemIndex - 1; // Previous element
						auto pelem = &node.elems[pelemIndex];
						auto pelemStart = pelemIndex ? pelem.firstBlockIndex : start;
						auto pelemLength = elemStart - pelemStart;
						auto extrapolatedElemBlockRef = pelem.firstBlockRef + pelemLength;
						return extrapolatedElemBlockRef == blockRef;
					}())
				{
					// Sequential write optimization - this block is a continuation of the previous extent.
					// Simply move the boundary one block to the right
					// (thus growing the extent on the left and shrinking the extent on the right).
					elem.firstBlockIndex++;
					elem.firstBlockRef = elem.firstBlockRef + 1;
					if (elem.firstBlockIndex == elemEnd)
					{
						// Shrunk extent is now empty, remove it
						foreach (i; elemIndex .. node.count)
							node.elems[i] = node.elems[i + 1];
						node.count--;
						optimize(elemIndex);
					}
					return true;
				}
				else
				if (elemStart + 1 == elemEnd)
				{
					// Extent of length 1, just overwrite it
					elem.firstBlockRef = blockRef;
					optimize(elemIndex + 1);
					return true;
				}
				else
				{
					// Split up the extent, on the left side
					if (node.count + 1 == btreeNodeLength)
						return false; // No room
					foreach_reverse (i; elemIndex .. node.count + 1)
						node.elems[i + 1] = node.elems[i];
					node.count++;
					auto nelem = &node.elems[elemIndex + 1];
					nelem.firstBlockIndex = blockIndex + 1;
					nelem.firstBlockRef = nelem.firstBlockRef + 1;
					// Now that the extent is split up,
					// `elem` points to an extent of length 1,
					// so overwrite it as above.
					elem.firstBlockRef = blockRef;
					optimize(elemIndex);
					return true;
				}
			}
			else
			if (blockIndex + 1 == elemEnd) // Write to the end of the extent
			{
				// Split up the extent, on the right side
				if (node.count + 1 == btreeNodeLength)
					return false; // No room
				foreach_reverse (i; elemIndex .. node.count + 1)
					node.elems[i + 1] = node.elems[i];
				node.count++;
				// Create new 1-length extent, on the right side of the extent being split up
				auto nelem = &node.elems[elemIndex + 1];
				nelem.firstBlockIndex = blockIndex;
				nelem.firstBlockRef = blockRef;
				optimize(elemIndex + 1);
				return true;
			}
			else // Write to the middle of an extent
			{
				if (node.count + 2 >= btreeNodeLength)
					return false; // No room
				foreach_reverse (i; elemIndex .. node.count + 1)
					node.elems[i + 2] = node.elems[i];
				node.count += 2;
				auto nelem = &node.elems[elemIndex + 1];
				nelem.firstBlockIndex = blockIndex;
				nelem.firstBlockRef = blockRef;
				auto n2elem = &node.elems[elemIndex + 2];
				n2elem.firstBlockIndex = blockIndex + 1;
				n2elem.firstBlockRef = n2elem.firstBlockRef + offset + 1;
				return true;
			}
		}
		else
		{
			if (!descend(blockMap[elem.childIndex], elemStart, elemEnd))
			{
				if (node.count + 1 == btreeNodeLength)
					return false; // We ourselves don't have room. Split us up first
				splitNode(node, elemIndex);
				// Adjust blockIndex after splitting
				if (blockIndex >= node.elems[elemIndex + 1].firstBlockIndex)
					elemIndex++;
				goto retry;
			}
			return true;
		}
	}

	while (!descend(blockMap[globals.btreeRoot], 0, totalBlocks))
	{
		// First, allocate new root
		if (globals.btreeLength >= blockMap.length)
			assert(false, "B-tree full");
		auto newRootIndex = globals.btreeLength++;
		auto newRoot = &blockMap[newRootIndex];
		assert(newRoot.count == 0);
		newRoot.elems[0].childIndex = globals.btreeRoot;
		globals.btreeRoot = newRootIndex;
		// Now split
		splitNode(*newRoot, 0);
	}

	debug(btree) dumpToStderr!dumpBtree(">>> putBlockRef after : ");
}

// *****************************************************************************
// COW store

/// COW block index
struct COWIndex
{
	enum Type : ubyte
	{
		lastBlock,  /// free - how many blocks have been allocated so far, includes zero
		nextFree,   /// free - index to next free block
		refCount,   /// used - number of references to this block
	}

	ulong value;

	string toString() const { return format("%s(%d)", type, offset); }

nothrow @nogc:
	this(Type type, ulong offset) { value = offset | (ulong(type) << 62); assert(this.type == type && this.offset == offset); }
	@property Type type() const { return cast(Type)(value >> 62); }
	@property ulong offset() const { return value & ((1L << 62) - 1); }

	@property bool free() const { return (value & (1L << 63)) == 0; } // type is lastBlock or nextFree
	@property ulong lastBlock() const { assert(type == Type.lastBlock); return offset; }
	@property void lastBlock(ulong i) { this = COWIndex(Type.lastBlock, i); }
	@property ulong nextFree() const { assert(type == Type.nextFree); return offset; }
	@property void nextFree(ulong i) { this = COWIndex(Type.nextFree, i); }
	@property ulong refCount() const { assert(type == Type.refCount); return offset; }
	@property void refCount(ulong i) { this = COWIndex(Type.refCount, i); }
}
/// Our lookup for COW blocks
COWIndex[] cowMap;
/// The raw storage for COW data (data which does not exist on upstream devices)
ubyte[] cowData;

/// Where the next block will go should it be added to the COW store.
BlockRef getNextCow() nothrow
{
	BlockRef result;
	final switch (cowMap[0].type)
	{
		case COWIndex.Type.lastBlock:
			result.cow = 1 + cowMap[0].lastBlock;
			if (result.cow >= cowMap.length)
				assert(false, "COW storage full");
			break;
		case COWIndex.Type.nextFree:
			result.cow = cowMap[0].nextFree;
			break;
		case COWIndex.Type.refCount:
			assert(false);
	}
	assert(result.cow > 0);
	assert(cowMap[result.cow].free);
	return result;
}

/// Write a new block to a free cell in the COW store.
/// `br` should be a fresh return value of `getNextCow`
void cowWrite(BlockRef br, const(ubyte)[] block) nothrow
{
	assert(br.type == BlockRef.Type.cow);
	assert(cowMap[br.cow].free);

	auto offset = br.cow * blockSize;
	cowData[offset .. offset + block.length] = block;
	debug(cow) stderr.writefln(">> new: block %s [%(%02X %)]", br, block[0 .. 8]).assertNotThrown;
	final switch (cowMap[0].type)
	{
		case COWIndex.Type.lastBlock:
			assert(br.cow == cowMap[0].lastBlock + 1);
			cowMap[0].lastBlock = br.cow;
			break;
		case COWIndex.Type.nextFree:
			cowMap[0] = cowMap[br.cow];
			break;
		case COWIndex.Type.refCount:
			assert(false);
	}
	cowMap[br.cow].refCount = 1;
	debug(cow) dumpToStderr!dumpCOW(">>> after writeBlock: ");
}

void dumpCOW(W)(ref W writer)
if (isOutputRange!(W, char))
{
	put(writer, "cow:\n");
	foreach (i, ci; cowMap)
	{
		writer.formattedWrite!"\t%d: %s\n"(i, ci);
		if (ci == COWIndex.init)
			break;
	}
}

// *****************************************************************************
// Flushing

/// Return type of `findDirty`.
struct DirtyExtent
{
	/// Target block index of the first dirty block.
	BlockIndex blockIndex;
	/// What the block points to.
	BlockRef blockRef;
	/// Length of this extent.
	BlockIndex length;

	/// Return value of `findDirty` when nothing was found.
	static immutable notFound = DirtyExtent(-1UL);
}

/// Finds the first dirty extent (blocks which would need to be flushed),
/// assuming that all blocks before `start` are clean.
/// If there are no dirty blocks at/after `start`,
/// returns `DirtyBlock.notFound`.
DirtyExtent findDirty(BlockIndex start)
{
	if (start == totalBlocks)
		return DirtyExtent.notFound;
	assert(start < totalBlocks);

	static DirtyExtent search(ref BTreeNode node, BlockIndex searchStart, BlockIndex nodeStart, BlockIndex nodeEnd) nothrow @nogc
	{
		for (auto elemIndex = searchStart ? node.find(searchStart) : 0; elemIndex <= node.count; elemIndex++)
		{
			auto elem = &node.elems[elemIndex];
			auto elemStart = elemIndex ? elem.firstBlockIndex : nodeStart;
			auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : nodeEnd;
			if (node.isLeaf)
			{
				if (elem.firstBlockRef.type != BlockRef.Type.upstream || elem.firstBlockRef.upstream != elemStart)
				{
					auto elemLen = elemEnd - elemStart;
					return DirtyExtent(elemStart, elem.firstBlockRef, elemLen);
				}
			}
			else
			{
				auto res = search(blockMap[elem.childIndex], searchStart, elemStart, elemEnd);
				if (res !is DirtyExtent.notFound)
					return res;
			}

			searchStart = 0;
		}
		return DirtyExtent.notFound;
	}

	return search(blockMap[globals.btreeRoot], start, 0, totalBlocks);
}

/// Which upstream blocks are "in use".
BitArray useMap; // TODO: could be more efficiently (but with more complexity) represented as a tree of ranges

/// Flush at most `numBlocks` blocks.
void flush(BlockIndex blocksToFlush)
{
	static ubyte[] blockBuf;

	stderr.writefln("Flushing at most %d blocks.", blocksToFlush);
	flushError = null;

	useMap[] = false;
	stderr.writeln("Collecting usage information...");

	void scan(ref BTreeNode node, BlockIndex nodeStart, BlockIndex nodeEnd)
	{
		foreach (elemIndex, ref elem; node.elems[0 .. 1 + node.count])
		{
			auto elemStart = elemIndex ? elem.firstBlockIndex : nodeStart;
			auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : nodeEnd;
			if (node.isLeaf)
			{
				if (elem.firstBlockRef.type == BlockRef.Type.upstream)
				{
					auto elemLen = elemEnd - elemStart;
					auto useStart = elem.firstBlockRef.upstream;
					auto useEnd = useStart + elemLen;
					useMap[useStart .. useEnd] = true;
				}
			}
			else
				scan(blockMap[elem.childIndex], elemStart, elemEnd);
		}
	}
	scan(blockMap[globals.btreeRoot], 0, totalBlocks);

	stderr.writeln("Flushing...");
	BlockIndex pos = 0;
	BlockIndex blocksFlushed = 0;
	while (blocksToFlush > 0)
	{
		auto dirty = findDirty(pos);
		if (dirty is DirtyExtent.notFound)
			break;

		foreach (i; 0 .. dirty.length)
		{
			auto blockIndex = dirty.blockIndex + i;
			if (useMap[blockIndex])
				continue; // Can't overwrite this block, it's in use
			auto blockRef = dirty.blockRef + i;
			auto upstreamBlockRef = BlockRef(BlockRef.Type.upstream, blockIndex);

			// Copy the block from its mapped location to the current position, thus flushing it.
			auto block = readBlock(blockRef, blockBuf);
			unhashBlock(upstreamBlockRef);
			flushBlock(blockIndex, block);

			unreferenceBlock(blockRef);
			putBlockRef(blockIndex, upstreamBlockRef);
			hashBlock(block, upstreamBlockRef);

			blocksFlushed++;
			if (!--blocksToFlush)
				break;
		}
		pos = dirty.blockIndex + dirty.length;
	}

	stderr.writefln("Flushed %d blocks.", blocksFlushed);

	if (blocksFlushed == 0 && blocksToFlush > 0)
	{
		// This can only happen in case of a data inter-dependency,
		// e.g. want to write A->B and B->A.
		// There is always at least one loop, each consisting of at least two blocks.
		// To break the loop, copy some (or all) blocks from the loop to our COW store.
		stderr.writefln("Failed to flush anything. Copying at most %d blocks to COW store...", blocksToFlush);

		pos = 0;
		while (blocksToFlush > 0)
		{
			auto dirty = findDirty(pos);
			if (dirty is DirtyExtent.notFound)
				break;

			foreach (i; 0 .. dirty.length)
			{
				auto blockIndex = dirty.blockIndex + i;
				auto blockRef = dirty.blockRef + i;
				if (blockRef.type != BlockRef.Type.upstream)
				{
					// Can't have COW references here,
					// because if there were COW references,
					// it would imply that the dependency chain is finite (i.e. not a loop).
					assert(false);
				}

				// Note: We are moving data from the target layer and not the upstream layer.
				// This will unpin (free) the reference pointed at by `blockRef`.

				auto block = readBlock(blockRef, blockBuf);
				unhashBlock(blockRef);

				auto nextCow = getNextCow();
				auto result = hashBlock(block, nextCow);
				if (result == nextCow)
					cowWrite(result, block);
				else
				if (result.type == BlockRef.Type.cow)
				{
					// Saved to COW, then discovered on upstream.
					// Reuse the old COW location.
					referenceBlock(result);
				}
				else
				{
					// Hashed to somewhere else..?
					unhashBlock(result);
					result = hashBlock(block, nextCow);
					cowWrite(result, block);
				}

				putBlockRef(blockIndex, result);

				blocksFlushed++;
				if (!--blocksToFlush)
					break;
			}
			pos = dirty.blockIndex + dirty.length;
		}
		stderr.writefln("Copied %d blocks.", blocksFlushed);
	}
}

void flushBlock(BlockIndex index, in ubyte[] block)
{
	auto dev = &findDev(index);
	auto offset = (index - dev.firstBlock) * blockSize;
	ulong pos = 0;
	do
	{
		auto bytesWritten = pwrite(dev.fd, block.ptr + pos, block.length - pos, offset + pos);
		if (bytesWritten <= 0)
		{
			dev.writeErrors++;
			throw new ErrnoException(format("Error flushing %d bytes to upstream device %s at %d", block.length - pos, dev.name, offset + pos));
		}
		pos += bytesWritten;
	} while (pos < blockSize && offset + pos < dev.size);
	dev.writes++;
}

string flushError;

void getFlushStatus(W)(ref W writer)
if (isOutputRange!(W, char))
{
	if (readOnlyUpstream)
		writer.put("disabled");
	else
	if (flushError)
	{
		writer.put("error\n");
		writer.put(flushError);
	}
	else
	if (findDirty(0) is DirtyExtent.notFound)
		writer.put("clean");
	else
		writer.put("ready");
}

void handleFlushClose(char[] data) nothrow
{
	try
	{
		data = data.chomp();
		switch (data)
		{
			case "full":
				while (findDirty(0) !is DirtyExtent.notFound)
					flush(BlockIndex.max);
				break;
			case "max":
				flush(BlockIndex.max);
				break;
			default:
				flush(data.to!BlockIndex);
		}
	}
	catch (Exception e)
	{
		flushError = e.msg;
		stderr.writeln(e).assertNotThrown();
	}
}

// *****************************************************************************
// I/O operations

/// Read a block from a device and a given address.
const(ubyte)[] readBlock(Dev* dev, size_t devBlockIndex, ref ubyte[] blockBuf)
{
	dev.readRequests++;
	BlockIndex blockIndex = dev.firstBlock + devBlockIndex;
	auto br = getBlockRef(blockIndex);
	auto block = readBlock(br, blockBuf);
	hashBlock(block, br);
	return block;
}

/// Read a block using its reference
/// (either from upstream or our COW store).
const(ubyte)[] readBlock(BlockRef br, ref ubyte[] blockBuf)
{
	final switch (br.type)
	{
		case BlockRef.Type.unknown:
			assert(false);
		case BlockRef.Type.upstream:
		{
			auto index = br.upstream;
			auto dev = &findDev(index);
			auto offset = (index - dev.firstBlock) * blockSize;
			blockBuf.length = blockSize;
			ulong pos = 0;
			do
			{
				auto bytesRead = pread(dev.fd, blockBuf.ptr + pos, blockSize - pos, offset + pos);
				if (bytesRead < 0)
				{
					dev.readErrors++;
					throw new ErrnoException(null);
				}
				if (bytesRead == 0)
				{
					blockBuf[pos .. $] = 0; // Zero-expand
					break;
				}
				pos += bytesRead;
			} while (pos < blockSize);
			dev.reads++;
			return blockBuf;
		}
		case BlockRef.Type.cow:
		{
			auto offset = br.cow * blockSize;
			return cowData[offset .. offset + blockSize];
		}
	}
}

/// As above, but don't trust that `br` is valid.
/// Return `null` if it isn't.
const(ubyte)[] tryReadBlock(BlockRef br, ref ubyte[] blockBuf) nothrow
{
	final switch (br.type)
	{
		case BlockRef.Type.unknown:
			return null;
		case BlockRef.Type.upstream:
			if (br.upstream >= totalBlocks)
				return null;
			try
				return readBlock(br, blockBuf);
			catch (ErrnoException)
				return null;
			catch (Exception e)
				assert(false, e.toString());
		case BlockRef.Type.cow:
			if (cowMap[br.cow].free)
				return null;
			return readBlock(br, blockBuf).assertNotThrown;
	}
}

/// Write a block to a device and a given address.
void writeBlock(Dev* dev, size_t devBlockIndex, const(ubyte)[] block) nothrow
{
	dev.writeRequests++;
	BlockIndex blockIndex = dev.firstBlock + devBlockIndex;
	unreferenceBlock(getBlockRef(blockIndex));

	// Check if we can extend from the previous block.
	if (blockIndex > 0)
	{
		static ubyte[] blockBuf;
		auto extrapolatedBlockRef = getBlockRef(blockIndex - 1) + 1;
		auto extrapolatedBlock = tryReadBlock(extrapolatedBlockRef, blockBuf);
		if (extrapolatedBlock && extrapolatedBlock == block)
		{
			referenceBlock(extrapolatedBlockRef);
			putBlockRef(blockIndex, extrapolatedBlockRef);
			writesDeduplicatedExtend++;
			writesTotal++;
			return;
		}
	}

	auto nextCow = getNextCow();
	auto result = hashBlock(block, nextCow);
	if (result == nextCow)
	{
		// New block - add to COW store
		cowWrite(result, block);
	}
	else
	{
		referenceBlock(result);
		writesDeduplicatedHash++;
	}
	putBlockRef(blockIndex, result);
	writesTotal++;

	// Check if we can retroactively deduplicate the previous block
	if (retroactiveDeduplication)
		while (blockIndex > 0 && result.offset > 0)
		{
			auto extrapolatedBlockRef = BlockRef(result.type, result.offset - 1);
			auto prevBlockIndex = blockIndex - 1;
			auto prevBlockRef = getBlockRef(prevBlockIndex);
			if (prevBlockRef == extrapolatedBlockRef)
				break; // Already contiguous
			if (prevBlockRef.type != BlockRef.Type.cow)
				break; // No need to deduplicate/defragment if it already points to upstream
			static ubyte[] blockBuf1, blockBuf2;
			auto block1 = tryReadBlock(prevBlockRef, blockBuf1);
			if (!block1)
				break; // Read error
			auto block2 = tryReadBlock(extrapolatedBlockRef, blockBuf2);
			if (!block2)
				break; // Read error
			if (block1 != block2)
				break; // Not the same data
			unreferenceBlock(prevBlockRef);
			putBlockRef(prevBlockIndex, extrapolatedBlockRef);
			writesDeduplicatedRetroactive++;
			writesTotal++; // "Fake" write, to keep deduplication % sane

			// Keep going
			blockIndex = prevBlockIndex;
			result = extrapolatedBlockRef;
		}
}

/// Indicates that we are now using one more reference to the given block.
void referenceBlock(BlockRef br) nothrow
{
	final switch (br.type)
	{
		case BlockRef.Type.unknown:
			assert(false);
		case BlockRef.Type.upstream:
			return; // No problem, it is on an upstream device (infinite lifetime)
		case BlockRef.Type.cow:
		{
			auto index = br.cow;
			assert(!cowMap[index].free);
			auto refCount = cowMap[index].refCount;
			debug(cow) stderr.writefln(">> reference: block %s [%(%02X %)] refcount %d -> %d",
					br, readBlock(br)[0 .. 8], refCount, refCount+1).assertNotThrown;
			cowMap[index].refCount = refCount + 1;
		}
		debug(cow) dumpToStderr!dumpCOW(">>> after referenceBlock: ");
	}
}

/// Indicates that we are no longer using one reference to the given block.
/// If it was stored in the COW store, decrement its reference count,
/// and if it reaches zero, delete it from there and the hash table.
void unreferenceBlock(BlockRef br) nothrow
{
	final switch (br.type)
	{
		case BlockRef.Type.unknown:
			return; // No problem, we never even looked at it
		case BlockRef.Type.upstream:
			return; // No problem, it is on an upstream device (infinite lifetime)
		case BlockRef.Type.cow:
		{
			auto index = br.cow;
			assert(!cowMap[index].free);
			auto refCount = cowMap[index].refCount;
			assert(refCount > 0);
			debug(cow) stderr.writefln(">> unreference: block %s [%(%02X %)] refcount %d -> %d",
					br, readBlock(br)[0 .. 8], refCount, refCount-1).assertNotThrown;
			refCount--;
			if (refCount == 0)
			{
				unhashBlock(br);
				cowMap[index] = cowMap[0];
				cowMap[0].nextFree = index;
			}
			else
				cowMap[index].refCount = refCount;
			debug(cow) dumpToStderr!dumpCOW(">>> after unreferenceBlock: ");
		}
	}
}

// *****************************************************************************
// fsck

bool fsck()
{
	ulong[] cowRefCount;
	auto btreeVisited = new bool[globals.btreeLength];
	size_t numErrors;

	void logError(string s)
	{
		stderr.writeln("- ERROR: ", s);
		stderr.flush();
		numErrors++;
	}

	{
		stderr.writeln("Scanning B-tree...");

		void scan(size_t nodeIndex, BlockIndex nodeStart, BlockIndex nodeEnd)
		{
			if (nodeIndex >= globals.btreeLength)
				logError(format!"Out-of-bounds B-tree child node index: %d/%d"(
					nodeIndex, globals.btreeLength,
				));
			else
			{
				if (btreeVisited[nodeIndex])
					logError(format!"Multiple references to B-tree child node %d/%d"(
						nodeIndex, globals.btreeLength,
					));
				btreeVisited[nodeIndex] = true;
			}

			auto node = &blockMap[nodeIndex];
			if (!node.isLeaf)
				stderr.write(nodeStart, " / ", totalBlocks, '\r'); stderr.flush();

			auto pos = nodeStart;
			foreach (elemIndex, ref elem; node.elems[0 .. 1 + node.count])
			{
				auto elemStart = elemIndex ? elem.firstBlockIndex : nodeStart;
				auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : nodeEnd;
				if (elemStart > elemEnd)
					logError(format!"Invalid range %d-%d for element %d in B-tree leaf node %d"(
						elemStart, elemEnd,
						elemIndex,
						nodeIndex,
					));
				else
				if (elemStart == elemEnd)
					logError(format!"Empty range %d-%d for element %d in B-tree leaf node %d"(
						elemStart, elemEnd,
						elemIndex,
						nodeIndex,
					));
				if (pos > elemStart)
					logError(format!"Descending range %d-%d (after %d) for element %d in B-tree leaf node %d"(
						elemStart, elemEnd,
						pos,
						elemIndex,
						nodeIndex,
					));
				pos = elemEnd;

				if (node.isLeaf)
				{
					switch (elem.firstBlockRef.type)
					{
						case BlockRef.Type.upstream:
						{
							auto elemLen = elemEnd - elemStart;
							auto useStart = elem.firstBlockRef.upstream;
							auto useEnd = useStart + elemLen;
							foreach (i, use; [useStart, useEnd].staticArray)
								if (use > totalBlocks)
									logError(format!"%s of element %d in B-tree leaf node %d references out-of-bounds upstream block %d (out of %d)"(
										i ? "End" : "Start",
										elemIndex,
										nodeIndex,
										use,
										totalBlocks,
									));
							break;
						}
						case BlockRef.Type.cow:
						{
							auto elemLen = elemEnd - elemStart;
							auto useStart = elem.firstBlockRef.cow;
							auto useEnd = useStart + elemLen;
							foreach (use; useStart .. useEnd)
							{
								if (use >= cowMap.length)
									logError(format!"Block %d in range of element %d in B-tree leaf node %d references out-of-bounds COW block %d (out of %d)"(
										use - useStart,
										elemIndex,
										nodeIndex,
										use,
										cowMap.length,
									));
								if (cowRefCount.length < use + 1)
									cowRefCount.length = use + 1;
								cowRefCount[use]++;
							}
							break;
						}
						default:
							logError(format!"Element %d in B-tree leaf node %d has unknown type %s"(
								elemIndex,
								nodeIndex,
								elem.firstBlockRef.type,
							));
					}
				}
				else
					scan(elem.childIndex, elemStart, elemEnd);
			}
		}
		scan(globals.btreeRoot, 0, totalBlocks);
	}

	{
		stderr.writeln("Checking B-tree usage...");

		foreach (nodeIndex, visited; btreeVisited)
			if (!visited)
				logError(format!"Unreferenced (lost) B-tree node: %d/%d"(
					nodeIndex, globals.btreeLength,
				));
	}

	ulong usedCowBlocks;
	{
		stderr.writeln("Scanning COW free list...");

		ulong cowLastBlock = ulong.max;
		ulong i = 0;
	cowLoop:
		while (true)
		{
			enforce(i < cowMap.length, "Out-of-bounds COW free list block");

			if (cowRefCount.length < i + 1)
				cowRefCount.length = i + 1;
			if (cowRefCount[i] == ulong.max)
				logError(format!"Item %d occurs several times in the COW free list"(
					i,
				));
			else
			{
				if (cowRefCount[i] != 0)
					logError(format!"Item %d is in the COW free list, but occurs in the B-tree %d times"(
						i,
						cowRefCount[i],
					));
				cowRefCount[i] = ulong.max;
			}

			switch (cowMap[i].type)
			{
				case COWIndex.Type.lastBlock:
					cowLastBlock = cowMap[i].lastBlock;
					break cowLoop;
				case COWIndex.Type.nextFree:
					i = cowMap[i].nextFree;
					break;
				default:
					logError(format!"Item %d in COW free list has unexpected type %s"(
						i,
						cowMap[i].type,
					));
			}
		}

		assert(cowLastBlock != ulong.max); // unreachable otherwise
		if (cowLastBlock >= cowMap.length)
			logError(format!"Out-of-bounds last COW block: %d/%d"(
				cowLastBlock,
				cowMap.length,
			));
		usedCowBlocks = cowLastBlock + 1;
	}

	{
		stderr.writeln("Scanning COW map...");

		foreach (i; 0 .. usedCowBlocks)
		{
			if (cowRefCount[i] == 0)
				logError(format!"COW block %d/%d (in map as %s) occurs neither in the free list nor B-tree"(
					i, usedCowBlocks,
					cowMap[i],
				));
			else
			if (cowRefCount[i] == ulong.max)
				assert(cowMap[i].free, // not possible
					format!"COW block %d/%d (in map as %s) is in the free list but is not free"(
						i, usedCowBlocks,
						cowMap[i],
					));

			if (cowRefCount[i] != ulong.max)
			{
				if (cowMap[i].free)
					logError(format!"COW block %d/%d (in map as %s) occurs in the B-tree %d times but is marked as free"(
						i, usedCowBlocks,
						cowMap[i],
						cowRefCount[i],
					));
				if (cowMap[i].refCount != cowRefCount[i])
					logError(format!"COW block %d/%d is in map as %s but occurs in the B-tree %d times"(
						i, usedCowBlocks,
						cowMap[i],
						cowRefCount[i],
					));
			}
		}

		foreach (i; usedCowBlocks .. cowMap.length)
		{
			if (i < cowRefCount.length && cowRefCount[i] != 0)
				logError(format!"Reserved COW block %d/%d (in map as %s) actually occurs in the B-tree %d times"(
					i, usedCowBlocks,
					cowMap[i],
					cowRefCount[i],
				));
			if (cowMap[i] !is COWIndex.init)
				logError(format!"Reserved COW block %d/%d (in map as %s) is non-null"(
					i, usedCowBlocks,
					cowMap[i],
				));
		}
	}

	if (!numErrors)
	{
		stderr.writeln("Found no errors.");
		return true;
	}
	else
	{
		stderr.writefln("Found %d errors!", numErrors);
		return false;
	}
}

// *****************************************************************************
// FUSE implementation

enum FuseHandle : uint64_t
{
	none,
	rootDir,
	devsDir,
	debugDir,

	firstDevice = 0x10000000_00000000,
	firstFile   = 0x20000000_00000000,
}

/// These are virtual files that are rendered at the time of opening,
/// so that their contents remains consistent throughout the file handle's lifetime.
char[][uint64_t] files;
uint64_t nextFileIndex = FuseHandle.firstFile;

/// What to do when a file opened for writing is closed.
alias CloseHandler = void function(char[]) nothrow;
CloseHandler[uint64_t] closeHandlers;

void dumpToStderr(alias fun)(string prefix) nothrow
{
	(){
		stderr.write(prefix);
		auto writer = stderr.lockingTextWriter();
		fun(writer);
	}().assertNotThrown();
}

void makeFile(alias fun)(fuse_file_info* fi) nothrow
{
	Appender!(char[]) appender;
	try
		fun(appender);
	catch (Exception e)
		put(appender, e.toString().assertNotThrown());
	auto fd = nextFileIndex++;
	files[fd] = appender.data;
	fi.fh = fd;
	fi.direct_io = true;
}

void makeWritableFile(fuse_file_info* fi, CloseHandler handleClose) nothrow
{
	makeFile!((w) {})(fi);
	closeHandlers[fi.fh] = handleClose;
}

extern(C) nothrow
{
	int fs_getattr(const char* c_path, stat_t* s)
	{
		auto path = c_path.fromStringz;
		switch (path)
		{
			case "/":
			case "/devs":
			case "/debug":
				s.st_mode = S_IFDIR | S_IRUSR | S_IXUSR;
				break;
			case "/debug/btree.txt":
			case "/debug/cow.txt":
			case "/debug/hash-table.txt":
			case "/stats.txt":
			case "/stats-full.txt":
				s.st_mode = S_IFREG | S_IRUSR;
				s.st_size = typeof(s.st_size).max;
				break;
			case "/flush":
				s.st_mode = S_IFREG | S_IRUSR | S_IWUSR;
				s.st_size = typeof(s.st_size).max;
				break;
			default:
				if (path.startsWith("/devs/"))
				{
					auto dev = getDev(path);
					if (!dev) return -ENOENT;
					s.st_size = dev.size;
					s.st_mode = S_IFREG | S_IRUSR | S_IWUSR;
				}
				else
					return -ENOENT;
		}
		s.st_mtime = 0;
		s.st_uid = getuid();
		s.st_gid = getgid();
		return 0;
	}

	int fs_readdir(const char* path, void* buf,
				fuse_fill_dir_t filler, off_t /*offset*/, fuse_file_info* fi)
	{
		switch (fi.fh)
		{
			case FuseHandle.rootDir:
			{
				static immutable char*[] rootDir = [
					"devs",
					"debug",
					"stats.txt",
					"stats-full.txt",
					"flush",
				];
				foreach (d; rootDir)
					filler(buf, cast(char*)d, null, 0);
				return 0;
			}
			case FuseHandle.devsDir:
				foreach (ref dev; devs)
					filler(buf, cast(char*)toStringz(dev.name), null, 0);
				return 0;
			case FuseHandle.debugDir:
			{
				static immutable char*[] debugDir = [
					"btree.txt",
					"cow.txt",
					"hash-table.txt",
				];
				foreach (d; debugDir)
					filler(buf, cast(char*)d, null, 0);
				return 0;
			}
			default:
				return -ENOTDIR;
		}
	}

	int fs_open(const char* c_path, fuse_file_info* fi)
	{
		auto path = c_path.fromStringz;
		switch (path)
		{
			case "/debug/btree.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!dumpBtree(fi);
				return 0;
			case "/debug/cow.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!dumpCOW(fi);
				return 0;
			case "/debug/hash-table.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!dumpHashTable(fi);
				return 0;
			case "/stats.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!(dumpStats!false)(fi);
				return 0;
			case "/stats-full.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!(dumpStats!true)(fi);
				return 0;
			case "/flush":
				if ((fi.flags & O_ACCMODE) == O_RDONLY)
					makeFile!getFlushStatus(fi);
				else
				if ((fi.flags & O_ACCMODE) == O_WRONLY)
					makeWritableFile(fi, &handleFlushClose);
				else
					return -EINVAL;
				return 0;
			default:
				if (path.startsWith("/devs/"))
				{
					auto dev = getDev(path);
					if (!dev) return -ENOENT;
					auto devIndex = dev - devs.ptr;
					fi.direct_io = true;
					fi.fh = FuseHandle.firstDevice + devIndex;
					return 0;
				}
				return -ENOENT;
		}
	}

	int fs_truncate(const char* c_path, off_t offset)
	{
		auto path = c_path.fromStringz;
		if (offset != 0)
			return -EINVAL;
		switch (path)
		{
			case "/flush":
				return 0;
			default:
				return -EINVAL;
		}
	}

	int fs_opendir(const char* c_path, fuse_file_info* fi)
	{
		auto path = c_path.fromStringz;
		switch (path)
		{
			case "/":
				fi.fh = FuseHandle.rootDir;
				return 0;
			case "/devs":
				fi.fh = FuseHandle.devsDir;
				return 0;
			case "/debug":
				fi.fh = FuseHandle.debugDir;
				return 0;
			default:
				return -ENOENT;
		}
	}

	int fs_release(const char* c_path, fuse_file_info* fi)
	{
		if (fi.fh >= FuseHandle.firstFile)
		{
			if (auto phandler = fi.fh in closeHandlers)
			{
				(*phandler)(files[fi.fh]);
				closeHandlers.remove(fi.fh);
			}
			files.remove(fi.fh);
		}
		return 0;
	}

	int fs_read(const char* /*path*/, char* buf_ptr, size_t size, off_t offset, fuse_file_info* fi)
	{
		auto buf = (cast(ubyte*)buf_ptr)[0 .. size];

		if (fi.fh >= FuseHandle.firstFile)
		{
			auto pfile = fi.fh in files;
			if (!pfile) return -EBADFD;
			auto file = *pfile;
			if (offset >= file.length)
				return 0;
			file = file[offset .. $];
			auto len = cast(int)min(size, file.length, int.max);
			buf_ptr[0 .. len] = file[0 .. len];
			return len;
		}
		if (fi.fh >= FuseHandle.firstDevice)
		{
			auto devIndex = fi.fh - FuseHandle.firstDevice;
			if (devIndex >= devs.length) return -EBADFD;
			auto dev = &devs[devIndex];

			auto end = min(offset + size, dev.size);
			if (offset >= end)
				return 0;
			auto head = buf.ptr;
			foreach (blockOffset; offset / blockSize .. (end - 1) / blockSize + 1)
			{
				const(ubyte)[] block;
				static ubyte[] blockBuf;
				try
					block = dev.readBlock(blockOffset, blockBuf);
				catch (ErrnoException e)
				{
					auto pos = head - buf.ptr;
					if (pos == 0)
						return -e.errno;
					else
						return cast(int)pos;
				}
				catch (Exception e)
					assert(false, e.toString());
				auto blockStart = blockOffset * blockSize;
				auto blockEnd = blockStart + blockSize;
				auto spanStart = max(blockStart, offset);
				auto spanEnd   = min(blockEnd  , end);
				auto len = spanEnd - spanStart;
				head[0 .. len] = block[spanStart - blockStart .. spanEnd - blockStart];
				head += len;
			}
			// assert(head == buf.ptr + buf.length);
			return cast(int)(end - offset);
		}
		return -EBADFD;
	}

	int fs_write(const char* /*path*/, char* data_ptr, size_t size,
                            off_t offset, fuse_file_info* fi)
	{
		if (size > int.max)
			size = int.max;

		auto data = (cast(ubyte*)data_ptr)[0 .. size];

		if (fi.fh >= FuseHandle.firstFile)
		{
			if (fi.fh !in files)
				return -EBADFD;
			if (fi.fh !in closeHandlers)
				return -EROFS;
			if (offset != files[fi.fh].length)
				return -EINVAL;
			files[fi.fh] ~= cast(char[])data;
			return cast(int)size;
		}
		if (fi.fh >= FuseHandle.firstDevice)
		{
			auto devIndex = fi.fh - FuseHandle.firstDevice;
			if (devIndex >= devs.length) return -EBADFD;
			auto dev = &devs[devIndex];

			auto end = min(offset + size, dev.size);
			if (offset >= end)
				return 0;
			auto head = data.ptr;
			foreach (blockOffset; offset / blockSize .. (end - 1) / blockSize + 1)
			{
				auto blockStart = blockOffset * blockSize;
				auto blockEnd = blockStart + blockSize;
				auto spanStart = max(blockStart, offset);
				auto spanEnd   = min(blockEnd  , end);
				auto len = spanEnd - spanStart;
				if (len == blockSize)
				{
					// Write the whole block
					dev.writeBlock(blockOffset, head[0 .. blockSize]);
				}
				else
				{
					// Read-modify-write
					static ubyte[] blockBuf;
					try
						dev.readBlock(blockOffset, blockBuf);
					catch (ErrnoException e)
					{
						auto pos = head - data.ptr;
						if (pos == 0)
							return -e.errno;
						else
							return cast(int)pos;
					}
					catch (Exception e)
						assert(false, e.toString());
					blockBuf[spanStart - blockStart .. spanEnd - blockStart] = head[0 .. len];
					dev.writeBlock(blockOffset, blockBuf[]);
				}
				head += len;
			}
			return cast(int)(end - offset);
		}
		return -EBADFD;
	}
}

// *****************************************************************************
// Entry point

@(`Create a deduplicated, COW view of block devices as a FUSE filesystem.`)
int thincow(
	Parameter!(string, "Where to mount the FUSE filesystem.") target,
	Option!(string, "Directory containing upstream devices/symlinks.", "PATH") upstream,
	Option!(string, "Directory where to store COW blocks.", "PATH") dataDir,
	Option!(string, "Directory where to store metadata. If unspecified, defaults to the data directory.\nSpecify \"-\" to use RAM.", "PATH") metadataDir = null,
	Option!(string, "Directory where to store large temporary data. If unspecified, defaults to \"-\" (RAM).", "PATH") tempDir = "-",
	Option!(size_t, "Block size.\nLarger blocks means smaller metadata, but writes smaller than one block will cause a read-modify-write. The default is 512.", "BYTES") blockSize = 512,
	Option!(size_t, "Hash table size.\nThe default is 1073741824 (1 GiB).", "BYTES") hashTableSize = 1024*1024*1024,
	Option!(size_t, "Maximum size of the block map.", "BYTES") maxBlockMapSize = 1L << 63,
	Option!(size_t, "Maximum number of blocks in the COW store.", "BLOCKS") maxCowBlocks = 1L << 63,
	Switch!("Enable retroactive deduplication (more I/O intensive).") retroactive = false,
	Switch!("Run in foreground.", 'f') foreground = false,
	Switch!("Open upstream devices in read-only mode (flushing will be disabled).", 'r') readOnlyUpstream = false,
	Option!(string[], "Additional FUSE options (e.g. debug).", "STR", 'o') options = null,
	Switch!("Perform data validity check on startup.") fsck = false,
	Switch!("Don't actually mount the filesystem, and exit after initialization/fsck.") noMount = false,
)
{
	enforce(upstream, "No upstream device directory specified");
	enforce(dataDir, "No data directory specified");
	if (!metadataDir)
		metadataDir = dataDir;

	.blockSize = blockSize;
	.retroactiveDeduplication = retroactive;
	.readOnlyUpstream = readOnlyUpstream;

	upstream.value.listDir!((de)
	{
		stderr.write(de.baseName, ": ");
		enforce(!de.isDir, "Directories are not supported");
		size_t deviceSize;
		auto fd = openat(de.dirFD, de.ent.d_name.ptr, readOnlyUpstream ? O_RDONLY : O_RDWR);
		errnoEnforce(fd >= 0, "open failed: " ~ de.fullName);
		if (de.isFile)
			deviceSize = de.size;
		else // assume device
		{
			enum BLKGETSIZE64 = _IOR!size_t(0x12,114);
			ioctl(fd, BLKGETSIZE64, &deviceSize);
		}
		auto numBlocks = (deviceSize + blockSize - 1) / blockSize;
		stderr.writeln(deviceSize, " bytes (", numBlocks, " blocks)");

		Dev dev;
		dev.name = de.baseName;
		dev.firstBlock = totalBlocks;
		dev.size = deviceSize;
		dev.fd = fd;
		devs ~= dev;
		totalBlocks += numBlocks;
	});

	void[] mapFile(string dir, string name, size_t recordSize, size_t numRecords)
	{
		auto size = recordSize * numRecords;
		enforce(size / recordSize == numRecords, "Overflow");
		stderr.writefln("%s: %d bytes (%d records)", name, size, numRecords);

		void* ptr;
		string path;
		if (dir == "-")
		{
			path = "(in memory) " ~ name;
			ptr = mmap(null, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
		}
		else
		{
			path = dir.buildPath(name);

			if (path.exists)
				enforce(path.getSize() == size, "Found existing file, but it is of the wrong size: " ~ path);

			auto fd = open(path.toStringz, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
			errnoEnforce(fd >= 0, "open failed: " ~ path);
			ftruncate(fd, size);
			ptr = mmap(null, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		}
		errnoEnforce(ptr != MAP_FAILED, "mmap failed: " ~ path);
		return ptr[0 .. size];
	}

	globals = cast(Globals*)mapFile(metadataDir, "globals", Globals.sizeof, 1).ptr;

	auto btreeMaxLength = totalBlocks; // worst case
	auto btreeMaxSize = btreeMaxLength * BTreeNode.sizeof;
	if (btreeMaxSize > maxBlockMapSize)
		btreeMaxSize = maxBlockMapSize;
	btreeMaxLength = btreeMaxSize / BTreeNode.sizeof;
	blockMap = cast(BTreeNode[])mapFile(metadataDir, "blockmap", BTreeNode.sizeof, btreeMaxLength);

	auto hashTableLength = hashTableSize / HashTableBucket.sizeof;
	enforce(hashTableLength * HashTableBucket.sizeof == hashTableSize, "Hash table size must be a multiple of %s".format(HashTableBucket.sizeof));
	enforce(hashTableLength == hashTableLength.roundUpToPowerOfTwo, "Hash table size must be a power of 2");
	enforce(hashTableLength <= 0x1_0000_0000, "Hash table is too large");
	hashTable = cast(HashTableBucket[])mapFile(metadataDir, "hashtable", HashTableBucket.sizeof, hashTableLength);

	auto maxCowBlocksLimit = totalBlocks + 2; // Index 0 is reserved, and one more for swapping
	maxCowBlocks = min(maxCowBlocks, maxCowBlocksLimit);
	cowMap = cast(COWIndex[])mapFile(metadataDir, "cowmap", COWIndex.sizeof, maxCowBlocks);
	cowData = cast(ubyte[])mapFile(dataDir, "cowdata", blockSize, maxCowBlocks);

	uint bitsNeeded(ulong maxValue) { assert(maxValue); maxValue--; return maxValue ? 1 + bsr(maxValue) : 0; }
	blockRefBits = bitsNeeded(maxCowBlocksLimit) + BlockRef.typeBits;

	if (!readOnlyUpstream)
	{
		auto useMapBuf = mapFile(tempDir, "usemap", 1, (totalBlocks + 7) / 8);
		useMap = BitArray(useMapBuf, totalBlocks);
	}

	if (!globals.btreeLength)
	{
		stderr.writeln("Initializing block map B-tree.");
		globals.btreeRoot = globals.btreeLength++;
		auto root = &blockMap[globals.btreeRoot];
		root.isLeaf = true;
		BlockRef br;
		br.upstream = 0;
		root.elems[0].firstBlockRef = br;
	}
	debug(btree) dumpToStderr!dumpBtree("");

	if (fsck)
		if (!.fsck())
			return 1;

	if (noMount)
		return 0;

	fuse_operations fsops;
	fsops.readdir = &fs_readdir;
	fsops.getattr = &fs_getattr;
	fsops.open = &fs_open;
	fsops.truncate = &fs_truncate;
	fsops.opendir = &fs_opendir;
	fsops.release = &fs_release;
	fsops.read = &fs_read;
	fsops.write = &fs_write;
	fsops.flag_nullpath_ok = 1;
	fsops.flag_nopath = 1;

	options ~= ["big_writes"]; // Essentially required, otherwise FUSE will use 4K writes and cause abysmal performance

	string[] args = ["thincow", target, "-o%-(%s,%)".format(options)];
	args ~= "-s"; // single-threaded
	if (foreground)
		args ~= "-f";
	auto c_args = new char*[args.length];
	foreach (i, arg; args)
		c_args[i] = cast(char*)arg.toStringz;
	auto f_args = FUSE_ARGS_INIT(cast(int)c_args.length, c_args.ptr);

	stderr.writeln("Starting FUSE filesystem.");
	scope(success) stderr.writeln("thincow exiting.");
	return fuse_main(f_args.argc, f_args.argv, &fsops, null);
}

mixin main!(funopt!thincow);

extern(C) int openat(int dirfd, const char *pathname, int flags, ...);
