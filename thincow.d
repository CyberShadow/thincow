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
import ae.utils.math;

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
		writer.formattedWrite!"B-tree nodes: %d (%d bytes)\n"(globals.btreeLength, globals.btreeLength * BTreeNode.sizeof);
		writer.formattedWrite!"B-tree depth: %d\n"(
			(&blockMap[globals.btreeRoot])
			.recurrence!((state, i) => state[0] && !state[0].isLeaf ? &blockMap[state[0].elems[0].childIndex] : null)
			.countUntil(null));
		writer.formattedWrite!"Current B-tree root: %d\n"(globals.btreeRoot);
		writer.formattedWrite!"Devices:\n"();
		foreach (i, ref dev; devs)
			writer.formattedWrite!"\tDevice #%d: %(%s%), %d bytes, first block: %d, %d read errors\n"(i, dev.name.only, dev.size, dev.firstBlock, dev.errors);
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
		{
			size_t spaceSavedUpstream;
			void scan(in ref BTreeNode node, BlockIndex start, BlockIndex end)
			{
				foreach (elemIndex, ref elem; node.elems[0 .. node.count + 1])
				{
					auto elemStart = elemIndex ? elem.firstBlockIndex : start;
					auto elemEnd = elemIndex < node.count ? node.elems[elemIndex + 1].firstBlockIndex : end;
					if (node.isLeaf)
					{
						if (elem.firstBlockRef.type == BlockRef.Type.upstream && elem.firstBlockRef.upstream != elemStart)
							spaceSavedUpstream += elemEnd - elemStart;
					}
					else
						scan(blockMap[elem.childIndex], elemStart, elemEnd);
				}
			}
			scan(blockMap[globals.btreeRoot], 0, totalBlocks);
		}
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
	/// Read errors
	ulong errors;
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
			assert(c.blockRef.type == BlockRef.Type.cow, "Unhashing non-COW block");
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
		uint count;  /// Number of keys in this B-tree node (1 - number of elements)
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
// I/O operations

/// Read a block from a device and a given address.
const(ubyte)[] readBlock(Dev* dev, size_t devBlockIndex, ref ubyte[] blockBuf)
{
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
				auto bytesRead = pread(dev.fd, blockBuf.ptr + pos, blockSize - pos, offset);
				if (bytesRead < 0)
				{
					dev.errors++;
					throw new ErrnoException(null);
				}
				if (bytesRead == 0)
				{
					blockBuf[pos .. $] = 0; // Zero-expand
					break;
				}
				pos += bytesRead;
			} while (pos < blockSize);
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

/// Where the next block will go should it be added to the COW store.
BlockRef getNextCow() nothrow
{
	BlockRef result;
	final switch (cowMap[0].type)
	{
		case COWIndex.Type.lastBlock:
			result.cow = 1 + cowMap[0].lastBlock;
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

/// Write a block to a device and a given address.
void writeBlock(Dev* dev, size_t devBlockIndex, const(ubyte)[] block) nothrow
{
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
		auto offset = result.cow * blockSize;
		cowData[offset .. offset + block.length] = block;
		debug(cow) stderr.writefln(">> new: block %s [%(%02X %)]", result, block[0 .. 8]).assertNotThrown;
		final switch (cowMap[0].type)
		{
			case COWIndex.Type.lastBlock:
				assert(result.cow == cowMap[0].lastBlock + 1);
				cowMap[0].lastBlock = result.cow;
				break;
			case COWIndex.Type.nextFree:
				cowMap[0] = cowMap[result.cow];
				break;
			case COWIndex.Type.refCount:
				assert(false);
		}
		cowMap[result.cow].refCount = 1;
		debug(cow) dumpToStderr!dumpCOW(">>> after writeBlock: ");
	}
	else
	{
		referenceBlock(result);
		writesDeduplicatedHash++;
	}
	putBlockRef(blockIndex, result);
	writesTotal++;

	// Check if we can retroactively deduplicate the previous block
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
string[uint64_t] files;
uint64_t nextFileIndex = FuseHandle.firstFile;

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
	Appender!string appender;
	try
		fun(appender);
	catch (Exception e)
		put(appender, e.toString().assertNotThrown());
	auto fd = nextFileIndex++;
	files[fd] = appender.data;
	fi.fh = fd;
	fi.direct_io = true;
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
			case "/":
				fi.fh = FuseHandle.rootDir;
				return 0;
			case "/devs":
				fi.fh = FuseHandle.devsDir;
				return 0;
			case "/debug":
				fi.fh = FuseHandle.debugDir;
				return 0;
			case "/debug/btree.txt":
				makeFile!dumpBtree(fi);
				return 0;
			case "/debug/cow.txt":
				makeFile!dumpCOW(fi);
				return 0;
			case "/debug/hash-table.txt":
				makeFile!dumpHashTable(fi);
				return 0;
			case "/stats.txt":
				makeFile!(dumpStats!false)(fi);
				return 0;
			case "/stats-full.txt":
				makeFile!(dumpStats!true)(fi);
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

	int fs_release(const char* c_path, fuse_file_info* fi)
	{
		if (fi.fh >= FuseHandle.firstFile)
			files.remove(fi.fh);
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
		if (fi.fh >= FuseHandle.firstFile)
			return -EROFS;
		if (fi.fh >= FuseHandle.firstDevice)
		{
			auto devIndex = fi.fh - FuseHandle.firstDevice;
			if (devIndex >= devs.length) return -EBADFD;
			auto dev = &devs[devIndex];

			auto data = (cast(ubyte*)data_ptr)[0 .. size];
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
void thincow(
	Parameter!(string, "Where to mount the FUSE filesystem.") target,
	Option!(string, "Directory containing upstream devices/symlinks.", "PATH") upstream,
	Option!(string, "Directory where to store COW blocks.", "PATH") dataDir,
	Option!(string, "Directory where to store metadata. If unspecified, defaults to the data directory.\nSpecify \"-\" to use RAM.", "PATH") metadataDir = null,
	Option!(size_t, "Block size.\nLarger blocks means smaller metadata, but writes smaller than one block will cause a read-modify-write. The default is 512.", "BYTES") blockSize = 512,
	Option!(size_t, "Hash table size.\nThe default is 1073741824 (1 GiB).", "BYTES") hashTableSize = 1024*1024*1024,
	Option!(size_t, "Maximum size of the block map.", "BYTES") maxBlockMapSize = 1L << 63,
	Option!(size_t, "Maximum number of blocks in the COW store.", "BLOCKS") maxCowBlocks = 1L << 63,
	Switch!("Run in foreground.", 'f') foreground = false,
	Option!(string[], "Additional FUSE options (e.g. debug).", "STR", 'o') options = null,
)
{
	enforce(upstream, "No upstream device directory specified");
	enforce(dataDir, "No data directory specified");
	if (!metadataDir)
		metadataDir = dataDir;

	.blockSize = blockSize;

	upstream.value.listDir!((de)
	{
		stderr.write(de.baseName, ": ");
		enforce(!de.isDir, "Directories are not supported");
		size_t deviceSize;
		auto fd = openat(de.dirFD, de.ent.d_name.ptr, O_RDONLY);
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

	fuse_operations fsops;
	fsops.readdir = &fs_readdir;
	fsops.getattr = &fs_getattr;
	fsops.open = &fs_open;
	fsops.opendir = &fs_open;
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

	fuse_main(f_args.argc, f_args.argv, &fsops, null);
	stderr.writeln("thincow exiting.");
}

mixin main!(funopt!thincow);

extern(C) int openat(int dirfd, const char *pathname, int flags, ...);
