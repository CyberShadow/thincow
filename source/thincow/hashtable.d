/**
 * Hash table
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

module thincow.hashtable;

import std.algorithm.searching : startsWith;
import std.digest.crc : CRC64ECMA;
import std.exception : assertNotThrown;
import std.format : formattedWrite;
import std.math : ceil, log2;
import std.range.primitives : isOutputRange;

import thincow.common;
import thincow.io;

__gshared: // disable TLS

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

version (unittest) mixin TestWriterFun!dumpHashTable;
