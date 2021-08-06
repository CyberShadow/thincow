/**
 * COW store
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

module thincow.cow;

import std.format;
import std.range.primitives;

import thincow.common;

__gshared: // disable TLS

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

version (unittest) mixin TestWriterFun!dumpCOW;
