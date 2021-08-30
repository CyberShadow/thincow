/**
 * Common
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

module thincow.common;

import std.format : formattedWrite;
import std.range.primitives : isOutputRange;

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

	unittest
	{
		import std.conv : to;
		assert(BlockRef.init.to!string == "unknown(0)");
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
	/// thincow data format version.
	uint dataVersion = 3;
	/// True if thincow exited uncleanly.
	bool dirty;
	/// Number of allocated B-tree nodes so far.
	BTreeBlockIndex btreeLength;
	/// B-tree node index of the root node.
	BTreeBlockIndex btreeRoot;
}
Globals* globals;
static assert(Globals.sizeof != 16); // Indistinguishable from v0

/// Command-line options.
bool retroactiveDeduplication;
bool readOnlyUpstream;

version (unittest)
mixin template TestWriterFun(alias fun)
{
	unittest
	{
		if (false)
		{
			import std.array : Appender;
			Appender!string appender;
			fun(appender);
		}
	}
}
