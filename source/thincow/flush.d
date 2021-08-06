/**
 * Flushing
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

module thincow.flush;

import core.sys.posix.unistd;

import std.bitmanip : BitArray;
import std.conv : to;
import std.exception : ErrnoException, assertNotThrown;
import std.format;
import std.range.primitives : isOutputRange;
import std.stdio : stderr;
import std.string;

import thincow.btree;
import thincow.common;
import thincow.cow;
import thincow.devices;
import thincow.hashtable : hashBlock, unhashBlock;
import thincow.io;

__gshared: // disable TLS

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

version (unittest) mixin TestWriterFun!getFlushStatus;

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
