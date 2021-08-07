/**
 * I/O operations
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

module thincow.io;

import std.exception : ErrnoException, assertNotThrown;

import core.sys.posix.unistd : pread;

import thincow.btree : getBlockRef, putBlockRef;
import thincow.cow;
import thincow.common;
import thincow.devices : Dev, findDev;
import thincow.hashtable : hashBlock, unhashBlock;
import thincow.stats;

__gshared: // disable TLS

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
			referenceBlock(extrapolatedBlockRef);
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
