/**
 * Integrity check
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

module thincow.fsck;

import std.algorithm.comparison;
import std.array : staticArray;
import std.format : format;
import std.stdio : stderr;

import thincow.btree : blockMap;
import thincow.common;
import thincow.cow : cowMap, COWIndex;

__gshared: // disable TLS

ulong[] cowRefCount;

bool fsck()
{
	auto btreeVisited = new bool[globals.btreeLength];
	size_t numErrors;
	ulong cowRealLastBlock = 0;

	void logError(string s)
	{
		stderr.writeln("- ERROR: ", s);
		stderr.flush();
		numErrors++;
	}

	{
		stderr.writeln("Checking globals...");
		if (globals.btreeLength > blockMap.length)
		{
			logError(format!"Out-of-bounds B-tree length: %d/%d"(
				globals.btreeLength, blockMap.length,
			));
		}
		if (globals.btreeRoot >= globals.btreeLength)
		{
			logError(format!"Out-of-bounds B-tree root node index: %d/%d"(
				globals.btreeRoot, globals.btreeLength,
			));
		}
	}

	{
		stderr.writeln("Scanning B-tree...");

		void scan(size_t nodeIndex, BlockIndex nodeStart, BlockIndex nodeEnd)
		{
			if (nodeIndex >= globals.btreeLength)
			{
				logError(format!"Out-of-bounds B-tree child node index: %d/%d"(
					nodeIndex, globals.btreeLength,
				));
			}
			if (nodeIndex >= blockMap.length)
				return;

			if (nodeIndex < globals.btreeLength)
			{
				if (btreeVisited[nodeIndex])
				{
					logError(format!"Multiple references to B-tree child node %d/%d"(
							nodeIndex, globals.btreeLength,
						));
					return;
				}
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
								else
								{
									cowRefCount[use]++;
									cowRealLastBlock = max(cowRealLastBlock, use);
								}
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

	if (false)
	{
		stderr.writeln("Rebuilding COW map...");
		ulong lastFree = 0;

		foreach (ulong i; 1 .. cowRealLastBlock + 1)
			if (cowRefCount[i] > 0)
				cowMap[i] = COWIndex(COWIndex.Type.refCount, cowRefCount[i]);
			else
			{
				cowMap[lastFree] = COWIndex(COWIndex.Type.nextFree, i);
				lastFree = i;
			}

		cowMap[lastFree] = COWIndex(COWIndex.Type.lastBlock, cowRealLastBlock);

		for (ulong i = cowRealLastBlock + 1; cowMap[i] !is COWIndex.init; i++)
			cowMap[i] = COWIndex.init;
	}

	ulong usedCowBlocks;
	{
		stderr.writeln("Scanning COW free list...");

		ulong cowLastBlock = ulong.max;
		ulong i = 0;
	cowLoop:
		while (true)
		{
			if (i >= cowMap.length)
			{
				logError("Out-of-bounds COW free list block");
				break;
			}

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

		if (cowLastBlock == ulong.max)
			logError("COW free list is corrupted");
		else
		if (cowLastBlock >= cowMap.length)
			logError(format!"Out-of-bounds last COW block: %d/%d"(
				cowLastBlock,
				cowMap.length,
			));
		else
			usedCowBlocks = cowLastBlock + 1;
	}

	if (usedCowBlocks == 0)
		logError("Not scanning COW map because the free list is corrupted.");
	else
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
		if (globals.dirty)
		{
			stderr.writeln("Clearing dirty flag. (However, this is not a guarantee that there is no data corruption.)");
			globals.dirty = false;
		}
		return true;
	}
	else
	{
		stderr.writefln("Found %d errors!", numErrors);
		return false;
	}
}
