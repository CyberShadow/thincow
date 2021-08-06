/**
 * Stats
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

module thincow.stats;

import std.algorithm.comparison;
import std.algorithm.iteration;
import std.algorithm.searching;
import std.format;
import std.range;
import std.range.primitives;
import std.string : leftJustifier;

import ae.utils.meta : enumLength;

import thincow.btree;
import thincow.common;
import thincow.cow;
import thincow.devices;
import thincow.hashtable;

__gshared: // disable TLS

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

version (unittest) mixin TestWriterFun!(dumpStats!false);
version (unittest) mixin TestWriterFun!(dumpStats!true);
