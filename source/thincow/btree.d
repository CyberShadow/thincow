/**
 * Block allocation B-tree
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

module thincow.btree;

import std.format;
import std.range.primitives : isOutputRange, put;

import thincow.common;

__gshared: // disable TLS

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

void dumpBtree(W)(ref W writer, BlockIndex root = globals.btreeRoot)
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
	dump(root);
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

version (unittest) mixin TestWriterFun!dumpBtree;
