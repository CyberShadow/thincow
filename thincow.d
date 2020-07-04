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

import core.sys.linux.sys.mman : MAP_ANONYMOUS;
import core.sys.posix.fcntl;
import core.sys.posix.sys.ioctl;
import core.sys.posix.sys.mman;
import core.sys.posix.unistd;

import std.algorithm.comparison;
import std.digest.murmurhash;
import std.exception;
import std.file;
import std.path;
import std.stdio;
import std.string;

import dfuse.fuse;

import ae.sys.file;
import ae.utils.funopt;
import ae.utils.main;
import ae.utils.math;

__gshared: // disable TLS

/// Block reference. Can refer to a block on an upstream device, or in the COW store, or nothing.
/// Used in the hash table and block map.
struct BlockIndex
{
	enum Type
	{
		unknown,  /// in block map, never yet read or written; in hash table, free cell
		upstream, /// references a data block on an upstream device
		cow,      /// references a data block in our COW store (1-based index)
	}

	long value;

	@property Type type() const { return value == 0 ? Type.unknown : value < 0 ? Type.upstream : Type.cow; }
	@property bool unknown() const { return type == Type.unknown; }
	@property ulong upstream() const { assert(type == Type.upstream); return ~value; }
	@property void upstream(ulong i) { value = ~i; assert(type == Type.upstream); }
	@property ulong cow() const { assert(type == Type.cow); return value; }
	@property void cow(ulong i) { value = i; assert(type == Type.cow); }
	string toString() const { return format("%s (%d)", type, value < 0 ? ~value : value); }
}

/// Block size we're operating with
size_t blockSize;

/// Hash -> block lookup
BlockIndex[] hashTable;

/// Per-device (file) information
struct Dev
{
	/// Memory mapped upstream device
	const(ubyte)[] data;
	/// Represents contents of the device we present via FUSE
	BlockIndex[] blockMap;
	/// First block index within the global block map
	size_t firstBlock;
	/// File name of the device (in the upstream directory, and in the FUSE filesystem)
	string name;
}
Dev[] devs;

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

	@property Type type() const { return cast(Type)(value >> 62); }
	@property bool free() const { return (value & (1L << 63)) == 0; } // type is lastBlock or nextFree

	@property size_t lastBlock() const { assert(type == Type.lastBlock); return value & ((1L << 62) - 1); }
	@property void lastBlock(size_t i) { value = i | (ulong(Type.lastBlock) << 62); assert(type == Type.lastBlock); }
	@property size_t nextFree() const { assert(type == Type.nextFree); return value & ((1L << 62) - 1); }
	@property void nextFree(size_t i) { value = i | (ulong(Type.nextFree) << 62); assert(type == Type.nextFree); }
	@property size_t refCount() const { assert(type == Type.refCount); return value & ((1L << 62) - 1); }
	@property void refCount(size_t i) { value = i | (ulong(Type.refCount) << 62); assert(type == Type.refCount); }
}
/// Our lookup for COW blocks
COWIndex[] cowMap;
/// The raw storage for COW data (data which does not exist on upstream devices)
ubyte[] cowData;

/// Hash some bytes.
ulong hash(const(ubyte)[] block)
{
	MurmurHash3!128 hash;
	hash.start();
	hash.put(block);
	auto result = hash.finish();
	return *cast(ulong*)result.ptr;
}

/// Add a block to the hash table, so that we can find it later.
/// Does nothing if the block is already in the hash table.
/// `bi` indicates where the block's data can be found.
BlockIndex hashBlock(const(ubyte)[] block, BlockIndex bi)
{
	assert(block.length == blockSize);

	auto h = hash(block) % hashTable.length;
	while (true)
	{
		auto e = &hashTable[h];
		if (e.unknown)
		{
			// Free cell (new entry)
			*e = bi;
			return *e;
		}

		if (block == readBlock(*e))
		{
			// Cache hit
			return *e;
		}

		h = (h + 1) % hashTable.length; // Keep looking
	}
}

/// Remove a block from the hash table.
void unhashBlock(BlockIndex bi)
{
	auto block = readBlock(bi);
	assert(block.length == blockSize);

	auto i = hash(block) % hashTable.length;
	while (true)
	{
		auto e = &hashTable[i];
		if (e.unknown)
			assert(false, "Can't find entry to unhash");

		if (*e == bi)
		{
			assert(block == readBlock(*e), "Matched BlockIndex but not data in hash table");
			assert(e.type == BlockIndex.Type.cow, "Unhashing non-COW block");
			break;
		}

		i = (i + 1) % hashTable.length;
	}

	// Standard erasing algorithm for open-addressing hash tables
	auto j = i;
	while (true)
	{
		hashTable[i] = BlockIndex.init;
		ulong k;
		do
		{
			j = (j + 1) % hashTable.length;
			if (hashTable[j].unknown)
				return;
			k = hash(readBlock(hashTable[j])) % hashTable.length;
		} while (i <= j
			? i < k && k <= j
			: i < k || k <= j);
		hashTable[i] = hashTable[j];
		i = j;
	}
}

const(ubyte)[] zeroExpand(const(ubyte)[] block)
{
	if (block.length < blockSize)
	{
		// Zero-expand partial trailing blocks in memory
		static ubyte[] buf;
		if (!buf.length)
			buf.length = blockSize;
		buf[0 .. block.length] = block;
		buf[block.length .. $] = 0;
		block = buf[];
	}
	return block;
}

/// Read a block from a device and a given address.
const(ubyte)[] readBlock(Dev* dev, size_t blockIndex)
{
	auto bi = &dev.blockMap[blockIndex];
	if (bi.unknown)
	{
		// We've never looked at this block before,
		// so add it to our hash table,
		// and make it refer to itself.
		bi.upstream = dev.firstBlock + blockIndex;
		auto offset = blockIndex * blockSize;
		auto block = dev.data[offset .. min(offset + blockSize, $)].zeroExpand;
		hashBlock(block, *bi);
		return block;
	}
	else
		return readBlock(*bi);
}

/// Read a block using its reference
/// (either from upstream or our COW store).
const(ubyte)[] readBlock(BlockIndex bi)
{
	final switch (bi.type)
	{
		case BlockIndex.Type.unknown:
			assert(false);
		case BlockIndex.Type.upstream:
		{
			auto index = bi.upstream;
			foreach (ref dev2; devs)
				if (dev2.firstBlock <= index && dev2.firstBlock + dev2.blockMap.length > index)
				{
					auto offset = (index - dev2.firstBlock) * blockSize;
					return dev2.data[offset .. min(offset + blockSize, $)].zeroExpand;
				}
			assert(false, "Out-of-range upstream block index");
		}
		case BlockIndex.Type.cow:
		{
			auto offset = bi.cow * blockSize;
			return cowData[offset .. offset + blockSize];
		}
	}
}

/// Where the next block will go should it be added to the COW store.
BlockIndex getNextCow()
{
	BlockIndex result;
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
void writeBlock(Dev* dev, size_t blockIndex, const(ubyte)[] block)
{
	auto bi = &dev.blockMap[blockIndex];
	unreferenceBlock(*bi);

	auto nextCow = getNextCow();
	auto result = hashBlock(block, nextCow);
	if (result == nextCow)
	{
		// New block - add to COW store
		auto offset = result.cow * blockSize;
		cowData[offset .. offset + block.length] = block;
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
	}
	else
		referenceBlock(result);
	*bi = result;
}

/// Indicates that we are now using one more reference to the given block.
void referenceBlock(BlockIndex bi)
{
	final switch (bi.type)
	{
		case BlockIndex.Type.unknown:
			assert(false);
		case BlockIndex.Type.upstream:
			return; // No problem, it is on an upstream device (infinite lifetime)
		case BlockIndex.Type.cow:
		{
			auto index = bi.cow;
			assert(!cowMap[index].free);
			cowMap[index].refCount = cowMap[index].refCount + 1;
		}
	}
}

/// Indicates that we are no longer using one reference to the given block.
/// If it was stored in the COW store, decrement its reference count,
/// and if it reaches zero, delete it from there and the hash table.
void unreferenceBlock(BlockIndex bi)
{
	final switch (bi.type)
	{
		case BlockIndex.Type.unknown:
			return; // No problem, we never even looked at it
		case BlockIndex.Type.upstream:
			return; // No problem, it is on an upstream device (infinite lifetime)
		case BlockIndex.Type.cow:
		{
			auto index = bi.cow;
			assert(!cowMap[index].free);
			auto refCount = cowMap[index].refCount;
			assert(refCount > 0);
			refCount--;
			if (refCount == 0)
			{
				unhashBlock(bi);
				cowMap[index] = cowMap[0];
				cowMap[0].nextFree = index;
			}
			else
				cowMap[index].refCount = refCount;
		}
	}
}

class ThinCOWOperations : Operations
{
	private final Dev* getDev(const(char)[] path)
	{
		enforce(path.length && path[0] == '/', "Invalid path");
		path = path[1..$];
		foreach (ref dev; devs)
			if (dev.name == path)
				return &dev;
		throw new Exception("No such device");
	}

	override void getattr(const(char)[] path, ref stat_t s)
	{
		if (path == "/")
			s.st_mode = S_IFDIR | S_IRWXU;
		else
		{
			s.st_size = getDev(path).data.length;
			s.st_mode = S_IFREG | S_IRUSR | S_IWUSR;
		}
		s.st_mtime = 0;
		s.st_uid = getuid();
		s.st_gid = getgid();
	}

	override bool access(const(char)[] path, int mode)
	{
		if (path == "/")
			return true;
		try
		{
			getDev(path);
			return true;
		}
		catch (Exception)
			return false;
	}

	override string[] readdir(const(char)[] path)
	{
		enforce(path == "/", "No such directory");
		string[] result;
		foreach (ref dev; devs)
			result ~= dev.name;
		return result;
	}

	override ulong read(const(char)[] path, ubyte[] buf, ulong offset)
	{
		auto dev = getDev(path);
		auto end = min(offset + buf.length, dev.data.length);
		if (offset >= end)
			return 0;
		auto head = buf.ptr;
		foreach (blockOffset; offset / blockSize .. (end - 1) / blockSize + 1)
		{
			auto block = dev.readBlock(blockOffset);
			auto blockStart = blockOffset * blockSize;
			auto blockEnd = blockStart + blockSize;
			auto spanStart = max(blockStart, offset);
			auto spanEnd   = min(blockEnd  , end);
			auto len = spanEnd - spanStart;
			head[0 .. len] = block[spanStart - blockStart .. spanEnd - blockStart];
			head += len;
		}
		// assert(head == buf.ptr + buf.length);
		return end - offset;
	}

	override int write(const(char)[] path, in ubyte[] data, ulong offset)
	{
		auto dev = getDev(path);
		auto end = min(offset + data.length, dev.data.length);
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
				static ubyte[] buf;
				if (!buf.length)
					buf.length = blockSize;
				buf[] = dev.readBlock(blockOffset);
				buf[spanStart - blockStart .. spanEnd - blockStart] = head[0 .. len];
				dev.writeBlock(blockOffset, buf[]);
			}
			head += len;
		}
		return cast(int)(end - offset);
	}
}

@(`Create a deduplicated, COW view of block devices as a FUSE filesystem.`)
void thincow(
	Parameter!(string, "Where to mount the FUSE filesystem.") target,
	Option!(string, "Directory containing upstream devices/symlinks.", "PATH") upstream,
	Option!(string, "Directory where to store COW blocks.", "PATH") dataDir,
	Option!(string, "Directory where to store metadata.\nIf unspecified, defaults to the data directory.\nSpecify - to use RAM.", "PATH") metadataDir = null,
	Option!(size_t, "Block size. Larger blocks means smaller metadata,\nbut writes smaller than one block\nwill cause a read-modify-write.\nThe default is 65536 (64 KiB).", "BYTES") blockSize = 64*1024,
	Option!(double, "Ratio to calculate hash table size\n(from upstream block count).\nThe default is 2.0.") hashTableRatio = 2.0,
	Switch!("Run in foreground.", 'f') foreground = false,
	Option!(string[], "Additional FUSE options (e.g. debug).", "STR", 'o') options = null,
)
{
	enforce(upstream, "No upstream device directory specified");
	enforce(dataDir, "No data directory specified");
	if (!metadataDir)
		metadataDir = dataDir;

	.blockSize = blockSize;

	size_t totalBlocks;
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

		auto ptr = cast(const(ubyte)*)mmap(null, deviceSize, PROT_READ, MAP_PRIVATE, fd, 0);
		errnoEnforce(ptr != MAP_FAILED, "mmap failed");

		Dev dev;
		dev.name = de.baseName;
		dev.firstBlock = totalBlocks;
		dev.data = ptr[0 .. deviceSize];
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

	auto blockMap = cast(BlockIndex[])mapFile(metadataDir, "blockmap", BlockIndex.sizeof, totalBlocks);
	foreach (ref dev; devs)
	{
		auto numBlocks = (dev.data.length + blockSize - 1) / blockSize;
		dev.blockMap = blockMap[dev.firstBlock .. dev.firstBlock + numBlocks];
	}

	auto hashTableLength = cast(ulong)(totalBlocks * hashTableRatio);
	hashTableLength = hashTableLength.roundUpToPowerOfTwo;
	hashTable = cast(BlockIndex[])mapFile(metadataDir, "hashtable", BlockIndex.sizeof, hashTableLength);

	auto maxCowBlocks = totalBlocks + 2; // Index 0 is reserved, and one more for swapping
	cowMap = cast(COWIndex[])mapFile(metadataDir, "cowmap", COWIndex.sizeof, maxCowBlocks);
	cowData = cast(ubyte[])mapFile(dataDir, "cowdata", blockSize, maxCowBlocks);

	auto fs = new Fuse("thincow", foreground, false);
	options ~= ["big_writes"]; // Essentially required, otherwise FUSE will use 4K writes and cause abysmal performance
	fs.mount(new ThinCOWOperations(), target, options);
}

mixin main!(funopt!thincow);

extern(C) int openat(int dirfd, const char *pathname, int flags, ...);
