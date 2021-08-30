/**
 * Entry point
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

module thincow.main;

import core.bitop : bsr;
import core.sys.linux.sys.mman : MAP_ANONYMOUS, MAP_NORESERVE;
import core.sys.posix.fcntl;
import core.sys.posix.sys.ioctl;
import core.sys.posix.sys.mman;
import core.sys.posix.unistd;

import std.algorithm.comparison;
import std.bitmanip : BitArray;
import std.exception : enforce, errnoEnforce;
import std.file;
import std.math.traits : isPowerOf2;
import std.path;
import std.stdio : stderr;
import std.string;
import std.traits : hasIndirections;

import c.fuse.fuse;

import ae.sys.file : listDir;
import ae.utils.funopt;
import ae.utils.main;
import ae.utils.math : roundUpToPowerOfTwo;

import thincow.btree;
import thincow.common;
import thincow.cow : cowMap, cowData, COWIndex;
import thincow.devices;
import thincow.flush : useMap;
import thincow.fsck : cowRefCount, fsck;
import thincow.fuse;
import thincow.hashtable : hashTable, HashTableBucket;
import thincow.integrity;

__gshared: // disable TLS

@(`Create a deduplicated, COW view of block devices as a FUSE filesystem.`)
int program(
	Parameter!(string, "Where to mount the FUSE filesystem.") target,
	Option!(string, "Directory containing upstream devices/symlinks.", "PATH") upstream,
	Option!(string, "Directory where to store COW blocks.", "PATH") dataDir,
	Option!(string, "Directory where to store metadata. If unspecified, defaults to the data directory.\nSpecify \"-\" to use RAM.", "PATH") metadataDir = null,
	Option!(string, "Directory where to store large temporary data. If unspecified, defaults to \"-\" (RAM).", "PATH") tempDir = "-",
	Option!(size_t, "Block size.\nLarger blocks means smaller metadata, but writes smaller than one block will cause a read-modify-write. The default is 512.", "BYTES") blockSize = 512,
	Option!(size_t, "Hash table size.\nThe default is 1073741824 (1 GiB).", "BYTES") hashTableSize = 1024*1024*1024,
	Option!(size_t, "Maximum size of the block map.", "BYTES") maxBlockMapSize = 1L << 63,
	Option!(size_t, "Maximum number of blocks in the COW store.", "BLOCKS") maxCowBlocks = 1L << 63,
	Option!(uint, "Enable integrity checking, BITS per block.", "BITS") checksumBits = 0,
	Switch!hiddenOption noReserve = false, // Dangerous!
	Switch!("Enable retroactive deduplication (more I/O intensive).") retroactive = false,
	Switch!("Run in foreground.", 'f') foreground = false,
	Switch!("Open upstream devices in read-only mode (flushing will be disabled).", 'r') readOnlyUpstream = false,
	Option!(string[], "Additional FUSE options (e.g. debug).", "STR", 'o') options = null,
	Switch!("Perform data validity check on startup.") fsck = false,
	Switch!("Don't actually mount the filesystem, and exit after initialization/fsck.") noMount = false,
)
{
	enforce(upstream, "No upstream device directory specified");
	enforce(dataDir, "No data directory specified");
	if (!metadataDir)
		metadataDir = dataDir;

	.blockSize = blockSize;
	.retroactiveDeduplication = retroactive;
	.readOnlyUpstream = readOnlyUpstream;

	upstream.value.listDir!((de)
	{
		stderr.write(de.baseName, ": ");
		enforce(!de.isDir, "Directories are not supported");
		size_t deviceSize;
		auto fd = openat(de.dirFD, de.ent.d_name.ptr, readOnlyUpstream ? O_RDONLY : O_RDWR);
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

	void[] mapData(string dir, string name, size_t recordSize, size_t numRecords, bool requireFresh = false)
	{
		auto size = recordSize * numRecords;
		enforce(size / recordSize == numRecords, "Overflow");
		stderr.writefln("%s: %d bytes (%d records)", name, size, numRecords);

		void* ptr;
		string path;
		if (dir == "-")
		{
			path = "(in memory) " ~ name;
			auto flags = MAP_PRIVATE | MAP_ANONYMOUS;
			if (noReserve)
				flags |= MAP_NORESERVE;
			ptr = mmap(null, size, PROT_READ | PROT_WRITE, flags, 0, 0);
		}
		else
		{
			path = dir.buildPath(name);

			if (path.exists)
			{
				if (requireFresh)
					remove(path);
				else
					enforce(path.getSize() == size, "Found existing file, but it is of the wrong size: " ~ path);
			}

			auto fd = open(path.toStringz, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
			errnoEnforce(fd >= 0, "open failed: " ~ path);
			ftruncate(fd, size);
			ptr = mmap(null, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		}
		errnoEnforce(ptr != MAP_FAILED, "mmap failed: " ~ path);
		return ptr[0 .. size];
	}

	T[] mapRecords(T)(string dir, string name, size_t numRecords, bool requireFresh = false)
	{
		static assert(!hasIndirections!T);
		enum recordSize = T.sizeof;
		return cast(T[])mapData(dir, name, recordSize, numRecords, requireFresh);
	}

	globals = mapRecords!Globals(metadataDir, "globals", 1).ptr;

	auto btreeMaxLength = totalBlocks; // worst case
	auto btreeMaxSize = btreeMaxLength * BTreeNode.sizeof;
	if (btreeMaxSize > maxBlockMapSize)
		btreeMaxSize = maxBlockMapSize;
	btreeMaxLength = btreeMaxSize / BTreeNode.sizeof;
	blockMap = mapRecords!BTreeNode(metadataDir, "blockmap", btreeMaxLength);

	auto hashTableLength = hashTableSize / HashTableBucket.sizeof;
	enforce(hashTableLength * HashTableBucket.sizeof == hashTableSize, "Hash table size must be a multiple of %s".format(HashTableBucket.sizeof));
	enforce(hashTableLength == hashTableLength.roundUpToPowerOfTwo, "Hash table size must be a power of 2");
	enforce(hashTableLength <= 0x1_0000_0000, "Hash table is too large");
	hashTable = mapRecords!HashTableBucket(metadataDir, "hashtable", hashTableLength);

	auto maxCowBlocksLimit = totalBlocks + 2; // Index 0 is reserved, and one more for swapping
	maxCowBlocks = min(maxCowBlocks, maxCowBlocksLimit);
	cowMap = mapRecords!COWIndex(metadataDir, "cowmap", maxCowBlocks);
	cowData = cast(ubyte[])mapData(dataDir, "cowdata", blockSize, maxCowBlocks);

	uint bitsNeeded(ulong maxValue) { assert(maxValue); maxValue--; return maxValue ? 1 + bsr(maxValue) : 0; }
	blockRefBits = bitsNeeded(maxCowBlocksLimit) + BlockRef.typeBits;

	if (checksumBits)
	{
		enforce(checksumBits <= maxChecksumBits && isPowerOf2(checksumBits.value), "Invalid checksum size");
		.checksumBits = checksumBits;
		.checksums = mapRecords!Checksum(metadataDir, "checksums",
			(totalBlocks * checksumBits + maxChecksumBits - 1) / maxChecksumBits);
		.checksumMask = ((Checksum(1) << (checksumBits - 1)) << 1) - 1; // Avoid UB with shift overflow
	}

	if (!readOnlyUpstream)
	{
		auto useMapBuf = mapData(tempDir, "usemap", 1, (totalBlocks + 7) / 8);
		useMap = BitArray(useMapBuf, totalBlocks);
	}

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

	if (fsck)
	{
		cowRefCount = mapRecords!ulong(tempDir, "fsck-cow-refcount", maxCowBlocks, true);
		if (!.fsck())
			return 1;
	}

	if (noMount)
		return 0;

	fuse_operations fsops;
	fsops.readdir = &fs_readdir;
	fsops.getattr = &fs_getattr;
	fsops.open = &fs_open;
	fsops.truncate = &fs_truncate;
	fsops.opendir = &fs_opendir;
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

	stderr.writeln("Starting FUSE filesystem.");
	scope(success) stderr.writeln("thincow exiting.");
	return fuse_main(f_args.argc, f_args.argv, &fsops, null);
}

mixin main!(funopt!program);

extern(C) int openat(int dirfd, const char *pathname, int flags, ...);
