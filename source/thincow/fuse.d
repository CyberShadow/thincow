/**
 * FUSE implementation
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

module thincow.fuse;

import std.algorithm.comparison;
import std.array;
import std.exception;
import std.range.primitives;
import std.string;

import core.stdc.errno;
import core.stdc.stdint;
import core.sys.posix.fcntl;
import core.sys.posix.sys.stat;
import core.sys.posix.unistd;

import c.fuse.fuse;

import thincow.btree : dumpBtree;
import thincow.common;
import thincow.cow : dumpCOW;
import thincow.devices;
import thincow.flush : getFlushStatus, handleFlushClose;
import thincow.hashtable : dumpHashTable;
import thincow.io;
import thincow.stats : dumpStats;

__gshared: // disable TLS

enum FuseHandle : uint64_t
{
	none,
	rootDir,
	devsDir,
	debugDir,

	firstDevice = 0x10000000_00000000,
	firstFile   = 0x20000000_00000000,
}

/// These are virtual files that are rendered at the time of opening,
/// so that their contents remains consistent throughout the file handle's lifetime.
char[][uint64_t] files;
uint64_t nextFileIndex = FuseHandle.firstFile;

/// What to do when a file opened for writing is closed.
alias CloseHandler = void function(char[]) nothrow;
CloseHandler[uint64_t] closeHandlers;

void dumpToStderr(alias fun)(string prefix) nothrow
{
	(){
		stderr.write(prefix);
		auto writer = stderr.lockingTextWriter();
		fun(writer);
	}().assertNotThrown();
}

void makeFile(alias fun)(fuse_file_info* fi) nothrow
{
	Appender!(char[]) appender;
	try
		fun(appender);
	catch (Exception e)
		put(appender, e.toString().assertNotThrown());
	auto fd = nextFileIndex++;
	files[fd] = appender.data;
	fi.fh = fd;
	fi.direct_io = true;
}

void makeWritableFile(fuse_file_info* fi, CloseHandler handleClose) nothrow
{
	makeFile!((w) {})(fi);
	closeHandlers[fi.fh] = handleClose;
}

extern(C) nothrow
{
	int fs_getattr(const char* c_path, stat_t* s)
	{
		auto path = c_path.fromStringz;
		switch (path)
		{
			case "/":
			case "/devs":
			case "/debug":
				s.st_mode = S_IFDIR | S_IRUSR | S_IXUSR;
				break;
			case "/debug/btree.txt":
			case "/debug/cow.txt":
			case "/debug/hash-table.txt":
			case "/stats.txt":
			case "/stats-full.txt":
				s.st_mode = S_IFREG | S_IRUSR;
				s.st_size = typeof(s.st_size).max;
				break;
			case "/flush":
				s.st_mode = S_IFREG | S_IRUSR | S_IWUSR;
				s.st_size = typeof(s.st_size).max;
				break;
			default:
				if (path.startsWith("/devs/"))
				{
					auto dev = getDev(path);
					if (!dev) return -ENOENT;
					s.st_size = dev.size;
					s.st_mode = S_IFREG | S_IRUSR | (readOnly ? 0 : S_IWUSR);
				}
				else
					return -ENOENT;
		}
		s.st_mtime = 0;
		s.st_uid = getuid();
		s.st_gid = getgid();
		return 0;
	}

	int fs_readdir(const char* path, void* buf,
				fuse_fill_dir_t filler, off_t /*offset*/, fuse_file_info* fi)
	{
		switch (fi.fh)
		{
			case FuseHandle.rootDir:
			{
				static immutable char*[] rootDir = [
					"devs",
					"debug",
					"stats.txt",
					"stats-full.txt",
					"flush",
				];
				foreach (d; rootDir)
					filler(buf, cast(char*)d, null, 0);
				return 0;
			}
			case FuseHandle.devsDir:
				foreach (ref dev; devs)
					filler(buf, cast(char*)toStringz(dev.name), null, 0);
				return 0;
			case FuseHandle.debugDir:
			{
				static immutable char*[] debugDir = [
					"btree.txt",
					"cow.txt",
					"hash-table.txt",
				];
				foreach (d; debugDir)
					filler(buf, cast(char*)d, null, 0);
				return 0;
			}
			default:
				return -ENOTDIR;
		}
	}

	int fs_open(const char* c_path, fuse_file_info* fi)
	{
		auto path = c_path.fromStringz;
		switch (path)
		{
			case "/debug/btree.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!dumpBtree(fi);
				return 0;
			case "/debug/cow.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!dumpCOW(fi);
				return 0;
			case "/debug/hash-table.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!dumpHashTable(fi);
				return 0;
			case "/stats.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!(dumpStats!false)(fi);
				return 0;
			case "/stats-full.txt":
				if ((fi.flags & O_ACCMODE) != O_RDONLY) return -EINVAL;
				makeFile!(dumpStats!true)(fi);
				return 0;
			case "/flush":
				if ((fi.flags & O_ACCMODE) == O_RDONLY)
					makeFile!getFlushStatus(fi);
				else
				if ((fi.flags & O_ACCMODE) == O_WRONLY)
					makeWritableFile(fi, &handleFlushClose);
				else
					return -EINVAL;
				return 0;
			default:
				if (path.startsWith("/devs/"))
				{
					auto dev = getDev(path);
					if (!dev) return -ENOENT;
					auto devIndex = dev - devs.ptr;
					fi.direct_io = true;
					fi.fh = FuseHandle.firstDevice + devIndex;
					return 0;
				}
				return -ENOENT;
		}
	}

	int fs_truncate(const char* c_path, off_t offset)
	{
		auto path = c_path.fromStringz;
		if (offset != 0)
			return -EINVAL;
		switch (path)
		{
			case "/flush":
				return 0;
			default:
				return -EINVAL;
		}
	}

	int fs_opendir(const char* c_path, fuse_file_info* fi)
	{
		auto path = c_path.fromStringz;
		switch (path)
		{
			case "/":
				fi.fh = FuseHandle.rootDir;
				return 0;
			case "/devs":
				fi.fh = FuseHandle.devsDir;
				return 0;
			case "/debug":
				fi.fh = FuseHandle.debugDir;
				return 0;
			default:
				return -ENOENT;
		}
	}

	int fs_release(const char* c_path, fuse_file_info* fi)
	{
		if (fi.fh >= FuseHandle.firstFile)
		{
			if (auto phandler = fi.fh in closeHandlers)
			{
				(*phandler)(files[fi.fh]);
				closeHandlers.remove(fi.fh);
			}
			files.remove(fi.fh);
		}
		return 0;
	}

	int fs_read(const char* /*path*/, char* buf_ptr, size_t size, off_t offset, fuse_file_info* fi)
	{
		auto buf = (cast(ubyte*)buf_ptr)[0 .. size];

		if (fi.fh >= FuseHandle.firstFile)
		{
			auto pfile = fi.fh in files;
			if (!pfile) return -EBADFD;
			auto file = *pfile;
			if (offset >= file.length)
				return 0;
			file = file[offset .. $];
			auto len = cast(int)min(size, file.length, int.max);
			buf_ptr[0 .. len] = file[0 .. len];
			return len;
		}
		if (fi.fh >= FuseHandle.firstDevice)
		{
			auto devIndex = fi.fh - FuseHandle.firstDevice;
			if (devIndex >= devs.length) return -EBADFD;
			auto dev = &devs[devIndex];

			auto end = min(offset + size, dev.size);
			if (offset >= end)
				return 0;
			auto head = buf.ptr;
			foreach (blockOffset; offset / blockSize .. (end - 1) / blockSize + 1)
			{
				const(ubyte)[] block;
				static ubyte[] blockBuf;
				try
					block = dev.readBlock(blockOffset, blockBuf);
				catch (ErrnoException e)
				{
					auto pos = head - buf.ptr;
					if (pos == 0)
						return -e.errno;
					else
						return cast(int)pos;
				}
				catch (Exception e)
					assert(false, e.toString());
				auto blockStart = blockOffset * blockSize;
				auto blockEnd = blockStart + blockSize;
				auto spanStart = max(blockStart, offset);
				auto spanEnd   = min(blockEnd  , end);
				auto len = spanEnd - spanStart;
				head[0 .. len] = block[spanStart - blockStart .. spanEnd - blockStart];
				head += len;
			}
			// assert(head == buf.ptr + buf.length);
			return cast(int)(end - offset);
		}
		return -EBADFD;
	}

	int fs_write(const char* /*path*/, char* data_ptr, size_t size,
                            off_t offset, fuse_file_info* fi)
	{
		if (size > int.max)
			size = int.max;

		auto data = (cast(ubyte*)data_ptr)[0 .. size];

		if (fi.fh >= FuseHandle.firstFile)
		{
			if (fi.fh !in files)
				return -EBADFD;
			if (fi.fh !in closeHandlers)
				return -EROFS;
			if (offset != files[fi.fh].length)
				return -EINVAL;
			files[fi.fh] ~= cast(char[])data;
			return cast(int)size;
		}
		if (fi.fh >= FuseHandle.firstDevice)
		{
			if (readOnly)
				return -EROFS;

			auto devIndex = fi.fh - FuseHandle.firstDevice;
			if (devIndex >= devs.length) return -EBADFD;
			auto dev = &devs[devIndex];

			auto end = min(offset + size, dev.size);
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
					static ubyte[] blockBuf;
					try
						dev.readBlock(blockOffset, blockBuf);
					catch (ErrnoException e)
					{
						auto pos = head - data.ptr;
						if (pos == 0)
							return -e.errno;
						else
							return cast(int)pos;
					}
					catch (Exception e)
						assert(false, e.toString());
					blockBuf[spanStart - blockStart .. spanEnd - blockStart] = head[0 .. len];
					dev.writeBlock(blockOffset, blockBuf[]);
				}
				head += len;
			}
			return cast(int)(end - offset);
		}
		return -EBADFD;
	}
}
