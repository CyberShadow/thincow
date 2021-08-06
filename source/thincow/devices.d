/**
 * Devices
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

module thincow.devices;

import std.algorithm.searching : startsWith;

import thincow.common;

__gshared: // disable TLS

/// Per-device (file) information
struct Dev
{
	/// File descriptor
	int fd;
	/// Size in bytes
	ulong size;
	/// First block index within the global block map
	BlockIndex firstBlock;
	/// File name of the device (in the upstream directory, and in the FUSE filesystem)
	string name;
	/// Stats
	ulong reads, readErrors, writes, writeErrors, readRequests, writeRequests;
}
Dev[] devs;

/// Find device containing the given BlockIndex.
ref Dev findDev(BlockIndex blockIndex) nothrow
{
	// TODO: this could be a binary search
	foreach (i, ref dev; devs)
		if (dev.firstBlock > blockIndex)
			return devs[i-1];
	return devs[$-1];
}

/// Find device by path.
Dev* getDev(const(char)[] path) nothrow
{
	if (!path.startsWith("/devs/"))
		return null;
	path = path["/devs/".length .. $];
	foreach (ref dev; devs)
		if (dev.name == path)
			return &dev;
	return null;
}
