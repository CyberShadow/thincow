/**
 * Optional integrity checking
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

module thincow.integrity;

import thincow.common;

__gshared: // disable TLS

alias Checksum = ulong; // size of biggest checksum
enum maxChecksumBits = Checksum.sizeof * 8;

Checksum[] checksums;
uint checksumBits; // some power of 2
Checksum checksumMask;

Checksum getChecksum(BlockIndex blockIndex) nothrow
{
	auto bitIndex = blockIndex * checksumBits;
	auto arrIndex = bitIndex / maxChecksumBits;
	auto bitShift = bitIndex % maxChecksumBits;
	Checksum mask = checksumMask;
	return (checksums[arrIndex] >> bitShift) & mask;
}

void putChecksum(BlockIndex blockIndex, Checksum checksum) nothrow
{
	auto bitIndex = blockIndex * checksumBits;
	auto arrIndex = bitIndex / maxChecksumBits;
	auto bitShift = bitIndex % maxChecksumBits;
	Checksum mask = checksumMask;
	checksum &= mask;
	auto target = &checksums[arrIndex];
	*target = (*target & ~(mask << bitShift)) | (checksum << bitShift);
}
