thincow [![Build Status](https://travis-ci.org/CyberShadow/thincow.svg?branch=master)](https://travis-ci.org/CyberShadow/thincow) [![Coverage Status](https://coveralls.io/repos/github/CyberShadow/thincow/badge.svg?branch=master)](https://coveralls.io/github/CyberShadow/thincow?branch=master)
=======

`thincow` enables non-destructive, copy-on-write edits of block devices involving copying/moving a lot of data around, using deduplication for minimal additional disk usage.

Use cases include:

- Simple-to-use deduplication layer
- Trying dangerous operations on filesystems, such as invasive repair or conversion
- Reshaping storage volumes or filesystems in order to prepare to move them (block-wise) to new media

Building
--------

- Install [a D compiler](https://dlang.org/download.html)
- Install [Dub](https://github.com/dlang/dub), if it wasn't included with your D compiler
- Run `dub build -b release`

Usage
-----

1. Create a directory containing the upstream device(s).
   You can use symlinks to add block devices in `/dev`.

2. Create data (and optionally metadata) directories for `thincow`'s use.

3. Create an empty directory to serve as the `thincow` mount point.

3. Run `thincow`:

       $ ./thincow \
           --upstream=path/to/upstream/dir \
           --data-dir=path/to/data/dir \
           path/to/mount-point

   Run `thincow --help` to get a full description of all options.

4. You can now access and modify the data in the files under the specified mount-point.
   Modifications will be deduplicated and stored to `thincow`'s data files.

5. When done, run `fusermount -u path/to/mount-point` to shut down `thincow` gracefully.

Notes:

- `thincow` can be used purely for deduplication of new data by placing a large empty (sparse) file (e.g. using `truncate` or `dd seek=...`) in the upstream directory.
  All writes to the file will be deduplicated in the COW store.

- `thincow` can be stopped and resumed using the same data / metadata store, 
  assuming it is always stopped gracefully (`umount` or `fusermount -u`).
  Indicating an already populated data/metadata directory with `--data-dir` / `--metadata-dir` 
  will cause `thincow` to use the data there.
  The underlying (upstream) devices must also not change between invocations.

- It is not safe to resume after an ungraceful termination or power failure.

Details
-------

`thincow` takes as input (the `--upstream` parameter) a directory containing the "upstream" block devices (or symlinks to them, or plain files).
These are never written to, and `thincow` assumes that they won't change for the lifetime of its data/metadata storage.

The `--data-dir` and `--metadata-dir` parameters indicate where to save modified data. (Files in `--metadata-dir` are smaller and may thus be placed on faster media.) 

The following files are created:

- `blockmap` is a B-tree holding information for where a block is stored. Information is organized into extents (a continuous range of blocks). Each extent can point to one of:
  - An extent from the upstream device (not necessarily at the same position as this element)
  - An extent in the COW store, for blocks containing data which was never seen on an upstream device
- `hashtable` contains a hash table of all blocks seen so far. Each element is the same as in `blockmap`.
- `cowmap` contains an index for the COW block store. It is used to store a reference count for every block, so that `thincow` knows when a COW block can be freed.
- `globals` is a fixed-length record containing some global variables, such as the B-tree size.
- `cowdata` (stored in the `--data-dir` directory) holds the contents of COW blocks (blocks containing data which was never seen on an upstream device). Each block's contents is unique.

Note that the above files are created as large sparse files, big enough to accommodate the worst case, but initially don't consume any real disk space.

Finally, `thincow` allows accessing and manipulating the above via a FUSE filesystem, specified as a target directory parameter. 
The exposed filesystem contains a `devs` directory, whose file list mirrors that of the upstream device directory.

Reading from these files initially produces the same contents as on the upstream devices. When a block is read for the first time, `thincow` makes note of its contents so that it can recognize when a block with the same contents is written back elsewhere later.

Writing to these files will cause `thincow` to deduplicate blocks according to its hash table of the blocks it has seen so far:

- If a block was seen (during reading) on an upstream device, then it is mapped to said block on the upstream device. 
- Otherwise, if it matches a block that's already in the COW store, it is mapped to that block (and its reference counter is incremented).
- Otherwise, it is saved as a new block to the COW store.

Example
-------

For a small practical example, see the [`t/t-3_btrfs-1_raid10_to_single.sh`](https://github.com/cybershadow/thincow/blob/master/t/t-3_btrfs-1_raid10_to_single.sh) test. 
The goal of the test is to model converting a btrfs RAID10 filesystem of four devices to a single, larger device with a `single` profile.

- In the test, we set up the btrfs filesystem (consisting of four 128 MB devices) and fill it with 150 MB of data.
- Then, we add an empty file to serve as a logical target for the destination device.
- After starting `thincow`, the four source devices are mounted, then the target device is added to the filesystem.
- After a balance, the four original devices can be removed from a filesystem, effectively forcing the data to be moved to the new device.
- At the end, we verify that less than 2 MB of real disk space was used for the above conversion, despite the operation moving around at least 150 MB of data.
  This can also be seen by checking `stats-full.txt` under the mount point:
  
      Block size: 4096
      Total blocks: 212992 (872415232 bytes)
      B-tree nodes: 36 (147456 bytes)
      Current B-tree root: 1
      Devices:
          Device #0: "single", 335544320 bytes, first block: 0
          Device #1: "d", 134217728 bytes, first block: 81920
          Device #2: "c", 134217728 bytes, first block: 114688
          Device #3: "b", 134217728 bytes, first block: 147456
          Device #4: "a", 134217728 bytes, first block: 180224
      Hash table: 16384 buckets (1048576 bytes), 8 entries per bucket (64 bytes)
      Hash table occupancy: 38876/131072 (29%)
          0 slots: ##############.......................... 1521 buckets
          1 slots: ##################################...... 3658 buckets
          2 slots: ######################################## 4249 buckets
          3 slots: ###############################......... 3399 buckets
          4 slots: ###################..................... 2032 buckets
          5 slots: #########............................... 959 buckets
          6 slots: ###..................................... 410 buckets
          7 slots: #....................................... 108 buckets
          8 slots: ........................................ 48 buckets
      Blocks written: 42015 total, 41049 (168136704 bytes, 98%) deduplicated
      COW store: 420 blocks (1720320 bytes), 0 free for reuse
      Total COW references: 1058 blocks (4333568 bytes)
      Disk space savings:
          Deduplicated to upstream: 40059 blocks (164081664 bytes)
          Deduplicated to COW store: 639 blocks (2617344 bytes)
          Total: 40698 blocks (166699008 bytes)


License
-------

`thincow` is available under the [Mozilla Public License, v. 2.0](http://mozilla.org/MPL/2.0/).
