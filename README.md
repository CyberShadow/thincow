# thincow [![test](https://github.com/CyberShadow/aconfmgr/actions/workflows/test.yml/badge.svg)](https://github.com/CyberShadow/aconfmgr/actions/workflows/test.yml) [![codecov](https://codecov.io/gh/CyberShadow/thincow/branch/master/graph/badge.svg?token=RKDOR5ID8L)](https://codecov.io/gh/CyberShadow/thincow)

`thincow` enables non-destructive, copy-on-write edits of block devices involving copying/moving a lot of data around, using deduplication for minimal additional disk usage.

Use cases include:

- Simple-to-use deduplication layer
- Trying dangerous operations on filesystems, such as invasive repair or conversion
- Reshaping storage volumes or filesystems in order to prepare to move them (block-wise) to new media

## Features

- Thinly provisioned Copy-on-Write store
- Inline block-based deduplication
- Extent (B-tree) based data representation
- Fault tolerant (errors from upstream devices are propagated to FUSE client)
- Optional [incremental flushing](#flushing)
- Optional integrity verification

## Usage

1. Create a directory containing the upstream device(s), which will contain the initial data.
   You can use symlinks to add block devices from `/dev`,
   or sparse files to create virtual null-backed devices.

2. Create data (and optionally metadata) directories for `thincow`'s use.

3. Create an empty directory to serve as the `thincow` mount point.

3. Run `thincow`:

       $ ./thincow \
           --upstream=path/to/upstream/dir \
           --data-dir=path/to/data/dir \
           path/to/mount-point

   See the man page or run `thincow --help` to get a full description of all options.

4. You can now access and modify the data in the files under the specified mount-point.
   Modifications will be deduplicated and stored to `thincow`'s data files.

5. To flush changes back to the upstream devices, you can run `echo full > path/to/mount-point/flush`.
   See [Flushing](#flushing) for details.

6. When done, run `fusermount -u path/to/mount-point` to shut down `thincow` gracefully.

### Details

`thincow` takes as input (the `--upstream` parameter) a directory containing the "upstream" block devices (or symlinks to them, or plain files).
These are never written to, and `thincow` assumes that they won't change for the lifetime of its data/metadata storage.

The `--data-dir` and `--metadata-dir` parameters indicate where to save modified data. (Files in `--metadata-dir` are smaller and may thus be placed on faster media.)
See the [Data Files](#data-files) section for a description of each file.

Finally, `thincow` allows accessing and manipulating the above via a FUSE filesystem, specified as a target directory parameter. 
The exposed filesystem contains a `devs` directory, whose file list mirrors that of the upstream device directory.

Reading from these files initially produces the same contents as on the upstream devices. When a block is read for the first time, `thincow` makes note of its contents so that it can recognize when a block with the same contents is written back elsewhere later.

Writing to these files will cause `thincow` to deduplicate blocks according to its hash table of the blocks it has seen so far:

- If a block was seen (during reading) on an upstream device, then it is mapped to said block on the upstream device. 
- Otherwise, if it matches a block that's already in the COW store, it is mapped to that block (and its reference counter is incremented).
- Otherwise, it is saved as a new block to the COW store.

### Notes

- `thincow` can be used purely for deduplication of new data by placing a large empty (sparse) file (e.g. using `truncate` or `dd seek=...`) in the upstream directory.
  All writes to the file will be saved and deduplicated to the COW store, in thincow's data directory.

- `thincow` can be stopped and resumed using the same data / metadata store, 
  assuming it is always stopped gracefully (`umount` or `fusermount -u`).
  Indicating an already populated data/metadata directory with `--data-dir` / `--metadata-dir` 
  will cause `thincow` to use the data there.
  The underlying (upstream) devices must also not change between invocations.

- It is not safe to resume after an ungraceful termination or power failure.
  See [Freezing](#freezing) for instructions for how to safely create periodic backups during long-running operations.

### Example

For a small practical example, see the [`t/t-9_btrfs-1_raid10_to_single.sh`](https://github.com/cybershadow/thincow/blob/master/t/t-9_btrfs-1_raid10_to_single.sh) test. 
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

### Filesystem Entries

A mounted `thincow` filesystem will contain the following entries:

- `/devs/` - directory containing the virtual block devices. Writes to these will be deduplicated.
- `/debug/` - directory containing virtual files which produce a dump of internal data structures.
  Note that these can be very large and take a long time to produce for large instances.
- `/stats.txt` - text file containing information about the `thincow` instance.
  This file only contains information which can be obtained quickly (O(1)).
- `/stats-full.txt` - text file containing information about the `thincow` instance.
  This file contains all information, and may take a long time to produce for large instances.
- `/flush` - control file to facilitate [flushing](#flushing).

### Data Files

`thincow` creates the following files in its data / metadata directories:

- `blockmap` is a B-tree holding information for where a block is stored. Information is organized into extents (a continuous range of blocks). Each extent can point to one of:
  - An extent from the upstream device (not necessarily at the same position as this element).
  - An extent in the COW store, for blocks containing data which was never seen on an upstream device.
- `hashtable` contains a hash table of all blocks seen so far. Elements have the same format as in `blockmap`.
- `cowmap` contains an index for the COW block store. It is used to store a reference count for every block, so that `thincow` knows when a COW block can be freed.
- `globals` is a fixed-length record containing some global variables, such as the B-tree size.
- `cowdata` (stored in the `--data-dir` directory) holds the contents of COW blocks (blocks containing data which was never seen on an upstream device). Each block's contents is unique (insofar as the hash table capacity allows).
- `checksums` will contain the per-block checksums, if `--checksum-bits` is enabled.

Note that the above files are created as large sparse files, big enough to accommodate the worst case, but initially don't consume any real disk space.

### Flushing

`thincow` exposes a `/flush` control [file](#filesystem-entries) to facilitate writing changes back to the upstream devices.
This allows using `thincow` to preview the outcome of a potentially destructive operation: 
perform the operation, examine the result, and then flush the changes back to the physical device.

Reading from `/flush` produces one of the following strings:

- `clean` - there are no changes to flush.
- `ready` - there are changes, and `thincow` is ready to flush them.
- `error`, followed by a newline, followed by an error message - the last flush operation failed due to an error.
- `disabled` - flushing is disabled because `thincow` was run with `--read-only-upstream`.

Flushing is done by writing a command to `/flush`. Accepted commands are:

- `full` - flush everything in one go.
- `max` - flush one pass. (Multiple passes may be necessary in case of data dependencies.)
- decimal number - flush at most this many blocks (block size is 512 by default).

### Freezing

When `thincow` receives the `SIGTSTP` signal, it waits for the current operation to finish and clears the dirty bit before suspending. This allows safely saving a snapshot of its data and metadata without stopping thincow or unmounting any filesystems mounted on top of thincow.

- Start `thincow` with the `--pid-file` option.

  ```console
  $ thincow ... --pid-file=thincow.pid ...
  ```

- To freeze thincow:

  1. First, you may want to sync / freeze / unmount any filesystems running on top of thincow, starting from inner-most to outer-most.

  2. Send `SIGTSTP` to the PID contained in the PID file specified by the `--pid-file` option.
  
     ```console
     $ kill -TSTP $(cat thincow.pid)
     ```
  
  3. Wait until the thincow process enters the `T (stopped)` state.
  
     ```console
     $ while [ "$(awk '/^State:/{print $2}' /proc/$(cat thincow.pid)/status)" != T ] ; do sleep 1 ; done
     ```

  4. It is now safe to make a copy of thincow's data and metadata directories.
  
  5. To resume thincow, send `SIGCONT` in the same way:
  
     ```console
     $ kill -CONT $(cat thincow.pid)
     ```

  6. Unfreeze / remount any filesystems that were running on top of thincow.

## Building

- Install [a D compiler](https://dlang.org/download.html)
- Install [Dub](https://github.com/dlang/dub), if it wasn't included with your D compiler
- Run `dub build -b release`

## License

`thincow` is available under the [Mozilla Public License, v. 2.0](http://mozilla.org/MPL/2.0/).
