#!/bin/bash
source common.bash

# Like t-4_flush-4_multi, but clear the hash table in-between arranging the duplicates.

block_count=16
sink_block_count=$((hash_table_size / 8 * 64)) # Enough to completely fill the hash table

dd if=/dev/zero of=upstream/sink bs=$block_size seek=$sink_block_count count=0 status=none
( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/a bs=$block_size count=$block_count status=none
( seq 2 1e99 || true ) | dd iflag=fullblock of=upstream/b bs=$block_size count=$block_count status=none
( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/c bs=$block_size count=$block_count status=none
( seq 2 1e99 || true ) | dd iflag=fullblock of=upstream/d bs=$block_size count=$block_count status=none

# Set up a<->b
run_thincow
dd if=target/devs/a of=/dev/null bs=$block_size status=none # Populate hash table
dd if=target/devs/b of=/dev/null bs=$block_size status=none # Populate hash table
# Note that we leave one block at the end, to prevent thincow from merging the extents of b and c.
dd if=upstream/a of=target/devs/b bs=$block_size status=none conv=notrunc count=$((block_count-1))
dd if=upstream/b of=target/devs/a bs=$block_size status=none conv=notrunc count=$((block_count-1))
stop_thincow

# Nuke hash table
run_thincow
( seq 3 1e99 || true ) | dd iflag=fullblock of=target/devs/sink bs=$block_size count=$sink_block_count status=none conv=notrunc
stop_thincow

# Set up c<->d
run_thincow
dd if=target/devs/c of=/dev/null bs=$block_size status=none # Populate hash table
dd if=target/devs/d of=/dev/null bs=$block_size status=none # Populate hash table
dd if=upstream/c of=target/devs/d bs=$block_size status=none conv=notrunc count=$((block_count-1))
dd if=upstream/d of=target/devs/c bs=$block_size status=none conv=notrunc count=$((block_count-1))
stop_thincow

# Now, flush
run_thincow
echo full > target/flush
stop_thincow

diff -q <(dd if=upstream/a bs=$block_size count=$((block_count-1)) status=none) <(( seq 2 1e99 || true ) | head -c $(((block_count - 1) * block_size)))
diff -q <(dd if=upstream/b bs=$block_size count=$((block_count-1)) status=none) <(( seq 1 1e99 || true ) | head -c $(((block_count - 1) * block_size)))
diff -q <(dd if=upstream/c bs=$block_size count=$((block_count-1)) status=none) <(( seq 2 1e99 || true ) | head -c $(((block_count - 1) * block_size)))
diff -q <(dd if=upstream/d bs=$block_size count=$((block_count-1)) status=none) <(( seq 1 1e99 || true ) | head -c $(((block_count - 1) * block_size)))
