#!/bin/bash
source common.bash

# Test dependency cycles with some extant COW hashes.

block_count=16
sink_block_count=$((hash_table_size / 8 * 64)) # Enough to completely fill the hash table

( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/a bs=$block_size count=$block_count status=none
( seq 2 1e99 || true ) | dd iflag=fullblock of=upstream/b bs=$block_size count=$block_count status=none
dd if=/dev/zero of=upstream/sink bs=$block_size seek=$sink_block_count count=0 status=none

run_thincow

# Create data loop
dd if=target/devs/a of=/dev/null bs=$block_size status=none
dd if=target/devs/b of=/dev/null bs=$block_size status=none
dd if=upstream/a of=target/devs/b bs=$block_size status=none conv=notrunc
dd if=upstream/b of=target/devs/a bs=$block_size status=none conv=notrunc

# Nuke hash table
( seq 3 1e99 || true ) | dd iflag=fullblock of=target/devs/sink bs=$block_size count=$sink_block_count status=none conv=notrunc

# Repopulate hash table, but now with COW references
( seq 2 1e99 || true ) | dd iflag=fullblock of=target/devs/sink bs=$block_size count=$block_count status=none conv=notrunc

stop_thincow

run_thincow
echo full > target/flush
stop_thincow

diff -q upstream/a <(( seq 2 1e99 || true ) | head -c $((block_count * block_size)))
diff -q upstream/b <(( seq 1 1e99 || true ) | head -c $((block_count * block_size)))
