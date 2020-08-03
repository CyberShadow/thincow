#!/bin/bash
source common.bash

# Test flushing with multiple references to the same data.

block_count=16

( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/a bs=$block_size count=$block_count status=none
( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/b bs=$block_size count=$block_count status=none
( seq 2 1e99 || true ) | dd iflag=fullblock of=upstream/c bs=$block_size count=$block_count status=none
( seq 2 1e99 || true ) | dd iflag=fullblock of=upstream/d bs=$block_size count=$block_count status=none
run_thincow
dd if=target/devs/a of=/dev/null bs=$block_size status=none
dd if=target/devs/b of=/dev/null bs=$block_size status=none
dd if=target/devs/c of=/dev/null bs=$block_size status=none
dd if=target/devs/d of=/dev/null bs=$block_size status=none
dd if=upstream/a of=target/devs/c bs=$block_size status=none conv=notrunc
dd if=upstream/b of=target/devs/d bs=$block_size status=none conv=notrunc
dd if=upstream/c of=target/devs/a bs=$block_size status=none conv=notrunc
dd if=upstream/d of=target/devs/b bs=$block_size status=none conv=notrunc
stop_thincow

run_thincow
echo full > target/flush
stop_thincow

diff -q upstream/a <(( seq 2 1e99 || true ) | head -c $((block_count * block_size)))
diff -q upstream/b <(( seq 2 1e99 || true ) | head -c $((block_count * block_size)))
diff -q upstream/c <(( seq 1 1e99 || true ) | head -c $((block_count * block_size)))
diff -q upstream/d <(( seq 1 1e99 || true ) | head -c $((block_count * block_size)))
