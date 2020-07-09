#!/bin/bash
source common.bash

# Test that thincow deduplicates many identical blocks to one.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
( yes || true ) | dd of=target/test bs=$block_size count=16 status=none conv=notrunc
fusermount -u target
test "$(get_usage data/cowdata)" -le $((2 * block_size))

# Delete pattern (test unreferencing and unhashing)
run_thincow
dd if=/dev/zero of=target/test bs=1M count=1 status=none conv=notrunc
fusermount -u target
test "$(get_usage data/cowdata)" -le $((3 * block_size))
