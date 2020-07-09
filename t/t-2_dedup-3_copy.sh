#!/bin/bash
source common.bash

# Test that thincow deduplicates data copied from one place to another, with no extra disk usage.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
dd if=target/test of=target/test bs=512K seek=1 count=1 status=none conv=notrunc
fusermount -u target
diff -u <(get_usage data/cowdata) /dev/stdin <<< 0

run_thincow
diff -q <(dd if=target/test bs=512K count=1 status=none) <(dd if=target/test bs=512K skip=1 count=1 status=none)
fusermount -u target
