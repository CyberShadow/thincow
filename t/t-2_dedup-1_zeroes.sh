#!/bin/bash
source common.bash

# Test that thincow deduplicates many zero blocks to one.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
dd if=/dev/zero of=target/devs/test bs=$block_size count=16 status=none conv=notrunc
stop_thincow
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((1 * block_size))
