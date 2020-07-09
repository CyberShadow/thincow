#!/bin/bash
source common.bash

# Test that thincow deduplicates data copied from one place to another, with no extra disk usage.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
dd if=target/devs/test of=target/devs/test bs=$block_size seek=8 count=8 status=none conv=notrunc
stop_thincow
diff -u <(get_usage data/cowdata) /dev/stdin <<< 0

run_thincow
diff -q <(dd if=target/devs/test bs=$block_size count=8 status=none) <(dd if=target/devs/test bs=$block_size skip=8 count=8 status=none)
stop_thincow
