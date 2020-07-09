#!/bin/bash
source common.bash

# Test that thincow consumes a constant amount of disk space
# when the same block is written to repeatedly.

dd if=/dev/zero of=upstream/test bs=64K seek=1 count=0 status=none
run_thincow
for _ in $(seq 100)
do
	( seq 1e99 || true ) | dd iflag=fullblock of=target/devs/test bs=$block_size count=1 status=none conv=notrunc
done
stop_thincow
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((1 * block_size))
