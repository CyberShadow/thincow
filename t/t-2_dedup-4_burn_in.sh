#!/bin/bash
source common.bash

# Test that thincow consumes a constant amount of disk space
# when the same block is written to repeatedly.

dd if=/dev/zero of=upstream/test bs=64K seek=1 count=0 status=none
run_thincow
for _ in $(seq 100)
do
	dd if=/dev/urandom of=target/test bs=64K count=1 status=none conv=notrunc
done
fusermount -u target
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((cow_data_header + 1 * block_size))
