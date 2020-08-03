#!/bin/bash
source common.bash

# Test basic flushing.

dd if=/dev/zero of=upstream/test bs=$block_size seek=1 count=0 status=none
run_thincow
( seq 1e99 || true ) | dd iflag=fullblock of=target/devs/test bs=$block_size count=1 status=none conv=notrunc
echo full > target/flush
stop_thincow
diff upstream/test <( ( seq 1e99 || true ) | head -c $block_size)
