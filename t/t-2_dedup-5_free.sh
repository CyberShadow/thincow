#!/bin/bash
source common.bash

# Test that thincow reuses COW block storage.

dd if=/dev/zero of=upstream/test bs=64K seek=2 count=0 status=none
run_thincow
dd if=target/test of=/dev/null bs=64K status=none # populate hash table

# First block
( seq 1e99 || true ) | dd iflag=fullblock of=target/test bs=64K count=1 status=none conv=notrunc
dd if=/dev/zero                           of=target/test bs=64K count=1 status=none conv=notrunc

# Second block
( seq 1e99 || true ) | dd iflag=fullblock of=target/test bs=64K count=1 status=none conv=notrunc seek=1
dd if=/dev/zero                           of=target/test bs=64K count=1 status=none conv=notrunc seek=1

stop_thincow
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((1 * block_size))
