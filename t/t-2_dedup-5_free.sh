#!/bin/bash
source common.bash

# Test that thincow reuses COW block storage.

dd if=/dev/zero of=upstream/test bs=64K seek=2 count=0 status=none
run_thincow
dd if=target/test of=/dev/null bs=64K status=none # populate hash table

# First block
dd if=/dev/urandom of=target/test bs=64K count=1 status=none conv=notrunc
dd if=/dev/zero    of=target/test bs=64K count=1 status=none conv=notrunc

# Second block
dd if=/dev/urandom of=target/test bs=64K count=1 status=none conv=notrunc seek=1
dd if=/dev/zero    of=target/test bs=64K count=1 status=none conv=notrunc seek=1

fusermount -u target
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((1 * block_size))
