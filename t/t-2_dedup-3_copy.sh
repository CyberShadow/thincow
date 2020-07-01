#!/bin/bash
source common.bash

dd if=/dev/urandom of=upstream/test bs=1M count=1 status=none
run_thincow
dd if=target/test of=target/test bs=512K seek=1 count=1 status=none conv=notrunc
fusermount -u target
diff -u <(get_usage data/cowdata) /dev/stdin <<< 0

run_thincow
diff -q <(dd if=target/test bs=512K count=1 status=none) <(dd if=target/test bs=512K skip=1 count=1 status=none)
fusermount -u target
