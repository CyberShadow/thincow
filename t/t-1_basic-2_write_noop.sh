#!/bin/bash
source common.bash

# Test that writing the same data to thincow does not corrupt it.

dd if=/dev/urandom of=upstream/test bs=1M count=1 status=none
run_thincow
dd if=upstream/test of=target/test bs=1M count=1 status=none conv=notrunc
fusermount -u target
run_thincow
diff -q target/test upstream/test
fusermount -u target
