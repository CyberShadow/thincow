#!/bin/bash
source common.bash

# Test that writing the same data to thincow does not corrupt it.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
dd if=upstream/test of=target/test bs=$block_size count=16 status=none conv=notrunc
fusermount -u target
run_thincow
diff -q target/test upstream/test
fusermount -u target
