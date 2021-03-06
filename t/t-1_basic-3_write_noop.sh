#!/bin/bash
source common.bash

# Test that writing the same data to thincow does not corrupt it.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
dd if=upstream/test of=target/devs/test bs=$block_size count=16 status=none conv=notrunc
stop_thincow
run_thincow
diff -q target/devs/test upstream/test
stop_thincow
