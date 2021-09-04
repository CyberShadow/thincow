#!/bin/bash
source common.bash

# Test that thincow can work and read data with --read-only.

( seq     1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
( seq 1e9 1e99 || true ) | dd iflag=fullblock of=newdata       bs=$block_size count=16 status=none
run_thincow
dd if=newdata of=target/devs/test bs=$block_size count=16 status=none conv=notrunc
stop_thincow
run_thincow --read-only
diff -q target/devs/test newdata
stop_thincow
