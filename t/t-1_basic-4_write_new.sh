#!/bin/bash
source common.bash

# Test that thincow can preserve data written to it, even across restarts.

( seq     1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
( seq 1e9 1e99 || true ) | dd iflag=fullblock of=newdata       bs=$block_size count=16 status=none
run_thincow
dd if=newdata of=target/test bs=$block_size count=16 status=none conv=notrunc
stop_thincow
run_thincow
diff -q target/test newdata
stop_thincow
