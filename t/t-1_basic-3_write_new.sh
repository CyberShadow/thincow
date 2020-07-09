#!/bin/bash
source common.bash

# Test that thincow can preserve data written to it, even across restarts.

( seq     1e99 || true ) | dd iflag=fullblock of=upstream/test bs=1M count=1 status=none
( seq 1e9 1e99 || true ) | dd iflag=fullblock of=newdata       bs=1M count=1 status=none
run_thincow
dd if=newdata of=target/test bs=1M count=1 status=none conv=notrunc
fusermount -u target
run_thincow
diff -q target/test newdata
fusermount -u target
