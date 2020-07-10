#!/bin/bash
source common.bash

# Test swapping data on two upstream devices.

( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/a bs=$block_size count=16 status=none
( seq 2 1e99 || true ) | dd iflag=fullblock of=upstream/b bs=$block_size count=16 status=none
run_thincow
dd if=target/devs/a of=/dev/null bs=$block_size status=none
dd if=target/devs/b of=/dev/null bs=$block_size status=none
dd if=upstream/a of=target/devs/b bs=$block_size status=none conv=notrunc
dd if=upstream/b of=target/devs/a bs=$block_size status=none conv=notrunc
stop_thincow

run_thincow
diff -q upstream/a target/devs/b
diff -q upstream/b target/devs/a
stop_thincow
diff -u <(get_usage data/cowdata) /dev/stdin <<< 0
