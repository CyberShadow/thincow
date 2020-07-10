#!/bin/bash
source common.bash

# Test writing blocks in backwards order.

( seq     1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
( seq 1e9 1e99 || true ) | dd iflag=fullblock of=newdata       bs=$block_size count=16 status=none
run_thincow
dd if=target/devs/test of=/dev/null bs=$block_size status=none
for ((i = 15; i >= 0; i--))
do
	dd if=newdata of=target/devs/test bs=$block_size count=1 status=none conv=notrunc seek="$i" skip="$i"
done
stop_thincow
run_thincow
diff -q target/devs/test newdata
for ((i = 15; i >= 0; i--))
do
	dd if=upstream/test of=target/devs/test bs=$block_size count=1 status=none conv=notrunc seek="$i" skip="$i"
done
stop_thincow
run_thincow
diff -q target/devs/test upstream/test
stop_thincow
