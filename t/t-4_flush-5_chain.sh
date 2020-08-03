#!/bin/bash
source common.bash

# Test flushing a long dependency chain.

block_count=16

( seq 1 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=$block_count status=none
run_thincow
dd if=target/devs/test of=/dev/null bs=$block_size status=none
dd if=upstream/test of=target/devs/test bs=$block_size seek=1 status=none conv=notrunc count=$((block_count-1))
stop_thincow

run_thincow
echo full > target/flush
stop_thincow

diff \
	<(dd if=upstream/test bs=$block_size skip=1 count=$((block_count-1)) status=none) \
	<(( seq 1 1e99 || true ) | head -c $(((block_count - 1) * block_size)))
