#!/bin/bash
source common.bash

# Test incremental flushing.

block_count=64
batch_size=16

dd if=/dev/zero of=upstream/test bs=$block_size seek=$block_count count=0 status=none
run_thincow
( seq 1e99 || true ) | dd iflag=fullblock of=target/devs/test bs=$block_size count=$block_count status=none conv=notrunc

iterations=0
while [[ "$(cat target/flush)" == ready ]]
do
	stop_thincow
	run_thincow
	echo $batch_size > target/flush
	iterations=$((iterations+1))
done

stop_thincow
diff upstream/test <( ( seq 1e99 || true ) | head -c $((block_size * block_count)))
diff <(echo "$iterations") <(echo $((block_count / batch_size)))
