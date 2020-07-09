#!/bin/bash
source common.bash

# Test removal of blocks from the hash table.

dd if=/dev/zero of=upstream/test bs=$block_size seek=2 count=0 status=none
run_thincow
dd if=target/test of=/dev/null bs=$block_size status=none # populate hash table

( seq 1e99 || true ) | dd iflag=fullblock of=target/test bs=$block_size count=2 status=none conv=notrunc
dd if=/dev/zero                           of=target/test bs=$block_size count=2 status=none conv=notrunc
( seq 1e99 || true ) | dd iflag=fullblock of=target/test bs=$block_size count=2 status=none conv=notrunc

stop_thincow
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((2 * block_size))
