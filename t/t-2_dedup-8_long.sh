#!/bin/bash
source common.bash

# Test retroactive deduplication of data that thincow forgot about.

block_count=$((hash_table_size * 3 / 2 / 8)) # 150% hash table capacity

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=$block_count status=none
run_thincow
dd if=target/devs/test of=/dev/null bs=$block_size status=none # populate hash table
dd if=/dev/zero of=target/devs/test bs=$block_size status=none count=$block_count conv=notrunc # wipe
stop_thincow
run_thincow
dd if=upstream/test of=target/devs/test bs=$block_size status=none conv=notrunc # deduplicate
grep -qF 'Total COW references: 0 blocks (0 bytes)' target/stats-full.txt
stop_thincow
