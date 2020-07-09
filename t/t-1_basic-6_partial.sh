#!/bin/bash
source common.bash

# Test partial trailing blocks.

bytes=$((block_size * 3 / 2))

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$bytes count=1 status=none
run_thincow
diff -q target/test upstream/test

( seq 2 1e99 || true ) | dd iflag=fullblock of=target/test bs=$bytes count=1 status=none conv=notrunc
diff -q target/test <( seq 2 1e99 | head -c "$bytes" )

stop_thincow
