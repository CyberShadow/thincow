#!/bin/bash
source common.bash

# Test that upstream devices are readable.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow
diff -q target/test upstream/test
fusermount -u target
