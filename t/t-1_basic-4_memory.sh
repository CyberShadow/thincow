#!/bin/bash
source common.bash

# Test in-memory metadata.

( seq 1e99 || true ) | dd iflag=fullblock of=upstream/test bs=$block_size count=16 status=none
run_thincow --metadata-dir=-
diff -q target/test upstream/test
fusermount -u target
