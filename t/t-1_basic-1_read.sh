#!/bin/bash
source common.bash

# Test that upstream devices are readable.

dd if=/dev/urandom of=upstream/test bs=1M count=1 status=none
run_thincow
diff -q target/test upstream/test
fusermount -u target
