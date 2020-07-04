#!/bin/bash
source common.bash

# Test in-memory metadata.

dd if=/dev/urandom of=upstream/test bs=1M count=1 status=none
run_thincow --metadata-dir=-
diff -q target/test upstream/test
fusermount -u target
