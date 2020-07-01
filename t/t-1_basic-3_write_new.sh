#!/bin/bash
source common.bash

dd if=/dev/urandom of=upstream/test bs=1M count=1 status=none
dd if=/dev/urandom of=random bs=1M count=1 status=none
run_thincow
dd if=random of=target/test bs=1M count=1 status=none conv=notrunc
fusermount -u target
run_thincow
diff -q target/test random
fusermount -u target
