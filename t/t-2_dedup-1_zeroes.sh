#!/bin/bash
source common.bash

dd if=/dev/urandom of=upstream/test bs=1M count=1 status=none
run_thincow
dd if=/dev/zero of=target/test bs=1M count=1 status=none conv=notrunc
fusermount -u target
diff -u <(get_usage data/cowdata) /dev/stdin <<< $((4096 + 65536))
