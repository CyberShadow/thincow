#!/bin/bash
source common.bash

# Test directory enumeration.

for f in a b c d e
do
	echo -n "$f" > upstream/"$f"
done

run_thincow
diff -q <(ls upstream) <(ls target/devs)
stop_thincow
