#!/bin/bash
set -eu

rm -rf tmp/cov
mkdir -p tmp/cov
export THINCOW_COV=1

for f in ./t-*.sh
do
	printf -- '\n--- Running test %s\n\n' "$f"
	"$f"
done
