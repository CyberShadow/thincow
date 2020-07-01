#!/bin/bash
set -eu

for f in ./t-*.sh
do
	printf -- '\n--- Running test %s\n\n' "$f"
	"$f"
done
