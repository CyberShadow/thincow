set -eEuo pipefail

test_name=$(basename "$0" .sh)
test_dir=tmp/$test_name

if ! rm -rf "$test_dir"
then
	fusermount -u "$test_dir"/target
	rm -rf "$test_dir"
fi
mkdir -p "$test_dir"
cd "$test_dir"
mkdir data target upstream

function run_thincow() {
	if [[ -v THINCOW_TEST_DEBUG ]]
	then
		../../../thincow -f -o debug --upstream=upstream --data-dir=data target &> log.txt &
		sleep 1
	else
		../../../thincow --upstream=upstream --data-dir=data target
	fi
}

# Get disk usage of a file, in bytes.
# More reliable than du.
function get_usage() {
	local script
	script=$(cat <<'EOF'
seek STDIN,0,2;
$total=0; $pos=0;
while (1) {
	  seek STDIN,$pos,3;
	  $pos = tell STDIN;
	  seek STDIN,$pos,4;
	  $pos2 = tell STDIN;
	  last if $pos == $pos2;
	  $total += $pos2 - $pos;
	  $pos = $pos2;
}
print $total;
EOF
)

	perl -le "$script" < "$1"
}
