set -eEuo pipefail

test_name=$(basename "$0" .sh)
test_dir=tmp/$test_name

block_size=$((64*1024))

root=$(dirname "$PWD")

#########################################################################
# Cleanup

test_dir_real=$(realpath "$test_dir")

# Clean up mounts (preemptively)
while read -r what where _
do
	if [[ "$where" == "$test_dir_real"/* ]]
	then
		fusermount -u "$where" || umount "$what" || true
	fi
done < /proc/mounts

# Clean up loop devices
losetup | \
	while read -r name _ _ _ _ file
	do
		if [[ "$file" == "$test_dir_real"/* ]]
		then
			losetup -d "$name"
		fi
	done

# Clean up mounts
while read -r what where _
do
	if [[ "$where" == "$test_dir_real"/* ]]
	then
		fusermount -u "$where" || umount "$what"
	fi
done < /proc/mounts


if ! rm -rf "$test_dir"
then
	fusermount -u "$test_dir"/target
	rm -rf "$test_dir"
fi

#########################################################################
# Setup

mkdir -p "$test_dir"
cd "$test_dir"
mkdir data target upstream

function run_thincow() {
	args=("$root"/thincow --upstream=upstream --data-dir=data target --block-size="$block_size" "$@")
	if [[ -v THINCOW_TEST_DEBUG ]]
	then
		"${args[@]}" -f -o debug &> log.txt &
		sleep 1
	else
		"${args[@]}"
	fi
}

# Get disk usage of a file, in bytes.
# More reliable than du.
function get_usage() {
	local script
	script=$(cat <<'EOF'
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
