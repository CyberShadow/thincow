set -eEuo pipefail

test_name=$(basename "$0" .sh)
test_dir=tmp/$test_name

block_size=4096 # Can't reliably measure disk usage in lower granularity than page size

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
	# Build
	if [[ $EUID != 0 ]]
	then
		(
			flock 9
			cd "$root"
			args=(
				rdmd -Ilib/import -L-lfuse -g -debug --build-only -oft/tmp/thincow
			)
			if (( ${THINCOW_COV:-0} ))
			then
				args+=(
					-cov
				)
			fi
			"${args[@]}" thincow.d
		) 9>> ../build.lock
	else
		echo '(Not rebuilding thincow as root...)'
	fi

	# Pipe to signal thincow exit
	rm -f fifo
	mkfifo fifo
	cat fifo &
	fifo_pid=$!

	args=(
		../thincow
		--upstream=upstream
		--data-dir=data
		--block-size="$block_size"
		--hash-table-size=$((1024*1024))
		target
		"$@"
	)
	if (( ${THINCOW_COV:-0} ))
	then
		args+=(
			--DRT-covopt="merge:1 srcpath:$root dstpath:$(dirname "$PWD")/cov"
		)
	fi

	if [[ -v THINCOW_TEST_DEBUG ]]
	then
		"${args[@]}" -f -o debug &>> log.txt &
		sleep 1
	else
		"${args[@]}"
	fi 9> fifo
}

function stop_thincow() {
	cp target/debug/*.txt ./
	fusermount -u target
	wait $fifo_pid
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
print STDERR "$total bytes used on disk\n";
print $total;
EOF
)
	printf '%s: ' "$1" 1>&2
	perl -le "$script" < "$1"
}
