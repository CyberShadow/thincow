#!/bin/bash
source common.bash

# As t-9_btrfs-1_raid10_to_single, but flush the result to upstream afterwards.

if [[ "$EUID" -ne 0 ]]
then
	echo 'Not root, skipping'
	exit
fi

hash_table_size=$((1024*1024))

devs=(a b c d)
loops=()
for d in "${devs[@]}"
do
	dd if=/dev/zero of=upstream/"$d" bs=1M seek=128 count=0 status=none
	loops+=("$(losetup -f --show upstream/"$d")")
done
mkfs.btrfs "${loops[@]}" -d raid10 -m raid1c4
mkdir mnt
mount -o "$( IFS=, ; echo "${loops[*]/#/device=}" )" "${loops[0]}" mnt

( seq 1e99 || true ) | dd iflag=fullblock of=mnt/file bs=1M count=150
hash=$(md5sum < mnt/file)
umount mnt

losetup -d "${loops[@]}"

dd if=/dev/zero of=upstream/single bs=1M seek=320 count=0

run_thincow

loops=()
for d in "${devs[@]}"
do
	loops+=("$(losetup -f --show target/devs/"$d")")
done
mount -o "$( IFS=, ; echo "${loops[*]/#/device=}" )" "${loops[0]}" mnt

single_loop=$(losetup -f --show target/devs/single)
btrfs device add "$single_loop" mnt

btrfs balance start --force -dconvert=single -mconvert=single mnt

btrfs balance start -dusage=0 -musage=0 mnt

btrfs device remove "${loops[@]}" mnt

umount mnt

losetup -d "${loops[@]}" "$single_loop"

mkdir expected
for d in "${devs[@]}" single
do
	dd if=target/devs/"$d" of=expected/"$d" bs=1M status=none
done

stop_thincow

run_thincow
echo full > target/flush
stop_thincow

for d in "${devs[@]}" single
do
	diff upstream/"$d" expected/"$d"
done
