#!/bin/bash
source common.bash

# As the previous test, but with a faulty device.

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

mv upstream/b b
orig_loop=$(losetup -f --show b)
dmsetup create thincow-dust --table "0 $(blockdev --getsz "$orig_loop") dust $orig_loop 0 $block_size"
ln -s /dev/mapper/thincow-dust upstream/b
for ((b=128 ; b < 128*1024*1024/block_size - 128 ; b+=17))
do
	dmsetup message thincow-dust 0 addbadblock "$b"
done
dmsetup message thincow-dust 0 enable

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
losetup -d "$single_loop"

mount target/devs/single mnt

diff -u <(md5sum < mnt/file) /dev/stdin <<< "$hash"

umount mnt

losetup -d "${loops[@]}"

stop_thincow

dmsetup status thincow-dust
dmsetup remove thincow-dust
losetup -d "$orig_loop"

[[ "$(get_usage data/cowdata)" -lt $((2*1024*1024)) ]]
