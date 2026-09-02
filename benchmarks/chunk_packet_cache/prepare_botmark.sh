#!/bin/sh
set -eu

destination=${1:-/tmp/pumpkin-botmark}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if [ ! -d "$destination/.git" ]; then
    git clone https://github.com/Pumpkin-MC/BotMark.git "$destination"
fi

for patch in botmark-chunk-batches.patch botmark-master-compat.patch; do
    if git -C "$destination" apply --reverse --check "$script_dir/$patch" >/dev/null 2>&1; then
        printf 'note: %s already applied, skipping\n' "$patch"
    else
        git -C "$destination" apply "$script_dir/$patch"
    fi
done

cargo build --release --manifest-path "$destination/Cargo.toml"
