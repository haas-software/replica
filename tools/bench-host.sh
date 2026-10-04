#!/bin/sh
# De lat op de host: tools/bench-host.c met de unix-VFS van SQLite, gebouwd uit
# de gevendorde amalgamation met de host-clang, op een tijdelijke map van het
# lokale bestandssysteem. tools/bench-host.sh [MiB] (standaard 64).
set -eu
cd "$(dirname "$0")/.."
: "${CC:=clang}"
mkdir -p out/bench-host
"$CC" -O2 -DNDEBUG -DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_DEFAULT_MEMSTATUS=0 \
    -Ivendor/sqlite -o out/bench-host/bench-host vendor/sqlite/sqlite3.c tools/bench-host.c
dir="$(mktemp -d -t replica-bench-host.XXXXXX)"
trap 'rm -rf "$dir"' EXIT
out/bench-host/bench-host "${1:-64}" "$dir/bench.db"
