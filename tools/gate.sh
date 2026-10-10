#!/bin/sh
# De bron, hostcontracten en twee freestanding builds; QEMU is een aparte poort.
set -eu
cd "$(dirname "$0")/.."

# Apple's clang kent aapcs-soft nog niet; LLVM 23.1.0 is hier getoetst.
if [ -x /opt/homebrew/opt/llvm/bin/clang ]; then
    : "${SQLITE_CC:=/opt/homebrew/opt/llvm/bin/clang}"
    : "${SQLITE_AR:=/opt/homebrew/opt/llvm/bin/llvm-ar}"
else
    : "${SQLITE_CC:=clang}"
    : "${SQLITE_AR:=llvm-ar}"
fi
export SQLITE_CC SQLITE_AR

python3 - <<'PY'
import hashlib, json
from pathlib import Path
root = Path('vendor/sqlite')
manifest = json.loads((root / 'SOURCE.json').read_text())
for name, expected in manifest['sha256'].items():
    actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f'FAIL: gewijzigde SQLite-bron {name}: {actual}')
print('SQLite-bronhashes: OK')
PY
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
# Beide apps voor beide targets: de RAM-probe en het HopFS-pad van replica-hopos.
# Een RISC-V-build is nog geen RISC-V-apprun.
for target in aarch64-unknown-none-softfloat riscv64gc-unknown-none-elf; do
    cargo build --locked --release --target "$target" \
        -p replica-sqlite-smoke -p replica-sqlite-persist -p replica-sqlite-bench \
        -p replica-hopos -p replica-core -p replica-s3
    cargo clippy --locked --release --target "$target" \
        -p replica-sqlite-smoke -p replica-sqlite-persist -p replica-sqlite-bench \
        -p replica-hopos -p replica-core -p replica-s3 -- -D warnings
done
if [ "${QEMU:-0}" = 1 ]; then
    python3 tools/qemu-smoke.py
fi
