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
python3 - <<'PY'
# De README noemt de echte pins: elke git-tag in een Cargo.toml staat in de
# tabel "Versies en pins", per repo één tag (replica-plan R8).
import re
from pathlib import Path
repos = {'HopOS.git': 'HopOS', 'lean.git': 'Lean', 'hop.git': 'Hop'}
readme = {}
for line in Path('README.md').read_text().splitlines():
    m = re.match(r'\| (HopOS|Lean|Hop) \(.*\| `(v[0-9.]+)` \|$', line)
    if m:
        readme[m.group(1)] = m.group(2)
fail = []
for toml in sorted(Path('.').glob('**/Cargo.toml')):
    if 'vendor' in toml.parts or 'target' in toml.parts:
        continue
    for m in re.finditer(r'git = "[^"]*/([A-Za-z]+\.git)", tag = "([^"]+)"', toml.read_text()):
        repo = repos.get(m.group(1))
        if repo is None or readme.get(repo) != m.group(2):
            fail.append(f'{toml}: {m.group(1)} {m.group(2)}, README {readme.get(repo)}')
if fail or len(readme) != len(repos):
    raise SystemExit('FAIL: README-pins wijken af van Cargo.toml:\n' + '\n'.join(fail))
print('README-pins: OK', readme)
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
