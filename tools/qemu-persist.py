#!/usr/bin/env python3
"""SQLite -> private C-stack -> async system-API -> OP_SYNC -> harde stop -> koude herstart.

Alles komt van de gepinde SDK-tag (Cargo.lock): de app bouwt ertegen, en de kern
en de harde-stopproef (tools/qemu-test-sync.py) komen uit dezelfde checkout.
Hop wordt als bestand gebouwd uit zijn eigen repo ($HOP_DIR, standaard ../hop/hop).
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
TARGET = 'aarch64-unknown-none-softfloat'
OUT = ROOT / 'out/persist'


def main():
    env = os.environ.copy()
    if Path('/opt/homebrew/opt/llvm/bin/clang').exists():
        env.setdefault('SQLITE_CC', '/opt/homebrew/opt/llvm/bin/clang')
        env.setdefault('SQLITE_AR', '/opt/homebrew/opt/llvm/bin/llvm-ar')
    subprocess.run(['cargo', 'build', '--locked', '--release', '--target', TARGET,
                    '-p', 'replica-sqlite-persist'], cwd=ROOT, env=env, check=True)
    binary = ROOT / 'target' / TARGET / 'release/replica-sqlite-persist'
    meta = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--format-version', '1'], cwd=ROOT))
    sdk = Path(next(p['manifest_path'] for p in meta['packages']
                    if p['name'] == 'applib')).parent.parent
    # qemu-run.sh zoekt Hop naast zijn eigen repo; de cargo-checkout heeft die buur niet.
    os.environ.setdefault('HOP_DIR', str(ROOT.parent / 'hop/hop'))
    spec = importlib.util.spec_from_file_location('hopos_sync_probe', sdk / 'tools/qemu-test-sync.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.probe(binary, ('REPLICA_SQLITE_PERSIST_WRITE', 'REPLICA_SQLITE_PERSIST_READ'),
                 OUT, 'replica-persist')
    logs = [(OUT / f'boot-{boot}.log').read_text(errors='replace') for boot in (0, 1)]
    for boot, log in enumerate(logs):
        for marker in ('REPLICA_PIPELINE_OK', 'REPLICA_SQLITE_CANCEL_OK', 'REPLICA_OWNER_OK'):
            if marker not in log:
                raise SystemExit(f'FAIL: boot {boot} mist {marker}')
    for marker, why in (('REPLICA_RESTORED_COLD_OK', 'herstelde database overleefde de koude boot niet'),
                        ('REPLICA_PIPELINE_REPAIR_OK', 'verloren testbucket niet uit de lokale database herbouwd')):
        if marker not in logs[1]:
            raise SystemExit(f'FAIL: {why}')
    checks = json.loads((OUT / 'checks.json').read_text())
    checks.update(replication_pipeline=True, restored_database_cold=True,
                  cancelled_query_reopened=True, owner_lifecycle=True, lost_bucket_repaired=True)
    (OUT / 'checks.json').write_text(json.dumps(checks, indent=2) + '\n')
    for boot, log in enumerate(logs):
        for line in log.splitlines():
            if 'REPLICA_' in line:
                print(f'boot {boot}: {line.strip()}')
    print(f'PASS: HopFS-commit, SIGKILL, koude herstart, capture/restore en annulering; bewijs in {OUT}')


if __name__ == '__main__':
    main()
