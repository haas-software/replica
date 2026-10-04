#!/usr/bin/env python3
"""De meetkern van SQLite op HopOS/QEMU: één boot, een grote schijf, één job met
een eigen volume, en de REPLICA_BENCH_*-regels als uitkomst (out/bench/boot.log).

Zoals tools/qemu-persist.py komt alles van de gepinde SDK-tag; Hop uit $HOP_DIR.
REPLICA_BENCH_MIB kiest de datamaat (standaard 64 op QEMU), DISK_MIB de schijf
(standaard 1024). QEMU-getallen zijn relatief; het ijzer doet dezelfde app.
"""
import functools
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
TARGET = 'aarch64-unknown-none-softfloat'
OUT = ROOT / 'out/bench'
MIB = int(os.environ.get('REPLICA_BENCH_MIB', '64'))


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_):
        pass


def main():
    env = os.environ.copy()
    if Path('/opt/homebrew/opt/llvm/bin/clang').exists():
        env.setdefault('SQLITE_CC', '/opt/homebrew/opt/llvm/bin/clang')
        env.setdefault('SQLITE_AR', '/opt/homebrew/opt/llvm/bin/llvm-ar')
    subprocess.run(['cargo', 'build', '--locked', '--release', '--target', TARGET,
                    '-p', 'replica-sqlite-bench'], cwd=ROOT, env=env, check=True)
    binary = ROOT / 'target' / TARGET / 'release/replica-sqlite-bench'
    meta = json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--locked', '--format-version', '1'], cwd=ROOT))
    sdk = Path(next(p['manifest_path'] for p in meta['packages']
                    if p['name'] == 'applib')).parent.parent
    env = dict(os.environ, APP='hop')
    env.setdefault('HOP_DIR', str(ROOT.parent / 'hop/hop'))
    env.setdefault('DISK_MIB', '1024')
    for key in ('ROLE', 'APPENV', 'ARTIFACT', 'WEBPORT', 'GUI'):
        env.pop(key, None)
    held = []
    for name in ('SYSPORT', 'AGENTPORT', 'LEADERPORT'):
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        env[name] = str(sock.getsockname()[1])
        held.append(sock)
    OUT.mkdir(parents=True, exist_ok=True)
    log = OUT / 'boot.log'
    with tempfile.TemporaryDirectory(prefix='replica-bench-') as tmp:
        tmp = Path(tmp)
        env['DISK'] = str(tmp / 'disk.img')
        sysroot = subprocess.check_output(['rustc', '--print', 'sysroot'], cwd=ROOT, text=True).strip()
        objcopy = next(Path(sysroot).glob('lib/rustlib/*/bin/rust-objcopy'))
        subprocess.run([str(objcopy), '--strip-debug', str(binary), str(tmp / 'bench.elf')], check=True)
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Quiet, directory=str(tmp)))
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        job = json.loads((ROOT / 'apps/sqlite-bench/job.json').read_text())
        job['artifacts'] = [{'url': f'http://10.0.2.2:{server.server_port}/bench.elf'}]
        job['env']['REPLICA_BENCH_MIB'] = str(MIB)
        for sock in held:
            sock.close()
        with log.open('w') as stream:
            p = subprocess.Popen(['sh', str(sdk / 'image/qemu-run.sh')], cwd=sdk, env=env,
                                 stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                                 start_new_session=True)
            posted = False
            done = False
            try:
                end = time.monotonic() + float(os.environ.get('BENCH_TIMEOUT', '3600'))
                while time.monotonic() < end:
                    data = log.read_text(errors='replace')
                    if any(x in data for x in ('REPLICA_BENCH_FAIL', 'HOPOS_PANIC', 'HOPOS_EXCEPTION', 'HOPOS_APP_PANIC')):
                        break
                    if not posted and 'HOP_LEADER' in data and 'HOP_UP' in data:
                        req = urllib.request.Request(f"http://127.0.0.1:{env['LEADERPORT']}/v1/jobs",
                                                     data=json.dumps(job).encode(),
                                                     headers={'Content-Type': 'application/json'}, method='POST')
                        with urllib.request.urlopen(req, timeout=15) as response:
                            if response.status not in (200, 201, 202):
                                raise SystemExit('FAIL: jobspec geweigerd')
                        posted = True
                    if 'REPLICA_BENCH_DONE' in data:
                        done = True
                        break
                    if p.poll() is not None:
                        break
                    time.sleep(0.2)
            finally:
                if p.poll() is None:
                    os.killpg(p.pid, signal.SIGKILL)
                p.wait(timeout=5)
                server.shutdown()
                server.server_close()
    text = log.read_text(errors='replace')
    lines = [line.strip() for line in text.splitlines() if 'REPLICA_BENCH_' in line or 'HOPOS_DISK_UP' in line]
    print('\n'.join(lines))
    if not done:
        raise SystemExit(f'FAIL: geen REPLICA_BENCH_DONE; zie {log}')
    print(f'PASS: meetkern van {MIB} MiB op QEMU; log {log}')


if __name__ == '__main__':
    main()
