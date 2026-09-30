#!/usr/bin/env python3
"""Toetst native SQLite in de gepinde HopOS-kern, op een tijdelijke QEMU-schijf."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
TARGET = "aarch64-unknown-none-softfloat"


def main():
    subprocess.run(["cargo", "build", "--locked", "--release", "--target", TARGET,
                    "-p", "replica-sqlite-smoke"], cwd=ROOT, check=True)
    meta = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1"], cwd=ROOT))
    sdk = Path(next(p["manifest_path"] for p in meta["packages"]
                    if p["name"] == "applib")).parent.parent
    # Alleen de gepinde SDK-cache wordt gebouwd, niet de werkboom van HopOS.
    env = os.environ.copy()
    env["APP"] = str(ROOT / "target" / TARGET / "release" / "replica-sqlite-smoke")
    env["ROLE"] = "0"
    env.pop("ARTIFACT", None)
    env.pop("WEBPORT", None)
    held = []
    for name in ("SYSPORT", "AGENTPORT", "LEADERPORT"):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        held.append(sock)
        env[name] = str(sock.getsockname()[1])
    out = ROOT / "out"
    out.mkdir(exist_ok=True)
    log = out / "qemu-sqlite.log"
    with tempfile.TemporaryDirectory(prefix="replica-qemu-") as tmp:
        env["DISK"] = str(Path(tmp) / "disk.img")
        for sock in held:
            sock.close()
        with log.open("w") as stream:
            proc = subprocess.Popen(["sh", str(sdk / "image/qemu-run.sh")],
                                    cwd=sdk, env=env, stdout=stream,
                                    stderr=subprocess.STDOUT, start_new_session=True)
            passed = False
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    text = log.read_text(errors="replace")
                    if any(marker in text for marker in (
                        "REPLICA_SQLITE_FAIL", "HOPOS_PANIC", "HOPOS_EXCEPTION",
                        "HOPOS_APP_PANIC")):
                        break
                    if all(f"HOPOS_SLOT_DONE slot={slot} exit=0" in text
                           and f"slot {slot}: REPLICA_SQLITE_OK" in text
                           for slot in (1, 2)):
                        passed = True
                        break
                    if proc.poll() is not None:
                        break
                    time.sleep(0.2)
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL)
                        proc.wait()
        text = log.read_text(errors="replace")
        if not passed:
            print(text)
            raise SystemExit(f"FAIL: zie {log}")
        print("\n".join(line for line in text.splitlines()
                        if "REPLICA_SQLITE_" in line or "HOPOS_SLOT_DONE" in line))
        print(f"PASS: twee onafhankelijke HopOS-partities; log {log}")


if __name__ == "__main__":
    main()
