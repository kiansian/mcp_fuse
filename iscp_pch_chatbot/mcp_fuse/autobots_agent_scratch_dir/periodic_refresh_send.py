#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RESEND_SCRIPT = BASE_DIR / "resend_latest_report.py"

INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "7200"))
TO_ADDRS = os.environ.get("TO_ADDRS", "").strip()
FORCE_SEND = os.environ.get("FORCE_SEND", "1").strip()
ONCE = os.environ.get("ONCE", "0").strip().lower() in {"1", "true", "yes", "y"}
STOP_AT = os.environ.get("STOP_AT", "").strip()

_stop = False


def _handle_signal(signum, frame):
    global _stop
    _stop = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _run_once() -> int:
    env = os.environ.copy()
    env["FORCE_SEND"] = FORCE_SEND or "1"
    if TO_ADDRS:
        env["TO_ADDRS"] = TO_ADDRS

    cmd = [sys.executable, str(RESEND_SCRIPT)]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] RUN: {' '.join(cmd)}")
    if TO_ADDRS:
        print(f"[{ts}] TO_ADDRS={TO_ADDRS}")

    proc = subprocess.run(cmd, cwd=str(BASE_DIR), env=env)
    return proc.returncode


def _compute_stop_at_epoch(stop_at: str):
    if not stop_at:
        return None

    try:
        hour_str, minute_str = stop_at.split(":", 1)
        hour = int(hour_str)
        minute = int(minute_str)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
    except Exception:
        return None

    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return int(target.timestamp())


def main() -> int:
    if not RESEND_SCRIPT.exists():
        print(f"ERROR: Missing script: {RESEND_SCRIPT}")
        return 2

    if INTERVAL_SECONDS < 60:
        print("ERROR: INTERVAL_SECONDS must be >= 60")
        return 2

    stop_at_epoch = _compute_stop_at_epoch(STOP_AT)
    if STOP_AT and stop_at_epoch is None:
        print("ERROR: STOP_AT must be in HH:MM 24-hour format (example: 10:00)")
        return 2

    if stop_at_epoch is not None:
        stop_ts = datetime.fromtimestamp(stop_at_epoch).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Will stop at local time: {stop_ts}")

    while not _stop:
        if stop_at_epoch is not None and int(time.time()) >= stop_at_epoch:
            print("Reached STOP_AT cutoff; exiting.")
            break

        rc = _run_once()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] DONE: rc={rc}")

        if ONCE:
            break

        slept = 0
        while slept < INTERVAL_SECONDS and not _stop:
            if stop_at_epoch is not None and int(time.time()) >= stop_at_epoch:
                print("Reached STOP_AT cutoff during wait; exiting.")
                break
            time.sleep(1)
            slept += 1

    print("Stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
