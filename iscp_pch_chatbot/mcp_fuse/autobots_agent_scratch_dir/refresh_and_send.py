#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
FULL_REPORT_SCRIPT = BASE_DIR / "resend_latest_report.py"


def _candidate_interpreters() -> list[Path]:
    candidates: list[Path] = []

    env_python = os.environ.get("AUTOBOTS_SDK_PYTHON", "").strip()
    if env_python:
        candidates.append(Path(env_python))

    default_sdk_python = Path("/p/hdk/pu_tu/prd/autobots_sdk/2.2.2/venv_autobots/bin/python")
    candidates.append(default_sdk_python)

    tool_root = os.environ.get("AUTOBOTS_SDK_TOOL_PATH", "").strip()
    if tool_root:
        root = Path(tool_root)
        candidates.append(root / "venv_autobots/bin/python")
        candidates.append(root / "prod/venv_autobots/bin/python")

    venv_path = os.environ.get("AUTOBOTS_SDK_VENV_PATH", "").strip()
    if venv_path:
        site_packages = Path(venv_path)
        # Expected shape: .../venv_autobots/lib/pythonX.Y/site-packages
        if site_packages.name == "site-packages":
            candidates.append(site_packages.parents[2] / "bin/python")

    return candidates


def _resolve_python() -> str:
    for candidate in _candidate_interpreters():
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return sys.executable


def _has_autobots_sdk(python_cmd: str) -> bool:
    check = subprocess.run(
        [python_cmd, "-c", "import autobots_sdk"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return check.returncode == 0


def _maybe_bootstrap_autobots_sdk(python_cmd: str, env: dict[str, str]) -> bool:
    if _has_autobots_sdk(python_cmd):
        return True

    bootstrap = env.get("AUTOBOTS_BOOTSTRAP_INSTALL", "").strip().lower() in {"1", "true", "yes", "y"}
    if not bootstrap:
        print("WARN: autobots_sdk not importable and bootstrap disabled (set AUTOBOTS_BOOTSTRAP_INSTALL=1 to enable)")
        return False

    cmd = [python_cmd, "-m", "pip", "install", "autobots_sdk"]
    index_url = env.get("AUTOBOTS_PIP_INDEX_URL", "").strip()
    extra_index_url = env.get("AUTOBOTS_PIP_EXTRA_INDEX_URL", "").strip()
    trusted_host = env.get("AUTOBOTS_PIP_TRUSTED_HOST", "").strip()

    if index_url:
        cmd.extend(["--index-url", index_url])
    if extra_index_url:
        cmd.extend(["--extra-index-url", extra_index_url])
    if trusted_host:
        cmd.extend(["--trusted-host", trusted_host])

    print("INFO: autobots_sdk missing; attempting bootstrap install")
    install = subprocess.run(cmd, cwd=str(BASE_DIR), env=env)
    if install.returncode != 0:
        print(f"ERROR: bootstrap install failed (rc={install.returncode})")
        return False

    if not _has_autobots_sdk(python_cmd):
        print("ERROR: autobots_sdk still not importable after bootstrap install")
        return False

    print("INFO: autobots_sdk bootstrap install complete")
    return True


def main() -> int:
    if not FULL_REPORT_SCRIPT.exists():
        print(f"ERROR: Missing full-report script: {FULL_REPORT_SCRIPT}")
        return 2

    env = os.environ.copy()
    env.setdefault("FORCE_SEND", "1")

    python_cmd = _resolve_python()
    if python_cmd != sys.executable:
        print(f"INFO: using Autobots SDK Python: {python_cmd}")
    else:
        print(f"INFO: using current Python: {python_cmd}")

    if not _maybe_bootstrap_autobots_sdk(python_cmd, env):
        return 4

    proc = subprocess.run([python_cmd, str(FULL_REPORT_SCRIPT)], cwd=str(BASE_DIR), env=env)
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
