"""Build the AgentCore Runtime code zip: our code + dependencies as linux/arm64 wheels.

AgentCore "direct code deployment" runs Python on Graviton (arm64), so wheels with compiled parts
(pydantic-core, etc.) must be the aarch64 builds, not the ones for this x86 machine. pip can fetch
them for another platform with --platform/--only-binary. No Docker needed.

The zip is reproducible (fixed timestamps, sorted entries): unchanged code gives the same file hash,
so `cdk deploy` only updates the runtime when something actually changed.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
DEFAULT_ZIP = BUILD / "analyst_runtime.zip"
OUR_PACKAGES = ["analyst", "common"]
PLATFORMS = ["manylinux_2_28_aarch64", "manylinux2014_aarch64"]
PYTHON_VERSION = "3.12"
FIXED_TIME = (2026, 1, 1, 0, 0, 0)
SKIP_PARTS = {"__pycache__", "tests"}


def install_dependencies(target: Path) -> None:
    cmd = [
        sys.executable, "-m", "pip", "install", "--quiet", "--no-compile", "--upgrade",
        "--target", str(target), "--implementation", "cp", "--python-version", PYTHON_VERSION,
        "--only-binary=:all:", "-r", str(ROOT / "agent" / "requirements.txt"),
    ]
    for p in PLATFORMS:
        cmd += ["--platform", p]
    subprocess.run(cmd, check=True)


def copy_sources(target: Path) -> None:
    for pkg in OUR_PACKAGES:
        dest = target / pkg
        shutil.rmtree(dest, ignore_errors=True)
        shutil.copytree(ROOT / "src" / pkg, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    shutil.copy2(ROOT / "agent" / "main.py", target / "main.py")


def write_zip(source: Path, out: Path) -> int:
    out.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(p for p in source.rglob("*") if p.is_file() and not SKIP_PARTS & set(p.relative_to(source).parts) and p.suffix != ".pyc")
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for path in files:
            info = zipfile.ZipInfo(str(path.relative_to(source)), date_time=FIXED_TIME)
            info.external_attr = (0o755 if path.stat().st_mode & 0o111 else 0o644) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, path.read_bytes())
    return len(files)


def build(out: Path = DEFAULT_ZIP, skip_deps: bool = False) -> Path:
    staging = BUILD / "analyst_pkg"
    if not skip_deps:
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(parents=True)
        install_dependencies(staging)
    copy_sources(staging)
    count = write_zip(staging, out)
    print(f"wrote {out}: {count} files, {out.stat().st_size / 1e6:.1f} MB")
    return out
