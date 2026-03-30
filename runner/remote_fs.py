"""Primitive remote filesystem operations (SSH / rsync).

These thin wrappers are the *only* place that executes external SSH / rsync
commands.  They form the testability seam: replace them in tests with
equivalents that operate on a local "fake-remote" directory — all higher-level
logic in alexandria.py and algorithm_runner.py then runs for real.
"""

import subprocess
from pathlib import Path


def remote_file_exists(host: str, path: str) -> bool:
    """Return True if *path* exists as a regular file on *host*."""
    result = subprocess.run(
        ["ssh", host, f'test -f {path} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "exists"


def remote_dir_exists(host: str, path: str) -> bool:
    """Return True if *path* exists as a directory on *host*."""
    result = subprocess.run(
        ["ssh", host, f'test -d {path} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "exists"


def remote_mkdir(host: str, path: str) -> None:
    """Create *path* (and parents) on *host* via SSH."""
    subprocess.run(["ssh", host, f"mkdir -p {path}"])


def remote_find(
    host: str,
    path: str,
    mindepth: int,
    maxdepth: int,
    type_: str = "d",
) -> list[str]:
    """Run ``find`` on *host* and return matching paths as a list of strings.

    Returns an empty list when nothing is found or the command fails.
    """
    result = subprocess.run(
        [
            "ssh",
            host,
            f"find {path} -mindepth {mindepth} -maxdepth {maxdepth} -type {type_}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().split("\n")
    return []


def remote_read_first_line(host: str, path: str) -> str | None:
    """Return the first line of a remote file, or None on failure."""
    result = subprocess.run(
        ["ssh", host, f"head -n 1 {path}"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return None


def rsync_pull(host: str, src: str, dst: Path) -> bool:
    """Rsync a single file from *host*:*src* to *dst* (local).

    Creates parent directories automatically.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["rsync", "-avz", "--progress", f"{host}:{src}", str(dst)],
    )
    return result.returncode == 0
