"""Git operations for repository management."""

import subprocess
import sys
from pathlib import Path

from .display import print_header, print_step, print_success, print_warning

# ---------------------------------------------------------------------------
# Primitives — thin wrappers around git CLI network operations.
# These are the only functions that touch the network / remote; replace them
# in tests to keep the pipeline logic exercised without real git remotes.
# ---------------------------------------------------------------------------


def git_clone(url: str, branch: str, path: Path) -> bool:
    """Clone *url* (branch *branch*) into *path*. Return True on success."""
    result = subprocess.run(
        ["git", "clone", "-b", branch, url, str(path)],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def git_fetch(path: Path) -> None:
    """Fetch from origin for the repo at *path* (no-op if already up to date)."""
    subprocess.run(["git", "-C", str(path), "fetch"], capture_output=True)


def git_count_behind(path: Path, branch: str) -> int:
    """Return number of commits the local HEAD is behind *origin/branch*."""
    result = subprocess.run(
        ["git", "-C", str(path), "rev-list", "--count", f"HEAD..origin/{branch}"],
        capture_output=True,
        text=True,
    )
    try:
        return int(result.stdout.strip())
    except ValueError:
        return 0


def git_pull_rebase(path: Path) -> bool:
    """Pull with rebase for the repo at *path*. Return True on success."""
    result = subprocess.run(
        ["git", "-C", str(path), "pull", "--rebase"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def git_get_branch(path: Path) -> str:
    """Return the current branch name for the repo at *path*."""
    result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def git_checkout(path: Path, branch: str) -> None:
    """Check out *branch* in the repo at *path*."""
    subprocess.run(["git", "-C", str(path), "checkout", branch])


# ---------------------------------------------------------------------------


def check_or_clone_repo(config: dict) -> bool:
    """
    Check if repository exists, clone if not, pull if exists.
    Returns True if any changes were made.
    """
    print_header("Repository Status")

    repo_path = Path(__file__).parent.parent / config["denovo_benchmarks"]["local_path"]
    repo_url = config["denovo_benchmarks"]["repo_url"]
    branch = config["denovo_benchmarks"]["branch"]

    if not repo_path.exists():
        print_step(f"Repository not found at: {repo_path}")
        print_step(f"Cloning from {repo_url} (branch: {branch})...")

        if git_clone(repo_url, branch, repo_path):
            print_success(f"Repository cloned successfully to {repo_path}")
            return True
        else:
            print("✗ Error cloning repository")
            sys.exit(1)
    else:
        print_step(f"Repository exists at: {repo_path}")

        current_branch = git_get_branch(repo_path)

        if current_branch != branch:
            print_warning(f"Repository is on branch '{current_branch}', expected '{branch}'")
            print_step(f"Switching to branch '{branch}'...")
            git_checkout(repo_path, branch)

        # Fetch latest
        print_step("Checking for updates...")
        git_fetch(repo_path)

        commits_behind = git_count_behind(repo_path, branch)

        if commits_behind > 0:
            print_step(f"Repository is {commits_behind} commit(s) behind. Pulling...")
            if git_pull_rebase(repo_path):
                print_success("Repository updated successfully")
                return True
            else:
                print("✗ Error updating repository")
                return False
        else:
            print_success("Repository is up to date")
            return False
