"""Git operations for repository management."""

import subprocess
import sys
from pathlib import Path

from .display import print_header, print_step, print_success, print_warning


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

        # Clone the repository
        result = subprocess.run(
            ["git", "clone", "-b", branch, repo_url, str(repo_path)], capture_output=True, text=True
        )

        if result.returncode == 0:
            print_success(f"Repository cloned successfully to {repo_path}")
            return True
        else:
            print(f"✗ Error cloning repository: {result.stderr}")
            sys.exit(1)
    else:
        print_step(f"Repository exists at: {repo_path}")

        # Check current branch
        result = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
        )
        current_branch = result.stdout.strip()

        if current_branch != branch:
            print_warning(f"Repository is on branch '{current_branch}', expected '{branch}'")
            print_step(f"Switching to branch '{branch}'...")
            subprocess.run(["git", "-C", str(repo_path), "checkout", branch])

        # Fetch latest
        print_step("Checking for updates...")
        subprocess.run(["git", "-C", str(repo_path), "fetch"], capture_output=True)

        # Check if update needed
        result = subprocess.run(
            ["git", "-C", str(repo_path), "rev-list", "--count", f"HEAD..origin/{branch}"],
            capture_output=True,
            text=True,
        )

        commits_behind = int(result.stdout.strip())

        if commits_behind > 0:
            print_step(f"Repository is {commits_behind} commit(s) behind. Pulling...")
            result = subprocess.run(
                ["git", "-C", str(repo_path), "pull"], capture_output=True, text=True
            )
            if result.returncode == 0:
                print_success("Repository updated successfully")
                return True
            else:
                print(f"✗ Error updating repository: {result.stderr}")
                return False
        else:
            print_success("Repository is up to date")
            return False
