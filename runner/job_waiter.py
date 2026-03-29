"""Utilities for waiting on Slurm jobs to complete."""

import subprocess
import time
from pathlib import Path


def check_slurm_job_status(job_id: str) -> str:
    """
    Check Slurm job status.
    Returns: 'running', 'completed', 'failed', or 'unknown'
    """
    # Check if job is in queue
    result = subprocess.run(
        ["squeue", "-j", job_id, "-h", "-o", "%T"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0 or not result.stdout.strip():
        # Job not in queue, check sacct for completion
        result = subprocess.run(
            ["sacct", "-j", job_id, "-n", "-o", "State", "--parsable2"],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0 and result.stdout.strip():
            state = result.stdout.strip().split("\n")[0].upper()
            if "COMPLETED" in state:
                return "completed"
            elif any(
                x in state
                for x in [
                    "FAILED",
                    "CANCELLED",
                    "TIMEOUT",
                    "NODE_FAIL",
                    "OUT_OF_MEMORY",
                ]
            ):
                return "failed"

        # Job not in queue and sacct found nothing - might have completed
        # or sacct is unavailable
        return "unknown"

    # Job still in queue
    return "running"


def check_job_logs_for_completion(
    log_pattern: str, success_marker: str, log_dir: Path = None
) -> str:
    """
    Check job logs for completion markers.
    Returns: 'completed', 'failed', or 'unknown'

    Args:
        log_pattern: glob pattern for log files (e.g., "build_*_123.out")
        success_marker: string to look for indicating success
        log_dir: directory containing logs (defaults to runner/logs)
    """
    if log_dir is None:
        runner_dir = Path(__file__).parent.parent
        log_dir = runner_dir / "logs"

    if not log_dir.exists():
        return "unknown"

    # Check error logs first
    error_logs = list(log_dir.glob(log_pattern.replace(".out", ".err")))
    if error_logs:
        with open(error_logs[0], "r") as f:
            content = f.read()
            if "FATAL:" in content or "exit status 1" in content or "Error:" in content:
                return "failed"

    # Check output logs
    output_logs = list(log_dir.glob(log_pattern))
    if output_logs:
        with open(output_logs[0], "r") as f:
            content = f.read()
            if success_marker in content:
                return "completed"
            elif "error" in content.lower() or "failed" in content.lower():
                return "failed"

    return "unknown"


def wait_for_job_completion(
    job_id: str,
    job_name: str,
    check_interval: int = 60,
    log_pattern: str = None,
    success_marker: str = None,
) -> tuple[bool, str]:
    """
    Wait for a Slurm job to complete, polling at regular intervals.

    Args:
        job_id: Slurm job ID
        job_name: Human-readable job name for logging
        check_interval: Seconds between status checks (default: 60)
        log_pattern: Optional glob pattern for log files
        success_marker: Optional success marker in logs

    Returns:
        (success: bool, status: str)
    """
    from .display import print_error, print_info, print_step, print_success

    print_info(f"Waiting for {job_name} (job {job_id}) to complete...")
    print_info(f"Checking status every {check_interval} seconds...")

    checks = 0
    while True:
        status = check_slurm_job_status(job_id)

        if status == "completed":
            print_success(f"{job_name} completed successfully!")
            return True, "completed"

        elif status == "failed":
            print_error(f"{job_name} failed!")
            return False, "failed"

        elif status == "unknown":
            # Try checking logs if available
            if log_pattern and success_marker:
                log_status = check_job_logs_for_completion(log_pattern, success_marker)
                if log_status == "completed":
                    print_success(f"{job_name} completed (verified from logs)")
                    return True, "completed"
                elif log_status == "failed":
                    print_error(f"{job_name} failed (detected from logs)")
                    return False, "failed"

            # If we can't determine status, assume it's still running or just completed
            # Give it a few more checks before giving up
            if checks > 3:
                print_error(f"Cannot determine status of {job_name} after multiple checks")
                return False, "unknown"

        elif status == "running":
            checks += 1
            if checks % 5 == 0:  # Print progress every 5 checks
                print_step(f"{job_name} still running... (checked {checks} times)")

        # Wait before next check
        time.sleep(check_interval)
