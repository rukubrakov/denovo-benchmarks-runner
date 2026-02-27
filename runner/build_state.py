"""Container build state tracking."""

import json
import subprocess
from datetime import datetime
from pathlib import Path


class BuildState:
    """Track container build states."""

    def __init__(self, state_file: Path = None):
        if state_file is None:
            state_file = Path(__file__).parent.parent / "build_state.json"
        self.state_file = state_file
        self.states = self._load()

    def _load(self) -> dict:
        """Load build states from JSON file."""
        if not self.state_file.exists():
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def _save(self):
        """Save build states to JSON file."""
        with open(self.state_file, "w") as f:
            json.dump(self.states, f, indent=2)

    def get_key(self, algo_name: str, version: str) -> str:
        """Get state key for algorithm and version."""
        return f"{algo_name}@{version}"

    def mark_building(self, algo_name: str, version: str, job_id: str):
        """Mark container as building."""
        key = self.get_key(algo_name, version)
        self.states[key] = {
            "status": "building",
            "job_id": job_id,
            "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        self._save()

    def mark_completed(self, algo_name: str, version: str):
        """Mark container build as completed."""
        key = self.get_key(algo_name, version)
        if key in self.states:
            self.states[key]["status"] = "completed"
            self.states[key]["updated_at"] = datetime.now().isoformat()
            self._save()

    def mark_failed(self, algo_name: str, version: str, error: str = None):
        """Mark container build as failed."""
        key = self.get_key(algo_name, version)
        if key in self.states:
            self.states[key]["status"] = "failed"
            self.states[key]["updated_at"] = datetime.now().isoformat()
            if error:
                self.states[key]["error"] = error
            self._save()

    def get_status(self, algo_name: str, version: str) -> dict | None:
        """Get build status for algorithm."""
        key = self.get_key(algo_name, version)
        return self.states.get(key)

    def clear_status(self, algo_name: str, version: str):
        """Clear build status for algorithm (useful for retrying after fixing errors)."""
        key = self.get_key(algo_name, version)
        if key in self.states:
            del self.states[key]
            self._save()

    def check_job_status(self, job_id: str) -> str:
        """
        Check Slurm job status.
        Returns: 'running', 'completed', 'failed', or 'unknown'
        """
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
            
            # Fallback: sacct unavailable or failed, check log files
            # This handles systems with accounting disabled
            return self._check_job_logs(job_id)

        # Job still in queue
        return "running"

    def _check_job_logs(self, job_id: str) -> str:
        """
        Fallback method to check job status from log files.
        Used when sacct is unavailable (accounting disabled).
        """
        runner_dir = Path(__file__).parent.parent
        logs_dir = runner_dir / "logs"
        
        # Find error log for this job
        error_logs = list(logs_dir.glob(f"build_*_{job_id}.err"))
        
        if not error_logs:
            return "unknown"
        
        error_log = error_logs[0]
        
        try:
            with open(error_log, "r") as f:
                content = f.read()
                
            # Check for failure indicators
            failure_indicators = [
                "FATAL:",
                "exit status 1",
                "✗ Container build failed",
                "✗ Transfer to Alexandria failed",
                "Error:",
                "FAILED",
            ]
            
            if any(indicator in content for indicator in failure_indicators):
                return "failed"
            
            # Check for success indicators
            success_indicators = [
                "Container Build Complete!",
                "✓ Container transferred to Alexandria",
            ]
            
            if any(indicator in content for indicator in success_indicators):
                return "completed"
                
        except Exception:
            pass
        
        return "unknown"

    def update_from_slurm(self, config: dict = None):
        """
        Update build states by checking Slurm for running jobs.
        If config provided, also verify container exists on Alexandria when job completes.
        """
        updates = []
        for key, state in self.states.items():
            if state["status"] == "building":
                job_id = state.get("job_id")
                if job_id:
                    job_status = self.check_job_status(job_id)
                    algo_name, version = key.split("@")

                    if job_status == "completed":
                        # Verify container exists on Alexandria if config provided
                        if config:
                            import subprocess

                            host = config["alexandria"]["host"]
                            containers_path = config["alexandria"]["containers_path"]
                            container_path = (
                                f"{containers_path}/{algo_name}/{version}/container.sif"
                            )

                            result = subprocess.run(
                                [
                                    "ssh",
                                    host,
                                    f'test -f {container_path} && echo "exists" || echo "missing"',
                                ],
                                capture_output=True,
                                text=True,
                            )

                            if result.stdout.strip() == "exists":
                                self.mark_completed(algo_name, version)
                                updates.append(f"✓ {algo_name} ({version}) build completed")
                            else:
                                self.mark_failed(
                                    algo_name,
                                    version,
                                    "Slurm job completed but container not found on Alexandria",
                                )
                                updates.append(
                                    f"✗ {algo_name} ({version}) build failed - container not found"
                                )
                        else:
                            self.mark_completed(algo_name, version)
                            updates.append(f"✓ {algo_name} ({version}) build completed")
                    elif job_status == "failed":
                        self.mark_failed(algo_name, version, "Slurm job failed")
                        updates.append(f"✗ {algo_name} ({version}) build failed")

        return updates
