"""Dataset management for Asimov with size and count limits."""

import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from .container_builder import get_runner_dir


class DatasetManager:
    """Manage datasets on Asimov with size and count constraints."""

    def __init__(self, state_file: Path = None):
        if state_file is None:
            state_file = get_runner_dir() / "dataset_state.json"
        self.state_file = state_file
        self.states = self._load()

    def _load(self) -> dict:
        """Load dataset states from JSON file."""
        if not self.state_file.exists():
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)

    def _save(self):
        """Save dataset states to JSON file."""
        with open(self.state_file, "w") as f:
            json.dump(self.states, f, indent=2)

    def mark_pulling(self, dataset_name: str, job_id: str, size_bytes: int):
        """Mark dataset as being pulled."""
        self.states[dataset_name] = {
            "status": "pulling",
            "job_id": job_id,
            "size_bytes": size_bytes,
            "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        self._save()

    def mark_available(self, dataset_name: str):
        """Mark dataset as available on Asimov."""
        if dataset_name in self.states:
            self.states[dataset_name]["status"] = "available"
            self.states[dataset_name]["updated_at"] = datetime.now().isoformat()
            self._save()

    def mark_in_use(self, dataset_name: str, job_ids: list[str]):
        """Mark dataset as in use by jobs."""
        if dataset_name in self.states:
            self.states[dataset_name]["status"] = "in_use"
            self.states[dataset_name]["using_jobs"] = job_ids
            self.states[dataset_name]["updated_at"] = datetime.now().isoformat()
            self._save()

    def mark_removed(self, dataset_name: str):
        """Mark dataset as removed from Asimov."""
        if dataset_name in self.states:
            del self.states[dataset_name]
            self._save()

    def cleanup_dataset(self, dataset_name: str) -> bool:
        """Remove dataset from Asimov and update state."""
        from .display import print_info, print_success

        datasets_dir = get_runner_dir() / "datasets"
        dataset_path = datasets_dir / dataset_name

        if dataset_path.exists():
            print_info(f"Removing dataset from Asimov: {dataset_name}")
            shutil.rmtree(dataset_path)
            self.mark_removed(dataset_name)
            print_success(f"✓ Cleaned up dataset: {dataset_name}")
            return True
        else:
            print_info(f"Dataset not found on Asimov: {dataset_name}")
            self.mark_removed(dataset_name)
            return False

    def get_status(self, dataset_name: str) -> dict | None:
        """Get status of dataset."""
        return self.states.get(dataset_name)

    def get_available_datasets(self) -> list[str]:
        """Get list of datasets currently available on Asimov."""
        return [name for name, state in self.states.items() if state["status"] == "available"]

    def get_in_use_datasets(self) -> list[str]:
        """Get list of datasets currently in use."""
        return [name for name, state in self.states.items() if state["status"] == "in_use"]

    def get_total_size(self) -> int:
        """Get total size of datasets on Asimov (bytes)."""
        return sum(
            state.get("size_bytes", 0)
            for state in self.states.values()
            if state["status"] in ["available", "in_use", "pulling"]
        )

    def get_dataset_count(self) -> int:
        """Get number of datasets on Asimov."""
        return len(
            [
                s
                for s in self.states.values()
                if s["status"] in ["available", "in_use", "pulling"]
            ]
        )

    def can_fit_dataset(self, size_bytes: int, max_size_bytes: int, max_count: int) -> bool:
        """Check if dataset can fit within size and count limits."""
        current_size = self.get_total_size()
        current_count = self.get_dataset_count()

        fits_size = (current_size + size_bytes) <= max_size_bytes
        fits_count = (current_count + 1) <= max_count

        return fits_size and fits_count

    def get_removable_datasets(self) -> list[str]:
        """Get datasets that can be removed (not in use)."""
        return [name for name, state in self.states.items() if state["status"] == "available"]

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
            
            # Fallback: check log files if sacct unavailable
            return self._check_job_logs(job_id)

        # Job still in queue
        return "running"

    def _check_job_logs(self, job_id: str) -> str:
        """
        Fallback method to check job status from log files.
        Used when sacct is unavailable.
        """
        # Look for log files in logs directory
        runner_dir = get_runner_dir()
        log_dir = runner_dir / "logs"
        
        if not log_dir.exists():
            return "unknown"
        
        # Find error log for this job
        error_logs = list(log_dir.glob(f"pull_*_{job_id}.err"))
        if error_logs:
            with open(error_logs[0], "r") as f:
                content = f.read()
                if "FATAL:" in content or "exit status 1" in content or "error:" in content.lower():
                    return "failed"
        
        # Find output log for this job
        output_logs = list(log_dir.glob(f"pull_*_{job_id}.out"))
        if output_logs:
            with open(output_logs[0], "r") as f:
                content = f.read()
                if "Dataset Pull Complete!" in content:
                    return "completed"
                elif "error" in content.lower() or "failed" in content.lower():
                    return "failed"
        
        return "unknown"

    def update_pulling_status(self, datasets_dir: Path):
        """Check pulling jobs and update status to available when completed."""
        pulling = [
            (name, state)
            for name, state in self.states.items()
            if state["status"] == "pulling"
        ]
        
        for dataset_name, state in pulling:
            job_id = state.get("job_id")
            if not job_id:
                continue
            
            job_status = self.check_job_status(job_id)
            
            if job_status == "completed":
                # Verify dataset actually exists on disk
                if check_dataset_on_asimov(datasets_dir, dataset_name):
                    self.mark_available(dataset_name)
                else:
                    self.mark_removed(dataset_name)
            elif job_status == "failed":
                # Remove failed pull from state
                self.mark_removed(dataset_name)


def get_dataset_size_on_alexandria(config: dict, dataset_name: str) -> int:
    """Get size of dataset on Alexandria in bytes."""
    host = config["alexandria"]["host"]
    datasets_path = config["alexandria"]["datasets_path"]
    dataset_path = f"{datasets_path}/{dataset_name}"

    result = subprocess.run(
        ["ssh", host, f"du -sb {dataset_path} 2>/dev/null || echo '0'"],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0 and result.stdout.strip():
        size_str = result.stdout.strip().split()[0]
        return int(size_str)
    return 0


def check_dataset_on_asimov(datasets_dir: Path, dataset_name: str) -> bool:
    """Check if dataset exists on Asimov."""
    dataset_path = datasets_dir / dataset_name
    return dataset_path.exists() and dataset_path.is_dir()


def remove_dataset_from_asimov(datasets_dir: Path, dataset_name: str) -> bool:
    """Remove dataset from Asimov."""
    dataset_path = datasets_dir / dataset_name
    if dataset_path.exists():
        shutil.rmtree(dataset_path)
        return True
    return False


def get_available_space(path: Path) -> int:
    """Get available disk space at path in bytes."""
    stat = shutil.disk_usage(path)
    return stat.free


def submit_pull_job(
    config: dict, dataset_name: str, dataset_manager: DatasetManager
) -> str | None:
    """
    Submit a Slurm job to pull dataset from Alexandria.
    Returns job ID if successful, None otherwise.
    """
    from .display import print_error, print_success

    runner_dir = get_runner_dir()

    # Get dataset size on Alexandria
    size_bytes = get_dataset_size_on_alexandria(config, dataset_name)
    if size_bytes == 0:
        print_error(f"Dataset {dataset_name} not found on Alexandria or empty")
        return None

    # Check if we can fit this dataset
    max_size_gb = config["local_datasets"]["max_size_gb"]
    max_count = config["local_datasets"]["max_count"]
    max_size_bytes = max_size_gb * 1024 * 1024 * 1024

    if not dataset_manager.can_fit_dataset(size_bytes, max_size_bytes, max_count):
        print_error(
            f"Cannot fit dataset {dataset_name} ({size_bytes / (1024**3):.2f} GB) "
            f"within limits ({max_size_gb} GB, {max_count} datasets)"
        )
        return None

    # Prepare job script
    template_path = runner_dir / "templates" / "pull_dataset.slurm.sh"
    job_script_dir = runner_dir / "slurm_jobs"
    job_script_dir.mkdir(exist_ok=True)

    job_script_path = job_script_dir / f"pull_{dataset_name}.sh"

    # Get Slurm resources from config or use defaults
    slurm_resources = config.get("slurm", {})
    resources = {
        "PARTITION": slurm_resources.get("partition", "one_hour"),
        "CPUS": str(slurm_resources.get("cpus", 4)),
        "MEMORY": slurm_resources.get("memory", "16G"),
        "TIME": slurm_resources.get("time", "01:00:00"),
    }

    # Read template and substitute variables
    with open(template_path, "r") as f:
        template = f.read()

    datasets_dir = runner_dir / config["local_datasets"]["path"]
    datasets_dir.mkdir(exist_ok=True)

    job_script = template.format(
        RUNNER_DIR=str(runner_dir),
        DATASET_NAME=dataset_name,
        DATASETS_DIR=str(datasets_dir),
        ALEXANDRIA_HOST=config["alexandria"]["host"],
        ALEXANDRIA_PATH=config["alexandria"]["datasets_path"],
        **resources,
    )

    # Write job script
    with open(job_script_path, "w") as f:
        f.write(job_script)

    # Submit job
    result = subprocess.run(["sbatch", str(job_script_path)], capture_output=True, text=True)

    if result.returncode == 0:
        # Parse job ID from sbatch output
        output = result.stdout.strip()
        if "Submitted batch job" in output:
            job_id = output.split()[-1]
            print_success(f"Dataset pull job submitted: {job_id}")

            # Mark as pulling in state
            dataset_manager.mark_pulling(dataset_name, job_id, size_bytes)

            return job_id
    else:
        error_msg = result.stderr.strip()
        if "Unable to contact slurm controller" in error_msg:
            print_error("Slurm controller is unreachable - cluster may be down")
            print_error("Please check cluster status or contact administrators")
        else:
            print_error(f"Failed to submit pull job: {error_msg}")
        return None


def submit_and_wait_for_pull(
    config: dict, dataset_name: str, dataset_manager: DatasetManager
) -> bool:
    """
    Submit a dataset pull job and wait for it to complete.
    Returns True if successful, False otherwise.
    """
    from .job_waiter import wait_for_job_completion

    job_id = submit_pull_job(config, dataset_name, dataset_manager)
    
    if not job_id:
        return False
    
    # Wait for completion
    runner_dir = get_runner_dir()
    datasets_dir = runner_dir / config["local_datasets"]["path"]
    
    success, status = wait_for_job_completion(
        job_id=job_id,
        job_name=f"dataset {dataset_name} pull",
        check_interval=60,
        log_pattern=f"pull_{dataset_name}_{job_id}.out",
        success_marker="Dataset Pull Complete!",
    )
    
    if success:
        # Verify dataset exists on disk
        if check_dataset_on_asimov(datasets_dir, dataset_name):
            dataset_manager.mark_available(dataset_name)
            return True
        else:
            from .display import print_error
            print_error(f"Pull completed but dataset not found on Asimov")
            dataset_manager.mark_removed(dataset_name)
            return False
    else:
        dataset_manager.mark_removed(dataset_name)
        return False


def check_and_pull_datasets(config: dict, datasets: list[str]):
    """
    Check dataset status and submit pull jobs for missing datasets.
    Also cleanup datasets that are no longer needed.
    """
    from .display import print_header, print_info, print_success, print_warning

    print_header("Dataset Management")

    dataset_manager = DatasetManager()
    runner_dir = get_runner_dir()
    datasets_dir = runner_dir / config["local_datasets"]["path"]

    # Update status of pulling jobs (pulling -> available/failed)
    dataset_manager.update_pulling_status(datasets_dir)

    # Check what datasets we need
    available_datasets = dataset_manager.get_available_datasets()
    pulling_datasets = [
        name
        for name, state in dataset_manager.states.items()
        if state["status"] == "pulling"
    ]

    # Report current status
    total_size = dataset_manager.get_total_size()
    total_count = dataset_manager.get_dataset_count()
    max_size_gb = config["local_datasets"]["max_size_gb"]
    max_count = config["local_datasets"]["max_count"]

    print_info(
        f"Dataset storage: {total_size / (1024**3):.2f} GB / {max_size_gb} GB "
        f"({total_count} / {max_count} datasets)"
    )

    if available_datasets:
        print_success(f"Available datasets ({len(available_datasets)}): {', '.join(available_datasets)}")

    if pulling_datasets:
        print_info(f"Pulling datasets ({len(pulling_datasets)}): {', '.join(pulling_datasets)}")

    # Check for datasets that need pulling
    missing_datasets = []
    for dataset in datasets:
        if dataset not in available_datasets and dataset not in pulling_datasets:
            missing_datasets.append(dataset)

    if not missing_datasets:
        print_success("All required datasets are available or being pulled.")
    else:
        print_warning(f"Missing datasets ({len(missing_datasets)}): {', '.join(missing_datasets)}")

        # Try to submit pull jobs for missing datasets
        for dataset in missing_datasets:
            print_info(f"Submitting pull job for {dataset}...")
            job_id = submit_pull_job(config, dataset, dataset_manager)
            if job_id:
                print_success(f"  → Job {job_id} submitted")
            else:
                print_warning(f"  → Could not submit pull job")

    # Cleanup: remove datasets that are no longer needed
    removable = dataset_manager.get_removable_datasets()
    unneeded = [d for d in removable if d not in datasets]
    
    if unneeded:
        print_info(f"Cleaning up unneeded datasets ({len(unneeded)}): {', '.join(unneeded)}")
        for dataset in unneeded:
            if remove_dataset_from_asimov(datasets_dir, dataset):
                dataset_manager.mark_removed(dataset)
                print_success(f"  → Removed {dataset}")
            else:
                print_warning(f"  → Failed to remove {dataset}")
