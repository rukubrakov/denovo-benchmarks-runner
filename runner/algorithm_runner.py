"""Algorithm execution orchestration."""

import time
from pathlib import Path

from .container_builder import get_runner_dir
from .display import print_info, print_step, print_success
from .job_waiter import wait_for_job_completion


def get_slurm_resources_for_run(config: dict) -> dict:
    """Get Slurm resources for algorithm runs."""
    slurm = config.get("slurm", {})
    return {
        "partition": slurm.get("partition", "one_hour"),
        "cpus": slurm.get("cpus", 4),
        "memory": slurm.get("memory", "16G"),
        "gpus": slurm.get("gpus", 1),
        "time": slurm.get("time", "01:00:00"),
    }


def pull_container_from_alexandria(
    config: dict, algo_name: str, version: str
) -> bool:
    """Pull algorithm container from Alexandria to Asimov."""
    import subprocess

    alexandria_host = config["alexandria"]["host"]
    containers_path = config["alexandria"]["containers_path"]

    runner_dir = get_runner_dir()
    local_container_dir = runner_dir / "containers" / algo_name / version
    local_container_file = local_container_dir / "container.sif"

    # Check if already exists locally
    if local_container_file.exists():
        print_info(f"Container already on Asimov: {local_container_file}")
        return True

    # Prepare directories
    local_container_dir.mkdir(parents=True, exist_ok=True)

    # Check if exists on Alexandria
    alexandria_container = f"{containers_path}/{algo_name}/{version}/container.sif"
    check_cmd = f'ssh {alexandria_host} "[ -f {alexandria_container} ] && echo exists"'

    result = subprocess.run(
        check_cmd, shell=True, capture_output=True, text=True
    )

    if "exists" not in result.stdout:
        print_info(f"Container not found on Alexandria: {alexandria_container}")
        return False

    # Pull container
    print_step(f"Pulling container for {algo_name} ({version})...")
    rsync_cmd = [
        "rsync",
        "-avz",
        "--progress",
        f"{alexandria_host}:{alexandria_container}",
        str(local_container_file),
    ]

    result = subprocess.run(rsync_cmd)
    success = result.returncode == 0

    if success:
        print_success(f"✓ Container pulled: {local_container_file}")
    else:
        print_info(f"✗ Failed to pull container for {algo_name}")

    return success


def pull_evaluation_container(config: dict) -> bool:
    """Pull evaluation container from Alexandria to Asimov benchmarks directory."""
    import subprocess

    alexandria_host = config["alexandria"]["host"]
    containers_path = config["alexandria"]["containers_path"]

    runner_dir = get_runner_dir()
    benchmarks_dir = runner_dir / "denovo_benchmarks"
    local_container_file = benchmarks_dir / "evaluation.sif"

    # Check if already exists locally
    if local_container_file.exists():
        print_info(f"Evaluation container already on Asimov: {local_container_file}")
        return True

    # Check if exists on Alexandria
    alexandria_container = f"{containers_path}/evaluation/evaluation.sif"
    check_cmd = f'ssh {alexandria_host} "[ -f {alexandria_container} ] && echo exists"'

    result = subprocess.run(
        check_cmd, shell=True, capture_output=True, text=True
    )

    if "exists" not in result.stdout:
        print_info(f"Evaluation container not found on Alexandria: {alexandria_container}")
        return False

    # Pull evaluation container
    print_step(f"Pulling evaluation container...")
    rsync_cmd = [
        "rsync",
        "-avz",
        "--progress",
        f"{alexandria_host}:{alexandria_container}",
        str(local_container_file),
    ]

    result = subprocess.run(rsync_cmd)
    success = result.returncode == 0

    if success:
        print_success(f"✓ Evaluation container pulled: {local_container_file}")
    else:
        print_info(f"✗ Failed to pull evaluation container")

    return success


def submit_run_job(
    config: dict,
    algo_name: str,
    version: str,
    dataset: str,
    slurm_resources: dict,
) -> int | None:
    """Submit Slurm job to run algorithm on dataset."""
    import subprocess

    runner_dir = get_runner_dir()
    benchmarks_dir = runner_dir / config["denovo_benchmarks"]["local_path"]

    template_path = runner_dir / "templates" / "run_algorithm.slurm.sh"

    with open(template_path, "r") as f:
        template = f.read()

    # Format template
    job_script = template.format(
        ALGO_NAME=algo_name,
        VERSION=version,
        DATASET=dataset,
        RUNNER_DIR=str(runner_dir),
        BENCHMARKS_DIR=str(benchmarks_dir),
        ALEXANDRIA_HOST=config["alexandria"]["host"],
        ALEXANDRIA_OUTPUTS_PATH=config["alexandria"]["outputs_path"],
        TIME=slurm_resources["time"],
        CPUS=slurm_resources["cpus"],
        MEMORY=slurm_resources["memory"],
        GPUS=slurm_resources["gpus"],
        PARTITION=slurm_resources["partition"],
    )

    # Write job script to temp file
    job_file = runner_dir / f"run_{algo_name}_{dataset}.slurm.sh"
    with open(job_file, "w") as f:
        f.write(job_script)

    # Submit job
    result = subprocess.run(
        ["sbatch", str(job_file)], capture_output=True, text=True
    )

    if result.returncode != 0:
        print_info(f"Failed to submit job: {result.stderr}")
        return None

    # Extract job ID
    # Output format: "Submitted batch job 12345"
    job_id_str = result.stdout.strip().split()[-1]
    try:
        job_id = int(job_id_str)
        print_info(f"Submitted job {job_id} for {algo_name} on {dataset}")
        return job_id
    except ValueError:
        print_info(f"Failed to parse job ID from: {result.stdout}")
        return None


def submit_and_wait_for_run(
    config: dict, algo_name: str, version: str, dataset: str, timeout_minutes: int = 120
) -> bool:
    """Submit algorithm run job and wait for completion."""
    slurm_resources = get_slurm_resources_for_run(config)

    # Submit job
    job_id = submit_run_job(config, algo_name, version, dataset, slurm_resources)

    if job_id is None:
        return False

    # Wait for completion
    print_info(f"Waiting for job {job_id} ({algo_name} on {dataset})...")
    success, status = wait_for_job_completion(
        job_id=str(job_id),
        job_name=f"{algo_name} on {dataset}",
        check_interval=60
    )

    if success:
        print_success(f"✓ Run completed: {algo_name} on {dataset}")
    else:
        print_info(f"✗ Run failed or timed out: {algo_name} on {dataset}")

    return success


def check_output_exists_on_alexandria(
    config: dict, algo_name: str, version: str, dataset: str
) -> bool:
    """Check if output already exists on Alexandria."""
    import subprocess

    alexandria_host = config["alexandria"]["host"]
    outputs_path = config["alexandria"]["outputs_path"]

    output_file = f"{outputs_path}/{algo_name}/{version}/{dataset}/output.csv"
    check_cmd = f'ssh {alexandria_host} "[ -f {output_file} ] && echo exists"'

    result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)

    return "exists" in result.stdout


def cleanup_local_container(algo_name: str, version: str) -> None:
    """Remove algorithm container from local storage."""
    runner_dir = get_runner_dir()
    container_file = runner_dir / "containers" / algo_name / version / "container.sif"

    if container_file.exists():
        container_file.unlink()
        print_info(f"Removed container: {container_file}")

        # Remove empty directories
        if container_file.parent.exists():
            try:
                container_file.parent.rmdir()
                if not any(container_file.parent.parent.iterdir()):
                    container_file.parent.parent.rmdir()
            except OSError:
                pass  # Directory not empty, that's fine


def augment_existing_output(config: dict, algo_name: str, version: str, dataset: str) -> bool:
    """
    Augment an existing output.csv with RT and SA predictions via Slurm job.
    Returns True if successful, False otherwise.
    """
    import subprocess
    
    runner_dir = get_runner_dir()
    benchmarks_dir = runner_dir / config["denovo_benchmarks"]["local_path"]
    evaluation_container = benchmarks_dir / "evaluation.sif"
    
    # Check if evaluation container exists
    if not evaluation_container.exists():
        print_info(f"Evaluation container not found: {evaluation_container}")
        return False
    
    # Check if dataset exists locally, if not pull it
    dataset_dir = runner_dir / "datasets" / dataset
    temp_dataset = False
    
    if not dataset_dir.exists():
        print_info(f"Pulling dataset {dataset}...")
        from .dataset_manager import DatasetManager, submit_and_wait_for_pull
        dm = DatasetManager()
        success = submit_and_wait_for_pull(config, dataset, dm)
        if not success:
            print_info(f"✗ Failed to pull dataset")
            return False
        temp_dataset = True
    
    # Read and fill augmentation template
    template_path = runner_dir / "templates" / "augment_output.slurm.sh"
    with open(template_path) as f:
        template = f.read()
    
    # Fill template
    job_script = template.format(
        ALGO_NAME=algo_name,
        VERSION=version,
        DATASET=dataset,
        RUNNER_DIR=str(runner_dir),
        BENCHMARKS_DIR=str(benchmarks_dir),
        ALEXANDRIA_HOST=config["alexandria"]["host"],
        ALEXANDRIA_OUTPUTS_PATH=config["alexandria"]["outputs_path"],
    )
    
    # Write job script
    job_script_path = runner_dir / "slurm_jobs" / f"augment_{algo_name}_{version}_{dataset}.sh"
    job_script_path.parent.mkdir(parents=True, exist_ok=True)
    with open(job_script_path, "w") as f:
        f.write(job_script)
    
    # Submit job
    print_step(f"Submitting augmentation job for {algo_name} + {dataset}...")
    result = subprocess.run(
        ["sbatch", str(job_script_path)],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        print_info(f"✗ Failed to submit job: {result.stderr}")
        return False
    
    # Extract job ID
    job_id = result.stdout.strip().split()[-1]
    print_info(f"Submitted job {job_id}")
    
    # Wait for completion
    from .job_waiter import wait_for_job_completion
    success, status = wait_for_job_completion(
        job_id=job_id,
        job_name=f"augment {algo_name} + {dataset}",
        check_interval=30,
        log_pattern=f"augment_{algo_name}_{dataset}_{job_id}.out",
        success_marker="Complete! Augmented:",
    )
    
    # Cleanup temp dataset if we pulled it
    if temp_dataset and dataset_dir.exists():
        import shutil
        from .dataset_manager import DatasetManager
        print_info(f"Cleaning up temporary dataset...")
        shutil.rmtree(dataset_dir)
        dm = DatasetManager()
        dm.mark_removed(dataset)
    
    if success:
        print_success(f"✓ Augmented {algo_name} + {dataset}")
    else:
        print_info(f"✗ Augmentation failed for {algo_name} + {dataset}")
    
    return success
