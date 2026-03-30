"""Algorithm execution orchestration."""

from pathlib import Path

from .container_builder import get_runner_dir
from .display import print_info, print_step, print_success
from .job_waiter import wait_for_job_completion
from .remote_fs import remote_file_exists, rsync_pull


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


def pull_container_from_alexandria(config: dict, algo_name: str, version: str) -> bool:
    """Pull algorithm container from Alexandria to Asimov."""
    alexandria_host = config["alexandria"]["host"]
    containers_path = config["alexandria"]["containers_path"]

    runner_dir = get_runner_dir()
    local_container_file = runner_dir / "containers" / algo_name / version / "container.sif"

    # Check if already exists locally
    if local_container_file.exists():
        print_info(f"Container already on Asimov: {local_container_file}")
        return True

    # Check if exists on Alexandria
    alexandria_container = f"{containers_path}/{algo_name}/{version}/container.sif"
    if not remote_file_exists(alexandria_host, alexandria_container):
        print_info(f"Container not found on Alexandria: {alexandria_container}")
        return False

    # Pull container
    print_step(f"Pulling container for {algo_name} ({version})...")
    success = rsync_pull(alexandria_host, alexandria_container, local_container_file)

    if success:
        print_success(f"✓ Container pulled: {local_container_file}")
    else:
        print_info(f"✗ Failed to pull container for {algo_name}")

    return success


def pull_evaluation_container(config: dict) -> bool:
    """Pull evaluation container from Alexandria to Asimov benchmarks directory."""
    alexandria_host = config["alexandria"]["host"]
    containers_path = config["alexandria"]["containers_path"]

    runner_dir = get_runner_dir()
    local_container_file = runner_dir / "evaluation.sif"

    # Check if already exists locally
    if local_container_file.exists():
        print_info(f"Evaluation container already on Asimov: {local_container_file}")
        return True

    # Check if exists on Alexandria
    alexandria_container = f"{containers_path}/evaluation/evaluation.sif"
    if not remote_file_exists(alexandria_host, alexandria_container):
        print_info(f"Evaluation container not found on Alexandria: {alexandria_container}")
        return False

    # Pull evaluation container
    print_step("Pulling evaluation container...")
    success = rsync_pull(alexandria_host, alexandria_container, local_container_file)

    if success:
        print_success(f"✓ Evaluation container pulled: {local_container_file}")
    else:
        print_info("✗ Failed to pull evaluation container")

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

    job_script_dir = runner_dir / "slurm_jobs"
    job_script_dir.mkdir(exist_ok=True)
    job_file = job_script_dir / f"run_{algo_name}_{version}_{dataset}.slurm.sh"
    with open(job_file, "w") as f:
        f.write(job_script)

    # Submit job
    result = subprocess.run(["sbatch", str(job_file)], capture_output=True, text=True)

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
) -> tuple[bool, str | None]:
    """Submit algorithm run job and wait for completion.

    Returns:
        (success, job_id) tuple
    """
    slurm_resources = get_slurm_resources_for_run(config)

    # Submit job
    job_id = submit_run_job(config, algo_name, version, dataset, slurm_resources)

    if job_id is None:
        return False, None

    # Wait for completion
    print_info(f"Waiting for job {job_id} ({algo_name} on {dataset})...")
    success, status = wait_for_job_completion(
        job_id=str(job_id), job_name=f"{algo_name} on {dataset}", check_interval=60
    )

    if success:
        print_success(f"✓ Run completed: {algo_name} on {dataset}")
    else:
        print_info(f"✗ Run failed or timed out: {algo_name} on {dataset}")

    return success, str(job_id)


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
    evaluation_container = runner_dir / "evaluation.sif"

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
            print_info("✗ Failed to pull dataset")
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

        print_info("Cleaning up temporary dataset...")
        shutil.rmtree(dataset_dir)
        dm = DatasetManager()
        dm.mark_removed(dataset)

    if success:
        print_success(f"✓ Augmented {algo_name} + {dataset}")
    else:
        print_info(f"✗ Augmentation failed for {algo_name} + {dataset}")

    return success


def submit_evaluation_job(
    config: dict,
    dataset: str,
    temp_outputs_dir: Path | None = None,
    slurm_resources: dict | None = None,
) -> str | None:
    """Submit Slurm job to evaluate predictions for a dataset.

    Args:
        config: Configuration dict
        dataset: Dataset name
        temp_outputs_dir: Directory containing pulled outputs for evaluation
        slurm_resources: Optional Slurm resource overrides

    Returns:
        Job ID if successful, None otherwise
    """
    import subprocess

    runner_dir = get_runner_dir()
    benchmarks_dir = runner_dir / config["denovo_benchmarks"]["local_path"]

    template_path = runner_dir / "templates" / "evaluate_dataset.slurm.sh"

    with open(template_path, "r") as f:
        template = f.read()

    # Get Slurm resources from config or use defaults
    if slurm_resources is None:
        slurm = config.get("slurm", {})
        slurm_resources = {
            "partition": slurm.get("partition", "one_hour"),
            "cpus": slurm.get("cpus", 4),
            "memory": slurm.get("memory", "32G"),
            "time": slurm.get("time", "02:00:00"),
        }

    # Paths for evaluation
    # Use temp pulled outputs if provided, otherwise use local outputs
    if temp_outputs_dir is None:
        output_root_dir = benchmarks_dir / "outputs"
    else:
        output_root_dir = temp_outputs_dir

    datasets_dir = runner_dir / config["local_datasets"]["path"]
    dataset_dir = datasets_dir / dataset
    results_dir = benchmarks_dir / "results"
    evaluation_container = runner_dir / "evaluation.sif"

    # Format template
    job_script = template.format(
        DATASET=dataset,
        RUNNER_DIR=str(runner_dir),
        BENCHMARKS_DIR=str(benchmarks_dir),
        OUTPUT_ROOT_DIR=str(output_root_dir),
        DATASET_DIR=str(dataset_dir),
        RESULTS_DIR=str(results_dir),
        EVALUATION_CONTAINER=str(evaluation_container),
        TIME=slurm_resources["time"],
        CPUS=slurm_resources["cpus"],
        MEMORY=slurm_resources["memory"],
        PARTITION=slurm_resources["partition"],
    )

    # Write job script to slurm_jobs directory
    job_script_dir = runner_dir / "slurm_jobs"
    job_script_dir.mkdir(exist_ok=True)
    job_file = job_script_dir / f"evaluate_{dataset}.slurm.sh"
    with open(job_file, "w") as f:
        f.write(job_script)

    # Submit job
    result = subprocess.run(["sbatch", str(job_file)], capture_output=True, text=True)

    if result.returncode != 0:
        print_info(f"✗ Failed to submit evaluation job: {result.stderr}")
        return None

    # Extract job ID
    job_id_str = result.stdout.strip().split()[-1]
    try:
        job_id = int(job_id_str)
        print_info(f"Submitted evaluation job {job_id} for {dataset}")
        return str(job_id)
    except ValueError:
        print_info(f"Failed to parse job ID from: {result.stdout}")
        return None


def submit_and_wait_for_evaluation(
    config: dict,
    dataset: str,
    slurm_resources: dict | None = None,
) -> tuple[bool, str | None]:
    """Submit evaluation job for a dataset and wait for completion.

    Pulls:
    - Dataset (ground truth labels + spectra)
    - Algorithm outputs from Alexandria

    Then runs evaluation against ground truth.
    Cleans up temporary files after completion.

    Returns:
        (success, job_id) tuple
    """
    runner_dir = get_runner_dir()
    benchmarks_dir = runner_dir / config["denovo_benchmarks"]["local_path"]
    datasets_dir = runner_dir / config["local_datasets"]["path"]
    dataset_dir = datasets_dir / dataset

    # Ensure dataset exists locally for ground truth (labels, spectra)
    temp_dataset = False
    if not dataset_dir.exists():
        print_info(f"Dataset {dataset} not found locally, pulling for evaluation...")
        from .dataset_manager import DatasetManager, submit_and_wait_for_pull

        dataset_manager = DatasetManager()
        pulled = submit_and_wait_for_pull(config, dataset, dataset_manager)
        if not pulled:
            print_info(f"✗ Failed to pull dataset {dataset} for evaluation")
            return False, None
        temp_dataset = True

    # Create temp outputs directory for pulling Alexandria outputs
    temp_outputs_dir = benchmarks_dir / "outputs_eval_temp"
    temp_outputs_dir.mkdir(parents=True, exist_ok=True)

    print_info(f"Pulling algorithm outputs for {dataset} from Alexandria...")
    alexandria_host = config["alexandria"]["host"]
    alexandria_outputs_path = config["alexandria"]["outputs_path"]

    import shlex
    import subprocess

    find_cmd = (
        f"find {shlex.quote(alexandria_outputs_path)} "
        f"-mindepth 3 -maxdepth 3 -type d -name {shlex.quote(dataset)}"
    )
    find_result = subprocess.run(
        ["ssh", alexandria_host, find_cmd],
        capture_output=True,
        text=True,
    )

    if find_result.returncode != 0:
        print_info(f"✗ Failed to list outputs on Alexandria: {find_result.stderr.strip()}")
        import shutil

        shutil.rmtree(temp_outputs_dir, ignore_errors=True)
        if temp_dataset and dataset_dir.exists():
            print_info(f"Cleaning up temporary dataset {dataset}...")
            shutil.rmtree(dataset_dir)
            from .dataset_manager import DatasetManager

            DatasetManager().mark_removed(dataset)
        return False, None

    remote_dataset_dirs = [line.strip() for line in find_result.stdout.splitlines() if line.strip()]
    if not remote_dataset_dirs:
        print_info(f"✗ No outputs found on Alexandria for dataset {dataset}")
        import shutil

        shutil.rmtree(temp_outputs_dir, ignore_errors=True)
        if temp_dataset and dataset_dir.exists():
            print_info(f"Cleaning up temporary dataset {dataset}...")
            shutil.rmtree(dataset_dir)
            from .dataset_manager import DatasetManager

            DatasetManager().mark_removed(dataset)
        return False, None

    pulled_paths: list[Path] = []
    for remote_dataset_dir in remote_dataset_dirs:
        remote_path = Path(remote_dataset_dir)
        if len(remote_path.parts) < 3:
            continue
        algo_name = remote_path.parts[-3]
        algo_version = remote_path.parts[-2]
        local_dataset_dir = temp_outputs_dir / algo_name / algo_version / dataset
        local_dataset_dir.mkdir(parents=True, exist_ok=True)

        rsync_result = subprocess.run(
            [
                "rsync",
                "-az",
                "--delete",
                f"{alexandria_host}:{remote_dataset_dir}/",
                f"{local_dataset_dir}/",
            ],
            capture_output=True,
            text=True,
        )
        if rsync_result.returncode != 0:
            print_info(
                f"✗ Failed pulling outputs from {remote_dataset_dir}: {rsync_result.stderr.strip()}"
            )
            import shutil

            shutil.rmtree(temp_outputs_dir, ignore_errors=True)
            if temp_dataset and dataset_dir.exists():
                print_info(f"Cleaning up temporary dataset {dataset}...")
                shutil.rmtree(dataset_dir)
                from .dataset_manager import DatasetManager

                DatasetManager().mark_removed(dataset)
            return False, None
        pulled_paths.append(local_dataset_dir)

    pulled_output_files = list(temp_outputs_dir.glob(f"*/*/{dataset}/output.csv"))
    if not pulled_output_files:
        print_info(
            f"✗ Pulled {len(pulled_paths)} dataset directories but found no output.csv files "
            f"for {dataset}"
        )
        import shutil

        shutil.rmtree(temp_outputs_dir, ignore_errors=True)
        if temp_dataset and dataset_dir.exists():
            print_info(f"Cleaning up temporary dataset {dataset}...")
            shutil.rmtree(dataset_dir)
            from .dataset_manager import DatasetManager

            DatasetManager().mark_removed(dataset)
        return False, None

    print_success(f"✓ Pulled {len(pulled_output_files)} output.csv file(s) for {dataset}")

    job_id = submit_evaluation_job(config, dataset, temp_outputs_dir, slurm_resources)

    if job_id is None:
        import shutil

        shutil.rmtree(temp_outputs_dir, ignore_errors=True)
        if temp_dataset and dataset_dir.exists():
            print_info(f"Cleaning up temporary dataset {dataset}...")
            shutil.rmtree(dataset_dir)
            from .dataset_manager import DatasetManager

            DatasetManager().mark_removed(dataset)
        return False, None

    # Wait for completion
    print_info(f"Waiting for evaluation job {job_id} for {dataset}...")
    success, status = wait_for_job_completion(
        job_id=job_id,
        job_name=f"evaluation for {dataset}",
        check_interval=60,
        log_pattern=f"evaluate_{dataset}_{job_id}.out",
        success_marker="✓ Evaluation completed successfully!",
    )

    if success:
        print_success(f"✓ Evaluation completed for {dataset}")
    else:
        print_info(f"✗ Evaluation failed or timed out for {dataset}")

    # Cleanup temporary files
    import shutil

    print_info(f"Cleaning up temporary outputs for {dataset}...")
    shutil.rmtree(temp_outputs_dir, ignore_errors=True)

    if temp_dataset and dataset_dir.exists():
        print_info(f"Cleaning up temporary dataset {dataset}...")
        shutil.rmtree(dataset_dir)
        from .dataset_manager import DatasetManager

        DatasetManager().mark_removed(dataset)

    return success, job_id
