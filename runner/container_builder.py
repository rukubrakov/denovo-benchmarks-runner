"""Container building operations."""

import subprocess
from pathlib import Path

from .build_state import BuildState
from .display import print_error, print_header, print_info, print_step, print_success, print_warning


def get_container_def_path(
    config: dict, algo_name: str, version: str, benchmarks_dir: Path
) -> Path:
    """
    Get path to container.def, checking for overrides first.
    Returns path to override if exists, otherwise original.
    Special handling for evaluation container.
    """
    # Special case: evaluation container
    if algo_name == "evaluation":
        return benchmarks_dir / "evaluation.def"

    runner_dir = Path(__file__).parent.parent
    override_path = runner_dir / "container_overrides" / algo_name / version / "container.def"

    if override_path.exists():
        return override_path

    # Fall back to original from denovo_benchmarks
    original_path = benchmarks_dir / "algorithms" / algo_name / "container.def"
    return original_path


def submit_build_job(
    config: dict,
    algo_name: str,
    version: str,
    build_state: BuildState,
    slurm_resources: dict | None = None,
) -> str | None:
    """
    Submit a Slurm job to build a container.
    Returns job ID if successful, None otherwise.
    """
    runner_dir = Path(__file__).parent.parent
    benchmarks_dir = runner_dir / config["denovo_benchmarks"]["local_path"]

    # Get container def path (check for override)
    container_def = get_container_def_path(config, algo_name, version, benchmarks_dir)

    if not container_def.exists():
        print_error(f"Container definition not found: {container_def}")
        return None

    # Prepare job script
    template_path = runner_dir / "templates" / "build_container.slurm.sh"
    job_script_dir = runner_dir / "slurm_jobs"
    job_script_dir.mkdir(exist_ok=True)

    job_script_path = job_script_dir / f"build_{algo_name}_{version}.sh"

    # Get Slurm resources from config or use defaults
    if slurm_resources is None:
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

    job_script = template.format(
        ALGO_NAME=algo_name,
        VERSION=version,
        RUNNER_DIR=str(runner_dir),
        BENCHMARKS_DIR=str(benchmarks_dir),
        CONTAINER_DEF=str(container_def),
        ALEXANDRIA_HOST=config["alexandria"]["host"],
        ALEXANDRIA_PATH=config["alexandria"]["containers_path"],
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
            print_success(f"Build job submitted: {job_id}")

            # Mark as building in state
            build_state.mark_building(algo_name, version, job_id)

            return job_id
    else:
        print_error(f"Failed to submit build job: {result.stderr}")
        return None


def check_and_display_builds(
    config: dict, algorithms: list[dict[str, str]], container_status: dict[str, bool]
):
    """
    Check build status and display information.
    Submit builds for missing containers if not already building.
    """
    print_header("Container Build Status")

    build_state = BuildState()

    # Update states from Slurm
    print_step("Checking ongoing builds...")
    updates = build_state.update_from_slurm(config)

    # Display any status updates
    if updates:
        print()
        for update in updates:
            print(f"  {update}")
        print()

    # Reconcile build state with Alexandria reality
    # If container exists but state says building/failed, mark as completed
    for algo in algorithms:
        algo_name = algo["name"]
        version = algo["version"]
        container_exists = container_status.get(algo_name, False)
        build_status = build_state.get_status(algo_name, version)

        if container_exists and build_status and build_status["status"] != "completed":
            print_step(
                f"Reconciling: {algo_name} ({version}) exists, state was '{build_status['status']}'"
            )
            build_state.mark_completed(algo_name, version)

    # Check each algorithm
    needs_building = []
    currently_building = []
    has_errors = []

    for algo in algorithms:
        algo_name = algo["name"]
        version = algo["version"]

        # Skip if container exists on Alexandria
        if container_status.get(algo_name, False):
            continue

        # Check build state
        status = build_state.get_status(algo_name, version)

        if status is None:
            # Never built, needs building
            needs_building.append((algo_name, version))
        elif status["status"] == "building":
            # Currently building
            currently_building.append((algo_name, version, status["job_id"]))
        elif status["status"] == "failed":
            # Previous build failed
            has_errors.append((algo_name, version, status.get("error", "Unknown error")))
            needs_building.append((algo_name, version))
        elif status["status"] == "completed":
            # Build completed but container not found on Alexandria
            print_warning(
                f"{algo_name} ({version}): Build completed but container missing on Alexandria"
            )

    # Display status
    if currently_building:
        print_info(f"{len(currently_building)} container(s) currently building:")
        for algo_name, version, job_id in currently_building:
            print(f"    🔨 {algo_name} ({version}) - Job: {job_id}")
        print()

    if has_errors:
        print_warning(f"{len(has_errors)} container(s) with previous build errors:")
        for algo_name, version, error in has_errors:
            print(f"    ❌ {algo_name} ({version}): {error}")
        print()

    if needs_building:
        print_step(f"{len(needs_building)} container(s) need to be built")

        # Check for overrides
        runner_dir = Path(__file__).parent.parent
        for algo_name, version in needs_building:
            override_path = (
                runner_dir / "container_overrides" / algo_name / version / "container.def"
            )
            if override_path.exists():
                print(f"    📝 {algo_name} ({version}) [has override]")
            else:
                print(f"    📦 {algo_name} ({version})")
        print()
    else:
        print_success("All containers are built or building!")

    return needs_building, build_state


def check_and_build_evaluation_container(config: dict, evaluation_exists: bool) -> bool:
    """
    Check and build evaluation container if needed.
    Returns True if evaluation container needs building, False otherwise.
    """
    print_header("Evaluation Container Status")

    build_state = BuildState()

    # Update state from Slurm
    print_step("Checking evaluation container status...")
    updates = build_state.update_from_slurm(config)

    if updates:
        print()
        for update in updates:
            if "evaluation" in update.lower():
                print(f"  {update}")
        print()

    # Reconcile with Alexandria reality
    if evaluation_exists:
        eval_status = build_state.get_status("evaluation", "evaluation")
        if eval_status and eval_status["status"] != "completed":
            print_step(
                f"Reconciling: evaluation container exists but state was '{eval_status['status']}'"
            )
            build_state.mark_completed("evaluation", "evaluation")
        print_success("✓ Evaluation container exists on Alexandria")
        return False

    # Check build state
    status = build_state.get_status("evaluation", "evaluation")

    if status is None:
        # Never built
        print_step("Evaluation container needs to be built")
        return True
    elif status["status"] == "building":
        # Currently building
        print_info(f"Evaluation container is building (Job: {status['job_id']})")
        return False
    elif status["status"] == "failed":
        # Previous build failed
        print_warning(f"Previous evaluation build failed: {status.get('error', 'Unknown error')}")
        print_step("Will retry building evaluation container")
        return True
    elif status["status"] == "completed":
        # Build completed but container missing
        print_warning("Evaluation build completed but container missing on Alexandria")
        return True

    return False


def submit_evaluation_build(config: dict, build_state: BuildState) -> str | None:
    """
    Submit evaluation container build job with minimal resources.
    Returns job ID if successful, None otherwise.
    """
    # Lower resources for evaluation (CPU-only, no GPU needed)
    eval_slurm_resources = {
        "partition": "one_hour",
        "cpus": 4,
        "memory": "8G",
        "time": "01:00:00",
    }

    print_step("Submitting evaluation container build...")
    job_id = submit_build_job(config, "evaluation", "evaluation", build_state, eval_slurm_resources)

    return job_id
