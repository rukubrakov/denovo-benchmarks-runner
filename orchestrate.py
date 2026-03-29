#!/usr/bin/env python3
"""
Denovo Benchmarks Orchestration System
Prefect-powered workflow for managing benchmark runs.
"""

import sys
from pathlib import Path

import yaml
from prefect import flow, task

from runner import (
    BuildState,
    DatasetManager,
    check_and_display_builds,
    check_containers,
    check_evaluation_container,
    check_or_clone_repo,
    check_output_exists_on_alexandria,
    check_outputs,
    cleanup_local_container,
    display_algorithms,
    get_algorithms,
    get_outputs_needing_augmentation,
    print_banner,
    print_header,
    print_info,
    print_step,
    print_success,
    pull_container_from_alexandria,
    submit_and_wait_for_build,
    submit_and_wait_for_evaluation,
    submit_and_wait_for_pull,
    submit_and_wait_for_run,
)


@task(name="Load Configuration")
def load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@task(name="Analyze Missing Combinations")
def analyze_missing(
    config: dict,
    algorithms: list[dict[str, str]],
    existing_outputs: set[tuple[str, str]],
    container_status: dict[str, bool],
) -> tuple[list[tuple[str, str]], set[str]]:
    """
    Analyze and report what's missing.
    Returns: (missing_with_container, needed_datasets)
    """
    print_header("Missing Combinations Analysis")

    datasets = config["datasets"]

    # Calculate all possible combinations
    all_combinations = set()
    for algo in algorithms:
        for dataset in datasets:
            all_combinations.add((algo["name"], dataset))

    # Find missing
    missing = all_combinations - existing_outputs

    if not missing:
        print_success("All combinations have outputs! Nothing to process.")
        return [], set()

    print_info(f"Total possible combinations: {len(all_combinations)}")
    print_info(f"Existing outputs: {len(existing_outputs)}")
    print_info(f"Missing outputs: {len(missing)}")

    # Group by container status
    missing_with_container = []
    missing_without_container = []

    for algo_name, dataset in sorted(missing):
        if container_status.get(algo_name, False):
            missing_with_container.append((algo_name, dataset))
        else:
            missing_without_container.append((algo_name, dataset))

    if missing_with_container:
        print_step("Ready to run (container exists):")
        for algo, dataset in missing_with_container:
            print(f"    ✓ {algo} + {dataset}")

    if missing_without_container:
        print_step("Need container first (container missing):")
        for algo, dataset in missing_without_container:
            print(f"    ⚠ {algo} + {dataset}")

    # Extract needed datasets (only for missing combinations)
    needed_datasets = set(dataset for _, dataset in missing)

    # Summary
    print_header("Summary")
    print(f"  • Algorithms found: {len(algorithms)}")
    print(f"  • Datasets configured: {len(datasets)}")
    print(f"  • Total combinations: {len(all_combinations)}")
    print(f"  • Completed: {len(existing_outputs)}")
    print(f"  • Missing: {len(missing)}")
    print(f"    - Ready to run: {len(missing_with_container)}")
    print(f"    - Need container: {len(missing_without_container)}")

    return missing_with_container, needed_datasets


def read_error_log(log_pattern: str, lines: int = 20) -> str:
    log_dir = Path(__file__).parent / "logs"
    error_logs = list(log_dir.glob(log_pattern))
    if not error_logs:
        return ""
    with open(error_logs[0], "r") as f:
        all_lines = f.readlines()
        excerpt = "".join(all_lines[-lines:])
        return excerpt


@task(task_run_name="Build Container: {algo_name}")
def build_single_container(
    config: dict, algo_name: str, version: str, build_state: BuildState
) -> bool:
    """Build a single container and wait for completion."""
    print_info(f"Building {algo_name} ({version})...")
    success, job_id = submit_and_wait_for_build(config, algo_name, version, build_state)
    if success:
        print_success(f"✓ {algo_name} ({version}) built successfully")
        return True
    else:
        error_msg = f"Failed to build {algo_name} ({version})"
        if job_id:
            error_msg += f"\n\nSlurm Job ID: {job_id}"
            log_excerpt = read_error_log(f"build_{algo_name}_{version}_{job_id}.err")
            if log_excerpt:
                error_msg += f"\n\nError log (last 20 lines):\n{log_excerpt}"
        raise Exception(error_msg)


@task(task_run_name="Augment {algo_name} + {dataset}")
def augment_single_output(config: dict, algo_name: str, version: str, dataset: str) -> bool:
    """Augment a single existing output with RT and SA predictions."""
    from runner.algorithm_runner import augment_existing_output

    success = augment_existing_output(config, algo_name, version, dataset)
    if not success:
        raise Exception(f"Failed to augment {algo_name} output for {dataset}")
    return True


@task(name="Augment Existing Outputs")
def augment_existing_outputs_task(config: dict, to_augment: list[tuple[str, str, str]]) -> int:
    """Augment existing outputs that lack SA/pred_RT columns (parallel)."""
    if not to_augment:
        return 0

    print_header("Augmenting Existing Outputs (Parallel)")
    print_info(f"Augmenting {len(to_augment)} existing output(s)...")

    # Submit all augmentations in parallel
    futures = []
    for algo_name, version, dataset in to_augment:
        future = augment_single_output.submit(config, algo_name, version, dataset)
        futures.append(future)

    # Wait for all to complete and count successes
    successful = 0
    for future in futures:
        try:
            future.result()
            successful += 1
        except Exception:
            pass

    print_info(f"Augmented {successful}/{len(to_augment)} outputs")
    return successful


@task(task_run_name="Run {algo_name} on {dataset}")
def run_single_combination(config: dict, algo_name: str, version: str, dataset: str) -> bool:
    """Run a single algorithm-dataset combination."""

    # Check if output already exists
    if check_output_exists_on_alexandria(config, algo_name, version, dataset):
        print_success(f"✓ Output already exists for {algo_name} on {dataset}")
        return True

    # Check if dataset is available on Asimov
    dataset_manager = DatasetManager()
    dataset_status = dataset_manager.get_status(dataset)

    if not dataset_status or dataset_status["status"] != "available":
        print_info(f"⚠ Dataset {dataset} not available on Asimov, skipping {algo_name}")
        return False

    # Pull container from Alexandria
    print_step(f"Preparing {algo_name} ({version}) for {dataset}...")
    if not pull_container_from_alexandria(config, algo_name, version):
        raise Exception(f"Failed to pull container for {algo_name}")

    # Run algorithm
    print_info(f"Running {algo_name} on {dataset}...")
    success, job_id = submit_and_wait_for_run(config, algo_name, version, dataset)

    # Cleanup container to save space
    print_step(f"Cleaning up container for {algo_name}...")
    cleanup_local_container(algo_name, version)

    if success:
        print_success(f"✓ Completed {algo_name} on {dataset}")
        return True
    else:
        error_msg = f"Algorithm run failed for {algo_name} on {dataset}"
        if job_id:
            error_msg += f"\n\nSlurm Job ID: {job_id}"
            log_excerpt = read_error_log(f"run_{algo_name}_{dataset}_{job_id}.err")
            if log_excerpt:
                error_msg += f"\n\nError log (last 20 lines):\n{log_excerpt}"
        raise Exception(error_msg)


@task(name="Cleanup Workspace")
def cleanup_workspace() -> None:
    """Clean up all old files before starting the pipeline."""
    print_header("Cleanup Before Starting")
    import shutil

    runner_dir = Path(__file__).parent

    # Remove all old logs
    logs_dir = runner_dir / "logs"
    if logs_dir.exists():
        log_count = len(list(logs_dir.glob("*.out"))) + len(list(logs_dir.glob("*.err")))
        if log_count > 0:
            print_step(f"Removing {log_count} old log files...")
            for log in logs_dir.glob("*.out"):
                log.unlink()
            for log in logs_dir.glob("*.err"):
                log.unlink()
            print_success("✓ Logs cleaned")
        else:
            print_info("✓ No old logs to clean")

    # Remove all old slurm job scripts
    slurm_jobs_dir = runner_dir / "slurm_jobs"
    if slurm_jobs_dir.exists():
        job_count = len(list(slurm_jobs_dir.glob("*.sh")))
        if job_count > 0:
            print_step(f"Removing {job_count} old slurm job scripts...")
            for job in slurm_jobs_dir.glob("*.sh"):
                job.unlink()
            print_success("✓ Slurm jobs cleaned")
        else:
            print_info("✓ No old slurm jobs to clean")

    # Remove all orphaned outputs on Asimov
    outputs_dir = runner_dir / "denovo_benchmarks" / "outputs"
    if outputs_dir.exists():
        output_count = sum(1 for _ in outputs_dir.rglob("*") if _.is_file())
        if output_count > 0:
            print_step(f"Removing {output_count} orphaned output files...")
            for algo_dir in outputs_dir.iterdir():
                if algo_dir.is_dir():
                    shutil.rmtree(algo_dir)
            print_success("✓ Orphaned outputs cleaned")
        else:
            print_info("✓ No orphaned outputs to clean")

    # Remove old augmentation work directories
    augment_work_dir = runner_dir / "augment_work"
    if augment_work_dir.exists():
        work_count = sum(1 for d in augment_work_dir.iterdir() if d.is_dir())
        if work_count > 0:
            print_step(f"Removing {work_count} augmentation work directories...")
            shutil.rmtree(augment_work_dir)
            augment_work_dir.mkdir()
            print_success("✓ Augmentation work cleaned")
        else:
            print_info("✓ No augmentation work to clean")


@task(name="Ensure denovo-benchmarks Repository")
def check_repository(config: dict) -> None:
    """Check, clone, or update the benchmarks repository."""
    check_or_clone_repo(config)


@task(name="Discover available algorithms")
def discover_algorithms(config: dict) -> list[dict[str, str]]:
    """Discover and display available algorithms."""
    algorithms = get_algorithms(config)
    display_algorithms(algorithms)
    return algorithms


@task(name="Check Alexandria Outputs")
def check_alexandria_outputs(config: dict) -> set[tuple[str, str]]:
    """Check existing outputs on Alexandria."""
    return check_outputs(config)


@task(name="Check Containers existance on Alexandria")
def check_container_status(config: dict, algorithms: list[dict[str, str]]) -> dict[str, bool]:
    """Check which algorithm containers exist on Alexandria."""
    return check_containers(config, algorithms)


@task(name="Check Evaluation Container on Alexandria")
def check_evaluation_container_task(config: dict) -> bool:
    """Check if evaluation container exists on Alexandria."""
    return check_evaluation_container(config)


@task(name="Analyze Container Build Status")
def analyze_container_builds(
    config: dict, algorithms: list[dict[str, str]], container_status: dict[str, bool]
) -> tuple[list[tuple[str, str]], BuildState]:
    """Analyze which containers need building and return build queue."""
    return check_and_display_builds(config, algorithms, container_status)


@task(name="Pull Evaluation Container")
def pull_evaluation_container_task(config: dict) -> bool:
    """Pull evaluation container from Alexandria to Asimov."""
    from runner.algorithm_runner import pull_evaluation_container

    return pull_evaluation_container(config)


@task(name="Check Outputs Needing Augmentation")
def check_outputs_needing_augmentation(
    config: dict, existing_outputs: set[tuple[str, str]], algorithms: list[dict[str, str]]
) -> list[tuple[str, str, str]]:
    """Check which existing outputs need RT/SA augmentation."""
    return get_outputs_needing_augmentation(config, existing_outputs, algorithms)


@task(name="Scan Existing Datasets on Asimov")
def scan_existing_datasets() -> set[str]:
    """Scan for datasets already on Asimov and update state."""
    dataset_manager = DatasetManager()
    datasets_dir = Path(__file__).parent / "datasets"

    if datasets_dir.exists():
        print_step("Scanning for existing datasets on Asimov...")
        for dataset_dir in datasets_dir.iterdir():
            if dataset_dir.is_dir():
                dataset_name = dataset_dir.name
                status = dataset_manager.get_status(dataset_name)
                if not status:
                    # Dataset exists on disk but not in state - mark as available
                    print_info(f"Found existing dataset: {dataset_name}")
                    dataset_manager.states[dataset_name] = {
                        "status": "available",
                        "size_bytes": 0,
                        "updated_at": None,
                    }
                    dataset_manager._save()

    return set(dataset_manager.get_available_datasets())


@task(task_run_name="Pull Dataset: {dataset_name}")
def pull_single_dataset_task(config: dict, dataset_name: str) -> bool:
    """Pull a single dataset from Alexandria to Asimov."""
    dataset_manager = DatasetManager()

    # Check if already available
    status = dataset_manager.get_status(dataset_name)
    if status and status["status"] == "available":
        print_success(f"✓ {dataset_name} already available")
        return True

    print_info(f"Pulling {dataset_name}...")
    if submit_and_wait_for_pull(config, dataset_name, dataset_manager):
        print_success(f"✓ {dataset_name} pulled successfully")
        return True
    else:
        print_info(f"✗ {dataset_name} pull failed")
        return False


@flow(name="Pull Datasets", log_prints=True)
def pull_datasets_flow(config: dict, needed_datasets: set[str]) -> int:
    """Subflow: Pull needed datasets sequentially to avoid space conflicts."""
    if not needed_datasets:
        return 0

    print_header("Pulling Datasets (Sequential)")
    print_info(f"Pulling {len(needed_datasets)} datasets one at a time...")

    successful = 0
    # Pull sequentially to avoid space/state conflicts
    for dataset_name in needed_datasets:
        if pull_single_dataset_task(config, dataset_name):
            successful += 1

    return successful


@task(task_run_name="Cleanup Dataset: {dataset_name}")
def cleanup_dataset_task(dataset_name: str) -> None:
    """Clean up a dataset from Asimov after successful processing."""
    dataset_manager = DatasetManager()
    print_step(f"All algorithms succeeded - cleaning up dataset {dataset_name}...")
    dataset_manager.cleanup_dataset(dataset_name)


@task(task_run_name="Evaluate Dataset: {dataset_name}")
def evaluate_dataset_task(config: dict, dataset_name: str) -> bool:
    """Evaluate all algorithm predictions for a dataset."""
    print_info(f"Evaluating predictions for {dataset_name}...")
    success, job_id = submit_and_wait_for_evaluation(config, dataset_name)
    if success:
        print_success(f"✓ Evaluation completed for {dataset_name}")
        return True
    else:
        error_msg = f"Evaluation failed for {dataset_name}"
        if job_id:
            error_msg += f"\n\nSlurm Job ID: {job_id}"
            log_excerpt = read_error_log(f"evaluate_{dataset_name}_{job_id}.err")
            if log_excerpt:
                error_msg += f"\n\nError log (last 20 lines):\n{log_excerpt}"
        raise Exception(error_msg)


@task(name="Find Datasets Needing Evaluation")
def find_datasets_needing_evaluation(
    config: dict,
    algorithms: list[dict[str, str]],
    existing_outputs: set[tuple[str, str]],
) -> list[str]:
    """Find datasets that have complete outputs but missing evaluation results."""
    datasets = config["datasets"]
    algo_names = {algo["name"] for algo in algorithms}

    runner_dir = Path(__file__).parent
    benchmarks_dir = runner_dir / config["denovo_benchmarks"]["local_path"]
    results_dir = benchmarks_dir / "results"

    expected_result_files = [
        "metrics.csv",
        "peptide_precision_plot_data.csv",
        "AA_precision_plot_data.csv",
        "RT_difference_plot_data.csv",
        "SA_plot_data.csv",
        "number_of_proteome_matches_plot_data.csv",
    ]

    datasets_to_evaluate = []
    for dataset_name in datasets:
        has_all_outputs = all(
            (algo_name, dataset_name) in existing_outputs for algo_name in algo_names
        )
        if not has_all_outputs:
            continue

        dataset_results_dir = results_dir / dataset_name
        has_all_result_files = all(
            (dataset_results_dir / file_name).exists() for file_name in expected_result_files
        )
        if not has_all_result_files:
            datasets_to_evaluate.append(dataset_name)

    if datasets_to_evaluate:
        print_info(f"Datasets with complete outputs but missing results: {datasets_to_evaluate}")
    else:
        print_info("All complete datasets already have evaluation results")

    return datasets_to_evaluate


@flow(name="Evaluate Datasets", log_prints=True)
def evaluate_datasets_flow(config: dict, datasets_to_evaluate: list[str]) -> tuple[int, int]:
    """Evaluate datasets sequentially and return (total, successful)."""
    if not datasets_to_evaluate:
        return 0, 0

    print_header("Evaluating Complete Datasets")
    print_info(f"Evaluating {len(datasets_to_evaluate)} dataset(s)...")

    successful = 0
    for dataset_name in datasets_to_evaluate:
        try:
            evaluate_dataset_task(config, dataset_name)
            successful += 1
        except Exception as e:
            print_info(f"⚠ Evaluation failed for {dataset_name}")
            print_info(f"Error: {str(e)}")

    return len(datasets_to_evaluate), successful


@flow(name="Run Algorithm Benchmarks", log_prints=True)
def run_algorithm_benchmarks_flow(
    config: dict, missing_with_container: list[tuple[str, str]], algorithms: list[dict[str, str]]
) -> tuple[int, int]:
    """Subflow: Run all algorithm-dataset combinations and return (total, successful) counts."""
    if not missing_with_container:
        return 0, 0

    print_header("Running Algorithms on Datasets")

    # Check which datasets are actually available
    dataset_manager = DatasetManager()
    available_datasets = set(dataset_manager.get_available_datasets())

    # Filter to only combinations where dataset is available
    runnable_combinations = [
        (algo, dataset) for algo, dataset in missing_with_container if dataset in available_datasets
    ]

    if not runnable_combinations:
        print_info("No runnable combinations (datasets not available on Asimov)")
        print_info(f"Available datasets: {available_datasets}")
        print_info(f"Needed datasets: {set(d for _, d in missing_with_container)}")
        return 0, 0

    print_info(f"Processing {len(runnable_combinations)} algorithm-dataset combinations")
    print_info(f"Available datasets on Asimov: {available_datasets}")

    # Group by dataset to process one dataset at a time
    from collections import defaultdict

    by_dataset = defaultdict(list)
    for algo_name, dataset in runnable_combinations:
        # Get version for this algorithm
        algo = next((a for a in algorithms if a["name"] == algo_name), None)
        if algo:
            by_dataset[dataset].append((algo_name, algo["version"]))

    # Process each dataset's algorithms, then clean up dataset
    total_runs = 0
    successful_runs = 0

    for dataset_name, algo_list in by_dataset.items():
        print_header(f"Processing Dataset: {dataset_name}")
        print_info(f"Running {len(algo_list)} algorithms on {dataset_name}")

        # Run all algorithms for this dataset in parallel
        futures = []
        for algo_name, version in algo_list:
            future = run_single_combination.submit(config, algo_name, version, dataset_name)
            futures.append(future)
            total_runs += 1

        # Wait for all runs for this dataset to complete and handle failures
        dataset_successes = 0
        for future in futures:
            try:
                future.result()
                dataset_successes += 1
            except Exception:
                pass  # Task already logged the error, just count as failure
        successful_runs += dataset_successes

        print_info(f"Completed {dataset_successes}/{len(algo_list)} runs for {dataset_name}")

        # Only evaluate and clean up dataset if ALL algorithms succeeded
        if dataset_successes == len(algo_list):
            # Run evaluation for this dataset
            print_header(f"Evaluating Dataset: {dataset_name}")
            try:
                evaluate_dataset_task(config, dataset_name)
                print_success(f"✓ Evaluation completed for {dataset_name}")
            except Exception as e:
                print_info(f"⚠ Evaluation failed for {dataset_name}, but continuing...")
                print_info(f"Error: {str(e)}")

            # Clean up dataset
            cleanup_dataset_task(dataset_name)
        else:
            print_info(f"Some algorithms failed - keeping dataset {dataset_name} for retry")

    print_header("Algorithm Runs Summary")
    print_info(f"Total runs: {total_runs}")
    print_info(f"Successful: {successful_runs}")
    print_info(f"Failed: {total_runs - successful_runs}")

    return total_runs, successful_runs


@flow(name="Denovo Benchmarks Orchestration", log_prints=True)
def main():
    """Main orchestration flow."""
    print_banner()

    # Load config
    config = load_config()

    # Run independent tasks in parallel
    cleanup_future = cleanup_workspace.submit()
    repo_future = check_repository.submit(config)
    outputs_future = check_alexandria_outputs.submit(config)

    # Wait for all parallel tasks
    cleanup_future.result()
    repo_future.result()
    existing_outputs = outputs_future.result()

    # Discover algorithms
    algorithms = discover_algorithms(config)

    # Check containers
    container_status = check_container_status(config, algorithms)

    # Check evaluation container
    evaluation_exists = check_evaluation_container_task(config)

    # Check and manage container builds
    needs_building, build_state = analyze_container_builds(config, algorithms, container_status)

    # Add evaluation container to builds if needed
    if not evaluation_exists:
        print_info("Evaluation container needs building - adding to queue")
        needs_building.append(("evaluation", "latest"))

    # Build missing containers (including evaluation) in parallel and wait for completion
    if needs_building:
        print_header("Building Algorithm Containers (Parallel)")
        print_info(f"Submitting {len(needs_building)} build jobs in parallel...")

        # Submit all builds in parallel directly from flow
        build_futures = []
        for algo_name, version in needs_building:
            future = build_single_container.submit(config, algo_name, version, build_state)
            build_futures.append(future)

        # Wait for all to complete and count successes
        built_count = 0
        for future in build_futures:
            try:
                future.result()  # Will raise exception if build failed
                built_count += 1
            except Exception:
                pass  # Task already logged the error, just count as failure
        print_info(f"Built {built_count}/{len(needs_building)} containers")

        # Re-check container status after builds complete
        print_step("Rechecking container status...")
        container_status = check_containers(config, algorithms)
        evaluation_exists = check_evaluation_container_task(config)

    # Pull evaluation container if it exists on Alexandria
    if evaluation_exists:
        pull_success = pull_evaluation_container_task(config)
        if not pull_success:
            print_info("⚠ Failed to pull evaluation container -augmentation will be skipped")
            evaluation_exists = False  # Mark as unavailable

    # Check and augment existing outputs if evaluation container is available
    if evaluation_exists:
        outputs_needing_augmentation = check_outputs_needing_augmentation(
            config, existing_outputs, algorithms
        )
        if outputs_needing_augmentation:
            augmented_count = augment_existing_outputs_task(config, outputs_needing_augmentation)
            print_success(f"Augmented {augmented_count} existing outputs")

        datasets_to_evaluate = find_datasets_needing_evaluation(
            config, algorithms, existing_outputs
        )
        total_eval, successful_eval = evaluate_datasets_flow(config, datasets_to_evaluate)
        if total_eval > 0:
            print_info(
                f"Initial evaluation pass complete: {successful_eval}/{total_eval} successful"
            )

    # Main processing loop: keep pulling datasets and running algorithms until all done
    print_header("Starting Main Processing Loop")
    iteration = 0
    while True:
        iteration += 1
        print_header(f"Processing Iteration {iteration}")

        # Check current state of outputs on Alexandria
        print_step("Checking current output status...")
        existing_outputs = check_alexandria_outputs(config)

        # Analyze what's missing
        missing_with_container, needed_datasets = analyze_missing(
            config, algorithms, existing_outputs, container_status
        )

        # If nothing missing, we're done!
        if not missing_with_container:
            print_success("All outputs completed! Pipeline finished.")
            break

        # Scan for existing datasets on Asimov
        available_datasets = scan_existing_datasets()

        # Filter needed_datasets to exclude those already on Asimov
        needed_datasets = needed_datasets - available_datasets

        if needed_datasets:
            print_info(f"Need to pull {len(needed_datasets)} datasets")
        if available_datasets:
            print_info(
                f"{len(available_datasets)} datasets already on Asimov: {available_datasets}"
            )

        # Pull needed datasets and wait for completion
        if needed_datasets:
            pulled_count = pull_datasets_flow(config, needed_datasets)
            print_info(f"Pulled {pulled_count}/{len(needed_datasets)} datasets")

            # Only stop if we couldn't pull ANY datasets (real failure)
            # If we pulled some but not all, it's likely a space issue.
            # Continue processing what we have
            if pulled_count == 0:
                print_info("Failed to pull any datasets. Ending processing loop.")
                print_info(f"Completed iterations: {iteration}")
                break
            elif pulled_count < len(needed_datasets):
                print_info(f"Pulled {pulled_count}/{len(needed_datasets)} datasets")
                print_info("Will process these and loop back for remaining datasets")

        # Run algorithms on datasets
        total_runs, successful_runs = run_algorithm_benchmarks_flow(
            config, missing_with_container, algorithms
        )

        # Check results - stop if any failures occurred
        failed_runs = total_runs - successful_runs
        if failed_runs > 0:
            print_info(
                f"Algorithm run failures detected: {failed_runs}/{total_runs} runs failed."
                " Ending processing loop."
            )
            print_info(f"Completed iterations: {iteration}")
            break

        print_success(f"Iteration {iteration} complete: All {successful_runs} runs successful!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
