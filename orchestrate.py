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
    check_and_display_builds,
    check_containers,
    check_evaluation_container,
    check_or_clone_repo,
    check_outputs,
    check_output_exists_on_alexandria,
    cleanup_local_container,
    DatasetManager,
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
    print()

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
        print()

    if missing_without_container:
        print_step("Need container first (container missing):")
        for algo, dataset in missing_without_container:
            print(f"    ⚠ {algo} + {dataset}")
        print()

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
    print()

    return missing_with_container, needed_datasets


@task(task_run_name="Build Container: {algo_name}")
def build_single_container(config: dict, algo_name: str, version: str, build_state: BuildState) -> bool:
    """Build a single container and wait for completion."""
    print_info(f"Building {algo_name} ({version})...")
    success = submit_and_wait_for_build(config, algo_name, version, build_state)
    if success:
        print_success(f"✓ {algo_name} ({version}) built successfully")
    else:
        print_info(f"✗ {algo_name} ({version}) build failed")
    return success


@task(name="Pull All Datasets")
def pull_datasets_task(config: dict, needed_datasets: set[str]) -> int:
    """Pull needed datasets sequentially (to avoid space conflicts)."""
    if not needed_datasets:
        return 0
    
    print_header("Pulling Datasets (Sequential)")
    print_info(f"Pulling {len(needed_datasets)} datasets one at a time...")
    
    dataset_manager = DatasetManager()
    successful = 0
    
    # Pull sequentially to avoid space/state conflicts
    for dataset_name in needed_datasets:
        # Check if already available
        status = dataset_manager.get_status(dataset_name)
        if status and status["status"] == "available":
            print_success(f"✓ {dataset_name} already available")
            successful += 1
            continue
        
        print_info(f"Pulling {dataset_name}...")
        if submit_and_wait_for_pull(config, dataset_name, dataset_manager):
            successful += 1
            print_success(f"✓ {dataset_name} pulled successfully")
        else:
            print_info(f"✗ {dataset_name} pull failed")
        print()
    
    return successful


@task(task_run_name="Augment {algo_name} + {dataset}")
def augment_single_output(config: dict, algo_name: str, version: str, dataset: str) -> bool:
    """Augment a single existing output with RT and SA predictions."""
    from runner.algorithm_runner import augment_existing_output
    
    success = augment_existing_output(config, algo_name, version, dataset)
    return success


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
    
    # Wait for all to complete
    results = [future.result() for future in futures]
    successful = sum(1 for r in results if r)
    
    print_info(f"Augmented {successful}/{len(to_augment)} outputs")
    return successful


@task(task_run_name="Run {algo_name} on {dataset}")
def run_single_combination(
    config: dict, algo_name: str, version: str, dataset: str
) -> bool:
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
        print_info(f"✗ Failed to pull container for {algo_name}")
        return False
    
    # Run algorithm
    print_info(f"Running {algo_name} on {dataset}...")
    success = submit_and_wait_for_run(config, algo_name, version, dataset)
    
    # Cleanup container to save space
    print_step(f"Cleaning up container for {algo_name}...")
    cleanup_local_container(algo_name, version)
    
    if success:
        print_success(f"✓ Completed {algo_name} on {dataset}")
    else:
        print_info(f"✗ Failed {algo_name} on {dataset}")
    
    return success


@flow(name="Denovo Benchmarks Orchestration", log_prints=True)
def main():
    """Main orchestration flow."""
    print_banner()

    # Load config
    config = load_config()

    # Cleanup ALL old files before starting the pipeline
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
    
    print()

    # Check/clone/update repository
    check_or_clone_repo(config)

    # Get and display algorithms
    algorithms = get_algorithms(config)
    display_algorithms(algorithms)

    # Check Alexandria outputs
    existing_outputs = check_outputs(config)

    # Check containers
    container_status = check_containers(config, algorithms)

    # Check evaluation container
    evaluation_exists = check_evaluation_container(config)

    # Check and manage container builds
    needs_building, build_state = check_and_display_builds(config, algorithms, container_status)

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
        build_results = [future.result() for future in build_futures]
        built_count = sum(1 for success in build_results if success)
        print_info(f"Built {built_count}/{len(needs_building)} containers")
        
        # Re-check container status after builds complete
        print_step("Rechecking container status...")
        container_status = check_containers(config, algorithms)
        evaluation_exists = check_evaluation_container(config)
        print()

    # Pull evaluation container if it exists on Alexandria
    if evaluation_exists:
        print_header("Pulling Evaluation Container")
        from runner.algorithm_runner import pull_evaluation_container
        pull_success = pull_evaluation_container(config)
        if not pull_success:
            print_info("⚠ Failed to pull evaluation container - augmentation will be skipped")
            evaluation_exists = False  # Mark as unavailable
        print()

    # Check and augment existing outputs if evaluation container is available
    if evaluation_exists:
        outputs_needing_augmentation = get_outputs_needing_augmentation(config, existing_outputs, algorithms)
        if outputs_needing_augmentation:
            augmented_count = augment_existing_outputs_task(config, outputs_needing_augmentation)
            print_success(f"Augmented {augmented_count} existing outputs")
        print()

    # Analyze what's missing
    missing_with_container, needed_datasets = analyze_missing(config, algorithms, existing_outputs, container_status)

    # Check current dataset state and update if datasets exist but aren't tracked
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
        print()

    # Filter needed_datasets to exclude those already on Asimov
    available_datasets = set(dataset_manager.get_available_datasets())
    needed_datasets = needed_datasets - available_datasets
    
    if needed_datasets:
        print_info(f"Need to pull {len(needed_datasets)} datasets")
    if available_datasets:
        print_info(f"{len(available_datasets)} datasets already on Asimov: {available_datasets}")
    print()

    # Pull needed datasets and wait for completion
    if needed_datasets:
        pulled_count = pull_datasets_task(config, needed_datasets)
        print_info(f"Pulled {pulled_count}/{len(needed_datasets)} datasets")
        print()

    # Run algorithms on datasets
    if missing_with_container:
        print_header("Running Algorithms on Datasets")
        
        # Check which datasets are actually available
        dataset_manager = DatasetManager()
        available_datasets = set(dataset_manager.get_available_datasets())
        
        # Filter to only combinations where dataset is available
        runnable_combinations = [
            (algo, dataset) for algo, dataset in missing_with_container
            if dataset in available_datasets
        ]
        
        if not runnable_combinations:
            print_info("No runnable combinations (datasets not available on Asimov)")
            print_info(f"Available datasets: {available_datasets}")
            print_info(f"Needed datasets: {set(d for _, d in missing_with_container)}")
            print()
            return
        
        print_info(f"Processing {len(runnable_combinations)} algorithm-dataset combinations")
        print_info(f"Available datasets on Asimov: {available_datasets}")
        print()
        
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
            print()
            
            # Run all algorithms for this dataset in parallel
            futures = []
            for algo_name, version in algo_list:
                future = run_single_combination.submit(config, algo_name, version, dataset_name)
                futures.append(future)
                total_runs += 1
            
            # Wait for all runs for this dataset to complete
            results = [future.result() for future in futures]
            dataset_successes = sum(1 for r in results if r)
            successful_runs += dataset_successes
            
            print_info(f"Completed {dataset_successes}/{len(algo_list)} runs for {dataset_name}")
            
            # Only clean up dataset if ALL algorithms succeeded
            if dataset_successes == len(algo_list):
                print_step(f"All algorithms succeeded - cleaning up dataset {dataset_name}...")
                dataset_manager.cleanup_dataset(dataset_name)
            else:
                print_info(f"Some algorithms failed - keeping dataset {dataset_name} for retry")
            print()
        
        print_header("Algorithm Runs Summary")
        print_info(f"Total runs: {total_runs}")
        print_info(f"Successful: {successful_runs}")
        print_info(f"Failed: {total_runs - successful_runs}")

    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
