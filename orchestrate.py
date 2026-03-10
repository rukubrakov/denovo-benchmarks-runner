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
    DatasetManager,
    display_algorithms,
    get_algorithms,
    print_banner,
    print_header,
    print_info,
    print_step,
    print_success,
    submit_and_wait_for_build,
    submit_and_wait_for_pull,
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


@flow(name="Denovo Benchmarks Orchestration", log_prints=True)
def main():
    """Main orchestration flow."""
    print_banner()

    # Load config
    config = load_config()

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

    # Analyze what's missing
    missing_with_container, needed_datasets = analyze_missing(config, algorithms, existing_outputs, container_status)

    # Pull needed datasets and wait for completion
    if needed_datasets:
        pulled_count = pull_datasets_task(config, needed_datasets)
        print_info(f"Pulled {pulled_count}/{len(needed_datasets)} datasets")
        print()

    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
