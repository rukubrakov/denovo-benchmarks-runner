#!/usr/bin/env python3
"""
Denovo Benchmarks Orchestration System
Simple script to check status and prepare for benchmark runs.
"""

import sys
from pathlib import Path

import yaml

from runner import (
    check_and_build_evaluation_container,
    check_and_display_builds,
    check_containers,
    check_evaluation_container,
    check_or_clone_repo,
    check_outputs,
    display_algorithms,
    get_algorithms,
    print_banner,
    print_header,
    print_info,
    print_step,
    print_success,
    submit_build_job,
    submit_evaluation_build,
)


def load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def analyze_missing(
    config: dict,
    algorithms: list[dict[str, str]],
    existing_outputs: set[tuple[str, str]],
    container_status: dict[str, bool],
):
    """
    Analyze and report what's missing.
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
        return

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

    # Submit builds for missing containers (default behavior)
    if needs_building:
        print_header("Submitting Algorithm Container Build Jobs")
        for algo_name, version in needs_building:
            submit_build_job(config, algo_name, version, build_state)
        print()

        # Re-check container status after submitting builds
        print_step("Rechecking container status...")
        container_status = check_containers(config, algorithms)
        print()

    # Check and build evaluation container
    needs_eval_build = check_and_build_evaluation_container(config, evaluation_exists)
    if needs_eval_build:
        print_header("Submitting Evaluation Container Build Job")
        submit_evaluation_build(config, build_state)
        print()

    # Analyze what's missing
    analyze_missing(config, algorithms, existing_outputs, container_status)

    print()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
