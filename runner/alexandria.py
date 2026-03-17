"""Alexandria storage operations."""

import csv
import subprocess
import tempfile
from pathlib import Path

from .display import print_header, print_info, print_step, print_success, print_warning


def check_outputs(config: dict) -> set[tuple[str, str]]:
    """
    Check what outputs exist on Alexandria.
    Returns set of (algorithm, dataset) tuples.
    """
    print_header("Alexandria Outputs Status")

    host = config["alexandria"]["host"]
    outputs_path = config["alexandria"]["outputs_path"]

    print_step(f"Checking outputs on Alexandria at {outputs_path}...")

    # Check if outputs directory exists
    result = subprocess.run(
        ["ssh", host, f'test -d {outputs_path} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )

    if result.stdout.strip() == "missing":
        print_info("Outputs directory does not exist on Alexandria yet")
        print_step(f"Creating directory: {outputs_path}")
        subprocess.run(["ssh", host, f"mkdir -p {outputs_path}"])
        return set()

    # List outputs
    result = subprocess.run(
        ["ssh", host, f"find {outputs_path} -mindepth 3 -maxdepth 3 -type d"],
        capture_output=True,
        text=True,
    )

    existing = set()
    if result.returncode == 0 and result.stdout.strip():
        for path in result.stdout.strip().split("\n"):
            # Path format: /mnt/data/nkubrakov/denovo_benchmarks/outputs/algo/version/dataset
            parts = Path(path).parts
            if len(parts) >= 3:
                algo = parts[-3]
                dataset = parts[-1]
                existing.add((algo, dataset))

    if existing:
        print_success(f"Found {len(existing)} existing output(s)")
        for algo, dataset in sorted(existing):
            print(f"    • {algo} + {dataset}")
    else:
        print_info("No outputs found yet")

    return existing


def check_evaluation_container(config: dict) -> bool:
    """
    Check if evaluation container exists on Alexandria.
    Returns True if exists, False otherwise.
    """
    host = config["alexandria"]["host"]
    containers_base = config["alexandria"]["containers_path"]
    evaluation_path = f"{containers_base}/evaluation/evaluation.sif"

    result = subprocess.run(
        ["ssh", host, f'test -f {evaluation_path} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )

    return result.stdout.strip() == "exists"


def check_container_exists(config: dict, algo_name: str, version: str) -> bool:
    """
    Check if a specific container exists on Alexandria.
    Returns True if exists, False otherwise.
    """
    host = config["alexandria"]["host"]
    containers_base = config["alexandria"]["containers_path"]
    
    # Special handling for evaluation container
    if algo_name == "evaluation":
        container_path = f"{containers_base}/evaluation/evaluation.sif"
    else:
        container_path = f"{containers_base}/{algo_name}/{version}/container.sif"

    result = subprocess.run(
        ["ssh", host, f'test -f {container_path} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )

    return result.stdout.strip() == "exists"


def check_containers(config: dict, algorithms: list[dict[str, str]]) -> dict[str, bool]:
    """
    Check which containers exist on Alexandria.
    Returns dict mapping algorithm name to existence bool.
    """
    print_header("Container Status on Alexandria")

    host = config["alexandria"]["host"]
    containers_base = config["alexandria"]["containers_path"]

    print_step(f"Checking containers on Alexandria at {containers_base}...")

    # Check if containers directory exists
    result = subprocess.run(
        ["ssh", host, f'test -d {containers_base} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )

    if result.stdout.strip() == "missing":
        print_info("Containers directory does not exist on Alexandria yet")
        print_step(f"Creating directory: {containers_base}")
        subprocess.run(["ssh", host, f"mkdir -p {containers_base}"])
        return {algo["name"]: False for algo in algorithms}

    container_status = {}

    for algo in algorithms:
        algo_name = algo["name"]
        algo_version = algo["version"]
        container_path = f"{containers_base}/{algo_name}/{algo_version}/container.sif"

        result = subprocess.run(
            ["ssh", host, f'test -f {container_path} && echo "exists" || echo "missing"'],
            capture_output=True,
            text=True,
        )

        exists = result.stdout.strip() == "exists"
        container_status[algo_name] = exists

        if exists:
            print_success(f"{algo_name} ({algo_version}): Container exists")
        else:
            print_warning(f"{algo_name} ({algo_version}): Container missing")

    return container_status


def check_output_needs_augmentation(config: dict, algo_name: str, version: str, dataset: str) -> bool:
    """
    Check if an output CSV on Alexandria is missing augmentation (SA and pred_RT columns).
    Returns True if augmentation is needed, False otherwise.
    """
    host = config["alexandria"]["host"]
    outputs_path = config["alexandria"]["outputs_path"]
    output_csv = f"{outputs_path}/{algo_name}/{version}/{dataset}/output.csv"
    
    # Check if output.csv exists
    result = subprocess.run(
        ["ssh", host, f'test -f {output_csv} && echo "exists" || echo "missing"'],
        capture_output=True,
        text=True,
    )
    
    if result.stdout.strip() != "exists":
        return False
    
    # Download just the header line to check columns
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # Get first line (header) from the CSV
        result = subprocess.run(
            ["ssh", host, f"head -n 1 {output_csv}"],
            capture_output=True,
            text=True,
        )
        
        if result.returncode != 0:
            return False
        
        header = result.stdout.strip()
        
        # Check if SA and pred_RT columns exist
        has_sa = 'SA' in header.split(',')
        has_pred_rt = 'pred_RT' in header.split(',')
        
        # Needs augmentation if either column is missing
        return not (has_sa and has_pred_rt)
    
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def get_outputs_needing_augmentation(config: dict, existing_outputs: set[tuple[str, str]], algorithms: list[dict[str, str]]) -> list[tuple[str, str, str]]:
    """
    Check which existing outputs need augmentation.
    Returns list of (algo_name, version, dataset) tuples.
    """
    print_header("Checking Outputs for Augmentation")
    
    needs_augmentation = []
    
    # Create algo version lookup
    algo_versions = {algo["name"]: algo["version"] for algo in algorithms}
    
    for algo_name, dataset in existing_outputs:
        if algo_name not in algo_versions:
            continue
        
        version = algo_versions[algo_name]
        
        print_step(f"Checking {algo_name} + {dataset}...")
        if check_output_needs_augmentation(config, algo_name, version, dataset):
            print_warning(f"  ⚠ Missing augmentation (no SA/pred_RT)")
            needs_augmentation.append((algo_name, version, dataset))
        else:
            print_success(f"  ✓ Already augmented")
    
    if needs_augmentation:
        print_info(f"Found {len(needs_augmentation)} output(s) needing augmentation")
    else:
        print_success("All existing outputs are already augmented")
    
    return needs_augmentation
