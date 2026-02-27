"""Alexandria storage operations."""

import subprocess
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
