"""Algorithm discovery and version management."""

from pathlib import Path

from .display import print_header, print_success, print_warning


def get_algorithms(config: dict) -> list[dict[str, str]]:
    """
    Get list of algorithms with their latest versions from versions.log files.
    Filters out excluded algorithms from config.
    Returns list of dicts with 'name' and 'version' keys.
    """
    repo_path = Path(__file__).parent.parent / config["denovo_benchmarks"]["local_path"]
    algorithms_path = repo_path / "algorithms"

    excluded = set(config.get("excluded_algorithms", []))
    algorithms = []

    for algo_dir in algorithms_path.iterdir():
        if not algo_dir.is_dir() or algo_dir.name.startswith("."):
            continue

        # Skip excluded algorithms
        if algo_dir.name in excluded:
            continue

        versions_file = algo_dir / "versions.log"
        if not versions_file.exists():
            continue

        # Parse versions.log to get latest version
        try:
            with open(versions_file, "r") as f:
                content = f.read()
                # Simple parsing - get first container_version
                for line in content.split("\n"):
                    if "container_version:" in line:
                        version = line.split('"')[1]
                        algorithms.append({"name": algo_dir.name, "version": version})
                        break
        except Exception as e:
            print_warning(f"Could not parse {versions_file}: {e}")
            continue

    return algorithms


def display_algorithms(algorithms: list[dict[str, str]]):
    """Display discovered algorithms."""
    print_header("Discovering Algorithms")
    print_success(f"Found {len(algorithms)} algorithm(s) with versions:")
    for algo in algorithms:
        print(f"    • {algo['name']} (version: {algo['version']})")
