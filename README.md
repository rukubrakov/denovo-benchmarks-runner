# Denovo Benchmarks Orchestration System

Simple orchestration for running de novo peptide sequencing benchmarks on HPC with Slurm.

## Installation

### Prerequisites

- Python 3.12
- [uv](https://github.com/astral-sh/uv) package manager
- SSH access to Alexandria configured
- Slurm cluster access

### Install uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Clone and Setup

```bash
# Clone repository
git clone git@github.com:rukubrakov/denovo-benchmarks-runner.git
cd denovo-benchmarks-runner

# Install dependencies (creates .venv and installs packages)
uv sync

# Install dev dependencies (includes ruff for linting/formatting)
uv sync --all-extras

# Configure your setup
nano config.yaml  # Edit paths and settings
```

### Code Quality

```bash
# Format code with ruff
uv run ruff format .

# Lint code with ruff
uv run ruff check .

# Fix linting issues automatically
uv run ruff check --fix .
```

## Quick Start

**Run orchestration:**
```bash
uv run orchestrate.py
```

This will:
- Clone or update the `denovo_benchmarks` repository
- Discover all algorithms with their versions
- Check what containers and outputs exist on Alexandria
- **Automatically build missing algorithm and evaluation containers**
- Track build progress with persistent state in `build_state.json`
- Report what's missing and ready to run

**How container building works:**
- Algorithm and evaluation containers built on Asimov with Apptainer
- Automatically transferred to Alexandria after successful builds
- Build state tracked persistently to prevent duplicate submissions
- Failed builds can be automatically retried on next run
- Run `orchestrate.py` again to check build progress and status updates

## Project Structure

```
denovo-benchmarks-runner/
├── README.md                 # This file
├── config.yaml              # Configuration
├── pyproject.toml           # Python project definition
├── orchestrate.py           # Main entry point
├── runner/                  # Python package
│   ├── __init__.py
│   ├── display.py           # Display/UI utilities
│   ├── git_ops.py           # Git operations
│   ├── alexandria.py        # Alexandria storage operations
│   ├── algorithms.py        # Algorithm discovery
│   ├── build_state.py       # Container build state tracking
│   └── container_builder.py # Container building orchestration
├── container_overrides/     # Optional container.def overrides
│   └── algorithm_name/
│       └── version/
│           └── container.def
├── templates/
│   └── build_container.slurm.sh  # Container build job template
├── slurm_job_template.sh    # Benchmark run job template
└── submit_job.py            # Job submission script
```

## Configuration

Edit `config.yaml` for your setup:

```yaml
denovo_benchmarks:
  local_path: "denovo_benchmarks"  # Inside this directory
  repo_url: "git@github.com:bittremieuxlab/denovo_benchmarks.git"
  branch: "main"

alexandria:
  host: "nkubrakov@alexandria.uantwerpen.be"
  outputs_path: "/mnt/data/nkubrakov/denovo_benchmarks/outputs"
  containers_path: "/mnt/data/nkubrakov/denovo_benchmarks/containers"
  
datasets:
  - "test_dataset_human"  # Add more datasets as needed

# Exclude specific algorithms from processing
excluded_algorithms:
  - "algorithm_name"

# Slurm resource defaults for container builds
slurm:
  partition: "one_hour"
  cpus: 4
  memory: "16G"
  time: "01:00:00"

version_strategy: "latest_only"  # Only process latest versions
```

## Container Overrides

If you need to customize a container definition for a specific algorithm, create an override:

```bash
mkdir -p container_overrides/algorithm_name/version/
cp denovo_benchmarks/algorithms/algorithm_name/container.def \
   container_overrides/algorithm_name/version/container.def
# Edit the override file
```

Example: The casanovo override limits PyTorch to <2.6 to avoid compatibility issues.

When building, the system checks for overrides first and uses them if available.

## Workflow

1. Run `orchestrate.py` to see what's missing
2. System automatically builds missing algorithm and evaluation containers
3. Check build status by running `orchestrate.py` again
4. Run benchmark jobs for missing combinations
5. Results get stored on Alexandria

## Storage Structure

**On Alexandria:**
```
/mnt/data/nkubrakov/denovo_benchmarks/
├── containers/
│   ├── evaluation/
│   │   └── evaluation.sif     # Evaluation container
│   └── algorithm_name/
│       └── version/
│           └── container.sif  # Algorithm containers
└── outputs/
    └── algorithm_name/
        └── version/
            └── dataset_name/
                └── output.csv
```
