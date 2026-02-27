# Denovo Benchmarks Orchestration System

Simple orchestration for running de novo peptide sequencing benchmarks on HPC with Slurm.

## Installation

### Prerequisites

- Python 3.6+
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

**Check status:**
```bash
uv run orchestrate.py
```

This will:
- Clone or update the `denovo_benchmarks` repository
- Discover all algorithms with their versions
- Check what outputs are already on Alexandria
- Check which containers are built
- Report what's missing and ready to run

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
│   └── algorithms.py        # Algorithm discovery
├── slurm_job_template.sh    # Slurm job template
└── submit_job.py            # Job submission script
```

## Configuration

Edit `config.yaml` for your setup:

```yaml
denovo_benchmarks:
  local_path: "denovo_benchmarks"  # Inside this directory (auto-cloned)
  repo_url: "git@github.com:bittremieuxlab/denovo_benchmarks.git"
  branch: "main"

alexandria:
  host: "nkubrakov@alexandria.uantwerpen.be"
  outputs_path: "/mnt/data/nkubrakov/denovo_benchmarks/outputs"
  containers_path: "/mnt/data/nkubrakov/denovo_benchmarks/containers"
  
datasets:
  - "test_dataset_human"  # Add more datasets as needed

version_strategy: "latest_only"  # Only process latest versions
```

## Workflow

1. Run `orchestrate.py` to see what's missing
2. Build missing containers if needed
3. Run benchmark jobs for missing combinations
4. Results get stored on Alexandria

## Storage Structure

**On Alexandria:**
```
/mnt/data/nkubrakov/denovo_benchmarks/
├── containers/
│   └── algorithm_name/
│       └── version/
│           └── container.sif
└── outputs/
    └── algorithm_name/
        └── version/
            └── dataset_name/
                └── output.csv
```

## Testing Data Transfer

Test script is included to verify Asimov ↔ Alexandria connectivity:
```bash
sbatch test_alexandria_transfer.slurm.sh
```

## Requirements

- Python 3.6+
- [uv](https://github.com/astral-sh/uv) package manager
- SSH access to Alexandria configured
- Slurm cluster access

## Notes

- System focuses on **latest versions only** by default
- All outputs archived to Alexandria automatically
- Containers stored on Alexandria to save Asimov space

## Additional Documentation

See [README_OLD.md](README_OLD.md) for detailed technical documentation from the original setup, including:
- Manual container building steps
- Detailed troubleshooting
- Slurm configuration details
- Technical context for the system

See `README_OLD.md` for detailed technical documentation from previous setup.
