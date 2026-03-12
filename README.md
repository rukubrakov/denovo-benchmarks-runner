# Denovo Benchmarks Orchestration System

Prefect-powered orchestration for running de novo peptide sequencing benchmarks on HPC with Slurm.

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

### Option 1: Apptainer Deployment

**Containerized dashboard and worker with Slurm integration.**

**Initial setup:**
```bash
cd /home/nkubrakov/denovo-benchmarks-runner

# Build Prefect server container
./build_prefect_container.sh
```

**Start server:**
```bash
# Start Prefect server (runs in background)
./run_prefect_server.sh
# Dashboard available at http://localhost:4200
```

**Start worker:**
```bash
# Start worker in detached screen session
screen -dmS prefect-worker ./run_deployment_worker.sh
```

**Access dashboard from local machine:**
```bash
# SSH tunnel
ssh -L 4200:localhost:4200 nkubrakov@asimov.uantwerpen.be

# Open browser to: http://localhost:4200
```

**Trigger runs:**
- Navigate to **Deployments** → **denovo-benchmarks-runner** → **Run**
- Watch progress in **Flow Runs** tab with real-time updates

**Check worker status:**
```bash
# Check if screen session is running
screen -ls

# Attach to worker screen session (Ctrl+A, D to detach)
screen -r prefect-worker

# Check running processes
ps aux | grep -E "(prefect-worker|deploy)" | grep -v grep
```

**Stop everything:**
```bash
# Stop worker
screen -X -S prefect-worker quit

# Stop server
./stop_prefect_server.sh
```

**Architecture:**
- **Server**: Apptainer container (isolated, persists after logout)
- **Worker**: Apptainer container with host Slurm bindings (runs in screen)
- **Jobs**: Slurm queue (container builds, dataset pulls)

---

### Option 2: Direct Execution (Simple, No UI)

**For quick runs without dashboard:**
```bash
cd /home/nkubrakov/denovo-benchmarks-runner
uv run orchestrate.py
```

---

## What the Workflow Does

**Orchestration Process:**
1. Clone or update the `denovo_benchmarks` repository
2. Discover all algorithms with their versions (excluding those in config)
3. Check what containers and outputs exist on Alexandria
4. **Automatically build missing algorithm and evaluation containers**
5. Track build progress with persistent state in `build_state.json`
6. Pull needed datasets from Alexandria
7. Use configurable Slurm resources from `config.yaml`
8. Report what's missing and ready to run


## Project Structure

```
denovo-benchmarks-runner/
├── README.md                 # This file
├── config.yaml              # Configuration
├── pyproject.toml           # Python project definition
├── orchestrate.py           # Main workflow (run this!)
├── deploy.py                # Prefect deployment (optional, for UI triggers)
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
└── build_state.json         # Auto-generated build tracking
```

**Key Files:**
- **orchestrate.py**: Main entry point - run this to execute workflows
- **deploy.py**: Creates Prefect deployment - only needed for UI-triggered runs
- **config.yaml**: Configure datasets, algorithms, paths, Slurm resources

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
