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

### Option 1: Direct Execution (Simple, No UI)
```bash
cd /home/nkubrakov/denovo-benchmarks-runner
uv run orchestrate.py
```
**Use this when:** You just want to run the workflow once, no monitoring needed.

---

### Option 2: With Prefect UI (Full Dashboard & Control)

**Setup (one-time per session):**

**Step 1: Start Prefect server**
```bash
# On asimov, in a dedicated terminal or screen/tmux
cd /home/nkubrakov/denovo-benchmarks-runner
PREFECT_UI_API_URL="http://localhost:4200/api" uv run prefect server start
```

**Step 2: Start deployment worker** (optional - only if you want UI button to trigger runs)
```bash
# In another terminal on asimov
cd /home/nkubrakov/denovo-benchmarks-runner
uv run python deploy.py
```

**Step 3: SSH tunnel from your local machine**
```bash
# On your laptop/desktop
ssh -L 4200:localhost:4200 nkubrakov@asimov.uantwerpen.be
# Keep this terminal open while using the UI
```

**Step 4: Access dashboard**
```
Open browser to: http://localhost:4200
```

**Running workflows:**
- **From command line (recommended):** `uv run orchestrate.py` - runs appear in UI automatically
- **From UI (if deploy.py is running):** Deployments → "denovo-benchmarks-runner" → Run

**Stopping everything:**
```bash
# Stop deployment worker (Ctrl+C in deploy.py terminal)
# Stop Prefect server (Ctrl+C in server terminal)
# Or kill all: pkill -f "prefect server" && pkill -f "deploy.py"
```

---

**Which option to use?**
- **Quick runs, testing:** Use Option 1 (direct execution)
- **Monitoring multiple runs, team collaboration:** Use Option 2 (Prefect UI)
- **Scheduled/automated runs:** Use Option 2 with deployment

---

**Prefect UI Features:**
- 🎯 **Trigger workflows** manually or on schedule
- 📊 Real-time workflow visualization
- 📝 Task-level execution tracking and logs
- 🔄 Retry failed tasks from the UI
- 📈 Historical run comparison and metrics
- ⏸️ Pause/cancel running workflows

The orchestration will:
- Clone or update the `denovo_benchmarks` repository
- Discover all algorithms with their versions (excluding those in config)
- Check what containers and outputs exist on Alexandria
- **Automatically build missing algorithm and evaluation containers**
- Track build progress with persistent state in `build_state.json`
- Use configurable Slurm resources from `config.yaml`
- Report what's missing and ready to run

**Prefect UI Benefits:**
- Real-time workflow visualization
- Task-level execution tracking
- Centralized logs and metrics
- Historical run comparison
- Flow run management

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

## Workflow

### Typical Usage Patterns

**Daily Development/Testing:**
```bash
# Just run directly (no Prefect server needed)
cd /home/nkubrakov/denovo-benchmarks-runner
uv run orchestrate.py
```

**Team Monitoring/Collaboration:**
```bash
# Terminal 1: Start Prefect server (leave running)
PREFECT_UI_API_URL="http://localhost:4200/api" uv run prefect server start

# Terminal 2: Run workflows as needed
uv run orchestrate.py  # Appears in UI automatically

# Laptop: SSH tunnel to view dashboard
ssh -L 4200:localhost:4200 nkubrakov@asimov.uantwerpen.be
# Browser: http://localhost:4200
```

**Production/Scheduled Runs:**
```bash
# Terminal 1: Prefect server (or run in systemd/screen)
PREFECT_UI_API_URL="http://localhost:4200/api" uv run prefect server start &

# Terminal 2: Deployment worker (or run in systemd/screen)
uv run python deploy.py &

# Now you can trigger from UI or schedule recurring runs
```

### What the System Does

1. Run `orchestrate.py` to discover what's missing
2. System automatically builds missing algorithm and evaluation containers
3. Check build status by running `orchestrate.py` again
4. Run benchmark jobs for missing combinations (future)
5. Results get stored on Alexandria

### Process Management

**Check what's running:**
```bash
ps aux | grep -E "(prefect|deploy)" | grep -v grep
```

**Stop everything:**
```bash
pkill -f "prefect server"
pkill -f "deploy.py"
```

**Run in background (persistent):**
```bash
# Using screen
screen -S prefect-server
PREFECT_UI_API_URL="http://localhost:4200/api" uv run prefect server start
# Ctrl+A, D to detach

screen -S prefect-worker
cd /home/nkubrakov/denovo-benchmarks-runner && uv run python deploy.py
# Ctrl+A, D to detach

# Reattach later: screen -r prefect-server
```

## Production Deployment (Future)

**Current Setup:** Local execution with SSH tunnel for UI access

**For Production/Team Use, Options:**

### Option 1: Prefect Cloud (Easiest)
- Free tier available for small teams
- Managed hosting (no server maintenance)
- Configure: `uv run prefect cloud login`
- Team members get web access without SSH tunneling
- Built-in: scheduling, notifications, access control

### Option 2: Self-Hosted Prefect Server (Full Control)
- **With Reverse Proxy** (recommended):
  - Ask IT to set up nginx/Apache proxy
  - Access via: `https://prefect.asimov.uantwerpen.be`
  - Add HTTPS certificate for security
  - Configure authentication (Prefect + nginx auth)

- **With VPN/Jump Host**:
  - Access Prefect UI through institutional VPN
  - No firewall changes needed

- **Persistent Server**:
  ```bash
  # Run as systemd service or in screen/tmux
  screen -S prefect
  cd /home/nkubrakov/denovo-benchmarks-runner
  PREFECT_UI_API_URL="http://localhost:4200/api" uv run prefect server start
  # Ctrl+A, D to detach
  ```

### Option 3: Remove Prefect (Simplify)
- If UI overhead isn't worth it for your team
- Keep the modular Python code (already valuable!)
- Use simple JSON state tracking (what you have now)
- Monitor via logs and `build_state.json`

**Recommendation:** Start with current setup (local + SSH). If team adoption grows → Prefect Cloud. If need full control → Self-hosted with reverse proxy.

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
