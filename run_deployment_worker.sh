#!/bin/bash
# Run Prefect deployment worker in Apptainer container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="$SCRIPT_DIR/prefect.sif"

if [ ! -f "$CONTAINER" ]; then
    echo "✗ Container not found: $CONTAINER"
    echo "Run ./build_prefect_container.sh first"
    exit 1
fi

echo "Starting Prefect deployment worker (containerized)..."
echo "Server: http://localhost:4200"
echo "Press Ctrl+C to stop"
echo ""

cd "$SCRIPT_DIR"

# Run deployment worker in container with full Slurm/SSH access
# Use host's Slurm binaries to match server version
apptainer exec \
    --bind "$(pwd):/app" \
    --bind "$HOME/.ssh:/root/.ssh:ro" \
    --bind /etc/slurm:/etc/slurm:ro \
    --bind /run/munge:/run/munge \
    --bind /usr/bin/sbatch:/usr/local/bin/sbatch:ro \
    --bind /usr/bin/squeue:/usr/local/bin/squeue:ro \
    --bind /usr/bin/sacct:/usr/local/bin/sacct:ro \
    --bind /usr/bin/scontrol:/usr/local/bin/scontrol:ro \
    --bind /usr/lib64/slurm:/usr/lib64/slurm:ro \
    --pwd /app \
    --env PREFECT_API_URL="http://localhost:4200/api" \
    --env SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    --env PATH="/usr/local/bin:$PATH" \
    "$CONTAINER" \
    bash -c "cd /app && uv sync && uv run python deploy.py"
