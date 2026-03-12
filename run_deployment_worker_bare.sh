#!/bin/bash
# Run Prefect deployment worker on bare host (no container)
# This connects to the Prefect server running in Apptainer

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting Prefect deployment worker (bare host)..."
echo "Server: http://localhost:4200"
echo "Press Ctrl+C to stop"
echo ""

cd "$SCRIPT_DIR"

# Set API URL to connect to containerized Prefect server
export PREFECT_API_URL="http://localhost:4200/api"

# Ensure dependencies are installed
echo "Syncing dependencies..."
uv sync

echo "Starting worker..."
uv run python deploy.py
