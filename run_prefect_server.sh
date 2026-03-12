#!/bin/bash
# Start Prefect server in Apptainer container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="$SCRIPT_DIR/prefect.sif"
INSTANCE_NAME="prefect-server"

if [ ! -f "$CONTAINER" ]; then
    echo "✗ Container not found: $CONTAINER"
    echo "Run ./build_prefect_container.sh first"
    exit 1
fi

# Check if instance already running
if apptainer instance list | grep -q "$INSTANCE_NAME"; then
    echo "Prefect server is already running"
    echo "Access at: http://localhost:4200"
    echo ""
    echo "To stop: apptainer instance stop $INSTANCE_NAME"
    exit 0
fi

echo "Starting Prefect server..."

# Create .prefect directory if it doesn't exist
mkdir -p "$SCRIPT_DIR/.prefect"

# Start server as persistent instance
cd "$SCRIPT_DIR"
apptainer instance start \
    --bind "$(pwd):/app" \
    --env PREFECT_UI_API_URL="http://localhost:4200/api" \
    "$CONTAINER" \
    "$INSTANCE_NAME"

# Wait a moment for server to start
sleep 3

# Check if running
if apptainer instance list | grep -q "$INSTANCE_NAME"; then
    echo ""
    echo "✓ Prefect server started successfully"
    echo ""
    echo "Dashboard: http://localhost:4200"
    echo ""
    echo "To view logs: apptainer run $CONTAINER shell"
    echo "To stop:      apptainer instance stop $INSTANCE_NAME"
    echo ""
else
    echo "✗ Failed to start Prefect server"
    exit 1
fi
