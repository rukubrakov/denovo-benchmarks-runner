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
    --writable-tmpfs \
    --bind "$(pwd):/app" \
    --env PREFECT_UI_API_URL="http://localhost:4200/api" \
    --env PREFECT_HOME=/app/.prefect \
    "$CONTAINER" \
    "$INSTANCE_NAME"

# Install other dependencies if needed
echo "Setting up Python environment..."
apptainer exec instance://$INSTANCE_NAME bash -c "cd /app && /root/.local/bin/uv sync" > /dev/null 2>&1

# Now start Prefect server (uses system-wide installation with writable UI directory)
sleep 2
apptainer exec instance://$INSTANCE_NAME bash -c "cd /app && PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=1 nohup prefect server start --host 0.0.0.0 > /app/.prefect_server.log 2>&1 &"

# Wait for server to start
sleep 8

# Check if running
if apptainer instance list | grep -q "$INSTANCE_NAME" && ss -tlnp 2>/dev/null | grep -q :4200; then
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
    echo "Instance running but server may need more time to start"
    echo "Check with: ss -tlnp | grep 4200"
    exit 1
fi
