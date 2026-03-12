#!/bin/bash
# Build the Prefect Apptainer container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="prefect.sif"

echo "Building Prefect Apptainer container..."
echo "This may take several minutes..."

cd "$SCRIPT_DIR"

# Build container
apptainer build --force "$CONTAINER_NAME" prefect.def

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Container built successfully: $CONTAINER_NAME"
    echo ""
    echo "Next steps:"
    echo "  1. Start server:  ./run_prefect_server.sh"
    echo "  2. Run workflow:  ./run_orchestration.sh"
    echo ""
else
    echo "✗ Container build failed"
    exit 1
fi
