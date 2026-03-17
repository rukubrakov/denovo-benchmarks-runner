#!/bin/bash
#SBATCH --job-name=build_{ALGO_NAME}
#SBATCH --output={RUNNER_DIR}/logs/build_{ALGO_NAME}_{VERSION}_%j.out
#SBATCH --error={RUNNER_DIR}/logs/build_{ALGO_NAME}_{VERSION}_%j.err
#SBATCH --time={TIME}
#SBATCH --cpus-per-task={CPUS}
#SBATCH --mem={MEMORY}
#SBATCH --partition={PARTITION}

set -e

echo "========================================"
echo "Container Build Job"
echo "Algorithm: {ALGO_NAME}"
echo "Version: {VERSION}"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Start time: $(date)"
echo "========================================"
echo

# Directories
BENCHMARKS_DIR="{BENCHMARKS_DIR}"
RUNNER_DIR="{RUNNER_DIR}"
ALGO_NAME="{ALGO_NAME}"
VERSION="{VERSION}"
CONTAINER_DEF="{CONTAINER_DEF}"
ALEXANDRIA_HOST="{ALEXANDRIA_HOST}"
ALEXANDRIA_PATH="{ALEXANDRIA_PATH}"

BUILD_DIR="$HOME/container_builds/build_${{SLURM_JOB_ID}}"
CONTAINER_FILE="$BUILD_DIR/container.sif"

# Cleanup on exit (success or failure) - set AFTER BUILD_DIR is defined
trap 'rm -rf "$BUILD_DIR" 2>/dev/null || true' EXIT

# Use user's home directory for Apptainer cache but let system TMPDIR handle temp operations
# Setting TMPDIR can cause "no such file or directory" errors in nested temp dir creation
export APPTAINER_CACHEDIR="$BUILD_DIR/cache"
export PIP_CACHE_DIR="$BUILD_DIR/cache/pip"
export PIP_NO_CACHE_DIR=1

# Final location on Alexandria
# Special handling for evaluation container
if [ "$ALGO_NAME" = "evaluation" ]; then
    ALEXANDRIA_FULL_PATH="$ALEXANDRIA_PATH/evaluation"
    CONTAINER_FILENAME="evaluation.sif"
else
    ALEXANDRIA_FULL_PATH="$ALEXANDRIA_PATH/$ALGO_NAME/$VERSION"
    CONTAINER_FILENAME="container.sif"
fi

echo "Step 1: Preparing build environment..."
mkdir -p "$APPTAINER_CACHEDIR" "$PIP_CACHE_DIR"
cd "$BENCHMARKS_DIR"

echo "  Build directory: $BUILD_DIR"

# Clear Apptainer/Singularity environment variables
# These can be inherited from parent container and cause mount issues during build
unset APPTAINER_BIND APPTAINER_BINDPATH
unset SINGULARITY_BIND SINGULARITY_BINDPATH
unset APPTAINER_NAME SINGULARITY_NAME
unset APPTAINER_CONTAINER SINGULARITY_CONTAINER

# Clean up any existing container.sif and overlay files in algorithm directory to avoid copy conflicts
if [ "$ALGO_NAME" != "evaluation" ]; then
    rm -f "algorithms/$ALGO_NAME/container.sif" 2>/dev/null || true
    rm -f "algorithms/$ALGO_NAME"/overlay_*.img 2>/dev/null || true
fi

echo "Step 2: Building container..."
echo "  Using definition: $CONTAINER_DEF"
apptainer build "$CONTAINER_FILE" "$CONTAINER_DEF"

if [ $? -ne 0 ]; then
    echo "✗ Container build failed"
    exit 1
fi

echo "✓ Container built successfully"
ls -lh "$CONTAINER_FILE"
echo

echo "Step 3: Preparing Alexandria directory..."
ssh "$ALEXANDRIA_HOST" "mkdir -p $ALEXANDRIA_FULL_PATH"

echo "Step 4: Transferring container to Alexandria..."
rsync -avz --progress "$CONTAINER_FILE" \
    "$ALEXANDRIA_HOST:$ALEXANDRIA_FULL_PATH/$CONTAINER_FILENAME"

if [ $? -ne 0 ]; then
    echo "✗ Transfer to Alexandria failed"
    exit 1
fi

echo "✓ Container transferred to Alexandria"
echo

echo "Step 5: Verifying container on Alexandria..."
ssh "$ALEXANDRIA_HOST" "ls -lh $ALEXANDRIA_FULL_PATH/$CONTAINER_FILENAME"

echo "Step 6: Cleaning up temporary build directory..."
echo "✓ Will be cleaned by trap on exit"
echo

echo "========================================"
echo "Container Build Complete!"
echo "Location: $ALEXANDRIA_HOST:$ALEXANDRIA_FULL_PATH/$CONTAINER_FILENAME"
echo "End time: $(date)"
echo "========================================"
