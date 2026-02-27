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

# Build location on Asimov (temporary)
BUILD_DIR="/tmp/container_build_${{SLURM_JOB_ID}}"
CONTAINER_FILE="$BUILD_DIR/container.sif"

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
mkdir -p "$BUILD_DIR"
cd "$BENCHMARKS_DIR"

echo "Step 2: Building container..."
echo "  Using definition: $CONTAINER_DEF"
apptainer build "$CONTAINER_FILE" "$CONTAINER_DEF"

if [ $? -ne 0 ]; then
    echo "✗ Container build failed"
    rm -rf "$BUILD_DIR"
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
    rm -rf "$BUILD_DIR"
    exit 1
fi

echo "✓ Container transferred to Alexandria"
echo

echo "Step 5: Verifying container on Alexandria..."
ssh "$ALEXANDRIA_HOST" "ls -lh $ALEXANDRIA_FULL_PATH/$CONTAINER_FILENAME"

echo "Step 6: Cleaning up temporary build directory..."
rm -rf "$BUILD_DIR"
echo "✓ Cleanup complete"
echo

echo "========================================"
echo "Container Build Complete!"
echo "Location: $ALEXANDRIA_HOST:$ALEXANDRIA_FULL_PATH/$CONTAINER_FILENAME"
echo "End time: $(date)"
echo "========================================"
