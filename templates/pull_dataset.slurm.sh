#!/bin/bash
#SBATCH --job-name=pull_{DATASET_NAME}
#SBATCH --output={RUNNER_DIR}/logs/pull_{DATASET_NAME}_%j.out
#SBATCH --error={RUNNER_DIR}/logs/pull_{DATASET_NAME}_%j.err
#SBATCH --time={TIME}
#SBATCH --cpus-per-task={CPUS}
#SBATCH --mem={MEMORY}
#SBATCH --partition={PARTITION}

set -e

echo "========================================"
echo "Dataset Pull Job"
echo "Dataset: {DATASET_NAME}"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Start time: $(date)"
echo "========================================"
echo

# Paths
RUNNER_DIR="{RUNNER_DIR}"
DATASETS_DIR="{DATASETS_DIR}"
DATASET_NAME="{DATASET_NAME}"
ALEXANDRIA_HOST="{ALEXANDRIA_HOST}"
ALEXANDRIA_PATH="{ALEXANDRIA_PATH}"

# Local dataset path
LOCAL_DATASET_PATH="$DATASETS_DIR/$DATASET_NAME"
ALEXANDRIA_DATASET_PATH="$ALEXANDRIA_PATH/$DATASET_NAME"

echo "Step 1: Preparing dataset directory..."
mkdir -p "$DATASETS_DIR"

echo "Step 2: Checking if dataset exists on Alexandria..."
ssh "$ALEXANDRIA_HOST" "test -d $ALEXANDRIA_DATASET_PATH"
if [ $? -ne 0 ]; then
    echo "✗ Dataset not found on Alexandria: $ALEXANDRIA_DATASET_PATH"
    exit 1
fi

echo "✓ Dataset found on Alexandria"
echo

echo "Step 3: Pulling dataset from Alexandria..."
echo "  Source: $ALEXANDRIA_HOST:$ALEXANDRIA_DATASET_PATH"
echo "  Destination: $LOCAL_DATASET_PATH"
rsync -avz --progress "$ALEXANDRIA_HOST:$ALEXANDRIA_DATASET_PATH/" "$LOCAL_DATASET_PATH/"

if [ $? -ne 0 ]; then
    echo "✗ Dataset pull failed"
    rm -rf "$LOCAL_DATASET_PATH"
    exit 1
fi

echo "✓ Dataset pulled successfully"
echo

echo "Step 4: Verifying dataset..."
if [ ! -d "$LOCAL_DATASET_PATH" ]; then
    echo "✗ Dataset directory not found after pull"
    exit 1
fi

# Check for expected structure (labels.csv and mgf/)
if [ ! -f "$LOCAL_DATASET_PATH/labels.csv" ]; then
    echo "⚠ Warning: labels.csv not found in dataset"
fi

if [ ! -d "$LOCAL_DATASET_PATH/mgf" ]; then
    echo "⚠ Warning: mgf/ directory not found in dataset"
fi

echo "✓ Dataset structure verified"
ls -lh "$LOCAL_DATASET_PATH"
echo

echo "Step 5: Computing dataset size..."
DATASET_SIZE=$(du -sh "$LOCAL_DATASET_PATH" | cut -f1)
echo "  Dataset size: $DATASET_SIZE"
echo

echo "========================================"
echo "Dataset Pull Complete!"
echo "Dataset: $DATASET_NAME"
echo "Location: $LOCAL_DATASET_PATH"
echo "Size: $DATASET_SIZE"
echo "End time: $(date)"
echo "========================================"
