#!/bin/bash
#SBATCH --job-name=augment_{ALGO_NAME}_{DATASET}
#SBATCH --output={RUNNER_DIR}/logs/augment_{ALGO_NAME}_{DATASET}_%j.out
#SBATCH --error={RUNNER_DIR}/logs/augment_{ALGO_NAME}_{DATASET}_%j.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --partition=one_hour

set -e

# Clear ALL Apptainer environment variables inherited from Prefect worker
unset APPTAINER_BIND
unset APPTAINER_BINDPATH
unset APPTAINERENV_BIND
unset SINGULARITY_BIND
unset SINGULARITY_BINDPATH
unset SINGULARITYENV_BIND
# Also clear any leftover environment exports
for var in $(env | grep -E '^(APPTAINER|SINGULARITY)' | cut -d= -f1); do
    unset "$var"
done

echo "========================================"
echo "Augment Existing Output"
echo "Algorithm: {ALGO_NAME}"
echo "Version: {VERSION}"
echo "Dataset: {DATASET}"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "Start time: $(date)"
echo "========================================"
echo

# Directories
RUNNER_DIR="{RUNNER_DIR}"
BENCHMARKS_DIR="{BENCHMARKS_DIR}"
ALGO_NAME="{ALGO_NAME}"
VERSION="{VERSION}"
DATASET="{DATASET}"
ALEXANDRIA_HOST="{ALEXANDRIA_HOST}"
ALEXANDRIA_OUTPUTS_PATH="{ALEXANDRIA_OUTPUTS_PATH}"

# Paths
DATASET_DIR="$RUNNER_DIR/datasets/$DATASET"
EVALUATION_CONTAINER="$RUNNER_DIR/evaluation.sif"
WORK_DIR="$RUNNER_DIR/augment_work/${{SLURM_JOB_ID}}"
OUTPUT_DIR="$WORK_DIR/output"
ALEXANDRIA_OUTPUT_DIR="$ALEXANDRIA_OUTPUTS_PATH/$ALGO_NAME/$VERSION/$DATASET"
DATASET_TAGS_FILE="$BENCHMARKS_DIR/dataset_tags.tsv"

# Create work directory
mkdir -p "$OUTPUT_DIR"

echo "Step 1: Validating inputs..."
if [ ! -f "$EVALUATION_CONTAINER" ]; then
    echo "✗ Evaluation container not found: $EVALUATION_CONTAINER"
    exit 1
fi
echo "  ✓ Evaluation container: $EVALUATION_CONTAINER"

if [ ! -d "$DATASET_DIR" ]; then
    echo "✗ Dataset not found: $DATASET_DIR"
    exit 1
fi
echo "  ✓ Dataset: $DATASET_DIR"

if [ ! -f "$DATASET_TAGS_FILE" ]; then
    echo "✗ Dataset tags file not found: $DATASET_TAGS_FILE"
    exit 1
fi
echo "  ✓ Dataset tags: $DATASET_TAGS_FILE"
echo

echo "Step 2: Pulling output from Alexandria..."
rsync -az "$ALEXANDRIA_HOST:$ALEXANDRIA_OUTPUT_DIR/output.csv" "$OUTPUT_DIR/output.csv"
if [ $? -ne 0 ]; then
    echo "✗ Failed to pull output"
    exit 1
fi
echo "✓ Output pulled"
echo

echo "Step 3: Running augmentation..."
apptainer exec \
    -B "$OUTPUT_DIR":/mnt/output/$DATASET \
    -B "$DATASET_DIR":/mnt/dataset \
    -B "$DATASET_TAGS_FILE":/algo/dataset_tags.tsv:ro \
    -B "$BENCHMARKS_DIR":/benchmarks:ro \
    --env DATASET_TAGS_PATH=/algo/dataset_tags.tsv \
    --env PYTHONPATH=/benchmarks \
    "$EVALUATION_CONTAINER" \
    python -m evaluation.augment_predictions --output_dir /mnt/output/$DATASET --data_dir /mnt/dataset

if [ $? -ne 0 ]; then
    echo "✗ Augmentation failed"
    exit 1
fi
echo "✓ Augmentation complete"
echo

echo "Step 4: Uploading augmented output..."
rsync -az "$OUTPUT_DIR/output.csv" "$ALEXANDRIA_HOST:$ALEXANDRIA_OUTPUT_DIR/output.csv"
if [ $? -ne 0 ]; then
    echo "✗ Failed to upload"
    exit 1
fi
echo "✓ Uploaded to Alexandria"
echo

echo "Step 5: Cleaning up..."
rm -rf "$WORK_DIR"
echo "✓ Cleanup complete"
echo

echo "========================================"
echo "Complete! Augmented: $ALEXANDRIA_OUTPUT_DIR/output.csv"
echo "End time: $(date)"
echo "========================================"
