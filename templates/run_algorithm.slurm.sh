#!/bin/bash
#SBATCH --job-name=run_{ALGO_NAME}_{DATASET}
#SBATCH --output={RUNNER_DIR}/logs/run_{ALGO_NAME}_{DATASET}_%j.out
#SBATCH --error={RUNNER_DIR}/logs/run_{ALGO_NAME}_{DATASET}_%j.err
#SBATCH --time={TIME}
#SBATCH --cpus-per-task={CPUS}
#SBATCH --mem={MEMORY}
#SBATCH --gres=gpu:{GPUS}
#SBATCH --partition={PARTITION}

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
echo "Algorithm Run Job"
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
SPECTRA_DIR="$DATASET_DIR/mgf"
CONTAINER_FILE="$RUNNER_DIR/containers/$ALGO_NAME/$VERSION/container.sif"
OUTPUT_DIR="$BENCHMARKS_DIR/outputs/$ALGO_NAME/$VERSION/$DATASET"
ALEXANDRIA_OUTPUT_DIR="$ALEXANDRIA_OUTPUTS_PATH/$ALGO_NAME/$VERSION/$DATASET"

echo "Step 1: Validating inputs..."
if [ ! -d "$DATASET_DIR" ]; then
    echo "✗ Dataset not found: $DATASET_DIR"
    exit 1
fi

if [ ! -f "$CONTAINER_FILE" ]; then
    echo "✗ Container not found: $CONTAINER_FILE"
    exit 1
fi

echo "  ✓ Dataset: $DATASET_DIR"
echo "  ✓ Container: $CONTAINER_FILE"
echo

echo "Step 2: Checking for existing output on Alexandria..."
if ssh "$ALEXANDRIA_HOST" "[ -f $ALEXANDRIA_OUTPUT_DIR/output.csv ]"; then
    echo "  ⚠ Output already exists, skipping"
    exit 0
fi
echo "  ✓ No existing output"
echo

echo "Step 3: Preparing output directory..."
mkdir -p "$OUTPUT_DIR"
OUTPUT_FILE="$OUTPUT_DIR/output.csv"
TIME_LOG="$OUTPUT_DIR/time.log"
echo "  ✓ Output directory ready"
echo

echo "Step 4: Running algorithm with writable tmpfs..."
echo "  Running $ALGO_NAME on $DATASET..."
cd "$BENCHMARKS_DIR/algorithms/$ALGO_NAME"

# Set dataset tags path for algorithms that need dataset metadata
DATASET_TAGS_FILE="$BENCHMARKS_DIR/dataset_tags.tsv"

{{ time ( apptainer exec --nv \
    --writable-tmpfs \
    -B "$SPECTRA_DIR":/algo/$DATASET \
    -B "$OUTPUT_DIR":/mnt/output \
    -B "$DATASET_TAGS_FILE":/algo/dataset_tags.tsv:ro \
    --env DATASET_TAGS_PATH=/algo/dataset_tags.tsv \
    "$CONTAINER_FILE" \
    bash -c "cd /algo && ./make_predictions.sh $DATASET && cp /algo/outputs.csv /mnt/output/output.csv" 2>&1 ); }} 2> "$TIME_LOG"

if [ $? -ne 0 ]; then
    echo "✗ Algorithm failed"
    exit 1
fi
echo "✓ Algorithm completed"
echo

echo "Step 5: Augmenting predictions with RT and SA..."
DATASET_DIR="$RUNNER_DIR/datasets/$DATASET"
EVALUATION_CONTAINER="$RUNNER_DIR/evaluation.sif"

if [ -f "$EVALUATION_CONTAINER" ]; then
    apptainer exec \
        -B "$OUTPUT_DIR":/mnt/output/$DATASET \
        -B "$DATASET_DIR":/mnt/dataset \
        -B "$DATASET_TAGS_FILE":/algo/dataset_tags.tsv:ro \
        -B "$BENCHMARKS_DIR":/benchmarks:ro \
        --env DATASET_TAGS_PATH=/algo/dataset_tags.tsv \
        --env PYTHONPATH=/benchmarks \
        "$EVALUATION_CONTAINER" \
        python -m evaluation.augment_predictions --output_dir /mnt/output/$DATASET --data_dir /mnt/dataset || echo "⚠ Augmentation failed (non-critical)"
    echo "✓ Predictions augmented"
else
    echo "⚠ Evaluation container not found, skipping augmentation"
fi
echo

echo "Step 6: Verifying output..."

if [ ! -f "$OUTPUT_FILE" ]; then
    echo "✗ Failed to extract output"
    exit 1
fi
echo "✓ Output extracted: $(du -h "$OUTPUT_FILE" | cut -f1)"
echo

echo "Step 7: Uploading to Alexandria..."
ssh "$ALEXANDRIA_HOST" "mkdir -p $ALEXANDRIA_OUTPUT_DIR"
rsync -avz --progress "$OUTPUT_DIR/" "$ALEXANDRIA_HOST:$ALEXANDRIA_OUTPUT_DIR/"

if [ $? -ne 0 ]; then
    echo "✗ Upload failed"
    exit 1
fi
echo "✓ Uploaded to Alexandria"
echo

echo "Step 8: Cleaning up local output..."
rm -rf "$OUTPUT_DIR"
echo "✓ Cleanup complete"
echo

echo "========================================"
echo "Complete! Output at: $ALEXANDRIA_HOST:$ALEXANDRIA_OUTPUT_DIR/output.csv"
echo "End time: $(date)"
echo "========================================"
