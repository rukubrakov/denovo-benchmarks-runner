#!/bin/bash
#SBATCH --job-name=evaluate_{DATASET}
#SBATCH --output={RUNNER_DIR}/logs/evaluate_{DATASET}_%j.out
#SBATCH --error={RUNNER_DIR}/logs/evaluate_{DATASET}_%j.err
#SBATCH --chdir={RUNNER_DIR}
#SBATCH --time={TIME}
#SBATCH --cpus-per-task={CPUS}
#SBATCH --mem={MEMORY}
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

# Set environment variables required by evaluation module (inside container)
export DATASET_TAGS_PATH="/benchmarks/dataset_tags.tsv"
export PROTEOMES_DIR="/benchmarks/sample_data/proteomes"

echo "=========================================="
echo "Evaluation for Dataset: {DATASET}"
echo "=========================================="
echo "Output root dir: {OUTPUT_ROOT_DIR}"
echo "Dataset dir: {DATASET_DIR}"
echo "Results dir: {RESULTS_DIR}"
echo

# Create results directory if it doesn't exist
mkdir -p "{RESULTS_DIR}"

echo "Running evaluation module..."
apptainer exec --fakeroot \
    -B "{BENCHMARKS_DIR}":/benchmarks:ro \
    -B "{OUTPUT_ROOT_DIR}":/mnt/output:ro \
    -B "{DATASET_DIR}":/mnt/datasets/{DATASET}:ro \
    -B "{RESULTS_DIR}":/mnt/results \
    --env DATASET_TAGS_PATH="${{DATASET_TAGS_PATH}}" \
    --env PROTEOMES_DIR="${{PROTEOMES_DIR}}" \
    --env PYTHONPATH=/benchmarks \
    "{EVALUATION_CONTAINER}" \
    python -m evaluation.evaluate \
        /mnt/output \
        /mnt/datasets/{DATASET} \
        --results_dir /mnt/results

if [ $? -ne 0 ]; then
    echo "✗ Evaluation failed"
    exit 1
fi

echo
echo "✓ Evaluation completed successfully!"
echo "Results saved to: {RESULTS_DIR}"
