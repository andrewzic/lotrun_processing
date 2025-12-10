#!/bin/bash
#SBATCH --job-name=importuvfits_array
#SBATCH --output=logs/importuvfits_%A_%a.out
#SBATCH --error=logs/importuvfits_%A_%a.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=1G
#SBATCH --array=0-500
# Optional: set your partition/queue
# #SBATCH --partition=standard
# Optional: limit concurrency to avoid filesystem contention
# #SBATCH --array=0-500%10

set -euo pipefail

# -------------------- USER CONFIG --------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
UVFITS_PATTERN=${UVFITS_PATTERN:-"20??*/*beam*.20????????????*.uvfits"}
IMPORT_SCRIPT=${IMPORT_SCRIPT:-${PWD}/import_array.py}
FLINT_CASA_SIF=${FLINT_CASA_SIF:-/fred/oz451/${USER}/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
PYTHON=${PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${FLINT_CASA_SIF} python3}
# -----------------------------------------------------

root="${DATA_ROOT}/${SBID}"
search_glob="${root}/${UVFITS_PATTERN}"

mkdir -p logs

shopt -s nullglob
uvfits=( ${search_glob} )
shopt -u nullglob

if [[ ! -d "${root}" ]]; then
    echo "ERROR: Data root '${root}' not found." >&2
    exit 1
fi

if [[ ! -f "${IMPORT_SCRIPT}" ]]; then
    echo "ERROR: IMPORT_SCRIPT '${IMPORT_SCRIPT}' not found." >&2
    exit 1
fi

if [[ ${#uvfits[@]} -eq 0 ]]; then
    echo "ERROR: No .uvfits files matched pattern: '${search_glob}'" >&2
    exit 1
fi

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} starting on $(hostname)"
echo "SBID:          ${SBID}"
echo "Data root:     ${root}"
echo "Pattern:       ${UVFITS_PATTERN}"
echo "Files found:   ${#uvfits[@]}"
echo "Import script: ${IMPORT_SCRIPT}"
echo "Array index:   ${SLURM_ARRAY_TASK_ID}"

if (( SLURM_ARRAY_TASK_ID >= ${#uvfits[@]} )); then
    echo "Index ${SLURM_ARRAY_TASK_ID} out of range for ${#uvfits[@]} files - skipping."
    exit 0
fi

module load apptainer

$PYTHON "${IMPORT_SCRIPT}" -i "${SLURM_ARRAY_TASK_ID}" -f "${uvfits[@]}"

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} completed."
