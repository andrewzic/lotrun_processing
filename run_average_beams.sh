#!/bin/bash
#SBATCH --job-name=average_array
#SBATCH --output=logs/average_%A_%a.out
#SBATCH --error=logs/average_%A_%a.err
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=12G
#SBATCH --array=0-323 #usually about 350 craco ms per obs
# Optional: set your partition/queue
# #SBATCH --partition=standard
# Optional: limit concurrency to avoid filesystem contention
# #SBATCH --array=0-99%10

set -euo pipefail

# -------------------- USER CONFIG --------------------
# Path to the list of MS files (one per line)

SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"20??*/*beam*.20????????????.calB0.ms"}   # relative under data-root/SBID
#SCRIPT_DIR=${SCRIPT_DIR:-/fred/oz451/${USER}/scripts/lotrun_processing}
SCRIPT=${SCRIPT:-average_ms_beams.py}
CASA_SIF=${CASA_SIF:-/fred/oz451/${USER}/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
PYTHON=${PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CASA_SIF} python3}

# Column to use in average ("DATA" default)
TIMEBIN=${TIMEBIN:-"9.90s"}

# -----------------------------------------------------

root="${DATA_ROOT}/${SBID}"
glob="${PATTERN}"
search_glob="${root}/${glob}"

# Expand glob to list of MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

# Create log dir if not exists
mkdir -p logs

# # Check prerequisites
# if [[ ! -f "$MS_LIST_FILE" ]]; then
#     echo "ERROR: MS_LIST_FILE '$MS_LIST_FILE' not found." >&2
#     exit 1
# fi

if [[ ! -f "$SCRIPT" ]]; then
    echo "ERROR: SCRIPT '$SCRIPT' not found." >&2
    exit 1
fi

# Pick the MS for this array index
MSFILE=${msnames[${SLURM_ARRAY_TASK_ID}]} #$(sed -n "$((SLURM_ARRAY_TASK_ID+1))p" "$MS_LIST_FILE" || true)

# if [[ -z "${MSFILE:-}" ]]; then
#     echo "ERROR: No entry in '$MS_LIST_FILE' for SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID" >&2
#     exit 1
# fi

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} starting on $(hostname)"
echo "Using MS: $MSFILE"
echo "Averaging to time: $TIMEBIN"

# Load aoflagger if your cluster uses modules (uncomment/adapt as needed)
#module load aoflagger
module load apptainer

# Run the averaging
$PYTHON "$SCRIPT" --ms "$MSFILE" --timebin "${TIMEBIN}"

