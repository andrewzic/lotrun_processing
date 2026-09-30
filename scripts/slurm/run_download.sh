#!/bin/bash
#SBATCH --job-name=download_uvfits
#SBATCH --output=logs/download_%x_%j.out
#SBATCH --error=logs/download_%x_%j.err
#SBATCH --partition=datamover
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=0

set -euo pipefail

# -------------------- USER CONFIG --------------------
SBID="${SBID:-}"
CASDA_USERNAME="${CASDA_USERNAME:-}"
DOWNLOAD_WORKERS="${DOWNLOAD_WORKERS:-16}"
DATA_ROOT="${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
SCRIPT_DIR="${SCRIPT_DIR:-${USER_PATH:-/fred/oz451}/${USER}/scripts/lotrun_processing}"
DL_SCRIPT="${DL_SCRIPT:-${SCRIPT_DIR}/scripts/utils/download_uvfits.sh}"

mkdir -p logs

if [[ -z "${SBID}" ]]; then
    if [[ -n "${1:-}" ]]; then
        SBID="$1"
    else
        echo "ERROR: SBID is required" >&2
        exit 1
    fi
fi

if [[ -n "${2:-}" ]]; then
    CASDA_USERNAME="$2"
fi

if [[ -n "${3:-}" ]]; then
    DOWNLOAD_WORKERS="$3"
fi

echo "Job ${SLURM_JOB_ID:-manual} starting on $(hostname)"
echo "SBID:             ${SBID}"
echo "CASDA_USERNAME:   ${CASDA_USERNAME}"
echo "DOWNLOAD_WORKERS: ${DOWNLOAD_WORKERS}"
echo "DATA_ROOT:        ${DATA_ROOT}"
echo "DL_SCRIPT:        ${DL_SCRIPT}"

export SBID CASDA_USERNAME DOWNLOAD_WORKERS DATA_ROOT

"${DL_SCRIPT}" "${SBID}" "${CASDA_USERNAME}" "${DOWNLOAD_WORKERS}"
