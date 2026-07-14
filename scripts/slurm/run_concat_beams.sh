#!/bin/bash
#SBATCH --job-name=concat_ms
#SBATCH --output=logs/concat_%A_%a.out
#SBATCH --error=logs/concat_%A_%a.err
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# -------- User-configurable via --export or edit defaults here --------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
OUT_ROOT=${OUT_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
OUT_SUBDIR=${OUT_SUBDIR:-concat_beam}
PATTERN=${PATTERN:-"20??*/*beam{beam:02d}*.20????????????.avg.calB0.ms"}   # relative under data-root/SBID
PYTHON=${PYTHON:-'apptainer exec --bind ${USER_PATH:-/fred/oz451}:${USER_PATH:-/fred/oz451} ${USER_PATH:-/fred/oz451}/${USER}/containers/flint-containers_casa.sif python3'}
SCRIPT_DIR=${SCRIPT_DIR:-${USER_PATH:-/fred/oz451}/${USER}/scripts/lotrun_processing}
SCRIPT=${SCRIPT:-${SCRIPT_DIR}/src/casa/concat_ms_beams.py}
# ---------------------------------------------------------------------

# Resolve the beam-specific glob by formatting {beam:02d}
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"


mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT OUT_ROOT=$OUT_ROOT BEAM=$SLURM_ARRAY_TASK_ID"

module load apptainer

# ------------------------- EXECUTION LINE -----------------------------
$PYTHON "$SCRIPT" --sbid "$SBID" --data-root "$DATA_ROOT" --out-root "$OUT_ROOT" --out-subdir "$OUT_SUBDIR" --pattern "${glob}" --beam "$SLURM_ARRAY_TASK_ID" ${CONCAT_OPTS:-}
# ---------------------------------------------------------------------
