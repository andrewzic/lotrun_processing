#!/bin/bash
#SBATCH --job-name=bandpass_ms
#SBATCH --output=logs/bandpass_%A_%a.out
#SBATCH --error=logs/bandpass_%A_%a.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

module load apptainer

# -------- User-configurable via --export or edit defaults here --------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/azic/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.ms"}
CAL_DIR=${CAL_DIR:-cal}
EXTENSION=${EXTENSION:-"B0"}
SCRIPT=${SCRIPT:-applycal_ms_beams.py}
# Apptainer CASA container (flint-containers_casa) default runner:
CASA_SIF=${CASA_SIF:-/fred/oz451/azic/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
PYTHON=${PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CASA_SIF} python3}
# ---------------------------------------------------------------------

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT CAL_DIR=$CAL_DIR BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Container: ${CASA_SIF}; Bind: ${BIND_SRC}"

# Execute inside the CASA container
$PYTHON "$SCRIPT" --sbid "$SBID" --data-root "$DATA_ROOT" --pattern "$PATTERN" --cal-dir "$CAL_DIR" --extension "${EXTENSION}" --beam "$SLURM_ARRAY_TASK_ID"
