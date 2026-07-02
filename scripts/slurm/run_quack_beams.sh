#!/bin/bash
#SBATCH --job-name=unflag_ms
#SBATCH --output=logs/unflag_ms_%A_%a.out
#SBATCH --error=logs/unflag_ms_%A_%a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --array=0-35
set -euo pipefail
module load apptainer
# -------- User-configurable via --export or edit defaults here --------
SBID=${SBID:-SB82418}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}_averaged_cal*.ms"}
SCRIPT_DIR=${SCRIPT_DIR:-${USER_PATH:-/fred/oz451}/${USER}/scripts/lotrun_processing}
SCRIPT=${SCRIPT:-${SCRIPT_DIR}/src/casa/quack_ms_beams.py}
CASA_SIF=${CASA_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-${USER_PATH:-/fred/oz451}}
PYTHON=${PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CASA_SIF} python3}
# ---------------------------------------------------------------------
mkdir -p logs
printf 'Job %s.%s on %s\n' "${SLURM_JOB_ID}" "${SLURM_ARRAY_TASK_ID}" "$(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Container: ${CASA_SIF}; Bind: ${BIND_SRC}"
# Execute inside the CASA container
$PYTHON "$SCRIPT" --sbid "$SBID" --data-root "$DATA_ROOT" --pattern "$PATTERN" --beam "$SLURM_ARRAY_TASK_ID"
