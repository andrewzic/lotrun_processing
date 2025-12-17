#!/bin/bash
#SBATCH --job-name=clearcal_ms
#SBATCH --output=logs/clearcal_%A_%a.out
#SBATCH --error=logs/clearcal_%A_%a.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --array=0-35

set -euo pipefail

module load apptainer

# -------- User-configurable via --export or edit defaults here --------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"20*/*beam{beam:02d}*.calG6.ms"}
SCRIPT=${SCRIPT:-clearcal_ms_beams.py}
# Apptainer CASA container (flint-containers_casa) default runner:
CASA_SIF=${CASA_SIF:-/fred/oz451/${USER}/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
PYTHON=${PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CASA_SIF} python3}
# ---------------------------------------------------------------------

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Container: ${CASA_SIF}; Bind: ${BIND_SRC}"

# Execute inside the CASA container
$PYTHON "$SCRIPT" --sbid "$SBID" --data-root "$DATA_ROOT" --pattern "$PATTERN" --beam "$SLURM_ARRAY_TASK_ID"
