#!/bin/bash
#SBATCH --job-name=fastducc_periodicity
#SBATCH --output=logs/fastducc_periodicity_%A_%a.out
#SBATCH --error=logs/fastducc_periodicity_%A_%a.err
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --array=0-35

set -euo pipefail

# Environment variables
USE_CONTAINER=${USE_CONTAINER:-False}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/casacore.sif}

SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
# Pattern relative to data-root/SBID; {beam:02d} will be replaced with beam index
PATTERN=${PATTERN:-"20??*/*beam{beam:02d}*.calG9.uvsub.ms"}

# Periodicity search configurations
FDP_DM="${FDP_DM:-0.0}"
FDP_FMIN="${FDP_FMIN:-0.2}"
FDP_NPIX="${FDP_NPIX:-384}"
FDP_PIXSCALE_ARCSEC="${FDP_PIXSCALE_ARCSEC:-22.0}"
FDP_THRESHOLD_SIGMA="${FDP_THRESHOLD_SIGMA:-7.0}"
FDP_NHARM="${FDP_NHARM:-8}"
FDP_PARALLEL_MODE="${FDP_PARALLEL_MODE:-dask-slurm}"
FDP_NTHREADS="${FDP_NTHREADS:-8}"
FDP_SPW="${FDP_SPW:-0}"
FDP_POL="${FDP_POL:-0}"
FDP_DATA_COL="${FDP_DATA_COL:-DATA}"

beam="${SLURM_ARRAY_TASK_ID:-1}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/${beam2}}"

search_glob="${root}/${glob}"

# Expand glob to array
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=${SBID} beam=${beam2} using '${search_glob}'"
  exit 0
fi

echo "Discovered ${#msnames[@]} MS file(s) for periodicity search (SBID=${SBID}, beam=${beam2}):"
for ms in "${msnames[@]}"; do
  echo "  - ${ms}"
done

if [[ "${USE_CONTAINER}" == "True" && -n "${CRYSTALBALL_SIF}" ]]; then
    FASTDUCC=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} fastducc}
else
    module load python-scientific/3.11.5-foss-2023b 2>/dev/null || true
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    FASTDUCC=fastducc
fi

# Run fastducc periodicity on each discovered scan MS
for ms in "${msnames[@]}"; do
  echo "Running fastducc periodicity on: ${ms}"
  
  cmd=( ${FASTDUCC} periodicity
    --msname "${ms}"
    --spw "${FDP_SPW}"
    --pol "${FDP_POL}"
    --data-col "${FDP_DATA_COL}"
    --npix "${FDP_NPIX}"
    --pixscale-arcsec "${FDP_PIXSCALE_ARCSEC}"
    --fmin "${FDP_FMIN}"
    --dm "${FDP_DM}"
    --threshold-sigma "${FDP_THRESHOLD_SIGMA}"
    --nharm "${FDP_NHARM}"
    --nthreads "${FDP_NTHREADS}"
    --parallel-mode "${FDP_PARALLEL_MODE}"
    --write-products
    --detect
  )

  echo "Executing: ${cmd[*]}"
  "${cmd[@]}"
done
