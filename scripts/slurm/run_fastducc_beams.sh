#!/bin/bash
#SBATCH --job-name=fastducc_ms
#SBATCH --output=logs/fastducc_%A_%a.out
#SBATCH --error=logs/fastducc_%A_%a.err
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G
#SBATCH --array=0-35

set -euo pipefail

# environment variables
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-}

SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
EXTENSION=${EXTENSION:-"B0"}
# pattern relative to data-root/SBID; {beam:02d} will be replaced with the beam index
PATTERN=${PATTERN:-"*beam{beam:02d}*.cal${EXTENSION}.ms"}

OUT_PREFIX=${OUT_PREFIX:-"uvsub"}  # not used by fastducc; kept for compatibility/logging
INDEX=${INDEX:-1}
SELFCAL=${SELFCAL:-1}

# options for search/no search/plot_cands/only
FD_NO_VAR_SEARCH=${FD_NO_VAR_SEARCH:-}
FD_NO_BOX_SEARCH=${FD_NO_BOX_SEARCH:-}
FD_PLOT_CANDS_ONLY=${FD_PLOT_CANDS_ONLY:-}

beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/${beam2}}"

if (( SELFCAL == 1 )); then
  if (( INDEX > 0 )); then
    glob2="${glob/calB0/selfcal_${INDEX}}"
    glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" # catch all for continuum
  else
    glob2="${glob}"
  fi
else
  if (( INDEX > 0 )); then
    glob2="${glob/calB0/calG${INDEX}}"
    glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" # catch all for continuum
  else
    glob2="${glob}"
  fi
fi

search_glob="${root}/${glob2}"

# Expand glob to array
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=${SBID} beam=${beam2} using '${search_glob}'"
  exit 0
fi

echo "Discovered ${#msnames[@]} MS file(s) for SBID=${SBID}, beam=${beam2}:"
for ms in "${msnames[@]}"; do
  echo "  - ${ms}"
done

if [ -n "${CRYSTALBALL_SIF}" ]; then
    # container mode
    FASTDUCC=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} fastducc}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    FASTDUCC=${CRYSTALBALL:-fastducc}
fi

# ---------------------- Run fastducc on each MS ----------------------------
for ms in "${msnames[@]}"; do
  echo "Running fastducc on: ${ms}"
  
  cmd=( ${FASTDUCC} --msname "${ms}"
    --chunk-size 512
    --corr-mode single
    --basis linear
    --single-pol XX
    --npix-x 384
    --npix-y 384
    --pixsize-arcsec 22.0
    --threshold-sigma 8.0
    --boxcar-widths 1 2 4 8 12 16 24 32 48 64 96 128
    --var-threshold-sigma 8.0
    --parallel-mode dask-slurm
    --dask-workers 16
    --slurm-cores-per-worker 1
    --slurm-mem 32GB
    --slurm-walltime 00:45:00
    --continuum-dir "${root}/continuum_images/"
  )

  if [[ -n "${FD_NO_VAR_SEARCH}" ]]; then
    cmd+=( --no-var-search )
  fi
  if [[ -n "${FD_NO_BOX_SEARCH}" ]]; then
    cmd+=( --no-boxcar-search )
  fi
  if [[ -n "${FD_PLOT_CANDS_ONLY}" ]]; then
    cmd+=( --plot-cands-only )
  fi

  echo "Running command: ${cmd[*]}"
  "${cmd[@]}"
done

#   fastducc --msname "${ms}" \
#   --chunk-size 512 \
#   --corr-mode single \
#   --basis linear \
#   --single-pol XX \
#   --npix-x 384 \
#   --npix-y 384 \
#   --pixsize-arcsec 22.0 \
#   --threshold-sigma 8.0 \
#   --boxcar-widths 1 2 4 8 12 16 24 32 48 64 96 128 \
#   --var-threshold-sigma 8.0 \
#   --parallel-mode dask-slurm \
#   --dask-workers 16 \
#   --slurm-cores-per-worker 1 \
#   --slurm-mem 32GB \
#   --slurm-walltime 00:45:00 \
#   --continuum-dir "${root}/continuum_images/"
  
# done

