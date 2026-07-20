#!/bin/bash
#SBATCH --job-name=fastducc_agg
#SBATCH --output=logs/fastducc_agg_%A_%a.out
#SBATCH --error=logs/fastducc_agg_%A_%a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G

set -euo pipefail

# Env overrides
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
CHUNK_GLOB=${CHUNK_GLOB:-202?*}

USE_CONTAINER=${USE_CONTAINER:-False}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/casacore.sif}

root="${DATA_ROOT}/${SBID}"

# Discover chunk directories (sub-observations)
shopt -s nullglob
chunkdirs=("${root}"/${CHUNK_GLOB})
shopt -u nullglob

if [[ ${#chunkdirs[@]} -eq 0 ]]; then
  echo "WARN: No chunk directories found under '${root}' matching '${CHUNK_GLOB}'"
  exit 0
fi

idx=${SLURM_ARRAY_TASK_ID:-0}
if (( idx < 0 || idx >= ${#chunkdirs[@]} )); then
  echo "ERROR: SLURM_ARRAY_TASK_ID=${idx} out of range (0..$(( ${#chunkdirs[@]} - 1 )))"
  exit 2
fi

chunkdir="${chunkdirs[$idx]}"
echo "Aggregating candidates for chunkdir: ${chunkdir}"

# Ensure output directory exists
mkdir -p "${chunkdir}/candidates"

# Runtime environment
if [[ "${USE_CONTAINER}" == "True" && -n "${CRYSTALBALL_SIF}" ]]; then
    # container mode
    FASTDUCC=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} fastducc}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    FASTDUCC=${CRYSTALBALL:-fastducc}
fi

# Run both kinds
${FASTDUCC} aggregate --obs-root "${chunkdir}/" --kind boxcar --outdir "${chunkdir}/candidates/"
${FASTDUCC} aggregate --obs-root "${chunkdir}/" --kind variance --outdir "${chunkdir}/candidates/"

echo "Done: ${chunkdir}"
