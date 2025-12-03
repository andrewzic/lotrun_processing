#!/bin/bash
#SBATCH --job-name=uvsub_ms
#SBATCH --output=logs/uvsub_%A_%a.out
#SBATCH --error=logs/uvsub_%A_%a.err
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=12G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/azic/data}
EXTENSION=${EXTENSION:-"B0"}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.cal${EXTENSION}.ms"}  # relative under data-root/SBID
FLINT_CASA_SIF=${FLINT_CASA_SIF:-/fred/oz451/azic/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
SCRIPT=${SCRIPT:-uvsub_ms_beams.py}

# uvsub parameters (override per submission as needed)
OUT_PREFIX=${OUT_PREFIX:-"uvsub"}
INDEX=${INDEX:-1}
# ---------------------------------------------------------------------------

module load apptainer


# Format beam index and glob pattern
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"
if (( INDEX > 0 )); then
    glob2="${glob/calB0/selfcal_${INDEX}}"
else
    glob2="${glob}"
fi
search_glob="${root}/${glob2}"

# Discover MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

# beam="${SLURM_ARRAY_TASK_ID}"
# printf -v beam2 "%02d" "${beam}"
# root="${DATA_ROOT}/${SBID}"
# glob="${PATTERN//\{beam:02d\}/$beam2}"
# search_glob="${root}/${glob}"

# shopt -s nullglob
# msnames=( ${search_glob} )
# shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
  exit 0
fi

for ms in "${msnames[@]}"; do
  echo "uvsub on: ${ms}"
  apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${FLINT_CASA_SIF}" \
    python3 "${SCRIPT}" \
      --ms "${ms}" \
      --index "${INDEX}" \
      --out-prefix "${OUT_PREFIX}" \
done
