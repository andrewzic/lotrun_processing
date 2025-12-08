#!/bin/bash
#SBATCH --job-name=selfcal_ms
#SBATCH --output=logs/selfcal_%A_%a.out
#SBATCH --error=logs/selfcal_%A_%a.err
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=12G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}  # relative under data-root/SBID
FLINT_CASA_SIF=${FLINT_CASA_SIF:-/fred/oz451/${USER}/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
SCRIPT=${SCRIPT:-selfcal_ms_beams.py}

# Self-cal parameters (override per submission as needed)
SOLINT=${SOLINT:-"300s"}
INDEX=${INDEX:-1}
FIELD=${FIELD:-""}
CALMODE=${CALMODE:-"p"}
SPW=${SPW:-""}
REFANT=${REFANT:-""}
COMBINE=${COMBINE:-"scan"}
MINSNR=${MINSNR:-3.0}
PARANG=${PARANG:-""}               # set to non-empty (e.g., "1") to enable
CALTABLE_PREFIX=${CALTABLE_PREFIX:-"selfcal_p"}
PLOT_DIR=${PLOT_DIR:-"plots"}
APPLY_CALWT=${APPLY_CALWT:-"True"}
# ---------------------------------------------------------------------------

module load apptainer

mkdir -p logs "${PLOT_DIR}"

beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"
search_glob="${root}/${glob}"

shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
  exit 0
fi

for ms in "${msnames[@]}"; do
  echo "Self-cal (phase-only) on: ${ms}"
  apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${FLINT_CASA_SIF}" \
    python3 "${SCRIPT}" \
      --ms "${ms}" \
      --index "${INDEX}" \
      --solint "${SOLINT}" \
      --calmode "${CALMODE}" \
      --field "${FIELD}" \
      --spw "${SPW}" \
      --refant "${REFANT}" \
      --combine "${COMBINE}" \
      --minsnr "${MINSNR}" \
      --caltable-prefix "${CALTABLE_PREFIX}" \
      --plot-dir "${PLOT_DIR}" \
      $( [[ -n "${PARANG}" ]] && echo "--parang" ) \
      --apply-calwt "${APPLY_CALWT}"
done
