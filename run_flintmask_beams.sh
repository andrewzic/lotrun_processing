#!/bin/bash
#SBATCH --job-name=flint_mask
#SBATCH --output=logs/flint_mask_%A_%a.out
#SBATCH --error=logs/flint_mask_%A_%a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}    # relative under data-root/SBID
#BIND_SRC=${BIND_SRC:-/fred/oz451}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-/fred/oz451/${USER}/scripts/crystalball_nt/}
IMG_TAG=${IMG_TAG:-"initial"}
INDEX=${INDEX:-0}
SELFCAL=${SELFCAL:-1}

# flint_mask runtime options (all optional; tune as needed)
# flint_masking mask --flood-fill --flood-fill-positive-seed-clip 1.1 --flood-fill-positive-flood-clip 0.7 --flood-fill-use-mac --flood-fill-use-mac-box-size 350 --beam-shape-erode --beam-shape-erode-minimum-response 0.75
FLOOD_FILL_POSITIVE_SEED_CLIP=${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}
FLOOD_FILL_POSITIVE_FLOOD_CLIP=${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}
FLOOD_FILL_MAC_BOX_SIZE=${FLOOD_FILL_MAC_BOX_SIZE:-350}
BEAM_SHAPE_ERODE_MIN_RESPONSE=${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}

FLINT_MASK_OPTIONS="--flood-fill --flood-fill-positive-seed-clip ${FLOOD_FILL_POSITIVE_SEED_CLIP} --flood-fill-positive-flood-clip ${FLOOD_FILL_POSITIVE_FLOOD_CLIP} --flood-fill-use-mac --flood-fill-use-mac-box-size ${FLOOD_FILL_MAC_BOX_SIZE} --beam-shape-erode --beam-shape-erode-minimum-response ${BEAM_SHAPE_ERODE_MIN_RESPONSE}"

# ---------------------------------------------------------------------------

module load python-scientific/3.11.5-foss-2023b
unset PYTHONPATH
source ${CRYSTALBALL_ENV}/bin/activate

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Environment: ${CRYSTALBALL_ENV}"

# Format beam index and glob pattern
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"
if (( SELFCAL == 1 ))
then
    if (( INDEX > 0 )); then
	glob2="${glob/calB0/selfcal_${INDEX}}"
    else
	glob2="${glob}"
    fi
else
    if (( INDEX > 0 )); then
	glob2="${glob/calB0/calG${INDEX}}"
    else
	glob2="${glob}"
    fi
fi
search_glob="${root}/${glob2}"

# Discover MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
  exit 0
fi

for ms in "${msnames[@]}"; do
    # Derive the WSClean MFS image from earlier
    fits_image="${ms%.ms}.${IMG_TAG}_img-MFS-image.fits"
    if [[ ! -f "${fits_image}" ]]; then
	echo "ERROR: MFS fits image not found for MS '${ms}': expected '${fits_image}'"
	exit
    fi
    echo "Deriving fits mask from MFS fits image ${fits_image}"
    # Execute flint_masking CLI inside environment
    echo "running:"
    echo "${CRYSTALBALL_ENV}/bin/flint_masking mask ${fits_image} ${FLINT_MASK_OPTIONS}"     
    ${CRYSTALBALL_ENV}/bin/flint_masking mask  ${fits_image} ${FLINT_MASK_OPTIONS}
done
