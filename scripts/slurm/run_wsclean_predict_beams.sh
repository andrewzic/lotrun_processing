#!/bin/bash
#SBATCH --job-name=wsclean_predict
#SBATCH --output=logs/wsclean_predict_%A_%a.out
#SBATCH --error=logs/wsclean_predict_%A_%a.err
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --tmp=20GB
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=${SBID:-SB82418}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}
MODEL_MS_PATTERN=${MODEL_MS_PATTERN:-${SOURCE_LIST_PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}}
BIND_SRC=${BIND_SRC:-${USER_PATH:-/fred/oz451}}
FLINT_WSCLEAN_SIF=${FLINT_WSCLEAN_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/flint-containers_wsclean.sif}
CB_SUBDIR="${CB_SUBDIR}"
MODEL_MS_SUBDIR=${MODEL_MS_SUBDIR:-${CB_SRCLIST_SUBDIR:-"cont_combined"}}
IMG_TAG=${IMG_TAG:-"initial"}
INDEX=${INDEX:-0}
SELFCAL=${SELFCAL:-1}
CHANNELS_OUT=${CHANNELS_OUT:-4}

# Allow passing arbitrary WSClean predict options if needed
WSCLEAN_PREDICT_OPTS=${WSCLEAN_PREDICT_OPTS:-""}

# ---------------------------------------------------------------------------

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Container: ${FLINT_WSCLEAN_SIF}; Bind: ${BIND_SRC}"

module load apptainer

# Format beam index and glob pattern
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}/${CB_SUBDIR}"
root_model="${DATA_ROOT}/${SBID}/${MODEL_MS_SUBDIR}"
glob="${PATTERN//\{beam:02d\}/$beam2}"

if (( SELFCAL == 1 ))
then
    if (( INDEX > 0 )); then
	glob2="${glob/calB0/selfcal_${INDEX}}"
        glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" #catch all for continuum    
    else
	glob2="${glob}"
    fi
    #if in selfcal mode, match the model to the measurement set
    model_ms_glob="${glob2}"
else
    if (( INDEX > 0 )); then
	glob2="${glob/calB0/calG${INDEX}}"
        glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" #catch all for continuum
	model_ms_glob_="${MODEL_MS_PATTERN//\{beam:02d\}/$beam2}"
	model_ms_glob="${model_ms_glob_/calB0/selfcal_${INDEX}}"
    else
	glob2="${glob}"
	model_ms_glob="${MODEL_MS_PATTERN//\{beam:02d\}/$beam2}"	
    fi
fi

search_glob="${root}/${glob2}"
model_search_glob="${root_model}/${model_ms_glob}"

# Discover MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

shopt -s nullglob
model_msnames=( ${model_search_glob} )
shopt -u nullglob

echo "Looking for MS files with glob: ${search_glob}"
echo "Looking for model MS files with glob: ${model_search_glob}"
echo "Found ${#msnames[@]} MS files and ${#model_msnames[@]} model MS files for beam ${beam2}."

# Note: model_ms is the path to the original MS used during the imaging step.
# It does NOT store the model itself, but is used here solely to reconstruct 
# the prefix (e.g. model_msname.IMG_TAG_img) of the WSClean model .fits images.
# The 'ms' variable in the loop below is the actual MS into which we are predicting the model.
# Assume there is only one model MS relevant per beam
if [[ ${#model_msnames[@]} -eq 0 ]]; then
  echo "WARN: No model MS found for SBID=$SBID beam=${beam2} using '${model_search_glob}'"
  exit 1
fi
model_msname=${model_msnames[0]}

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
  exit 1
fi

for ms in "${msnames[@]}"; do
  outname="${model_msname%.ms}.${IMG_TAG}_img"

  echo "Predicting model -> MS=${ms}"
  echo "Using model image prefix: ${outname}"

  # Verify that the model images actually exist
  if ! ls "${outname}"-*-model.fits 1> /dev/null 2>&1; then
      echo "ERROR: Could not find any model images matching ${outname}-*-model.fits"
      echo "Make sure that wsclean imaging was run without deleting the per-channel model images."
      exit 1
  fi

  # Execute wsclean predict inside container
  echo "running:"
  echo "apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${FLINT_WSCLEAN_SIF} wsclean -predict -channels-out -pol xx ${CHANNELS_OUT} ${WSCLEAN_PREDICT_OPTS} -name ${outname} ${ms}"
  apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${FLINT_WSCLEAN_SIF}" wsclean -predict -pol xx -channels-out "${CHANNELS_OUT}" ${WSCLEAN_PREDICT_OPTS} -name "${outname}" "${ms}"
done
