#!/bin/bash
set -euo pipefail
# =============================================================================
# ASKAP high-time-res imaging/selfcal/uvsub/transient pipeline
# =============================================================================

# -------------------- Config loader --------------------
# Usage: ./pipeline.sh SBID=SBXXXXXX CONFIG=/path/to/config.sh [VERBOSE=1]
#   All arguments are optional; environment variables and defaults are used as fallback.
#   Priority: CLI arg > environment variable > default value
#   VERBOSE=1  — after each stage, echo the submitted command name and Slurm job ID to stderr

# Parse KEY=VALUE command-line arguments
for arg in "$@"; do
  case "${arg}" in
    SBID=*)    SBID="${arg#SBID=}" ;;
    CONFIG=*)  CONFIG="${arg#CONFIG=}" ;;
    START_STAGE=*) START_STAGE="${arg#START_STAGE=}" ;;
    VERBOSE=*) VERBOSE="${arg#VERBOSE=}" ;;
    NO_SYMLINK=*) NO_SYMLINK="${arg#NO_SYMLINK=}" ;;
    DRY_RUN=*) DRY_RUN="${arg#DRY_RUN=}" ;;
    *) echo "Unknown argument: ${arg}" >&2; exit 1 ;;
  esac
done
VERBOSE="${VERBOSE:-0}"
START_STAGE="${START_STAGE:-}"

# Default SBID if not provided via CLI or environment
DEFAULT_SBID="${DEFAULT_SBID:-SB77974}"
SBID="${SBID:-${DEFAULT_SBID}}"


# ----------- USER DEFAULTS (ideally edit config.sh to change these) -----------
USER="${USER:-$(whoami)}"
DATA_ROOT="${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
OUT_ROOT="${OUT_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
BIND_SRC="${BIND_SRC:-${USER_PATH:-/fred/oz451}}"
CONTAINER_DIR="${CONTAINER_DIR:-${USER_PATH:-/fred/oz451}/${USER}/containers}"
LOG_DIR="${LOG_DIR:-${USER_PATH:-/fred/oz451}/${USER}/lotrun_processing/logs}"
SCRIPT_DIR="${SCRIPT_DIR:-${USER_PATH:-/fred/oz451}/$USER/scripts/lotrun_processing}"

if [[ "${NO_SYMLINK:-0}" == "0" ]]; then
  echo "doing symlink"
  # 1) symlink uvfits
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    echo "DRY local: ./scripts/utils/symlink_uvfits.sh ${SBID}" >&2
  else
    ${SCRIPT_DIR}/scripts/utils/symlink_uvfits.sh "${SBID}"
  fi
fi

CONFIG="${CONFIG:-config.sh}"
if [[ -f "${CONFIG}" ]]; then
    # shellcheck source=/dev/null
    echo "sourcing config $CONFIG"
    source "${CONFIG}"
    echo "sourced config"
else
  echo "Config file not found: ${CONFIG}"
  echo "Create one (e.g., pipeline.config.sh) or pass CONFIG=/path/to/file"
  exit 1
fi
__DRY_JID_SEQ="${DRY_FAKE_START:-490000}"

# -------------------- STAGE SKIP LIST --------------------
declare -ag PIPELINE_STAGES=(
  "wsclean_${IMG_TAGS[6]}_final"
)

source "$(dirname "$0")/slurm_helpers.sh"

# -------------------- PIPELINE EXECUTION --------------------
mkdir -p logs plots

n_chunks=$( ls -d ${DATA_ROOT}/${SBID}/${CHUNK_GLOB} | wc -l )
if (( n_chunks > 0 ))
then
  CHUNK_ARRAY_SPEC="0-$((n_chunks-1))"
else
  CHUNK_ARRAY_SPEC="0-0"
fi

# Determine BIGARRAY_SPEC from the number of UVFITS matching UVFITS_PATTERN
n=$( ls -1d "${DATA_ROOT}/${SBID}"/${UVFITS_PATTERN} | wc -l )
BIGARRAY_SPEC="0-$((n>0 ? n-1 : 0))"

# 2) import uvfits
echo $SBID
echo $DATA_ROOT

NATIVE_ROOT="${NATIVE_OUT_ROOT}/${SBID}/${NATIVE_OUT_SUBDIR}"
CONT_ROOT="${CONT_OUT_ROOT}/${SBID}/${CONT_OUT_SUBDIR}"

#exit
jid_start=""

# Loop over selfcal rounds 1..N using arrays
for r in "${!SC_INDEX[@]}"; do
  idx="${SC_INDEX[$r]}"
  img_tag="${IMG_TAGS[$((r+1))]}"           # selfcal_1..selfcal_6
  prev_tag="${IMG_TAGS[$r]}"                # previous tag for FITS mask reference
  opts="${WSCLEAN_OPTS[$((r+1))]:-${WSCLEAN_OPTS[$r]}}"

done

# Final image/mask after last A+P round
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"
jid_fm_prev=""
jid_img_final=$( sbatch_submit "wsclean_${IMG_TAGS[6]}_final" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_fm_prev}" \
                 SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                 IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[6]}" FITS_MASK_TAG="${IMG_TAGS[6]}" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[6]}_final")


echo "Pipeline submitted."
