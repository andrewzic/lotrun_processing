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
  "importuvfits"
  "quack_native"
  "flag_native"
  "bandpass_B0"
  "flag_calB0"
  "average"
  "flag_avg"
  "concat"
  "wsclean_initial_scratch"
  "flintmask_initial_scratch"
)
for r in "${!SC_INDEX[@]}"; do
  img_tag="${IMG_TAGS[$((r+1))]}"
  PIPELINE_STAGES+=( "wsclean_${img_tag}" "flintmask_${img_tag}" "selfcal_${img_tag}" )
done
PIPELINE_STAGES+=(
  "wsclean_${IMG_TAGS[6]}_final"
  "copy_continuum"
  "flintmask_${IMG_TAGS[6]}_final"
  "uvsub_concat"
  "applycal_native"
  "predict_native"
  "uvsub_native"
  "concat_native"
  "wsclean_native"
  "fastducc"
  "fastducc_aggregate_chunks"
)
if [[ -n "${RUN_FASTDUCC_OBSAGG:-}" ]]; then
  PIPELINE_STAGES+=( "fastducc_obsagg" )
fi
for kind in "boxcar"; do
  PIPELINE_STAGES+=( "dstools_extract_${kind}" )
done

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
jid_imp=$( sbatch_submit "importuvfits" "${IMPORT_TIME}" "${IMPORT_CPUS}" "${IMPORT_MEM}" "${BIGARRAY_SPEC}" "${RUN_IMPORT}" "${jid_start}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" UVFITS_PATTERN="${UVFITS_PATTERN}" IMPORT_SCRIPT="${IMPORT_SCRIPT}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" CLOBBER="${CLOBBER}" )
jid_imp=$(chain "$jid_imp" "importuvfits")

jid_quack=$( sbatch_submit "quack_native" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_QUACK}" "$jid_imp" \
SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_QUACK_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_quack=$( chain "$jid_quack" "quack_native" )

# 3) flag native-resolution MS
jid_fl1=$( sbatch_submit "flag_native" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_quack}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_NATIVE_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_fl1=$(chain "$jid_fl1" "flag_native")

# 4) apply bandpass (B0) to native res
jid_ac1=$( sbatch_submit "bandpass_B0" "${APPLYCAL_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_BANDPASS}" "${jid_fl1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${BANDPASS_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
           SCRIPT="${BANDPASS_SCRIPT}" CAL_DIR="cal" EXTENSION="B0" DELETE_PREVIOUS="--delete-previous" )
jid_ac1=$(chain "$jid_ac1" "bandpass_B0")

# 5) flag after B0
jid_fl2=$( sbatch_submit "flag_calB0" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_ac1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_CALB0_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_fl2=$(chain "$jid_fl2" "flag_calB0")

# 6) average MS
jid_av1=$( sbatch_submit "average" "${AVERAGE_TIME}" "${AVERAGE_CPUS}" "${AVERAGE_MEM}" "${BIGARRAY_SPEC}" "${RUN_AVERAGE}" "${jid_fl2}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${AVERAGE_INPUT_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${AVERAGE_SCRIPT}" PYTHON="${AVERAGE_PYTHON}" TIMEBIN="${TIMEBIN}" )
jid_av1=$(chain "$jid_av1" "average")

# 7) flag averaged MS
jid_fl3=$( sbatch_submit "flag_avg" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_av1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_AVG_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_fl3=$(chain "$jid_fl3" "flag_avg")

# 8) concat beams (averaged)
jid_cat=$( sbatch_submit "concat" "${CONCAT_CONT_TIME}" "${CONCAT_CPUS}" "${CONCAT_MEM}" "${ARRAY_SPEC}" "${RUN_CONCAT}" "${jid_fl3}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" OUT_ROOT="${CONT_OUT_ROOT}" OUT_SUBDIR="${CONT_OUT_SUBDIR}" PATTERN="${CONCAT_AVG_INPUT_PATTERN}" \
           PYTHON="${CONCAT_PYTHON}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${CONCAT_SCRIPT}" )
jid_cat=$(chain "$jid_cat" "concat")

# -------------------- Imaging / Mask / Predict / Selfcal (looped) --------------------
# We operate on concatenated MS: *beam{beam:02d}.avg.calB0.ms
# Initial scratch image/mask/predict before the rounds loop
jid_img_init=$( sbatch_submit "wsclean_initial_scratch" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cat}" \
                SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                IMG_TAG="initial_scratch" INDEX="$(( SC_INDEX[0]-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[0]}" )
jid_img_init=$(chain "$jid_img_init" "wsclean_initial_scratch")

jid_fm_init=$( sbatch_submit "flintmask_initial_scratch" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img_init}" \
               SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[0]}" INDEX="$(( SC_INDEX[0]-1 ))" \
               FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
               FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
jid_fm_init=$(chain "$jid_fm_init" "flintmask_initial_scratch")

  # DON'T NEED TO DO CRYSTALBALL IF WSCLEAN RUNS WITH MGAIN
# jid_cb_init=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm_init}" \
#                SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[0]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
#                INDEX="$(( SC_INDEX[0]-1 ))" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" \
#                MODEL_CHUNKS="${CB_MODEL_CHUNKS}" MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_NWORKERS="${CB_DASK_NWORKERS}" \
#                CB_DASK_WORKER_CPUS="${CB_DASK_WORKER_CPUS}" CB_DASK_WORKER_MEM="${CB_DASK_WORKER_MEM}" )
# jid_cb_prev=$(chain "$jid_cb_init" "cb_initial_scratch")
jid_fm_prev=$(chain "$jid_fm_init" "flintmask_initial_scratch")

# Loop over selfcal rounds 1..N using arrays
for r in "${!SC_INDEX[@]}"; do
  idx="${SC_INDEX[$r]}"
  img_tag="${IMG_TAGS[$((r+1))]}"           # selfcal_1..selfcal_6
  prev_tag="${IMG_TAGS[$r]}"                # previous tag for FITS mask reference
  opts="${WSCLEAN_OPTS[$((r+1))]:-${WSCLEAN_OPTS[$r]}}"

  # Image with mask from previous round
  jid_img=$( sbatch_submit "wsclean_${img_tag}" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_fm_prev}" \
             SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" IMG_TAG="${img_tag}" \
             INDEX="$(( idx-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${opts}" FITS_MASK_TAG="${prev_tag}" )
  jid_img=$(chain "$jid_img" "wsclean_${img_tag}")

  # Build mask
  jid_fm=$( sbatch_submit "flintmask_${img_tag}" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img}" \
            SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" \
            FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
            FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
  jid_fm=$(chain "$jid_fm" "flintmask_${img_tag}")

  # DON'T NEED TO DO CRYSTALBALL IF WSCLEAN RUNS WITH MGAIN
  # # Predict
  # jid_cb=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm}" \
  #           SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
  #           INDEX="$(( idx-1 ))" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
  #           MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_NWORKERS="${CB_DASK_NWORKERS}" \
  #           CB_DASK_WORKER_CPUS="${CB_DASK_WORKER_CPUS}" CB_DASK_WORKER_MEM="${CB_DASK_WORKER_MEM}" )
  # jid_cb=$(chain "$jid_cb" "cb_${img_tag}")

  # Self-cal (mode/solint from arrays)
  jid_sc=$( sbatch_submit "selfcal_${img_tag}" "${SC_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_SELFCAL}" "${jid_fm}" \
            SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
            SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${SELFCAL_SCRIPT}" INDEX="${idx}" CALMODE="${SC_CALMODE[$r]}" SOLINT="${SC_SOLINT[$r]}" \
            FIELD="${SC_FIELD}" SPW="${SC_SPW}" REFANT="${SC_REFANT}" COMBINE="${SC_COMBINE}" MINSNR="${SC_MINSNR}" PARANG="${SC_PARANG}" \
            CALTABLE_PREFIX="${SC_PREFIX[$r]}" PLOT_DIR="plots" APPLY_CALWT="${SC_APPLY_CALWT}" )
  jid_fm_prev=$(chain "$jid_sc" "selfcal_${img_tag}")
done

# Final image/mask after last A+P round
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"

jid_img_final=$( sbatch_submit "wsclean_${IMG_TAGS[6]}_final" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_fm_prev}" \
                 SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                 IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[6]}" FITS_MASK_TAG="${IMG_TAGS[6]}" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[6]}_final")

jid_cp_final=$(
  sbatch_submit "copy_continuum" "${COPY_TIME}" "${COPY_CPUS}" "${COPY_MEM}" \
    "${ARRAY_SPEC}" "${RUN_COPY_CONTINUUM}" "${jid_img_final}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}"
)
jid_cp_final=$(chain "$jid_cp_final" "copy_continuum")

# don't care about the copy_continuum job for the purposes of dependencies
jid_fm_final=$( sbatch_submit "flintmask_${IMG_TAGS[6]}_final" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img_final}" \
                SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" \
                FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
                FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
jid_fm_final=$(chain "$jid_fm_final" "flintmask_${IMG_TAGS[6]}_final")

# # Final predict from latest source list (concatenated)
# jid_cb6=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm_final}" \
#           SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
#           INDEX="${last_idx}" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
#           MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_NWORKERS="${CB_DASK_NWORKERS}" \
#           CB_DASK_WORKER_CPUS="${CB_DASK_WORKER_CPUS}" CB_DASK_WORKER_MEM="${CB_DASK_WORKER_MEM}" )
# jid_cb6=$(chain "$jid_cb6" "cb_${IMG_TAGS[6]}")

# UVSUB on concatenated self-cal result (original: G6 inputs + ext B0)
jid_uvsub_concat=$( sbatch_submit "uvsub_concat" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "${jid_fm_final}" \
                    SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${UVSUB_CONCAT_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
                    BIND_SRC="${BIND_SRC}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${UVSUB_SCRIPT}" INDEX="${last_idx}" EXTENSION="B0" \
                    OUT_PREFIX="${UVSUB_OUT_PREFIX}" )
jid_uvsub_concat=$(chain "$jid_uvsub_concat" "uvsub_concat")

# -------------------- Apply selfcal to native res, predict, uvsub --------------------
jid_prev="$jid_uvsub_concat"

#link caltables to native-res directory for applycal script
_caltable_link="${CONT_OUT_ROOT}/${SBID}/${CONT_CALTABLE_DIR}"
_caltable_src="${CONT_OUT_ROOT}/${SBID}/${CONT_OUT_SUBDIR}/${CONT_CALTABLE_DIR}"
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "DRY local: ln -sf ${_caltable_src} ${CONT_OUT_ROOT}/${SBID}" >&2
else
  if [[ -d "${_caltable_link}" ]]; then
    echo "Caltable symlink already exists and is valid: ${_caltable_link}"
  elif [[ -L "${_caltable_link}" ]]; then
    echo "WARN: Dangling symlink at ${_caltable_link}, replacing"
    ln -sf "${_caltable_src}" "${CONT_OUT_ROOT}/${SBID}"
  else
    ln -s "${_caltable_src}" "${CONT_OUT_ROOT}/${SBID}"
  fi
fi

# Applycal loop on native-res: apply all calG<i>
applycal_pattern="${APPLYCAL_NATIVE_START_PATTERN}"
jid_applycal=$( sbatch_submit "applycal_native" "${APPLYCAL_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_APPLYCAL}" "${jid_uvsub_concat}" \
                 SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${applycal_pattern}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
                 SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${APPLYCAL_SCRIPT}" CAL_DIR="${CONT_CALTABLE_DIR}" EXTENSION="G*" DELETE_PREVIOUS="" )
jid_applycal=$(chain "$jid_applycal" "applycal_native")

# Predict from 2h continuum model onto native res (pattern is last produced calG<last_idx>)
# CB_NATIVE_INPUT_PATTERN="${applycal_pattern}"
jid_predict_native=$( sbatch_submit "predict_native" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_applycal}" \
                 SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${CB_NATIVE_INPUT_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
                 CB_SUBDIR="${CB_SUBDIR}" CB_SRCLIST_SUBDIR="${CB_SRCLIST_SUBDIR}" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" INDEX="${last_idx}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
                 MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_MODE="${CB_DASK_MODE}" \
                 CB_DASK_LOCAL_NWORKERS="${CB_DASK_LOCAL_NWORKERS}" CB_DASK_LOCAL_WORKER_CPUS="${CB_DASK_LOCAL_WORKER_CPUS}" CB_DASK_LOCAL_WORKER_MEM="${CB_DASK_LOCAL_WORKER_MEM}" \
                 CB_DASK_SLURM_NWORKERS="${CB_DASK_SLURM_NWORKERS}" CB_DASK_SLURM_WORKER_CPUS="${CB_DASK_SLURM_WORKER_CPUS}" CB_DASK_SLURM_WORKER_MEM="${CB_DASK_SLURM_WORKER_MEM}" \
                 CB_DASK_SLURM_WORKER_TIME="${CB_DASK_SLURM_WORKER_TIME}" CB_DASK_SLURM_ACCOUNT="${CB_DASK_SLURM_ACCOUNT}" CB_DASK_SLURM_PARTITION="${CB_DASK_SLURM_PARTITION}" CB_DASK_SLURM_TMP="${CB_DASK_SLURM_TMP}" \
                 SCRIPT_DIR="${SCRIPT_DIR}" CHANNELS_OUT="${PREDICT_CHANNELS_OUT}" )
jid_predict_native=$(chain "$jid_predict_native" "predict_native")

# UVSUB on native res (G6 calibrated)
jid_uvs_native=$( sbatch_submit "uvsub_native" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "${jid_predict_native}" \
                  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${UVSUB_NATIVE_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
                  BIND_SRC="${BIND_SRC}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${UVSUB_SCRIPT}" INDEX="${last_idx}" EXTENSION="G6" OUT_PREFIX="${UVSUB_OUT_PREFIX}" )
jid_uvs_native=$(chain "$jid_uvs_native" "uvsub_native")

# 8) concat beams (native)
jid_cat_native=$( sbatch_submit "concat_native" "${CONCAT_NATIVE_TIME}" "${CONCAT_CPUS}" "${CONCAT_MEM}" "${ARRAY_SPEC}" "${RUN_CONCAT}" "${jid_uvs_native}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" OUT_ROOT="${NATIVE_OUT_ROOT}" OUT_SUBDIR="${NATIVE_OUT_SUBDIR}" PATTERN="${CONCAT_NATIVE_INPUT_PATTERN}" PYTHON="${CONCAT_PYTHON}" SCRIPT="${CONCAT_SCRIPT}" CONCAT_OPTS="--no-timeaverage" )
jid_cat_native=$(chain "$jid_cat_native" "concat_native")

# quick image of native concatenated uvsubbed visibilities
jid_wsclean_native=$( sbatch_submit "wsclean_native" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cat_native}" \
                      SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_NATIVE_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                      IMG_TAG="native_uvsub" INDEX="0" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_NATIVE_OPTS}" )
jid_wsclean_native=$(chain "$jid_wsclean_native" "wsclean_native")

# fastducc on uvsubbed native MS
jid_fastducc=$( sbatch_submit "fastducc" "${FD_TIME}" "${FD_CPUS}" "${FD_MEM}" "${ARRAY_SPEC}" "${RUN_FASTDUCC}" "${jid_wsclean_native}" \
                SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FASTDUCC_INPUT_PATTERN}" BIND_SRC="${BIND_SRC}" INDEX="${last_idx}" \
                EXTENSION="G6" NO_VAR_SEARCH="${FD_NO_VAR_SEARCH}" NO_BOX_SEARCH="${FD_NO_BOX_SEARCH}" PLOT_CANDS_ONLY="${FD_PLOT_CANDS_ONLY}" )
jid_fastducc=$(chain "$jid_fastducc" "fastducc")

# aggregate per chunk
jid_agg=$( sbatch_submit "fastducc_aggregate_chunks" "${AGG_TIME}" "${AGG_CPUS}" "${AGG_MEM}" "${CHUNK_ARRAY_SPEC}" "${RUN_FASTDUCC_AGG}" "${jid_fastducc}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" CHUNK_GLOB="${FD_CHUNK_GLOB}" )
jid_agg=$(chain "$jid_agg" "fastducc_aggregate_chunks")

# (optional) aggregate observation if wrapper provided
if [[ -n "${RUN_FASTDUCC_OBSAGG:-}" ]]; then
  jid_obs=$( sbatch_submit "fastducc_obsagg" "${AGG_TIME}" "${AGG_CPUS}" "${AGG_MEM}" "" "${RUN_FASTDUCC_OBSAGG}" "${jid_agg}" \
             SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" )
  jid_prev="$jid_obs"
else
  jid_prev="$jid_agg"
fi

# dstools extract-ds for both kinds
for kind in "boxcar"; do
  KIND="$kind"
  jid_prev=$( sbatch_submit "dstools_extract_${kind}" "${EXTRACT_TIME}" "${EXTRACT_CPUS}" "${EXTRACT_MEM}" "" "${RUN_EXTRACT_DS}" "${jid_prev}" \
              SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" KIND="${KIND}" DS_N_WORKERS="${DS_N_WORKERS}" DS_CPUS="${DS_CPUS}" DS_MEM="${DS_MEM}" \
              DS_WALLTIME="${DS_WALLTIME}" DS_MIN_SNR="${DS_MIN_SNR}" DS_QUEUE="${DS_QUEUE}" DS_PROJECT="${DS_PROJECT}" \
              DS_BATCH_SIZE="${DS_BATCH_SIZE}" DS_RETRIES="${DS_RETRIES}" DS_SLEEP_BETWEEN_BATCHES="${DS_SLEEP_BETWEEN_BATCHES}" \
              DS_BEAM_SCOPE="${DS_BEAM_SCOPE}" DS_MATCH_ARCSEC="${DS_MATCH_ARCSEC}" DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE}" \
              DS_DATACOLUMN="${DS_DATACOLUMN}" DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM}" DS_NOFLAG="${DS_NOFLAG}" \
              DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE}" DS_MINUVDIST="${DS_MINUVDIST}" DS_VERBOSE="${DS_VERBOSE}" \
              DS_OVERWRITE="${DS_OVERWRITE}" DS_DRY_RUN="${DS_DRY_RUN}" DS_CATALOGUE="${DS_CATALOGUE}" )
  jid_prev=$(chain "$jid_prev" "dstools_extract_${kind}")
done

echo "Pipeline submitted."
