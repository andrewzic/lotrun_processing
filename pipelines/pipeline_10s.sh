#!/bin/bash
set -euo pipefail
# =============================================================================
# ASKAP 10s native-resolution selfcal + uvsub pipeline
# =============================================================================
# This variant operates on the 10s “scienceData.*.beamXX_averaged_cal.*.ms” files
# and skips: bandpass apply, initial flag-after-import, time-averaging, and
# concatenation. Instead it pre-processes by unflagging ANY existing flags and
# then runs aoflagger, before proceeding with the selfcal/imaging/uvsub steps.
# =============================================================================

# Usage: ./pipeline_10s.sh SBID=SBXXXXXX CONFIG=/path/to/config.10s.sh
#   Both arguments are optional; environment variables and defaults are used as fallback.
#   Priority: CLI arg > environment variable > default value

# Parse KEY=VALUE command-line arguments
for arg in "$@"; do
  case "${arg}" in
    SBID=*)   SBID="${arg#SBID=}" ;;
    CONFIG=*) CONFIG="${arg#CONFIG=}" ;;
    *) echo "Unknown argument: ${arg}" >&2; exit 1 ;;
  esac
done

# Default SBID if not provided via CLI or environment
DEFAULT_SBID="${DEFAULT_SBID:-SB77974}"
SBID="${SBID:-${DEFAULT_SBID}}"

# echo "doing symlink 10s"
# # 1) symlink uvfits
# if [[ "${DRY_RUN:-0}" == "1" ]]; then
#   echo "DRY local: ./symlink_uvfits_10s.sh ${SBID}" >&2
# else
#   ./scripts/utils/symlink_uvfits_10s.sh "${SBID}"
# fi

# -------------------- Config loader --------------------
CONFIG="${CONFIG:-config.10s.sh}"
if [[ -f "${CONFIG}" ]]; then
  # shellcheck source=/dev/null
  source "${CONFIG}"
else
  echo "Config file not found: ${CONFIG}"
  echo "Create one (e.g., pipeline_10s.config.sh) or pass CONFIG=/path/to/file"
  exit 1
fi

__DRY_JID_SEQ="${DRY_FAKE_START:-490000}"

source "$(dirname "$0")/slurm_helpers.sh"

# -------------------- PIPELINE --------------------------
mkdir -p logs plots

# 0) Optional: ensure .ms present (if only .ms.tar exist, untar separately before running)
#    This pipeline assumes the .ms have already been extracted alongside the .ms.tar

# A) PRE-PROCESS: fix_dir first
jid_fixdir=$( sbatch_submit "fixdir_ms" "${UNFLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FIXDIR}" "" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${FIXDIR_SCRIPT}" )
jid_fixdir=$(chain "$jid_fixdir" "fixdir_native")

#unflag then AOflagger on native 10s MS
# jid_unflag=$( sbatch_submit "unflag_ms" "${UNFLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_UNFLAG}" "${jid_fixdir}" \
#   SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" SCRIPT_DIR="${SCRIPT_DIR}" )
# jid_unflag=$(chain "$jid_unflag" "unflag_native")

if [ "$FLAG_OUTER" == "1" ]; then 
  jid_flagouter=$( sbatch_submit "flagouter" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FLAGOUTER}" "${jid_fixdir}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT=${FLAGOUTER_SCRIPT} COLUMN="${FLAG_COLUMN}" )
  jid_flagouter=$(chain "$jid_flagouter" "flagouter_native")
  jid_before_flag="$jid_flagouter"
else
  jid_before_flag="$jid_fixdir"
fi

jid_quack=$( sbatch_submit "quack_ms" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_QUACK}" "$jid_before_flag" \
SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_quack=$( chain "$jid_quack" "quack_native" )


jid_flag=$( sbatch_submit "aoflagger_array" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FLAG}" "$jid_quack" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_flag=$(chain "$jid_flag" "aoflagger_native")

# B) Imaging / Mask / Predict / Selfcal (on native 10s MS)
# Initial scratch
jid_img_init=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_flag" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
  IMG_TAG="initial_scratch" INDEX="0" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[0]}" )
jid_img_init=$(chain "$jid_img_init" "wsclean_initial_scratch")

jid_fm_init=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "$jid_img_init" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="initial_scratch" INDEX="0" \
  FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}" \
  FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE:-350}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}" )
jid_fm_init=$(chain "$jid_fm_init" "flintmask_initial_scratch")

# jid_cb_init=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "$jid_fm_init" \
#   SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="initial_scratch" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
#   INDEX="0" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
#   MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_NWORKERS="${CB_DASK_NWORKERS}" CB_DASK_WORKER_CPUS="${CB_DASK_WORKER_CPUS}" \
#   CB_DASK_WORKER_MEM="${CB_DASK_WORKER_MEM}" )
# jid_prev=$(chain "$jid_cb_init" "cb_initial_scratch")
jid_prev=$jid_fm_init

# Selfcal rounds
for r in "${!SC_INDEX[@]}"; do
  idx="${SC_INDEX[$r]}"
  img_tag="${IMG_TAGS[$((r+1))]}"
  prev_tag="${IMG_TAGS[$r]}"
  opts="${WSCLEAN_OPTS[$((r+1))]:-${WSCLEAN_OPTS[$r]}}"

  jid_img=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
    IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${opts}" FITS_MASK_TAG="${prev_tag}" )
  jid_img=$(chain "$jid_img" "wsclean_${img_tag}")

  jid_fm=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "$jid_img" \
    SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" \
    FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}" \
    FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE:-350}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}" )
  jid_fm=$(chain "$jid_fm" "flintmask_${img_tag}")

  # jid_cb=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "$jid_fm" \
  #   SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
  #   INDEX="$(( idx-1 ))" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
  #   MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_NWORKERS="${CB_DASK_NWORKERS}" CB_DASK_WORKER_CPUS="${CB_DASK_WORKER_CPUS}" \
  #   CB_DASK_WORKER_MEM="${CB_DASK_WORKER_MEM}" )
  # jid_prev=$(chain "$jid_cb" "cb_${img_tag}")
  jid_prev=$jid_fm
  jid_sc=$( sbatch_submit "selfcal_ms" "${SC_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_SELFCAL}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
    SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${SELFCAL_SCRIPT}" INDEX="${idx}" CALMODE="${SC_CALMODE[$r]}" SOLINT="${SC_SOLINT[$r]}" \
    FIELD="${SC_FIELD}" SPW="${SC_SPW}" REFANT="${SC_REFANT}" COMBINE="${SC_COMBINE}" MINSNR="${SC_MINSNR}" PARANG="${SC_PARANG}" \
    CALTABLE_PREFIX="${SC_PREFIX[$r]}" PLOT_DIR="plots" APPLY_CALWT="${SC_APPLY_CALWT}" )
  jid_prev=$(chain "$jid_sc" "selfcal_${img_tag}")

done

# Final image/mask/predict at last index
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"
jid_img_final=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_prev" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
  IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[6]}" FITS_MASK_TAG="${IMG_TAGS[5]}" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[6]}")

jid_cp_final=$(
  sbatch_submit "copy_continuum" "${COPY_TIME}" "${COPY_CPUS}" "${COPY_MEM}" \
    "${ARRAY_SPEC}" "${RUN_COPY_CONTINUUM}" "${jid_img_final}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}"
)
jid_cp_final=$(chain "$jid_cp_final" "copy_continuum")

# don't care about the copy_continuum job for the purposes of dependencies
jid_fm_final=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "$jid_img_final" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" \
  FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}" \
  FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE:-350}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}" )
jid_fm_final=$(chain "$jid_fm_final" "flintmask_${IMG_TAGS[6]}")

# jid_cb_final=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "$jid_fm_final" \
#   SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
#   INDEX="${last_idx}" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
#   MEMORY_FRACTION="${CB_MEMORY_FRACTION}" CB_DISTRIBUTED="${CB_DISTRIBUTED}" CB_DASK_NWORKERS="${CB_DASK_NWORKERS}" CB_DASK_WORKER_CPUS="${CB_DASK_WORKER_CPUS}" \
#   CB_DASK_WORKER_MEM="${CB_DASK_WORKER_MEM}" )
# jid_cb_final=$(chain "$jid_cb_final" "cb_${IMG_TAGS[6]}")

# C) Single uvsub on native 10s MS (no separate applycal loop)
jid_uvs_native=$( sbatch_submit "uvsub_ms" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "$jid_fm_final" \
  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
  BIND_SRC="${BIND_SRC}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${UVSUB_SCRIPT}" INDEX="${last_idx}" EXTENSION="G6" OUT_PREFIX="uvsub" )
jid_uvs_native=$(chain "$jid_uvs_native" "uvsub_native")

# D) fastducc on uvsubbed native MS
jid_fastducc=$( sbatch_submit "fastducc_ms" "${FD_TIME}" "${FD_CPUS}" "${FD_MEM}" "${ARRAY_SPEC}" "${RUN_FASTDUCC}" "$jid_uvs_native" \
  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FASTDUCC_INPUT_PATTERN}" BIND_SRC="${BIND_SRC}" INDEX="${last_idx}" EXTENSION="G6" )
jid_fastducc=$(chain "$jid_fastducc" "fastducc")

# E) dstools extract-ds
jid_prev="$jid_fastducc"
for kind in "boxcar"; do
  KIND="$kind"
  jid_prev=$( sbatch_submit "ds_extract" "${EXTRACT_TIME}" "${EXTRACT_CPUS}" "${EXTRACT_MEM}" "" "${RUN_EXTRACT_DS}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" KIND="${KIND}" DS_N_WORKERS="${DS_N_WORKERS}" DS_CPUS="${DS_CPUS}" DS_MEM="${DS_MEM}" \
    DS_WALLTIME="${DS_WALLTIME}" DS_MIN_SNR="${DS_MIN_SNR}" DS_QUEUE="${DS_QUEUE}" DS_PROJECT="${DS_PROJECT}" \
    DS_BATCH_SIZE="${DS_BATCH_SIZE}" DS_RETRIES="${DS_RETRIES}" DS_SLEEP_BETWEEN_BATCHES="${DS_SLEEP_BETWEEN_BATCHES}" \
    DS_BEAM_SCOPE="${DS_BEAM_SCOPE}" DS_MATCH_ARCSEC="${DS_MATCH_ARCSEC}" DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE}" \
    DS_DATACOLUMN="${DS_DATACOLUMN}" DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM}" DS_NOFLAG="${DS_NOFLAG}" \
    DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE}" DS_MINUVDIST="${DS_MINUVDIST}" DS_VERBOSE="${DS_VERBOSE}" \
    DS_OVERWRITE="${DS_OVERWRITE}" DS_DRY_RUN="${DS_DRY_RUN}" DS_CATALOGUE="${DS_CATALOGUE}" )
  jid_prev=$(chain "$jid_prev" "dstools_extract_${kind}")
done

echo "Pipeline_10s submitted."
