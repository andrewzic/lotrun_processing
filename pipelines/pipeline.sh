#!/bin/bash
set -euo pipefail
# =============================================================================
# ASKAP high-time-res imaging/selfcal/uvsub/transient pipeline
# =============================================================================

# -------------------- Config loader --------------------
# Run: CONFIG=/path/to/config.sh ./pipeline.sh
# Usage:
#   source config.sh          # sets defaults in current shell
#   CONFIG=my.config.sh ./pipeline.sh  # pipeline.sh will source CONFIG


# Default SBID if not provided via CLI or environment
DEFAULT_SBID="${DEFAULT_SBID:-SB77974}"

# Priority: CLI ($1) > ENV ($SBID) > default
SBID="${1:-${SBID:-$DEFAULT_SBID}}"


# ----------- USER DEFAULTS (ideally edit config.sh to change these) -----------
USER="${USER:-$(whoami)}"
DATA_ROOT="${DATA_ROOT:-/fred/oz451/${USER}/data}"
OUT_ROOT="${OUT_ROOT:-/fred/oz451/${USER}/data}"
BIND_SRC="${BIND_SRC:-/fred/oz451}"
CONTAINER_DIR="${CONTAINER_DIR:-/fred/oz451/${USER}/containers}"
LOG_DIR="${LOG_DIR:-/fred/oz451/${USER}/lotrun_processing/logs}"
SCRIPT_DIR="${SCRIPT_DIR:-/fred/oz451/$USER/scripts/lotrun_processing}"

echo "doing symlink"
# 1) symlink uvfits
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "DRY local: ./scripts/utils/symlink_uvfits.sh ${SBID}" >&2
else
  ${SCRIPT_DIR}/scripts/utils/symlink_uvfits.sh "${SBID}"
fi

CONFIG="${CONFIG:-config.SB82418.sh}"
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

source "$(dirname "$0")/slurm_helpers.sh"

# -------------------- PIPELINE EXECUTION --------------------
mkdir -p logs plots

# Determine BIGARRAY_SPEC from the number of UVFITS matching UVFITS_PATTERN
n=$( ls -1d "${DATA_ROOT}/${SBID}"/${UVFITS_PATTERN} | wc -l )
BIGARRAY_SPEC="0-$((n>0 ? n-1 : 0))"

# 2) import uvfits
echo $SBID
echo $DATA_ROOT
#exit
jid_start=""
jid_imp=$( sbatch_submit "importuvfits_array" "${IMPORT_TIME}" "${IMPORT_CPUS}" "${IMPORT_MEM}" "${BIGARRAY_SPEC}" "${RUN_IMPORT}" "${jid_start}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" UVFITS_PATTERN="${UVFITS_PATTERN}" IMPORT_SCRIPT="${IMPORT_SCRIPT}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_imp=$(chain "$jid_imp" "importuvfits")

jid_quack=$( sbatch_submit "quack_ms" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_QUACK}" "$jid_imp" \
SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_NATIVE_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_quack=$( chain "$jid_quack" "quack_native" )

# 3) flag native-resolution MS
jid_fl1=$( sbatch_submit "aoflagger_array" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_quack}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_NATIVE_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_fl1=$(chain "$jid_fl1" "flag_native")

# 4) apply bandpass (B0) to native res
jid_ac1=$( sbatch_submit "bandpass_ms" "${APPLYCAL_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_BANDPASS}" "${jid_fl1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${BANDPASS_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
           SCRIPT="src/casa/applycal_ms_beams.py" CAL_DIR="cal" EXTENSION="B0" DELETE_PREVIOUS="--delete-previous" )
jid_ac1=$(chain "$jid_ac1" "bandpass_B0")

# 5) flag after B0
jid_fl2=$( sbatch_submit "aoflagger_array" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_ac1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_CALB0_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_fl2=$(chain "$jid_fl2" "flag_calB0")

# 6) average MS
jid_av1=$( sbatch_submit "average_array" "${AVERAGE_TIME}" "${AVERAGE_CPUS}" "${AVERAGE_MEM}" "${BIGARRAY_SPEC}" "${RUN_AVERAGE}" "${jid_fl2}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${AVERAGE_INPUT_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${AVERAGE_SCRIPT}" PYTHON="${AVERAGE_PYTHON}" TIMEBIN="${TIMEBIN}" )
jid_av1=$(chain "$jid_av1" "average")

# 7) flag averaged MS
jid_fl3=$( sbatch_submit "aoflagger_array" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_av1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_AVG_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_fl3=$(chain "$jid_fl3" "flag_avg")

# 8) concat beams (averaged)
jid_cat=$( sbatch_submit "concat_ms" "${CONCAT_TIME}" "${CONCAT_CPUS}" "${CONCAT_MEM}" "${ARRAY_SPEC}" "${RUN_CONCAT}" "${jid_fl3}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" OUT_ROOT="${OUT_ROOT}" PATTERN="${CONCAT_INPUT_PATTERN}" PYTHON="${CONCAT_PYTHON}" SCRIPT="${CONCAT_SCRIPT}" )
jid_cat=$(chain "$jid_cat" "concat")

# -------------------- Imaging / Mask / Predict / Selfcal (looped) --------------------
# We operate on concatenated MS: *beam{beam:02d}.avg.calB0.ms
# Initial scratch image/mask/predict before the rounds loop
jid_img_init=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cat}" \
                SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                IMG_TAG="initial_scratch" INDEX="$(( SC_INDEX[0]-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[0]}" )
jid_img_init=$(chain "$jid_img_init" "wsclean_initial_scratch")

jid_fm_init=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img_init}" \
               SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[0]}" INDEX="$(( SC_INDEX[0]-1 ))" \
               FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
               FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
jid_fm_init=$(chain "$jid_fm_init" "flintmask_initial_scratch")

jid_cb_init=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm_init}" \
               SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[0]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
               INDEX="$(( SC_INDEX[0]-1 ))" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
               MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb_prev=$(chain "$jid_cb_init" "cb_initial_scratch")

# Loop over selfcal rounds 1..N using arrays
for r in "${!SC_INDEX[@]}"; do
  idx="${SC_INDEX[$r]}"
  img_tag="${IMG_TAGS[$((r+1))]}"           # selfcal_1..selfcal_6
  prev_tag="${IMG_TAGS[$r]}"                # previous tag for FITS mask reference
  opts="${WSCLEAN_OPTS[$((r+1))]:-${WSCLEAN_OPTS[$r]}}"

  # Image with mask from previous round
  jid_img=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cb_prev}" \
             SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" IMG_TAG="${img_tag}" \
             INDEX="$(( idx-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${opts}" FITS_MASK_TAG="${prev_tag}" )
  jid_img=$(chain "$jid_img" "wsclean_${img_tag}")

  # Build mask
  jid_fm=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img}" \
            SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" \
            FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
            FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
  jid_fm=$(chain "$jid_fm" "flintmask_${img_tag}")

  # Predict
  jid_cb=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm}" \
            SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
            INDEX="$(( idx-1 ))" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
            MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
  jid_cb=$(chain "$jid_cb" "cb_${img_tag}")

  # Self-cal (mode/solint from arrays)
  jid_sc=$( sbatch_submit "selfcal_ms" "${SC_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_SELFCAL}" "${jid_cb}" \
            SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
            SCRIPT="src/casa/selfcal_ms_beams.py" INDEX="${idx}" CALMODE="${SC_CALMODE[$r]}" SOLINT="${SC_SOLINT[$r]}" FIELD="${SC_FIELD}" SPW="${SC_SPW}" \
            REFANT="${SC_REFANT}" COMBINE="${SC_COMBINE}" MINSNR="${SC_MINSNR}" PARANG="${SC_PARANG}" CALTABLE_PREFIX="${SC_PREFIX[$r]}" \
            PLOT_DIR="plots" APPLY_CALWT="${SC_APPLY_CALWT}" )
  jid_cb_prev=$(chain "$jid_sc" "selfcal_${img_tag}")
done

# Final image/mask after last A+P round
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"

jid_img_final=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cb_prev}" \
                 SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                 IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[6]}" FITS_MASK_TAG="${IMG_TAGS[6]}" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[6]}")

jid_cp_final=$(
  sbatch_submit "copy_continuum" "${COPY_TIME}" "${COPY_CPUS}" "${COPY_MEM}" \
    "${ARRAY_SPEC}" "${RUN_COPY_CONTINUUM}" "${jid_img_final}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}"
)
jid_cp_final=$(chain "$jid_cp_final" "copy_continuum")

# don't care about the copy_continuum job for the purposes of dependencies
jid_fm_final=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img_final}" \
                SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" \
                FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
                FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
jid_fm_final=$(chain "$jid_fm_final" "flintmask_${IMG_TAGS[6]}")

# Final predict from latest source list (concatenated)
jid_cb6=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm_final}" \
          SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
          INDEX="${last_idx}" SOURCE_LIST_PATTERN="${CB_SOURCE_LIST_PATTERN}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
          MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb6=$(chain "$jid_cb6" "cb_${IMG_TAGS[6]}")

# UVSUB on concatenated self-cal result (original: G6 inputs + ext B0)
jid_uvsub_concat=$( sbatch_submit "uvsub_ms" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "${jid_cb6}" \
                    SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${UVSUB_CONCAT_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
                    BIND_SRC="${BIND_SRC}" SCRIPT="src/casa/uvsub_ms_beams.py" INDEX="${last_idx}" EXTENSION="B0" OUT_PREFIX="${UVSUB_OUT_PREFIX}" )
jid_uvsub_concat=$(chain "$jid_uvsub_concat" "uvsub_concat")

# -------------------- Apply selfcal to native res, predict, uvsub --------------------
jid_prev="$jid_uvsub_concat"

# Applycal loop on native-res: apply all calG<i>
applycal_pattern="${APPLYCAL_NATIVE_START_PATTERN}"
jid_applycal=$( sbatch_submit "applycal_ms" "${APPLYCAL_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_APPLYCAL}" "${jid_prev}" \
                 SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${applycal_pattern}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
                 SCRIPT="src/casa/applycal_ms_beams.py" CAL_DIR="caltables" EXTENSION="G*" DELETE_PREVIOUS="" )
jid_applycal=$(chain "$jid_applycal" "applycal_native")

# Predict from 2h continuum model onto native res (pattern is last produced calG<last_idx>)
CB_NATIVE_INPUT_PATTERN="${applycal_pattern}"

jid_cb_native=$( sbatch_submit "cb_predict_native" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_applycal}" \
                 SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${CB_NATIVE_INPUT_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
                 INDEX="${last_idx}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
                 MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb_native=$(chain "$jid_cb_native" "cb_native")

# UVSUB on native res (G6 calibrated)
UVSUB_NATIVE_INPUT_PATTERN="${CB_NATIVE_INPUT_PATTERN}"

jid_uvs_native=$( sbatch_submit "uvsub_ms" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "${jid_cb_native}" \
                  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${UVSUB_NATIVE_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
                  BIND_SRC="${BIND_SRC}" SCRIPT="src/casa/uvsub_ms_beams.py" INDEX="${last_idx}" EXTENSION="G6" OUT_PREFIX="${UVSUB_OUT_PREFIX}" )
jid_uvs_native=$(chain "$jid_uvs_native" "uvsub_native")

# fastducc on uvsubbed native MS
jid_fastducc=$( sbatch_submit "fastducc_ms" "${FD_TIME}" "${FD_CPUS}" "${FD_MEM}" "${ARRAY_SPEC}" "${RUN_FASTDUCC}" "${jid_uvs_native}" \
                SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FASTDUCC_INPUT_PATTERN}" BIND_SRC="${BIND_SRC}" INDEX="${last_idx}" \
                EXTENSION="G6" NO_VAR_SEARCH="${FD_NO_VAR_SEARCH}" NO_BOX_SEARCH="${FD_NO_BOX_SEARCH}" PLOT_CANDS_ONLY="${FD_PLOT_CANDS_ONLY}" )
jid_fastducc=$(chain "$jid_fastducc" "fastducc")

# aggregate per chunk
jid_agg=$( sbatch_submit "fastducc_agg" "${AGG_TIME}" "${AGG_CPUS}" "${AGG_MEM}" "${CHUNK_ARRAY_SPEC}" "${RUN_FASTDUCC_AGG}" "${jid_fastducc}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" CHUNK_GLOB="${CHUNK_GLOB}" )
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
  jid_prev=$( sbatch_submit "ds_extract" "${EXTRACT_TIME}" "${EXTRACT_CPUS}" "${EXTRACT_MEM}" "" "${RUN_EXTRACT_DS}" "${jid_prev}" \
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
