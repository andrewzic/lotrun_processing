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

# Default SBID if not provided via CLI or environment
DEFAULT_SBID="${DEFAULT_SBID:-SB77974}"

# Priority: CLI ($1) > ENV ($SBID) > default
SBID="${1:-${SBID:-$DEFAULT_SBID}}"

echo "doing symlink 10s"
# 1) symlink uvfits
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "DRY local: ./symlink_uvfits_10s.sh ${SBID}" >&2
else
  ./symlink_uvfits_10s.sh "${SBID}"
fi

# -------------------- Config loader --------------------
# Run: CONFIG=/path/to/config.sh ./pipeline_10s.sh SBXXXXX
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

# ----------- USER DEFAULTS (edit via CONFIG) -----------
USER="${USER:-$(whoami)}"
DATA_ROOT="${DATA_ROOT:-/fred/oz451/${USER}/data/continuum}"
OUT_ROOT="${OUT_ROOT:-/fred/oz451/${USER}/data/continuum}"
BIND_SRC="${BIND_SRC:-/fred/oz451}"
CONTAINER_DIR="${CONTAINER_DIR:-/fred/oz451/${USER}/containers}"
LOG_DIR="${LOG_DIR:-/fred/oz451/${USER}/lotrun_processing/logs}"
SCRIPT_DIR="${SCRIPT_DIR:-/fred/oz451/$USER/scripts/lotrun_processing}"

# -------------------- Containers -----------------------
FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF:-${CONTAINER_DIR}/flint-containers_wsclean.sif}"
FLINT_CASA_SIF="${FLINT_CASA_SIF:-${CONTAINER_DIR}/flint-containers_casa.sif}"

# -------------------- Wrapper scripts ------------------
RUN_WSCLEAN="${RUN_WSCLEAN:-run_wsclean_beams.sh}"
RUN_FLINT_MASK="${RUN_FLINT_MASK:-run_flintmask_beams.sh}"
RUN_CB="${RUN_CB:-run_crystalball_beams.sh}"
RUN_SELFCAL="${RUN_SELFCAL:-run_selfcal_beams.sh}"
RUN_UVSUB="${RUN_UVSUB:-run_uvsub_beams.sh}"
RUN_FASTDUCC="${RUN_FASTDUCC:-run_fastducc_beams.sh}"
RUN_FASTDUCC_AGG="${RUN_FASTDUCC_AGG:-run_fastducc_aggregate_chunks.sh}"
RUN_EXTRACT_DS="${RUN_EXTRACT_DS:-run_dstools_extract_cands.sh}"
# New pre-processing wrappers
RUN_UNFLAG="${RUN_UNFLAG:-run_unflag_beams.sh}"
RUN_FLAG="${RUN_FLAG:-run_flag.sh}"

# -------------------- Resources ------------------------
ARRAY_SPEC="${ARRAY_SPEC:-0-35}"
WSCLEAN_CPUS="${WSCLEAN_CPUS:-4}";   
WSCLEAN_MEM="${WSCLEAN_MEM:-16G}"
SC_CPUS="${SC_CPUS:-8}";             
SC_MEM="${SC_MEM:-4G}"
FM_CPUS="${FM_CPUS:-1}";             
FM_MEM="${FM_MEM:-1G}"
FD_CPUS="${FD_CPUS:-1}";             
FD_MEM="${FD_MEM:-32G}"; FD_TIME="${FD_TIME:-06:00:00}"
CB_TIME="${CB_TIME:-03:15:00}";      
CB_CPUS="${CB_CPUS:-32}"; CB_MEM="${CB_MEM:-54G}"
EXTRACT_TIME="${EXTRACT_TIME:-01:00:00}"; 
EXTRACT_CPUS="${EXTRACT_CPUS:-1}"; EXTRACT_MEM="${EXTRACT_MEM:-4G}"
FLAG_CPUS="${FLAG_CPUS:-4}";        
FLAG_MEM="${FLAG_MEM:-12G}"; 
FLAG_COLUMN="${FLAG_COLUMN:-DATA}"

IMPORT_TIME="${IMPORT_TIME:-00:10:00}"
UNFLAG_TIME="${UNFLAG_TIME:-00:30:00}"
FLAG_TIME="${FLAG_TIME:-00:30:00}"
AVERAGE_TIME="${AVERAGE_TIME:-01:00:00}"
CONCAT_TIME="${CONCAT_TIME:-01:00:00}"
WSCLEAN_TIME="${WSCLEAN_TIME:-04:00:00}"
FM_TIME="${FM_TIME:-00:30:00}"
SC_TIME="${SC_TIME:-02:00:00}"
UVSUB_TIME="${UVSUB_TIME:-02:00:00}"
BANDPASS_TIME="${BANDPASS_TIME:-02:00:00}"

# -------------------- Self-cal behaviour ---------------
SC_FIELD="${SC_FIELD:-}"
SC_SPW="${SC_SPW:-}"
SC_REFANT="${SC_REFANT:-}"
SC_COMBINE="${SC_COMBINE:-scan}"
SC_MINSNR="${SC_MINSNR:-3.0}"
SC_PARANG="${SC_PARANG:-}"
SC_APPLY_CALWT="${SC_APPLY_CALWT:-False}"
# Round tags & controls
declare -ag IMG_TAGS=("initial_scratch" "selfcal_1" "selfcal_2" "selfcal_3" "selfcal_4" "selfcal_5" "selfcal_6")
declare -ag SC_INDEX=(1 2 3 4 5 6)
declare -ag SC_CALMODE=("p" "p" "p" "p" "ap" "ap")
declare -ag SC_SOLINT=("480s" "300s" "120s" "30s" "600s" "300s")
declare -ag SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_p" "selfcal5_ap" "selfcal6_ap")

# -------------------- Imaging options ------------------
declare -ag WSCLEAN_OPTS
WSCLEAN_OPTS[0]="${WSCLEAN_OPTS0:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 25000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 3 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[1]="${WSCLEAN_OPTS1:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 2 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 8.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 3.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"

# -------------------- Crystalball settings --------------
CB_OUTPUT_COLUMN="${CB_OUTPUT_COLUMN:-MODEL_DATA}"
CB_NUM_WORKERS="${CB_NUM_WORKERS:-2048}"
CB_ROW_CHUNKS="${CB_ROW_CHUNKS:-0}"
CB_MODEL_CHUNKS="${CB_MODEL_CHUNKS:-0}"
CB_MEMORY_FRACTION="${CB_MEMORY_FRACTION:-0.8}"

# -------------------- Patterns (10s native) -------------
# Native 10s MS per beam (after untar if needed).
# Example: scienceData.*.SB82418.*.beam17_averaged_cal.leakage.ms
NATIVE10S_PATTERN="${NATIVE10S_PATTERN:-*beam{beam:02d}_averaged_cal.leakage.ms}"
# Imaging/selfcal operates on native 10s MS directly
WSCLEAN_PATTERN="${WSCLEAN_PATTERN:-${NATIVE10S_PATTERN}}"
# FASTDUCC reads uvsubbed native 10s MS
FASTDUCC_INPUT_PATTERN="${FASTDUCC_INPUT_PATTERN:-*beam{beam:02d}*.uvsub.ms}"

# -------------------- dstools extract-ds ----------------
DS_N_WORKERS="${DS_N_WORKERS:-48}"
DS_CPUS="${DS_CPUS:-1}"
DS_MEM="${DS_MEM:-8GB}"
DS_WALLTIME="${DS_WALLTIME:-01:00:00}"
DS_QUEUE="${DS_QUEUE:-}"
DS_PROJECT="${DS_PROJECT:-}"
DS_MIN_SNR="${DS_MIN_SNR:-8.0}"
DS_BATCH_SIZE="${DS_BATCH_SIZE:-200}"
DS_RETRIES="${DS_RETRIES:-1}"
DS_SLEEP_BETWEEN_BATCHES="${DS_SLEEP_BETWEEN_BATCHES:-0}"
DS_BEAM_SCOPE="${DS_BEAM_SCOPE:-union}"
DS_MATCH_ARCSEC="${DS_MATCH_ARCSEC:-35.0}"
DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE:-**/*uvsub.ms}"
DS_DATACOLUMN="${DS_DATACOLUMN:-DATA}"
DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM:-}"
DS_NOFLAG="${DS_NOFLAG:-false}"
DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE:-true}"
DS_MINUVDIST="${DS_MINUVDIST:-0.0}"
DS_VERBOSE="${DS_VERBOSE:-false}"
DS_OVERWRITE="${DS_OVERWRITE:-false}"
DS_DRY_RUN="${DS_DRY_RUN:-false}"
DS_CATALOGUE="${DS_CATALOGUE:-}"
DS_SCAN_SCOPE=${DS_SCAN_SCOPE:-all}

# -------------------- sbatch helper ---------------------
log(){ printf '[%s] %s\n' "$(date +'%F %T')" "$*" >&2; }
# sbatch_submit <name> <time> <cpus> <mem> <array_spec_or_empty> <wrapper> <dep_jid_or_empty> [KEY=VAL ...]
sbatch_submit() {
  local name="$1" time="$2" cpus="$3" mem="$4" array="$5" wrapper="$6" dep="${7:-}"; shift 7
  local -a exports=( "$@" )
  local export_arg="--export=ALL"
  if ((${#exports[@]})); then
    local joined="" kv
    for kv in "${exports[@]}"; do joined+="${joined:+,}${kv}"; done
    export_arg="--export=ALL,${joined}"
  fi
  local -a cmd=( sbatch --job-name="$name" --time="$time" --cpus-per-task="$cpus" --mem="$mem"
                    --output="logs/${name}_%A_%a.out" --error="logs/${name}_%A_%a.err" )
  [[ -n "$array" ]] && cmd+=( --array="$array" )
  [[ -n "$dep"   ]] && cmd+=( --dependency="afterok:${dep}" )
  cmd+=( "$export_arg" "$wrapper" )
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    printf 'DRY sbatch:' >&2; for t in "${cmd[@]}"; do
      if [[ "$t" =~ [[:space:]] ]]; then printf ' "%s"' "$t" >&2; else printf ' %s' "$t" >&2; fi
    done; printf '\n' >&2
    local fake_jid="${__DRY_JID_SEQ:-490000}"; __DRY_JID_SEQ=$(( fake_jid + 1 )); echo "${fake_jid}"; return 0
  fi
  "${cmd[@]}" | awk '{print $4}'
}
chain(){ local jid="$1" label="$2"; [[ -z "$jid" ]] && { echo "sbatch failed for ${label}"; exit 1; }; echo "$jid"; }

# -------------------- PIPELINE --------------------------
mkdir -p logs plots

# 0) Optional: ensure .ms present (if only .ms.tar exist, untar separately before running)
#    This pipeline assumes the .ms have already been extracted alongside the .ms.tar

# A) PRE-PROCESS: unflag then AOflagger on native 10s MS
jid_unflag=$( sbatch_submit "unflag_ms" "${UNFLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_UNFLAG}" "" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_unflag=$(chain "$jid_unflag" "unflag_native")

jid_flag=$( sbatch_submit "aoflagger_array" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FLAG}" "$jid_unflag" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" FLAG_SCRIPT="flag.sh" COLUMN="${FLAG_COLUMN}" RUN_FLAG="${RUN_FLAG}" )
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

jid_cb_init=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "$jid_fm_init" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="initial_scratch" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
  INDEX="0" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_prev=$(chain "$jid_cb_init" "cb_initial_scratch")

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

  jid_cb=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "$jid_fm" \
    SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
    INDEX="$(( idx-1 ))" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
  jid_prev=$(chain "$jid_cb" "cb_${img_tag}")

  jid_sc=$( sbatch_submit "selfcal_ms" "${SC_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_SELFCAL}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
    SCRIPT="selfcal_ms_beams.py" INDEX="${idx}" CALMODE="${SC_CALMODE[$r]}" SOLINT="${SC_SOLINT[$r]}" FIELD="${SC_FIELD}" SPW="${SC_SPW}" \
    REFANT="${SC_REFANT}" COMBINE="${SC_COMBINE}" MINSNR="${SC_MINSNR}" PARANG="${SC_PARANG}" CALTABLE_PREFIX="${SC_PREFIX[$r]}" \
    PLOT_DIR="plots" APPLY_CALWT="${SC_APPLY_CALWT}" )
  jid_prev=$(chain "$jid_sc" "selfcal_${img_tag}")

done

# Final image/mask/predict at last index
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"
jid_img_final=$( sbatch_submit "wsclean_ms" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_prev" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
  IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[6]}" FITS_MASK_TAG="${IMG_TAGS[5]}" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[6]}")

jid_fm_final=$( sbatch_submit "flint_mask" "${FM_TIME}" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "$jid_img_final" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" \
  FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}" \
  FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE:-350}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}" )
jid_fm_final=$(chain "$jid_fm_final" "flintmask_${IMG_TAGS[6]}")

jid_cb_final=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "$jid_fm_final" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
  INDEX="${last_idx}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb_final=$(chain "$jid_cb_final" "cb_${IMG_TAGS[6]}")

# C) Single uvsub on native 10s MS (no separate applycal loop)
jid_uvs_native=$( sbatch_submit "uvsub_ms" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "$jid_cb_final" \
  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
  BIND_SRC="${BIND_SRC}" SCRIPT="uvsub_ms_beams.py" INDEX="${last_idx}" EXTENSION="G6" OUT_PREFIX="uvsub" )
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
