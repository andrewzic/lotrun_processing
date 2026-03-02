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
CONFIG="${CONFIG:-config.sh}"
if [[ -f "${CONFIG}" ]]; then
  # shellcheck source=/dev/null
  source "${CONFIG}"
else
  echo "Config file not found: ${CONFIG}"
  echo "Create one (e.g., pipeline.config.sh) or pass CONFIG=/path/to/file"
  exit 1
fi
__DRY_JID_SEQ="${DRY_FAKE_START:-490000}"

# ----------- USER DEFAULTS (ideally edit config.sh to change these) -----------
USER="${USER:-$(whoami)}"
SBID="${SBID:-SB77974}"
DATA_ROOT="${DATA_ROOT:-/fred/oz451/${USER}/data}"
OUT_ROOT="${OUT_ROOT:-/fred/oz451/${USER}/data}"
BIND_SRC="${BIND_SRC:-/fred/oz451}"
CONTAINER_DIR="${CONTAINER_DIR:-/fred/oz451/${USER}/containers}"
LOG_DIR="${LOG_DIR:-/fred/oz451/${USER}/lotrun_processing/logs}"
SCRIPT_DIR="${SCRIPT_DIR:-/fred/oz451/$USER/scripts/lotrun_processing}"

# -------------------- Containers -----------------------
FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF:-${CONTAINER_DIR}/flint-containers_wsclean.sif}"
FLINT_CASA_SIF="${FLINT_CASA_SIF:-${CONTAINER_DIR}/flint-containers_casa.sif}"

# -------------------- Wrapper scripts ------------------
RUN_IMPORT="${RUN_IMPORT:-run_import.sh}"
RUN_FLAG="${RUN_FLAG:-run_flag.sh}"
RUN_AVERAGE="${RUN_AVERAGE:-run_average_beams.sh}"
RUN_CONCAT="${RUN_CONCAT:-run_concat_beams.sh}"
RUN_WSCLEAN="${RUN_WSCLEAN:-run_wsclean_beams.sh}"
RUN_FLINT_MASK="${RUN_FLINT_MASK:-run_flintmask_beams.sh}"
RUN_CB="${RUN_CB:-run_crystalball_beams.sh}"
RUN_SELFCAL="${RUN_SELFCAL:-run_selfcal_beams.sh}"
RUN_APPLYCAL="${RUN_APPLYCAL:-run_applycal_beams.sh}"
RUN_BANDPASS="${RUN_BANDPASS:-run_applycal_beams.sh}"
RUN_UVSUB="${RUN_UVSUB:-run_uvsub_beams.sh}"
RUN_FASTDUCC="${RUN_FASTDUCC:-run_fastducc_beams.sh}"
RUN_FASTDUCC_AGG="${RUN_FASTDUCC_AGG:-run_fastducc_aggregate_chunks.sh}"
# Optional obs-level aggregation (leave unset to skip)
# RUN_FASTDUCC_OBSAGG="run_fastducc_aggregate_obs.sh"
RUN_CLEARCAL="${RUN_CLEARCAL:-run_clearcal_beams.sh}"
RUN_EXTRACT_DS="${RUN_EXTRACT_DS:-run_dstools_extract_cands.sh}"

# -------------------- Tool scripts ---------------------
IMPORT_SCRIPT="${IMPORT_SCRIPT:-import_array.py}"
FLAG_SCRIPT="${FLAG_SCRIPT:-flag.sh}"
AVERAGE_SCRIPT="${AVERAGE_SCRIPT:-average_ms_beams.py}"
CONCAT_SCRIPT="${CONCAT_SCRIPT:-concat_ms_beams.py}"

# -------------------- Python launchers -----------------
AVERAGE_PYTHON="${AVERAGE_PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3}"
CONCAT_PYTHON="${CONCAT_PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3}"

# -------------------- Resource requests ----------------
ARRAY_SPEC="${ARRAY_SPEC:-0-35}"
BIGARRAY_SPEC="${BIGARRAY_SPEC:-0-500}"

IMPORT_CPUS="${IMPORT_CPUS:-2}";      IMPORT_MEM="${IMPORT_MEM:-1G}"
FLAG_CPUS="${FLAG_CPUS:-4}";          FLAG_MEM="${FLAG_MEM:-12G}"
AVERAGE_CPUS="${AVERAGE_CPUS:-4}";    AVERAGE_MEM="${AVERAGE_MEM:-4G}"
CONCAT_CPUS="${CONCAT_CPUS:-4}";      CONCAT_MEM="${CONCAT_MEM:-16G}"
WSCLEAN_CPUS="${WSCLEAN_CPUS:-4}";    WSCLEAN_MEM="${WSCLEAN_MEM:-16G}"
SC_CPUS="${SC_CPUS:-8}";              SC_MEM="${SC_MEM:-4G}"
FM_CPUS="${FM_CPUS:-1}";              FM_MEM="${FM_MEM:-1G}"
FD_CPUS="${FD_CPUS:-1}";              FD_MEM="${FD_MEM:-32G}"

CB_TIME="${CB_TIME:-03:15:00}"; CB_CPUS="${CB_CPUS:-32}"; CB_MEM="${CB_MEM:-54G}"
AGG_TIME="${AGG_TIME:-00:30:00}"; AGG_CPUS="${AGG_CPUS:-1}"; AGG_MEM="${AGG_MEM:-2G}"
FD_TIME="${FD_TIME:-06:00:00}"

EXTRACT_TIME="${EXTRACT_TIME:-01:00:00}"
EXTRACT_CPUS="${EXTRACT_CPUS:-1}"
EXTRACT_MEM="${EXTRACT_MEM:-4G}"

# -------------------- Stage parameters -----------------
TIMEBIN="${TIMEBIN:-9.90s}"
UVSUB_OUT_PREFIX="${UVSUB_OUT_PREFIX:-uvsub}"
CHUNK_GLOB="${CHUNK_GLOB:-202?*}"
KIND="${KIND:-boxcar}"
FLAG_COLUMN="${FLAG_COLUMN:-DATA}"

# -------------------- Self-cal behaviour ----------------
SC_FIELD="${SC_FIELD:-}"
SC_SPW="${SC_SPW:-}"
SC_REFANT="${SC_REFANT:-}"
SC_COMBINE="${SC_COMBINE:-scan}"
SC_MINSNR="${SC_MINSNR:-3.0}"
SC_PARANG="${SC_PARANG:-}"
SC_APPLY_CALWT="${SC_APPLY_CALWT:-False}"

# Round tags & controls
declare -ag IMG_TAGS=("initial" "selfcal_1" "selfcal_2" "selfcal_3" "selfcal_4" "selfcal_5" "selfcal_6")
declare -ag SC_INDEX=(1 2 3 4 5 6)
declare -ag SC_CALMODE=("p" "p" "p" "p" "ap" "ap")
declare -ag SC_SOLINT=("480s" "300s" "120s" "30s" "600s" "300s")
declare -ag SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_p" "selfcal5_ap" "selfcal6_ap")

# WSClean options (per round)
declare -ag WSCLEAN_OPTS
WSCLEAN_OPTS[0]="${WSCLEAN_OPTS0:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 25000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 3 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[1]="${WSCLEAN_OPTS1:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 2 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 8.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 3.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"

# Crystalball behaviour
CB_OUTPUT_COLUMN="${CB_OUTPUT_COLUMN:-MODEL_DATA}"
CB_NUM_WORKERS="${CB_NUM_WORKERS:-2048}"
CB_ROW_CHUNKS="${CB_ROW_CHUNKS:-0}"
CB_MODEL_CHUNKS="${CB_MODEL_CHUNKS:-0}"
CB_MEMORY_FRACTION="${CB_MEMORY_FRACTION:-0.8}"

# Flint mask thresholds
FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}"
FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}"
FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE:-350}"
BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}"

# -------------------- Stage-aware patterns ----------------
# Import
UVFITS_PATTERN="${UVFITS_PATTERN:-20??*/**beam*.uvfits}"

# Flag native
FLAG_NATIVE_PATTERN="${FLAG_NATIVE_PATTERN:-20??*/"*beam*"*.20????????????.ms}"

# Bandpass inputs (native)
BANDPASS_INPUT_PATTERN="${BANDPASS_INPUT_PATTERN:-20??*/"*beam*".20????????????.ms}"

# Flag calB0
FLAG_CALB0_PATTERN="${FLAG_CALB0_PATTERN:-20??*/"*beam*"*.20????????????.calB0.ms}"

# Average inputs (calB0)
AVERAGE_INPUT_PATTERN="${AVERAGE_INPUT_PATTERN:-20??*/"*beam*"*.20????????????.calB0.ms}"

# Flag averaged
FLAG_AVG_PATTERN="${FLAG_AVG_PATTERN:-20??*/"*beam*"*.20????????????.avg.calB0.ms}"

# Concat inputs (averaged)
CONCAT_INPUT_PATTERN="${CONCAT_INPUT_PATTERN:-20??*/"*beam*".20????????????.avg.calB0.ms}"

# Imaging/selfcal (concatenated MS path)
WSCLEAN_PATTERN="${WSCLEAN_PATTERN:-*beam{beam:02d}.avg.calB0.ms}"

# UVSUB on concatenated self-cal result (original: G6 inputs + B0 ext)
UVSUB_CONCAT_INPUT_PATTERN="${UVSUB_CONCAT_INPUT_PATTERN:-*beam{beam:02d}*.avg.calG6.ms}"

# Applycal on native res (start from calB0; loop produces calG<i>)
APPLYCAL_NATIVE_START_PATTERN="${APPLYCAL_NATIVE_START_PATTERN:-20??*/"*beam*".20????????????.calB0.ms}"

# fastducc on uvsubbed native res
FASTDUCC_INPUT_PATTERN="${FASTDUCC_INPUT_PATTERN:-20??*/"*beam*".20????????????.calB0.uvsub.ms}"

# -------------------- dstools extract-ds -----------------
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
DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE:-**/cracoData*%s*uvsub.ms}"
DS_DATACOLUMN="${DS_DATACOLUMN:-data}"
DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM:-}"
DS_NOFLAG="${DS_NOFLAG:-false}"
DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE:-true}"
DS_MINUVDIST="${DS_MINUVDIST:-0.0}"
DS_VERBOSE="${DS_VERBOSE:-false}"
DS_OVERWRITE="${DS_OVERWRITE:-false}"
DS_DRY_RUN="${DS_DRY_RUN:-false}"
DS_CATALOGUE="${DS_CATALOGUE:-}"
DS_SCAN_SCOPE=${DS_SCAN_SCOPE:-all}   # 'all' or 'catalogue'

# -------------------- GENERIC SUBMISSION + HELPERS --------------------
log(){ printf '[%s] %s\n' "$(date +'%F %T')" "$*" >&2; }

sbatch_submit() {
  # sbatch_submit <name> <time> <cpus> <mem> <array_spec_or_empty> <wrapper> <dep_jid_or_empty> [KEY=VAL ...]
  local name="$1" time="$2" cpus="$3" mem="$4" array="$5" wrapper="$6" dep="${7:-}"; shift 7
  local -a exports=( "$@" )
  local export_str
  if ((${#exports[@]})); then
    IFS=, read -r -d '' export_str <<<"$(printf "%s," "${exports[@]}")"$'\0'
    export_str="${export_str%,}"
  else
    export_str=""
  fi

  local cmd=( sbatch --job-name="$name" --time="$time" --cpus-per-task="$cpus" --mem="$mem"
              --output="logs/${name}_%A_%a.out" --error="logs/${name}_%A_%a.err"
              --export=ALL${export_str:+,${export_str}} "$wrapper" )
  [[ -n "$array" ]] && cmd=( "${cmd[@]:0:1}" "${cmd[@]:1}" --array="$array" )
  [[ -n "$dep"   ]] && cmd=( "${cmd[@]:0:1}" "${cmd[@]:1}" --dependency="afterok:${dep}" )

  if [[ "${DRY_RUN:-0}" == "1" ]]; then
      # Print the would-be command (to stderr so the caller can still capture JID from stdout)
      if [[ "${DRY_PRINT_CMDS:-1}" == "1" ]]; then
        printf 'DRY sbatch:' >&2
        # printable/quoted rendering of the array
        local part
        for part in "${cmd[@]}"; do
          # naive quoting; good enough for inspection
          printf ' %q' "$part" >&2
        done
        printf '\n' >&2
      fi

      # Return a fake job ID (stdout) so dependency chaining continues to work
      local fake_jid="$__DRY_JID_SEQ"
      __DRY_JID_SEQ=$(( __DRY_JID_SEQ + 1 ))
      echo "${fake_jid}"
      return 0
  fi

  # Submit the job and print the job ID (stdout)
  "${cmd[@]}" | awk '{print $4}'
}

chain() {
  local jid="$1" label="$2"
  if [[ -z "$jid" ]]; then
    echo "sbatch not successful for ${label}. exiting"
    exit 1
  fi
  echo "$jid"
}

# -------------------- PIPELINE EXECUTION --------------------
mkdir -p logs plots

# Determine BIGARRAY_SPEC from the number of UVFITS matching UVFITS_PATTERN
n=$( ls -1d "${DATA_ROOT}/${SBID}"/${UVFITS_PATTERN} 2>/dev/null | wc -l )
BIGARRAY_SPEC="0-$((n>0 ? n-1 : 0))"

# 1) symlink uvfits
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "DRY local: ./symlink_uvfits.sh ${SBID}" >&2
else
  ./symlink_uvfits.sh "${SBID}"
fi

# 2) import uvfits
jid_imp=$( sbatch_submit "importuvfits_array" "00:10:00" "${IMPORT_CPUS}" "${IMPORT_MEM}" "${BIGARRAY_SPEC}" "${RUN_IMPORT}" "" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" UVFITS_PATTERN="${UVFITS_PATTERN}" IMPORT_SCRIPT="${IMPORT_SCRIPT}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_imp=$(chain "$jid_imp" "importuvfits")

# 3) flag native-resolution MS
jid_fl1=$( sbatch_submit "aoflagger_array" "00:30:00" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_imp}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_NATIVE_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" FLAG_SCRIPT="${FLAG_SCRIPT}" COLUMN="${FLAG_COLUMN}" RUN_FLAG="${RUN_FLAG}" )
jid_fl1=$(chain "$jid_fl1" "flag_native")

# 4) apply bandpass (B0) to native res
jid_ac1=$( sbatch_submit "bandpass_ms" "02:00:00" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_BANDPASS}" "${jid_fl1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${BANDPASS_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
           SCRIPT="applycal_ms_beams.py" CAL_DIR="cal" EXTENSION="B0" DELETE_PREVIOUS="--delete-previous" )
jid_ac1=$(chain "$jid_ac1" "bandpass_B0")

# 5) flag after B0
jid_fl2=$( sbatch_submit "aoflagger_array" "00:30:00" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_ac1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_CALB0_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" FLAG_SCRIPT="${FLAG_SCRIPT}" COLUMN="${FLAG_COLUMN}" RUN_FLAG="${RUN_FLAG}" )
jid_fl2=$(chain "$jid_fl2" "flag_calB0")

# 6) average MS
jid_av1=$( sbatch_submit "average_array" "01:00:00" "${AVERAGE_CPUS}" "${AVERAGE_MEM}" "${BIGARRAY_SPEC}" "${RUN_AVERAGE}" "${jid_fl2}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${AVERAGE_INPUT_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${AVERAGE_SCRIPT}" PYTHON="${AVERAGE_PYTHON}" TIMEBIN="${TIMEBIN}" )
jid_av1=$(chain "$jid_av1" "average")

# 7) flag averaged MS
jid_fl3=$( sbatch_submit "aoflagger_array" "00:30:00" "${FLAG_CPUS}" "${FLAG_MEM}" "${BIGARRAY_SPEC}" "${RUN_FLAG}" "${jid_av1}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FLAG_AVG_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" FLAG_SCRIPT="${FLAG_SCRIPT}" COLUMN="${FLAG_COLUMN}" RUN_FLAG="${RUN_FLAG}" )
jid_fl3=$(chain "$jid_fl3" "flag_avg")

# 8) concat beams (averaged)
jid_cat=$( sbatch_submit "concat_ms" "01:00:00" "${CONCAT_CPUS}" "${CONCAT_MEM}" "${ARRAY_SPEC}" "${RUN_CONCAT}" "${jid_fl3}" \
           SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" OUT_ROOT="${OUT_ROOT}" PATTERN="${CONCAT_INPUT_PATTERN}" PYTHON="${CONCAT_PYTHON}" SCRIPT="${CONCAT_SCRIPT}" )
jid_cat=$(chain "$jid_cat" "concat")

# -------------------- Imaging / Mask / Predict / Selfcal (looped) --------------------
# We operate on concatenated MS: *beam{beam:02d}.avg.calB0.ms
# Initial scratch image/mask/predict before the rounds loop
jid_img_init=$( sbatch_submit "wsclean_ms" "04:00:00" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cat}" \
                SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                IMG_TAG="initial_scratch" INDEX="$(( SC_INDEX[0]-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[0]}" )
jid_img_init=$(chain "$jid_img_init" "wsclean_initial_scratch")

jid_fm_init=$( sbatch_submit "flint_mask" "00:30:00" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img_init}" \
               SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="initial_scratch" INDEX="$(( SC_INDEX[0]-1 ))" \
               FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
               FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
jid_fm_init=$(chain "$jid_fm_init" "flintmask_initial_scratch")

jid_cb_init=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm_init}" \
               SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="initial_scratch" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
               INDEX="$(( SC_INDEX[0]-1 ))" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
               MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb_prev=$(chain "$jid_cb_init" "cb_initial_scratch")

# Loop over selfcal rounds 1..N using arrays
for r in "${!SC_INDEX[@]}"; do
  idx="${SC_INDEX[$r]}"
  img_tag="${IMG_TAGS[$((r+1))]}"           # selfcal_1..selfcal_6
  prev_tag="${IMG_TAGS[$r]}"                # previous tag for FITS mask reference
  opts="${WSCLEAN_OPTS[$((r+1))]:-${WSCLEAN_OPTS[$r]}}"

  # Image with mask from previous round
  jid_img=$( sbatch_submit "wsclean_ms" "04:00:00" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cb_prev}" \
             SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" IMG_TAG="${img_tag}" \
             INDEX="$(( idx-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${opts}" FITS_MASK_TAG="${prev_tag}" )
  jid_img=$(chain "$jid_img" "wsclean_${img_tag}")

  # Build mask
  jid_fm=$( sbatch_submit "flint_mask" "00:30:00" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img}" \
            SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" \
            FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
            FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
  jid_fm=$(chain "$jid_fm" "flintmask_${img_tag}")

  # Predict
  jid_cb=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm}" \
            SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
            INDEX="$(( idx-1 ))" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
            MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
  jid_cb=$(chain "$jid_cb" "cb_${img_tag}")

  # Self-cal (mode/solint from arrays)
  jid_sc=$( sbatch_submit "selfcal_ms" "02:00:00" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_SELFCAL}" "${jid_cb}" \
            SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
            SCRIPT="selfcal_ms_beams.py" INDEX="${idx}" CALMODE="${SC_CALMODE[$r]}" SOLINT="${SC_SOLINT[$r]}" FIELD="${SC_FIELD}" SPW="${SC_SPW}" \
            REFANT="${SC_REFANT}" COMBINE="${SC_COMBINE}" MINSNR="${SC_MINSNR}" PARANG="${SC_PARANG}" CALTABLE_PREFIX="${SC_PREFIX[$r]}" \
            PLOT_DIR="plots" APPLY_CALWT="${SC_APPLY_CALWT}" )
  jid_cb_prev=$(chain "$jid_sc" "selfcal_${img_tag}")
done

# Final image/mask after last A+P round
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"

jid_img_final=$( sbatch_submit "wsclean_ms" "04:00:00" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "${jid_cb_prev}" \
                 SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
                 IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${WSCLEAN_OPTS[6]}" FITS_MASK_TAG="${IMG_TAGS[5]}" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[6]}")

jid_fm_final=$( sbatch_submit "flint_mask" "00:30:00" "${FM_CPUS}" "${FM_MEM}" "${ARRAY_SPEC}" "${RUN_FLINT_MASK}" "${jid_img_final}" \
                SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" INDEX="${last_idx}" \
                FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}" FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" \
                FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}" BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
jid_fm_final=$(chain "$jid_fm_final" "flintmask_${IMG_TAGS[6]}")

# Final predict from latest source list (concatenated)
jid_cb6=$( sbatch_submit "cb_predict" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_fm_final}" \
          SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
          INDEX="${last_idx}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
          MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb6=$(chain "$jid_cb6" "cb_${IMG_TAGS[6]}")

# UVSUB on concatenated self-cal result (original: G6 inputs + ext B0)
jid_uvsub_concat=$( sbatch_submit "uvsub_ms" "02:00:00" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "${jid_cb6}" \
                    SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${UVSUB_CONCAT_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
                    BIND_SRC="${BIND_SRC}" SCRIPT="uvsub_ms_beams.py" INDEX="${last_idx}" EXTENSION="B0" OUT_PREFIX="${UVSUB_OUT_PREFIX}" )
jid_uvsub_concat=$(chain "$jid_uvsub_concat" "uvsub_concat")

# -------------------- Apply selfcal to native res, predict, uvsub --------------------
jid_prev="$jid_uvsub_concat"

# Applycal loop on native-res: start from calB0, then calG<i>
applycal_pattern="${APPLYCAL_NATIVE_START_PATTERN}"
for i in "${SC_INDEX[@]}"; do
  [[ $i -gt 1 ]] && dp="--delete-previous" || dp=""
  jid_prev=$( sbatch_submit "applycal_ms" "02:00:00" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_APPLYCAL}" "${jid_prev}" \
             SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${applycal_pattern}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
             SCRIPT="applycal_ms_beams.py" CAL_DIR="caltables" EXTENSION="G${i}" DELETE_PREVIOUS="${dp}" )
  jid_prev=$(chain "$jid_prev" "applycal_G${i}")
  applycal_pattern="20??*/"*beam*".20????????????.calG${i}.ms"
done

# Predict from 2h continuum model onto native res (pattern is last produced calG<last_idx>)
CB_NATIVE_INPUT_PATTERN="${applycal_pattern}"

jid_cb_native=$( sbatch_submit "cb_predict_native" "${CB_TIME}" "${CB_CPUS}" "${CB_MEM}" "${ARRAY_SPEC}" "${RUN_CB}" "${jid_prev}" \
                 SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${CB_NATIVE_INPUT_PATTERN}" IMG_TAG="${IMG_TAGS[6]}" OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}" \
                 INDEX="${last_idx}" NUM_WORKERS="${CB_NUM_WORKERS}" ROW_CHUNKS="${CB_ROW_CHUNKS}" MODEL_CHUNKS="${CB_MODEL_CHUNKS}" \
                 MEMORY_FRACTION="${CB_MEMORY_FRACTION}" )
jid_cb_native=$(chain "$jid_cb_native" "cb_native")

# UVSUB on native res (G6 calibrated)
UVSUB_NATIVE_INPUT_PATTERN="${CB_NATIVE_INPUT_PATTERN}"

jid_uvs_native=$( sbatch_submit "uvsub_ms" "02:00:00" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "${jid_cb_native}" \
                  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${UVSUB_NATIVE_INPUT_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
                  BIND_SRC="${BIND_SRC}" SCRIPT="uvsub_ms_beams.py" INDEX="${last_idx}" EXTENSION="G6" OUT_PREFIX="${UVSUB_OUT_PREFIX}" )
jid_uvs_native=$(chain "$jid_uvs_native" "uvsub_native")

# fastducc on uvsubbed native MS
jid_fastducc=$( sbatch_submit "fastducc_ms" "06:00:00" "${FD_CPUS}" "${FD_MEM}" "${ARRAY_SPEC}" "${RUN_FASTDUCC}" "${jid_uvs_native}" \
                SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FASTDUCC_INPUT_PATTERN}" BIND_SRC="${BIND_SRC}" INDEX="${last_idx}" EXTENSION="G6" )
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
for kind in "boxcar" "variance"; do
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
