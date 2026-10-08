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

# Usage: ./pipeline_10s.sh SBID=SBXXXXXX CONFIG=/path/to/config.10s.sh [START_STAGE=stage] [END_STAGE=stage]
#   All arguments are optional; environment variables and defaults are used as fallback.
#   Priority: CLI arg > environment variable > default value

SHOW_HELP=0
CONFIG_CLI=""
SBID_CLI=""
START_STAGE="${START_STAGE:-}"
END_STAGE="${END_STAGE:-}"
VERBOSE="${VERBOSE:-0}"
BEAMS="${BEAMS:-}"

# Parse KEY=VALUE command-line arguments
for arg in "$@"; do
  case "${arg}" in
    -h|--help|help) SHOW_HELP=1 ;;
    SBID=*)        SBID="${arg#SBID=}"; SBID_CLI="${SBID}" ;;
    CONFIG=*)      CONFIG="${arg#CONFIG=}"; CONFIG_CLI="${CONFIG}" ;;
    BEAMS=*)       BEAMS="${arg#BEAMS=}" ;;
    START_STAGE=*) START_STAGE="${arg#START_STAGE=}" ;;
    END_STAGE=*)   END_STAGE="${arg#END_STAGE=}" ;;
    VERBOSE=*)     VERBOSE="${arg#VERBOSE=}" ;;
    DRY_RUN=*)     DRY_RUN="${arg#DRY_RUN=}" ;;
    *.sh)          CONFIG="${arg}"; CONFIG_CLI="${CONFIG}" ;;
    *) echo "Unknown argument: ${arg}" >&2; exit 1 ;;
  esac
done

CONFIG="${CONFIG:-config_10s.sh}"
# Default SBID if not provided via CLI or environment
DEFAULT_SBID="${DEFAULT_SBID:-SB77974}"
if [[ -z "${SBID_CLI:-}" ]]; then
  if [[ "${CONFIG}" =~ (SB[0-9]{5,}) ]]; then
    SBID="${BASH_REMATCH[1]}"
  else
    SBID="${SBID:-${DEFAULT_SBID}}"
  fi
else
  SBID="${SBID_CLI}"
fi

# ----------- USER DEFAULTS (ideally edit config_10s.sh to change these) -----------
USER="${USER:-$(whoami)}"
DATA_ROOT="${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
OUT_ROOT="${OUT_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
BIND_SRC="${BIND_SRC:-${USER_PATH:-/fred/oz451}}"
CONTAINER_DIR="${CONTAINER_DIR:-${USER_PATH:-/fred/oz451}/${USER}/containers}"
LOG_DIR="${LOG_DIR:-${USER_PATH:-/fred/oz451}/${USER}/lotrun_processing/logs}"
SCRIPT_DIR="${SCRIPT_DIR:-${USER_PATH:-/fred/oz451}/$USER/scripts/lotrun_processing}"

# -------------------- Config loader --------------------
CONFIG_LOADED=0
if [[ -f "${CONFIG}" ]]; then
  if [[ "${SHOW_HELP}" == "0" || -n "${CONFIG_CLI}" || "${CONFIG}" != "config_10s.sh" ]]; then
    CONFIG_LOADED=1
  fi
fi

if [[ "${CONFIG_LOADED}" == "1" ]]; then
  # shellcheck source=/dev/null
  if [[ "${SHOW_HELP}" == "0" ]]; then
    echo "sourcing config ${CONFIG}"
  fi
  source "${CONFIG}"
  if [[ "${SHOW_HELP}" == "0" ]]; then
    echo "sourced config"
  fi
  
  if [[ -n "${SBID_CLI:-}" ]]; then
    SBID="${SBID_CLI}"
  fi

  if [[ -n "${BEAMS:-}" ]]; then
    ARRAY_SPEC="${BEAMS}"
  fi
else
  if [[ "${SHOW_HELP}" == "1" ]]; then
    SC_INDEX=(1 2 3 4 5 6)
    IMG_TAGS=("initial_scratch" "selfcal_1" "selfcal_2" "selfcal_3" "selfcal_4" "selfcal_5" "selfcal_6")
    SC_CALMODE=("p" "p" "p" "p" "ap" "ap")
    FLAG_OUTER="1"
    IMAGE_24ANT_ENABLED="1"
  else
    echo "Config file not found: ${CONFIG}" >&2
    echo "Create one (e.g., configs/config_10s.sh) or pass CONFIG=/path/to/file" >&2
    exit 1
  fi
fi

__DRY_JID_SEQ="${DRY_FAKE_START:-490000}"

source "$(dirname "$0")/slurm_helpers.sh"

# Sync and normalize eyepatch arrays
[[ -n "${seedclipOptions+x}" && ${#seedclipOptions[@]} -gt 0 ]] && EYEPATCH_SEED_CLIP=("${seedclipOptions[@]}")
[[ -n "${floodclipOptions+x}" && ${#floodclipOptions[@]} -gt 0 ]] && EYEPATCH_FLOOD_CLIP=("${floodclipOptions[@]}")
[[ -n "${macboxsizeOptions+x}" && ${#macboxsizeOptions[@]} -gt 0 ]] && EYEPATCH_MAC_BOX_SIZE=("${macboxsizeOptions[@]}")
[[ -n "${beamerodeminresponseOptions+x}" && ${#beamerodeminresponseOptions[@]} -gt 0 ]] && EYEPATCH_BEAM_ERODE_MIN_RESPONSE=("${beamerodeminresponseOptions[@]}")
[[ -n "${cleanscalesOptions+x}" && ${#cleanscalesOptions[@]} -gt 0 ]] && EYEPATCH_CLEAN_SCALES=("${cleanscalesOptions[@]}")
[[ -n "${useMacAdaptiveStepFactorOptions+x}" && ${#useMacAdaptiveStepFactorOptions[@]} -gt 0 ]] && EYEPATCH_MAC_ADAPTIVE_STEP_FACTOR=("${useMacAdaptiveStepFactorOptions[@]}")

RUN_EYEPATCH="${RUN_EYEPATCH:-${RUN_FLINT_MASK:-${SCRIPT_DIR}/scripts/slurm/run_eyepatch_beams.sh}}"
EP_TIME="${EP_TIME:-${FM_TIME:-00:20:00}}"
EP_CPUS="${EP_CPUS:-${FM_CPUS:-1}}"
EP_MEM="${EP_MEM:-${FM_MEM:-2G}}"

# -------------------- STAGE SKIP LIST --------------------
last_idx="${SC_INDEX[$((${#SC_INDEX[@]}-1))]}"

declare -ag PIPELINE_STAGES=(
  "fixdir_native"
  "unflag_native"
)
if [[ "${FLAG_OUTER:-0}" == "1" ]]; then
  PIPELINE_STAGES+=( "flagouter_native" )
fi
PIPELINE_STAGES+=(
  "quack_native"
  "aoflagger_native"
  "wsclean_initial_scratch"
  "eyepatch_initial_scratch"
)
for r in "${!SC_INDEX[@]}"; do
  img_tag="${IMG_TAGS[$((r+1))]}"
  PIPELINE_STAGES+=( "wsclean_${img_tag}" "eyepatch_${img_tag}" "selfcal_${img_tag}" )

  do_flag=0
  if [[ "${SC_CALMODE[$r]:-}" == "ap" ]]; then
    do_flag=1
  elif [[ "${SC_CALMODE[$((r+1))]:-}" == "ap" ]]; then
    do_flag=1
  fi
  if (( do_flag == 1 )); then
    PIPELINE_STAGES+=( "flag_selfcal_${img_tag}" )
  fi
done
PIPELINE_STAGES+=(
  "wsclean_${IMG_TAGS[${last_idx}]}_final"
  "copy_continuum"
  "eyepatch_${IMG_TAGS[${last_idx}]}_final"
  "uvsub_native"
  "fastducc"
)
for kind in "boxcar"; do
  PIPELINE_STAGES+=( "dstools_extract_${kind}" )
done
if [[ "${IMAGE_24ANT_ENABLED:-1}" == "1" || "${IMAGE_24ANT_ENABLED:-}" == "true" ]]; then
  PIPELINE_STAGES+=( "image_24ant_craco_match" )
fi

if [[ "${SHOW_HELP}" == "1" ]]; then
  echo "============================================================================="
  echo " ASKAP 10s native-resolution selfcal + uvsub pipeline"
  echo "============================================================================="
  echo "Usage: ./pipeline_10s.sh [SBID=SBXXXXXX] [CONFIG=/path/to/config.sh] [START_STAGE=stage_name] ..."
  echo ""
  echo "Arguments (KEY=VALUE format):"
  echo "  SBID          Scheduling Block ID (default: ${DEFAULT_SBID:-SB77974})"
  echo "  CONFIG        Path to configuration file (default: config_10s.sh)"
  echo "  BEAMS         Slurm array spec or beam list (e.g. 0-35, 16)"
  echo "  START_STAGE   Stage to start the pipeline from (see list below)"
  echo "  END_STAGE     Stage to end the pipeline at"
  echo "  VERBOSE       Set to 1 to echo submitted command name and job ID to stderr (default: 0)"
  echo "  DRY_RUN       Set to 1 to simulate submission without actually submitting (default: 0)"
  echo ""
  if [[ "${CONFIG_LOADED}" == "1" ]]; then
    echo "Available START_STAGE selections (based on ${CONFIG}):"
  else
    if [[ -n "${CONFIG_CLI:-}" ]]; then
      echo "Note: Specified config file '${CONFIG}' was not found."
    else
      echo "Note: No config file was specified."
    fi
    echo "Showing default pipeline stages. For config-specific help and stages, specify your config file:"
    echo "  ./pipeline_10s.sh CONFIG=path/to/config_10s.sh --help"
    echo ""
    echo "Available START_STAGE selections (default template):"
  fi
  for stage in "${PIPELINE_STAGES[@]}"; do
    echo "  - ${stage}"
  done
  echo "============================================================================="
  exit 0
fi

validate_stages

# -------------------- PIPELINE --------------------------
mkdir -p logs plots

# A) PRE-PROCESS: fix_dir first
jid_fixdir=$( sbatch_submit "fixdir_native" "${UNFLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FIXDIR}" "" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${FIXDIR_SCRIPT}" )
jid_fixdir=$(chain "$jid_fixdir" "fixdir_native")

# # unflag then AOflagger on native 10s MS
# jid_unflag=$( sbatch_submit "unflag_native" "${UNFLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_UNFLAG}" "${jid_fixdir}" \
#   SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" SCRIPT_DIR="${SCRIPT_DIR}" )
# jid_unflag=$(chain "$jid_unflag" "unflag_native")
jid_unflag=${jid_fixdir}
if [[ "${FLAG_OUTER:-0}" == "1" ]]; then 
  jid_flagouter=$( sbatch_submit "flagouter_native" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FLAGOUTER}" "${jid_unflag}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT=${FLAGOUTER_SCRIPT} COLUMN="${FLAG_COLUMN}" )
  jid_flagouter=$(chain "$jid_flagouter" "flagouter_native")
  jid_before_flag="$jid_flagouter"
else
  jid_before_flag="$jid_unflag"
fi

jid_quack=$( sbatch_submit "quack_native" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_QUACK}" "$jid_before_flag" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" )
jid_quack=$( chain "$jid_quack" "quack_native" )

jid_flag=$( sbatch_submit "aoflagger_native" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FLAG}" "$jid_quack" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${NATIVE10S_PATTERN}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
jid_flag=$(chain "$jid_flag" "aoflagger_native")

# B) Imaging / Mask / Predict / Selfcal (on native 10s MS)
# Initial scratch
wsclean_opts_init="${WSCLEAN_OPTS[0]}"

jid_img_init=$( sbatch_submit "wsclean_initial_scratch" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_flag" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
  IMG_TAG="initial_scratch" INDEX="0" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${wsclean_opts_init}" )
jid_img_init=$(chain "$jid_img_init" "wsclean_initial_scratch")

jid_fm_init=$( sbatch_submit "eyepatch_initial_scratch" "${EP_TIME}" "${EP_CPUS}" "${EP_MEM}" "${ARRAY_SPEC}" "${RUN_EYEPATCH}" "$jid_img_init" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="initial_scratch" INDEX="0" \
  FLOOD_FILL_POSITIVE_SEED_CLIP="${EYEPATCH_SEED_CLIP[0]}" \
  FLOOD_FILL_POSITIVE_FLOOD_CLIP="${EYEPATCH_FLOOD_CLIP[0]}" \
  FLOOD_FILL_MAC_BOX_SIZE="${EYEPATCH_MAC_BOX_SIZE[0]}" \
  BEAM_SHAPE_ERODE_MIN_RESPONSE="${EYEPATCH_BEAM_ERODE_MIN_RESPONSE[0]}" \
  BEAM_SHAPE_ERODE_SCALES="${EYEPATCH_CLEAN_SCALES[0]:-}" \
  FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR="${EYEPATCH_MAC_ADAPTIVE_STEP_FACTOR[0]}" )
jid_fm_init=$(chain "$jid_fm_init" "eyepatch_initial_scratch")

jid_prev=$jid_fm_init

# Selfcal rounds
for r in "${!SC_INDEX[@]}"; do
  idx="${SC_INDEX[$r]}"
  img_tag="${IMG_TAGS[$((r+1))]}"
  prev_tag="${IMG_TAGS[$r]}"
  stage_idx=$((r+1))
  opts="${WSCLEAN_OPTS[${stage_idx}]:-${WSCLEAN_OPTS[$r]}}"
  nspws_val="${SC_NSPWS[$r]:-16}"

  jid_img=$( sbatch_submit "wsclean_${img_tag}" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
    IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${opts}" FITS_MASK_TAG="${prev_tag}" )
  jid_img=$(chain "$jid_img" "wsclean_${img_tag}")

  jid_fm=$( sbatch_submit "eyepatch_${img_tag}" "${EP_TIME}" "${EP_CPUS}" "${EP_MEM}" "${ARRAY_SPEC}" "${RUN_EYEPATCH}" "$jid_img" \
    SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${img_tag}" INDEX="$(( idx-1 ))" \
    FLOOD_FILL_POSITIVE_SEED_CLIP="${EYEPATCH_SEED_CLIP[${stage_idx}]}" \
    FLOOD_FILL_POSITIVE_FLOOD_CLIP="${EYEPATCH_FLOOD_CLIP[${stage_idx}]}" \
    FLOOD_FILL_MAC_BOX_SIZE="${EYEPATCH_MAC_BOX_SIZE[${stage_idx}]}" \
    BEAM_SHAPE_ERODE_MIN_RESPONSE="${EYEPATCH_BEAM_ERODE_MIN_RESPONSE[${stage_idx}]}" \
    BEAM_SHAPE_ERODE_SCALES="${EYEPATCH_CLEAN_SCALES[${stage_idx}]:-}" \
    FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR="${EYEPATCH_MAC_ADAPTIVE_STEP_FACTOR[${stage_idx}]}" )
  jid_fm=$(chain "$jid_fm" "eyepatch_${img_tag}")

  jid_prev=$jid_fm
  jid_sc=$( sbatch_submit "selfcal_${img_tag}" "${SC_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_SELFCAL}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" BIND_SRC="${BIND_SRC}" \
    SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${SELFCAL_SCRIPT}" INDEX="${idx}" CALMODE="${SC_CALMODE[$r]}" SOLINT="${SC_SOLINT[$r]}" \
    FIELD="${SC_FIELD}" SPW="${SC_SPW}" REFANT="${SC_REFANT}" COMBINE="${SC_COMBINE}" MINSNR="${SC_MINSNR}" PARANG="${SC_PARANG}" \
    CALTABLE_PREFIX="${SC_PREFIX[$r]}" PLOT_DIR="plots" APPLY_CALWT="${SC_APPLY_CALWT}" NSPWS="${nspws_val}" SC_UVRANGE="${SC_UVRANGE}" )

  do_flag=0
  if [[ "${SC_CALMODE[$r]}" == "ap" ]]; then
    do_flag=1
  elif [[ "${SC_CALMODE[$((r+1))]:-}" == "ap" ]]; then
    do_flag=1
  fi

  if (( do_flag == 1 )); then
    flag_pattern="${FLAG_SELFCAL_PATTERN//\{index\}/${idx}}"
    jid_sc_flag=$( sbatch_submit "flag_selfcal_${img_tag}" "${FLAG_TIME}" "${FLAG_CPUS}" "${FLAG_MEM}" "${ARRAY_SPEC}" "${RUN_FLAG}" "${jid_sc}" \
                  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${flag_pattern}" SCRIPT_DIR="${SCRIPT_DIR}" COLUMN="${FLAG_COLUMN}" )
    jid_prev=$(chain "$jid_sc_flag" "flag_selfcal_${img_tag}")
  else
    jid_prev=$(chain "$jid_sc" "selfcal_${img_tag}")
  fi
done

# Final image/mask/predict at last index
final_opts="${WSCLEAN_OPTS[${last_idx}]}"

jid_img_final=$( sbatch_submit "wsclean_${IMG_TAGS[${last_idx}]}_final" "${WSCLEAN_TIME}" "${WSCLEAN_CPUS}" "${WSCLEAN_MEM}" "${ARRAY_SPEC}" "${RUN_WSCLEAN}" "$jid_prev" \
  SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}" \
  IMG_TAG="${IMG_TAGS[${last_idx}]}" INDEX="${last_idx}" BIND_SRC="${BIND_SRC}" WSCLEAN_OPTS="${final_opts}" FITS_MASK_TAG="${IMG_TAGS[$((last_idx-1))]}" IGNORE_QC_FAIL="1" )
jid_img_final=$(chain "$jid_img_final" "wsclean_${IMG_TAGS[${last_idx}]}_final")

jid_cp_final=$(
  sbatch_submit "copy_continuum" "${COPY_TIME}" "${COPY_CPUS}" "${COPY_MEM}" \
    "${ARRAY_SPEC}" "${RUN_COPY_CONTINUUM}" "${jid_img_final}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}"
)
jid_cp_final=$(chain "$jid_cp_final" "copy_continuum")

# don't care about the copy_continuum job for the purposes of dependencies
jid_fm_final=$( sbatch_submit "eyepatch_${IMG_TAGS[${last_idx}]}_final" "${EP_TIME}" "${EP_CPUS}" "${EP_MEM}" "${ARRAY_SPEC}" "${RUN_EYEPATCH}" "$jid_img_final" \
  SELFCAL="1" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" IMG_TAG="${IMG_TAGS[${last_idx}]}" INDEX="${last_idx}" \
  FLOOD_FILL_POSITIVE_SEED_CLIP="${EYEPATCH_SEED_CLIP[${last_idx}]}" \
  FLOOD_FILL_POSITIVE_FLOOD_CLIP="${EYEPATCH_FLOOD_CLIP[${last_idx}]}" \
  FLOOD_FILL_MAC_BOX_SIZE="${EYEPATCH_MAC_BOX_SIZE[${last_idx}]}" \
  BEAM_SHAPE_ERODE_MIN_RESPONSE="${EYEPATCH_BEAM_ERODE_MIN_RESPONSE[${last_idx}]}" \
  BEAM_SHAPE_ERODE_SCALES="${EYEPATCH_CLEAN_SCALES[${last_idx}]:-}" \
  FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR="${EYEPATCH_MAC_ADAPTIVE_STEP_FACTOR[${last_idx}]}" \
  IGNORE_QC_FAIL="1" )
jid_fm_final=$(chain "$jid_fm_final" "eyepatch_${IMG_TAGS[${last_idx}]}_final")

jid_uvs_native=$( sbatch_submit "uvsub_native" "${UVSUB_TIME}" "${SC_CPUS}" "${SC_MEM}" "${ARRAY_SPEC}" "${RUN_UVSUB}" "$jid_fm_final" \
  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${WSCLEAN_PATTERN}" FLINT_CASA_SIF="${FLINT_CASA_SIF}" \
  BIND_SRC="${BIND_SRC}" SCRIPT_DIR="${SCRIPT_DIR}" SCRIPT="${UVSUB_SCRIPT}" INDEX="${last_idx}" EXTENSION="G${last_idx}" OUT_PREFIX="uvsub" )
jid_uvs_native=$(chain "$jid_uvs_native" "uvsub_native")

# D) fastducc on uvsubbed native MS
fd_array_spec="${ARRAY_SPEC}"
if [[ -n "${FD_ARRAY_CONCURRENCY:-}" ]]; then
  fd_array_spec="${ARRAY_SPEC}%${FD_ARRAY_CONCURRENCY}"
fi
jid_fastducc=$( sbatch_submit "fastducc" "${FD_TIME}" "${FD_CPUS}" "${FD_MEM}" "${fd_array_spec}" "${RUN_FASTDUCC}" "$jid_uvs_native" \
  SELFCAL="0" SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" PATTERN="${FASTDUCC_INPUT_PATTERN}" BIND_SRC="${BIND_SRC}" INDEX="${last_idx}" EXTENSION="G${last_idx}" \
  FD_ENABLE_LOCAL_STATS="${FD_ENABLE_LOCAL_STATS}" FD_LOCAL_BOX_SIZE="${FD_LOCAL_BOX_SIZE:-128}" \
  FD_ENABLE_VAR_CHUNK="${FD_ENABLE_VAR_CHUNK:-1}" FD_ENABLE_VAR_SCAN="${FD_ENABLE_VAR_SCAN:-1}" FD_ENABLE_VAR_OBS="${FD_ENABLE_VAR_OBS:-1}" \
  FD_WORKER_TIME="${FD_WORKER_TIME:-${FD_TIME:-06:00:00}}" \
  FD_CHUNK_SIZE="${FD_CHUNK_SIZE:-256}" \
  FD_PARALLEL_MODE="${FD_PARALLEL_MODE:-dask-slurm}" \
  FD_DASK_WORKERS="${FD_DASK_WORKERS:-0}" \
  FD_SLURM_CORES_PER_WORKER="${FD_SLURM_CORES_PER_WORKER:-1}" \
  FD_SLURM_MEM="${FD_SLURM_MEM:-64GB}" \
  FD_NPIX_X="${FD_NPIX_X:-1920}" FD_NPIX_Y="${FD_NPIX_Y:-1920}" FD_PIXSIZE_ARCSEC="${FD_PIXSIZE_ARCSEC:-4.4}" \
  FD_DM="${FD_DM:-}" FD_DM_LIST="${FD_DM_LIST:-}" \
  FD_DM_MIN="${FD_DM_MIN:-}" FD_DM_MAX="${FD_DM_MAX:-}" FD_DM_TOL="${FD_DM_TOL:-}" )
jid_fastducc=$(chain "$jid_fastducc" "fastducc")

# E) dstools extract-ds
jid_prev="$jid_fastducc"
for kind in "boxcar"; do
  KIND="$kind"
  jid_prev=$( sbatch_submit "dstools_extract_${kind}" "${EXTRACT_TIME}" "${EXTRACT_CPUS}" "${EXTRACT_MEM}" "" "${RUN_EXTRACT_DS}" "$jid_prev" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" KIND="${KIND}" DS_N_WORKERS="${DS_N_WORKERS}" DS_CPUS="${DS_CPUS}" DS_MEM="${DS_MEM}" \
    DS_WALLTIME="${DS_WALLTIME}" DS_MIN_SNR="${DS_MIN_SNR}" DS_QUEUE="${DS_QUEUE}" DS_PROJECT="${DS_PROJECT}" \
    DS_BATCH_SIZE="${DS_BATCH_SIZE}" DS_RETRIES="${DS_RETRIES}" DS_SLEEP_BETWEEN_BATCHES="${DS_SLEEP_BETWEEN_BATCHES}" \
    DS_BEAM_SCOPE="${DS_BEAM_SCOPE}" DS_MATCH_ARCSEC="${DS_MATCH_ARCSEC}" DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE}" \
    DS_DATACOLUMN="${DS_DATACOLUMN}" DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM}" DS_NOFLAG="${DS_NOFLAG}" \
    DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE}" DS_MINUVDIST="${DS_MINUVDIST}" DS_VERBOSE="${DS_VERBOSE}" \
    DS_OVERWRITE="${DS_OVERWRITE}" DS_DRY_RUN="${DS_DRY_RUN}" DS_CATALOGUE="${DS_CATALOGUE}" \
    DS_SCAN_SCOPE="${DS_SCAN_SCOPE:-all}" DS_JOB_EXTRA="${DS_JOB_EXTRA:-}" )
  jid_prev=$(chain "$jid_prev" "dstools_extract_${kind}")
done

# F) Extra: 24-antenna imaging matching CRACO data
if [[ "${IMAGE_24ANT_ENABLED:-1}" == "1" || "${IMAGE_24ANT_ENABLED:-}" == "true" ]]; then
  jid_img_24ant=$( sbatch_submit "image_24ant_craco_match" "${IMAGE_24ANT_TIME:-01:00:00}" "${IMAGE_24ANT_CPUS:-8}" "${IMAGE_24ANT_MEM:-32G}" \
    "${ARRAY_SPEC}" "${RUN_IMAGE_24ANT}" "${jid_prev}" \
    SBID="${SBID}" DATA_ROOT="${DATA_ROOT}" SCRIPT_DIR="${SCRIPT_DIR}" INDEX="${last_idx}" )
  jid_prev=$(chain "$jid_img_24ant" "image_24ant_craco_match")
fi

echo "Pipeline_10s submitted."
