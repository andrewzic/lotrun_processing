#!/usr/bin/env bash
# =============================================================================
# ASKAP high-time-res imaging/selfcal/uvsub/transient pipeline — configuration
# =============================================================================
# Usage:
#   source pipeline.config.sh          # sets defaults in current shell
#   CONFIG=my.config.sh ./pipeline.sh  # pipeline.sh will source CONFIG
#
# Notes:
# - Every value here can still be overridden by exporting the env var before
#   sourcing (or before invoking pipeline.sh).
# - Keep strings double-quoted; paths without spaces are fine unquoted.
# =============================================================================

# -------------------- Basic context --------------------
USER="${USER:-$(whoami)}"
SBID="${SBID:-SB82418}"
DATA_ROOT="${DATA_ROOT:-/fred/oz451/${USER}/data}"
OUT_ROOT="${OUT_ROOT:-/fred/oz451/${USER}/data}"
BIND_SRC="${BIND_SRC:-/fred/oz451}"
CONTAINER_DIR="${CONTAINER_DIR:-/fred/oz451/${USER}/containers}"
LOG_DIR="${LOG_DIR:-/fred/oz451/${USER}/lotrun_processing/logs}"
SCRIPT_DIR="${SCRIPT_DIR:-/fred/oz451/$USER/scripts/lotrun_processing}"

# -------------------- Dry-run controls --------------------
# When DRY_RUN=1, no sbatch calls are made; commands are printed and fake JIDs returned.
DRY_RUN="${DRY_RUN:-0}"

# Start sequence for fake job IDs (array and non-array jobs alike).
# Change this if you want different ranges per environment.
DRY_FAKE_START="${DRY_FAKE_START:-490000}"

# If set to 1, print the "DRY sbatch:" command lines (stderr).
DRY_PRINT_CMDS="${DRY_PRINT_CMDS:-1}"

# -------------------- Containers -----------------------
FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF:-${CONTAINER_DIR}/flint-containers_wsclean.sif}"
FLINT_CASA_SIF="${FLINT_CASA_SIF:-${CONTAINER_DIR}/flint-containers_casa.sif}"

# -------------------- Wrapper scripts ------------------
RUN_IMPORT="${RUN_IMPORT:-${SCRIPT_DIR}/scripts/slurm/run_import.sh}"
RUN_FLAG="${RUN_FLAG:-${SCRIPT_DIR}/scripts/slurm/run_flag.sh}"
RUN_AVERAGE="${RUN_AVERAGE:-${SCRIPT_DIR}/scripts/slurm/run_average_beams.sh}"
RUN_CONCAT="${RUN_CONCAT:-${SCRIPT_DIR}/scripts/slurm/run_concat_beams.sh}"
RUN_WSCLEAN="${RUN_WSCLEAN:-${SCRIPT_DIR}/scripts/slurm/run_wsclean_beams.sh}"
RUN_FLINT_MASK="${RUN_FLINT_MASK:-${SCRIPT_DIR}/scripts/slurm/run_flintmask_beams.sh}"
RUN_CB="${RUN_CB:-${SCRIPT_DIR}/scripts/slurm/run_crystalball_beams.sh}"
RUN_SELFCAL="${RUN_SELFCAL:-${SCRIPT_DIR}/scripts/slurm/run_selfcal_beams.sh}"
RUN_APPLYCAL="${RUN_APPLYCAL:-${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh}"
RUN_BANDPASS="${RUN_BANDPASS:-${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh}"
RUN_UVSUB="${RUN_UVSUB:-${SCRIPT_DIR}/scripts/slurm/run_uvsub_beams.sh}"
RUN_FASTDUCC="${RUN_FASTDUCC:-${SCRIPT_DIR}/scripts/slurm/run_fastducc_beams.sh}"
RUN_FASTDUCC_AGG="${RUN_FASTDUCC_AGG:-${SCRIPT_DIR}/scripts/slurm/run_fastducc_aggregate_chunks.sh}"
# Optional obs-level aggregation (leave unset to skip)
# RUN_FASTDUCC_OBSAGG="run_fastducc_aggregate_obs.sh"
RUN_CLEARCAL="${RUN_CLEARCAL:-${SCRIPT_DIR}/scripts/slurm/run_clearcal_beams.sh}"
RUN_EXTRACT_DS="${RUN_EXTRACT_DS:-${SCRIPT_DIR}/scripts/slurm/run_dstools_extract_cands.sh}"
RUN_FLAGOUTER="${RUN_FLAGOUTER:-${SCRIPT_DIR}/scripts/slurm/run_flagouter_beams.sh}"

# -------------------- Tool scripts ---------------------
IMPORT_SCRIPT="${IMPORT_SCRIPT:-${SCRIPT_DIR}/src/casa/import_array.py}"
AVERAGE_SCRIPT="${AVERAGE_SCRIPT:-${SCRIPT_DIR}/src/casa/average_ms_beams.py}"
CONCAT_SCRIPT="${CONCAT_SCRIPT:-${SCRIPT_DIR}/src/casa/concat_ms_beams.py}"
FLAGOUTER_SCRIPT="${FLAGOUTER_SCRIPT:-${SCRIPT_DIR}/src/casa/flagouter_beams.py}"

# -------------------- Python launchers -----------------
AVERAGE_PYTHON="${AVERAGE_PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3}"
CONCAT_PYTHON="${CONCAT_PYTHON:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3}"

IMPORT_CPUS="${IMPORT_CPUS:-2}"
IMPORT_MEM="${IMPORT_MEM:-2G}"
FLAG_CPUS="${FLAG_CPUS:-4}"
FLAG_MEM="${FLAG_MEM:-12G}"
AVERAGE_CPUS="${AVERAGE_CPUS:-4}"
AVERAGE_MEM="${AVERAGE_MEM:-4G}"
CONCAT_CPUS="${CONCAT_CPUS:-4}"
CONCAT_MEM="${CONCAT_MEM:-16G}"
WSCLEAN_CPUS="${WSCLEAN_CPUS:-4}"
WSCLEAN_MEM="${WSCLEAN_MEM:-16G}"
SC_CPUS="${SC_CPUS:-8}"
SC_MEM="${SC_MEM:-4G}"
FM_CPUS="${FM_CPUS:-1}"
FM_MEM="${FM_MEM:-1G}"
FD_CPUS="${FD_CPUS:-1}"
FD_MEM="${FD_MEM:-32G}"

CB_TIME="${CB_TIME:-03:15:00}"
CB_CPUS="${CB_CPUS:-32}"
CB_MEM="${CB_MEM:-54G}"
AGG_TIME="${AGG_TIME:-00:30:00}"
AGG_CPUS="${AGG_CPUS:-1}"
AGG_MEM="${AGG_MEM:-2G}"
FD_TIME="${FD_TIME:-06:00:00}"
APPLYCAL_TIME="${APPLYCAL_TIME:-04:00:00}"

EXTRACT_TIME="${EXTRACT_TIME:-01:00:00}"
EXTRACT_CPUS="${EXTRACT_CPUS:-1}"
EXTRACT_MEM="${EXTRACT_MEM:-4G}"

IMPORT_TIME="${IMPORT_TIME:-00:10:00}"
FLAG_TIME="${FLAG_TIME:-00:30:00}"
AVERAGE_TIME="${AVERAGE_TIME:-01:00:00}"
CONCAT_TIME="${CONCAT_TIME:-01:00:00}"
WSCLEAN_TIME="${WSCLEAN_TIME:-04:00:00}"
FM_TIME="${FM_TIME:-00:30:00}"
SC_TIME="${SC_TIME:-02:00:00}"
UVSUB_TIME="${UVSUB_TIME:-02:00:00}"
BANDPASS_TIME="${BANDPASS_TIME:-02:00:00}"

# -------------------- Stage parameters -----------------
TIMEBIN="${TIMEBIN:-9.90s}"
UVSUB_OUT_PREFIX="${UVSUB_OUT_PREFIX:-uvsub}"
CHUNK_GLOB="${CHUNK_GLOB:-202?*}"
KIND="${KIND:-boxcar}"
FLAG_COLUMN="${FLAG_COLUMN:-DATA}"

# -------------------- Resource requests ----------------
ARRAY_SPEC="${ARRAY_SPEC:-0-35}"
BIGARRAY_SPEC="${BIGARRAY_SPEC:-0-500}"
n_chunks=$( ls -d ${DATA_ROOT}/${SBID}/${CHUNK_GLOB} | wc -l )
if (( n_chunks > 0 ))
then
  CHUNK_ARRAY_SPEC="0-$((n_chunks-1))"
else
  CHUNK_ARRAY_SPEC="0-0"
fi

# -------------------- Self-cal behaviour ----------------
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

# options for fastducc search/no search/plot_cands/only
# set to 1 to enable the option; leave unset or set to empty string to disable
FD_NO_VAR_SEARCH=${FD_NO_VAR_SEARCH:-}
FD_NO_BOX_SEARCH=${FD_NO_BOX_SEARCH:-}
FD_PLOT_CANDS_ONLY=${FD_PLOT_CANDS_ONLY:-}

# -------------------- Stage-aware patterns ----------------
# Import
UVFITS_PATTERN="${UVFITS_PATTERN:-20??*/*beam*.uvfits}"

# Flag native
FLAG_NATIVE_PATTERN="${FLAG_NATIVE_PATTERN:-20??*/*beam*.20????????????.ms}"

# Bandpass inputs (native)
BANDPASS_INPUT_PATTERN="${BANDPASS_INPUT_PATTERN:-20??*/*beam*.20????????????.ms}"

# Flag calB0
FLAG_CALB0_PATTERN="${FLAG_CALB0_PATTERN:-20??*/*beam*.20????????????.calB0.ms}"

# Average inputs (calB0)
AVERAGE_INPUT_PATTERN="${AVERAGE_INPUT_PATTERN:-20??*/*beam*.20????????????.calB0.ms}"

# Flag averaged
FLAG_AVG_PATTERN="${FLAG_AVG_PATTERN:-20??*/*beam*.20????????????.avg.calB0.ms}"

# Concat inputs (averaged)
CONCAT_INPUT_PATTERN="${CONCAT_INPUT_PATTERN:-20??*/*beam*.20????????????.avg.calB0.ms}"

# Imaging/selfcal (concatenated)
WSCLEAN_PATTERN="${WSCLEAN_PATTERN:-*beam{beam:02d}.avg.calB0.ms}"

# UVSUB on concatenated self-cal result
# If filenames are exactly "...beam{NN}.avg.calG6.ms":
UVSUB_CONCAT_INPUT_PATTERN="${UVSUB_CONCAT_INPUT_PATTERN:-*beam{beam:02d}.avg.calG6.ms}"


# Applycal on native res (start from calB0; loop produces calG<i>)
APPLYCAL_NATIVE_START_PATTERN="${APPLYCAL_NATIVE_START_PATTERN:-20??*/*beam*.20????????????.calB0.ms}"

# fastducc on uvsubbed native res
FASTDUCC_INPUT_PATTERN="${FASTDUCC_INPUT_PATTERN:-20??*/*beam*.20????????????.calB0.uvsub.ms}"

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
DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE:-*/cracoData*%s*uvsub.ms}"
DS_DATACOLUMN="${DS_DATACOLUMN:-data}"
DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM:-}"
DS_NOFLAG="${DS_NOFLAG:-false}"
DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE:-true}"
DS_MINUVDIST="${DS_MINUVDIST:-0.0}"
DS_VERBOSE="${DS_VERBOSE:-false}"
DS_OVERWRITE="${DS_OVERWRITE:-false}"
DS_DRY_RUN="${DS_DRY_RUN:-false}"
DS_CATALOGUE="${DS_CATALOGUE:-}"
DS_SCAN_SCOPE="${DS_SCAN_SCOPE:-all}"   # 'all' or 'catalogue'

# -------------------- Sanity: required dirs --------------
mkdir -p "${OUT_ROOT}"
