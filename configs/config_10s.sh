#!/usr/bin/env bash
# ============================================================================
# ASKAP high-time-res imaging/selfcal/uvsub/transient pipeline - configuration
# 10s native-resolution variant
# ============================================================================
# Usage:
#   ./pipeline_10s.sh SBID=SBXXXXXX CONFIG=/path/to/config.10s.sh
#
# Notes:
# - Values set here are ALWAYS applied when this file is sourced, regardless
#   of any previously exported environment variables.  Edit this file to change
#   any setting; do not rely on exporting variables before invocation.
# - Exceptions (kept as ${VAR:-default} so they can be set externally):
#     DRY_RUN, DRY_FAKE_START, DRY_PRINT_CMDS, USER
# - Keep strings in double quotes.
# ============================================================================

# =============================================================================
# 1. Global Context & Resources
# =============================================================================
USER="$(whoami)"
# SBID="${SBID:-SB82418}"
USER_PATH="/fred/oz451"
DATA_ROOT="${USER_PATH}/${USER}/data/continuum"
OUT_ROOT="${USER_PATH}/${USER}/data/continuum"
BIND_SRC="${USER_PATH}"
CONTAINER_DIR="${USER_PATH}/${USER}/containers"
LOG_DIR="${USER_PATH}/${USER}/lotrun_processing/logs"
SCRIPT_DIR="${USER_PATH}/$USER/scripts/lotrun_processing"

# -------------------- Dry-run controls --------------------
DRY_RUN="${DRY_RUN:-0}"
DRY_FAKE_START="${DRY_FAKE_START:-490000}"
DRY_PRINT_CMDS="${DRY_PRINT_CMDS:-1}"

# -------------------- Containers -----------------------
FLINT_WSCLEAN_SIF="${CONTAINER_DIR}/flint-containers_wsclean.sif"
FLINT_CASA_SIF="${CONTAINER_DIR}/flint-containers_casa.sif"
CRYSTALBALL_SIF="${CONTAINER_DIR}/casacore_python.sif"

# -------------------- General Parameters ----------------
ARRAY_SPEC="0-35"
BIGARRAY_SPEC="0-500"
NATIVE10S_PATTERN="*beam{beam:02d}_averaged_cal.leakage.ms"

# =============================================================================
# 2. Import
# =============================================================================
RUN_IMPORT="${SCRIPT_DIR}/scripts/slurm/run_import.sh"
IMPORT_SCRIPT="${SCRIPT_DIR}/src/casa/import_array.py"
IMPORT_CPUS="2"
IMPORT_MEM="1G"
IMPORT_TIME="00:10:00"

# =============================================================================
# 3. Flagging, Quack & Fixdir
# =============================================================================
RUN_FIXDIR="${SCRIPT_DIR}/scripts/slurm/run_fixdir.sh"
FIXDIR_SCRIPT="${SCRIPT_DIR}/scripts/utils/fix_dir.py"

RUN_QUACK="${SCRIPT_DIR}/scripts/slurm/run_quack_beams.sh"
QUACK_SCRIPT="${SCRIPT_DIR}/src/casa/quack_ms_beams.py"
QUACK_MEM="2G"
QUACK_TIME="00:15:00"

RUN_UNFLAG="${SCRIPT_DIR}/scripts/slurm/run_unflag_beams.sh"
UNFLAG_TIME="00:30:00"

RUN_FLAG="${SCRIPT_DIR}/scripts/slurm/run_flag.sh"
FLAG_COLUMN="DATA"
FLAG_CPUS="4"
FLAG_MEM="12G"
FLAG_TIME="00:15:00"

RUN_FLAGOUTER="${SCRIPT_DIR}/scripts/slurm/run_flagouter_beams.sh"
FLAGOUTER_SCRIPT="${SCRIPT_DIR}/src/casa/flagouter_beams.py"
FLAG_OUTER="False" # Whether to flag outer antennas to match the inner antennas used by CRACO

# =============================================================================
# 4. Bandpass / Applycal
# =============================================================================
RUN_BANDPASS="${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh"
BANDPASS_SCRIPT="${SCRIPT_DIR}/src/casa/applycal_ms_beams.py"
BANDPASS_TIME="01:00:00"

RUN_APPLYCAL="${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh"
APPLYCAL_SCRIPT="${SCRIPT_DIR}/src/casa/applycal_ms_beams.py"
APPLYCAL_TIME="01:30:00"

# =============================================================================
# 5. Averaging
# =============================================================================
RUN_AVERAGE="${SCRIPT_DIR}/scripts/slurm/run_average_beams.sh"
AVERAGE_SCRIPT="${SCRIPT_DIR}/src/casa/average_ms_beams.py"
AVERAGE_PYTHON="apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3"
AVERAGE_CPUS="4"
AVERAGE_MEM="2G"
AVERAGE_TIME="00:15:00"
TIMEBIN="9.90s"

# =============================================================================
# 6. Concatenation
# =============================================================================
RUN_CONCAT="${SCRIPT_DIR}/scripts/slurm/run_concat_beams.sh"
CONCAT_SCRIPT="${SCRIPT_DIR}/src/casa/concat_ms_beams.py"
CONCAT_PYTHON="apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3"
CONCAT_CPUS="4"
CONCAT_MEM="4G"
CONCAT_TIME="00:30:00"

# =============================================================================
# 7. Continuum Imaging & Self-Calibration
# =============================================================================
# WSClean
RUN_WSCLEAN="${SCRIPT_DIR}/scripts/slurm/run_wsclean_beams.sh"
WSCLEAN_CPUS="4"
WSCLEAN_MEM="8G"
WSCLEAN_TIME="00:30:00"
WSCLEAN_PATTERN="${NATIVE10S_PATTERN}"

# WSCLEAN_OPTS0..6 env vars can still be used to override individual rounds from outside.
WSCLEAN_CHANNELS_OUT=4
declare -ag WSCLEAN_OPTS
WSCLEAN_OPTS[0]="${WSCLEAN_OPTS0:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 25000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 3 -auto-mask 15.0 -join-channels   -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[1]="${WSCLEAN_OPTS1:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 2 -auto-mask 15.0 -join-channels  -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 8.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 3.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"

# Flint Masking
RUN_FLINT_MASK="${SCRIPT_DIR}/scripts/slurm/run_flintmask_beams.sh"
FM_CPUS="1"
FM_MEM="1G"
FM_TIME="00:15:00"
FLOOD_FILL_POSITIVE_SEED_CLIP="1.1"
FLOOD_FILL_POSITIVE_FLOOD_CLIP="0.7"
FLOOD_FILL_MAC_BOX_SIZE="350"
BEAM_SHAPE_ERODE_MIN_RESPONSE="0.75"

# Selfcal
RUN_SELFCAL="${SCRIPT_DIR}/scripts/slurm/run_selfcal_beams.sh"
SELFCAL_SCRIPT="${SCRIPT_DIR}/src/casa/selfcal_ms_beams.py"
SC_CPUS="8"
SC_MEM="4G"
SC_TIME="00:15:00"
SC_FIELD=""
SC_SPW=""
SC_REFANT=""
SC_COMBINE="scan"
SC_MINSNR="3.0"
SC_PARANG=""
SC_APPLY_CALWT="False"

declare -ag IMG_TAGS=("initial_scratch" "selfcal_1" "selfcal_2" "selfcal_3" "selfcal_4" "selfcal_5" "selfcal_6")
declare -ag SC_INDEX=(1 2 3 4 5 6)
declare -ag SC_CALMODE=("p" "p" "p" "p" "ap" "ap")
declare -ag SC_SOLINT=("480s" "300s" "120s" "30s" "600s" "300s")
declare -ag SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_p" "selfcal5_ap" "selfcal6_ap")
declare -ag SC_NSPWS=(16 16 16 16 16 16)

# =============================================================================
# 8. Predict & UVSub
# =============================================================================
# Crystalball specifics
RUN_CB="${SCRIPT_DIR}/scripts/slurm/run_crystalball_beams.sh"

# NOTE:
# Crystalball runs in DISTRIBUTED mode.
# CB_CPUS/CB_MEM apply ONLY to the client job.
# Real compute happens in bounded per-beam Dask workers.
CB_TIME="03:15:00"
CB_CPUS="2"
CB_MEM="4G"
CB_SOURCE_LIST_PATTERN="*beam{beam:02d}_averaged_cal.leakage.ms"
CB_OUTPUT_COLUMN="MODEL_DATA"
CB_NUM_WORKERS="0"
CB_ROW_CHUNKS="0"
CB_MODEL_CHUNKS="0"
CB_MEMORY_FRACTION="0.8"
CB_DISTRIBUTED="1"

# Max number of Dask workers per beam
CB_DASK_NWORKERS="4"
# CPUs per Dask worker
CB_DASK_WORKER_CPUS="1"
# Memory per Dask worker  (4 workers × 4G = 16G, within 32G SLURM alloc)
CB_DASK_WORKER_MEM="4G"

# UVSub
RUN_UVSUB="${SCRIPT_DIR}/scripts/slurm/run_uvsub_beams.sh"
UVSUB_SCRIPT="${SCRIPT_DIR}/src/casa/uvsub_ms_beams.py"
UVSUB_TIME="01:00:00"
UVSUB_OUT_PREFIX="uvsub"


# =============================================================================
# 9. FastDUCC transient search
# =============================================================================
RUN_FASTDUCC="${SCRIPT_DIR}/scripts/slurm/run_fastducc_beams.sh"
RUN_FASTDUCC_AGG="${SCRIPT_DIR}/scripts/slurm/run_fastducc_aggregate_chunks.sh"
FD_CPUS="1"
FD_MEM="4G"
FD_TIME="02:00:00"
AGG_TIME="00:15:00"
AGG_CPUS="1"
AGG_MEM="1G"

CHUNK_GLOB="202?*"
KIND="boxcar"
FASTDUCC_INPUT_PATTERN="*beam{beam:02d}*.uvsub.ms"

# options for fastducc search/no search/plot_cands/only
# set to 1 to enable the option; leave empty string to disable
FD_NO_VAR_SEARCH=""
FD_NO_BOX_SEARCH=""
FD_PLOT_CANDS_ONLY=""

# =============================================================================
# 10. Dstools Extraction
# =============================================================================
RUN_EXTRACT_DS="${SCRIPT_DIR}/scripts/slurm/run_dstools_extract_cands.sh"
EXTRACT_TIME="01:00:00"
EXTRACT_CPUS="1"
EXTRACT_MEM="4G"

DS_N_WORKERS="48"
DS_CPUS="1"
DS_MEM="8GB"
DS_WALLTIME="01:00:00"
DS_QUEUE=""
DS_PROJECT=""
DS_MIN_SNR="8.0"
DS_BATCH_SIZE="200"
DS_RETRIES="1"
DS_SLEEP_BETWEEN_BATCHES="0"
DS_BEAM_SCOPE="union"
DS_MATCH_ARCSEC="35.0"
DS_MS_GLOB_TEMPLATE="**/*uvsub.ms"
DS_DATACOLUMN="DATA"
DS_PRIMARY_BEAM=""
DS_NOFLAG="false"
DS_BASELINE_AVERAGE="true"
DS_MINUVDIST="0.0"
DS_VERBOSE="false"
DS_OVERWRITE="false"
DS_DRY_RUN="false"
DS_CATALOGUE=""
DS_SCAN_SCOPE="all"

# =============================================================================
# 11. Utilities
# =============================================================================
RUN_COPY_CONTINUUM="${SCRIPT_DIR}/scripts/slurm/run_copy_continuum.sh"
COPY_TIME="00:15:00"
COPY_CPUS="1"
COPY_MEM="2G"

RUN_CLEARCAL="${SCRIPT_DIR}/scripts/slurm/run_clearcal_beams.sh"

# -------------------- Create output directory ------------
mkdir -p "${OUT_ROOT}"
