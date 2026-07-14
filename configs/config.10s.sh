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

# -------------------- Basic context --------------------
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

# -------------------- Wrapper scripts ------------------
RUN_FIXDIR="${SCRIPT_DIR}/scripts/slurm/run_fixdir.sh"
RUN_QUACK="${SCRIPT_DIR}/scripts/slurm/run_quack_beams.sh"
RUN_UNFLAG="${SCRIPT_DIR}/scripts/slurm/run_unflag_beams.sh"
RUN_FLAG="${SCRIPT_DIR}/scripts/slurm/run_flag.sh"

RUN_WSCLEAN="${SCRIPT_DIR}/scripts/slurm/run_wsclean_beams.sh"
RUN_FLINT_MASK="${SCRIPT_DIR}/scripts/slurm/run_flintmask_beams.sh"
RUN_CB="${SCRIPT_DIR}/scripts/slurm/run_crystalball_beams.sh"
RUN_SELFCAL="${SCRIPT_DIR}/scripts/slurm/run_selfcal_beams.sh"
RUN_UVSUB="${SCRIPT_DIR}/scripts/slurm/run_uvsub_beams.sh"
RUN_COPY_CONTINUUM="${SCRIPT_DIR}/scripts/slurm/run_copy_continuum.sh"
RUN_FASTDUCC="${SCRIPT_DIR}/scripts/slurm/run_fastducc_beams.sh"
RUN_FASTDUCC_AGG="${SCRIPT_DIR}/scripts/slurm/run_fastducc_aggregate_chunks.sh"
RUN_EXTRACT_DS="${SCRIPT_DIR}/scripts/slurm/run_dstools_extract_cands.sh"

# Legacy compatible
RUN_IMPORT="${SCRIPT_DIR}/scripts/slurm/run_import.sh"
RUN_AVERAGE="${SCRIPT_DIR}/scripts/slurm/run_average_beams.sh"
RUN_CONCAT="${SCRIPT_DIR}/scripts/slurm/run_concat_beams.sh"
RUN_CLEARCAL="${SCRIPT_DIR}/scripts/slurm/run_clearcal_beams.sh"
RUN_APPLYCAL="${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh"
RUN_BANDPASS="${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh"
RUN_FLAGOUTER="${SCRIPT_DIR}/scripts/slurm/run_flagouter_beams.sh"
FLAG_OUTER="False" # Whether to flag outer antennas to match the inner antennas used by CRACO

# -------------------- Tools ---------------------
FIXDIR_SCRIPT="${SCRIPT_DIR}/scripts/utils/fix_dir.py"
IMPORT_SCRIPT="${SCRIPT_DIR}/src/casa/import_array.py"
QUACK_SCRIPT="${SCRIPT_DIR}/src/casa/quack_ms_beams.py"
BANDPASS_SCRIPT="${SCRIPT_DIR}/src/casa/applycal_ms_beams.py"
AVERAGE_SCRIPT="${SCRIPT_DIR}/src/casa/average_ms_beams.py"
CONCAT_SCRIPT="${SCRIPT_DIR}/src/casa/concat_ms_beams.py"
FLAGOUTER_SCRIPT="${SCRIPT_DIR}/src/casa/flagouter_beams.py"
SELFCAL_SCRIPT="${SCRIPT_DIR}/src/casa/selfcal_ms_beams.py"
APPLYCAL_SCRIPT="${SCRIPT_DIR}/src/casa/applycal_ms_beams.py"
UVSUB_SCRIPT="${SCRIPT_DIR}/src/casa/uvsub_ms_beams.py"

# -------------------- Python launchers -----------------
AVERAGE_PYTHON="apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3"
CONCAT_PYTHON="apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3"

# -------------------- Resources ------------------------
ARRAY_SPEC="0-35"
BIGARRAY_SPEC="0-500"

IMPORT_CPUS="2"
IMPORT_MEM="1G"

FLAG_CPUS="4"
FLAG_MEM="12G"

AVERAGE_CPUS="4"
AVERAGE_MEM="4G"

CONCAT_CPUS="4"
CONCAT_MEM="16G"

WSCLEAN_CPUS="4"
WSCLEAN_MEM="32G"

SC_CPUS="8"
SC_MEM="4G"

FM_CPUS="1"
FM_MEM="1G"

FD_CPUS="1"
FD_MEM="8G"
FD_TIME="06:00:00"

# NOTE:
# Crystalball runs in DISTRIBUTED mode.
# CB_CPUS/CB_MEM apply ONLY to the client job.
# Real compute happens in bounded per-beam Dask workers.
CB_TIME="03:15:00"
CB_CPUS="2"
CB_MEM="4G"
APPLYCAL_TIME="04:00:00"

AGG_TIME="00:30:00"
AGG_CPUS="1"
AGG_MEM="2G"

EXTRACT_TIME="01:00:00"
EXTRACT_CPUS="1"
EXTRACT_MEM="4G"

IMPORT_TIME="00:10:00"
UNFLAG_TIME="00:30:00"
FLAG_TIME="00:30:00"
AVERAGE_TIME="01:00:00"
CONCAT_TIME="01:00:00"
WSCLEAN_TIME="04:00:00"
FM_TIME="00:30:00"
SC_TIME="02:00:00"
UVSUB_TIME="02:00:00"
BANDPASS_TIME="02:00:00"

COPY_TIME="00:15:00"
COPY_CPUS="1"
COPY_MEM="2G"

# -------------------- Stage parameters -----------------
TIMEBIN="9.90s"
UVSUB_OUT_PREFIX="uvsub"
CHUNK_GLOB="202?*"
KIND="boxcar"
FLAG_COLUMN="DATA"

# -------------------- Self-cal behaviour ----------------
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

# -------------------- Imaging options ------------------
# WSCLEAN_OPTS0..6 env vars can still be used to override individual rounds from outside.
declare -ag WSCLEAN_OPTS
WSCLEAN_OPTS[0]="${WSCLEAN_OPTS0:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 25000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 3 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[1]="${WSCLEAN_OPTS1:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 2 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 8.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 1.0 -auto-mask 3.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -mgain 0.8 -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol i -weight briggs 0.5 -scale 2.5asec -size 8192 8192 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"

# -------------------- Crystalball behaviour -------------
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

# -------------------- Flint mask thresholds -------------
FLOOD_FILL_POSITIVE_SEED_CLIP="1.1"
FLOOD_FILL_POSITIVE_FLOOD_CLIP="0.7"
FLOOD_FILL_MAC_BOX_SIZE="350"
BEAM_SHAPE_ERODE_MIN_RESPONSE="0.75"

# -------------------- Fastducc toggles ------------------
FD_NO_VAR_SEARCH=""
FD_NO_BOX_SEARCH=""
FD_PLOT_CANDS_ONLY=""

# -------------------- Patterns --------------------------
# Native 10s MS, e.g. scienceData.*.beamXX_averaged_cal*.ms
NATIVE10S_PATTERN="*beam{beam:02d}_averaged_cal.leakage.ms"

# Imaging/selfcal pattern
WSCLEAN_PATTERN="${NATIVE10S_PATTERN}"

# Fastducc input (native uvsub)
FASTDUCC_INPUT_PATTERN="*beam{beam:02d}*.uvsub.ms"

# -------------------- dstools extract-ds -----------------
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

# -------------------- Create output directory ------------
mkdir -p "${OUT_ROOT}"
