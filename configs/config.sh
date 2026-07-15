#!/usr/bin/env bash
# =============================================================================
# ASKAP high-time-res imaging/selfcal/uvsub/transient pipeline — configuration
# =============================================================================
# Usage:
#   ./pipeline.sh SBID=SBXXXXXX CONFIG=/path/to/config.sh
#
# Notes:
# - Values set here are ALWAYS applied when this file is sourced, regardless
#   of any previously exported environment variables.  Edit this file to change
#   any setting; do not rely on exporting variables before invocation.
# - Exceptions (kept as ${VAR:-default} so they can be set externally):
#     DRY_RUN, DRY_FAKE_START, DRY_PRINT_CMDS, USER
# - Keep strings double-quoted; paths without spaces are fine unquoted.
# =============================================================================

# =============================================================================
# 1. Global Context & Resources
# =============================================================================
USER="${USER:-$(whoami)}"
# SBID="${SBID:-SB77974}"
USER_PATH="/fred/oz451" #location on machine where user directory is
DATA_ROOT="${USER_PATH}/${USER}/data" # location where data is kept
OUT_ROOT="${USER_PATH}/${USER}/data" #location where output goes
CONT_OUT_ROOT="${USER_PATH}/${USER}/data"
NATIVE_OUT_ROOT="${USER_PATH}/${USER}/data"
CONT_OUT_SUBDIR="cont_combined"
CONT_CALTABLE_DIR="caltables"
NATIVE_OUT_SUBDIR="native_combined"
BIND_SRC="${USER_PATH}"
CONTAINER_DIR="${USER_PATH}/${USER}/containers"
LOG_DIR="${USER_PATH}/${USER}/lotrun_processing/logs"
SCRIPT_DIR="${USER_PATH}/$USER/scripts/lotrun_processing"

# -------------------- Dry-run controls --------------------
# When DRY_RUN=1, no sbatch calls are made; commands are printed and fake JIDs returned.
DRY_RUN="${DRY_RUN:-0}"
DRY_FAKE_START="${DRY_FAKE_START:-490000}"
DRY_PRINT_CMDS="${DRY_PRINT_CMDS:-1}"

# -------------------- Containers -----------------------
FLINT_WSCLEAN_SIF="${CONTAINER_DIR}/flint-containers_wsclean.sif"
FLINT_CASA_SIF="${CONTAINER_DIR}/flint-containers_casa.sif"
CRYSTALBALL_SIF="${CONTAINER_DIR}/casacore_python.sif"

# -------------------- General Parameters ----------------
CLOBBER="False"
ARRAY_SPEC="0-35"
BIGARRAY_SPEC="0-500"

# =============================================================================
# 2. Import
# =============================================================================
RUN_IMPORT="${SCRIPT_DIR}/scripts/slurm/run_import.sh"
IMPORT_SCRIPT="${SCRIPT_DIR}/src/casa/import_array.py"
IMPORT_CPUS="2"
IMPORT_MEM="1G"
IMPORT_TIME="00:10:00"
UVFITS_PATTERN="20??*/*beam*.uvfits"

# =============================================================================
# 3. Flagging & Quack
# =============================================================================
RUN_QUACK="${SCRIPT_DIR}/scripts/slurm/run_quack_beams.sh"
QUACK_SCRIPT="${SCRIPT_DIR}/src/casa/quack_ms_beams.py"
QUACK_MEM="2G"
QUACK_TIME="00:15:00"
FLAG_QUACK_PATTERN="20??*/*beam{beam:02d}*.20????????????.ms"

RUN_FLAG="${SCRIPT_DIR}/scripts/slurm/run_flag.sh"
FLAG_COLUMN="DATA"
FLAG_CPUS="4"
FLAG_MEM="12G"
FLAG_TIME="00:15:00"
FLAG_NATIVE_PATTERN="20??*/*beam*.20????????????.ms"
FLAG_CALB0_PATTERN="20??*/*beam*.20????????????.calB0.ms"
FLAG_AVG_PATTERN="20??*/*beam*.20????????????.avg.calB0.ms"

RUN_FLAGOUTER="${SCRIPT_DIR}/scripts/slurm/run_flagouter_beams.sh"
FLAGOUTER_SCRIPT="${SCRIPT_DIR}/src/casa/flagouter_beams.py"

# =============================================================================
# 4. Bandpass / Applycal
# =============================================================================
RUN_BANDPASS="${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh"
BANDPASS_SCRIPT="${SCRIPT_DIR}/src/casa/applycal_ms_beams.py"
BANDPASS_TIME="01:00:00"
BANDPASS_INPUT_PATTERN="20??*/*beam{beam:02d}*.20????????????.ms"

RUN_APPLYCAL="${SCRIPT_DIR}/scripts/slurm/run_applycal_beams.sh"
APPLYCAL_SCRIPT="${SCRIPT_DIR}/src/casa/applycal_ms_beams.py"
APPLYCAL_TIME="01:30:00"
APPLYCAL_NATIVE_START_PATTERN="20????????????/*beam{beam:02d}*.20????????????.calB0.ms"

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
AVERAGE_INPUT_PATTERN="20??*/*beam*.20????????????.calB0.ms"

# =============================================================================
# 6. Concatenation
# =============================================================================
RUN_CONCAT="${SCRIPT_DIR}/scripts/slurm/run_concat_beams.sh"
CONCAT_SCRIPT="${SCRIPT_DIR}/src/casa/concat_ms_beams.py"
CONCAT_PYTHON="apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CONTAINER_DIR}/flint-containers_casa.sif python3"
CONCAT_CPUS="4"
CONCAT_MEM="4G"
CONCAT_CONT_TIME="00:30:00"
CONCAT_NATIVE_TIME="00:30:00"
CONCAT_AVG_INPUT_PATTERN="20????????????/*beam{beam:02d}*.20????????????.avg.calB0.ms"
CONCAT_NATIVE_INPUT_PATTERN="20????????????/*beam{beam:02d}*.20????????????.calG6.uvsub.ms"

# =============================================================================
# 7. Continuum Imaging & Self-Calibration
# =============================================================================
# WSClean
RUN_WSCLEAN="${SCRIPT_DIR}/scripts/slurm/run_wsclean_beams.sh"
WSCLEAN_CPUS="4"
WSCLEAN_MEM="8G"
WSCLEAN_TIME="00:30:00"
WSCLEAN_PATTERN="cont_combined/*beam{beam:02d}.avg.calB0.ms"

# WSClean options (per round)
# WSCLEAN_OPTS0..7 env vars can still be used to override individual rounds from outside.
WSCLEAN_CHANNELS_OUT=4
declare -ag WSCLEAN_OPTS
WSCLEAN_OPTS[0]="${WSCLEAN_OPTS0:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 25000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 3 -auto-mask 15.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[1]="${WSCLEAN_OPTS1:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 2 -auto-mask 15.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 8.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 3.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 4.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"
WSCLEAN_OPTS[7]="${WSCLEAN_OPTS7:-"-data-column DATA -save-source-list -multiscale -mgain 0.8 -multiscale-scale-bias 0.8 -multiscale-max-scales 5 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 4.0 -join-channels -channels-out ${WSCLEAN_CHANNELS_OUT} -fit-spectral-pol 3"}"


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

# Round tags & selfcal controls
declare -ag IMG_TAGS=("initial_scratch" "selfcal_1" "selfcal_2" "selfcal_3" "selfcal_4" "selfcal_5" "selfcal_6" "selfcal_7")
declare -ag SC_INDEX=(1 2 3 4 5 6 7)
declare -ag SC_CALMODE=("p" "p" "p" "p" "ap" "ap" "ap")
declare -ag SC_SOLINT=("480s" "300s" "120s" "30s" "600s" "300s" "120s")
declare -ag SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_p" "selfcal5_ap" "selfcal6_ap" "selfcal7_ap")
declare -ag SC_NSPWS=(16 16 16 16 16 16 16 16)

# =============================================================================
# 8. Prediction & UVSub
# =============================================================================
PREDICT_TOOL="wsclean" # 'wsclean' or 'crystalball'

if [[ "${PREDICT_TOOL}" == "wsclean" ]]; then
    RUN_CB="${SCRIPT_DIR}/scripts/slurm/run_wsclean_predict_beams.sh"
    CB_TIME="01:00:00"
    CB_CPUS="4"
    CB_MEM="8G"
else
    RUN_CB="${SCRIPT_DIR}/scripts/slurm/run_crystalball_beams.sh"
    CB_TIME="03:15:00"
    CB_CPUS="4"
    CB_MEM="32G"
fi

# Crystalball specifics
CB_SOURCE_LIST_PATTERN="*beam{beam:02d}*.avg.calB0.ms"
CB_SUBDIR=""
CB_SRCLIST_SUBDIR="cont_combined"
CB_OUTPUT_COLUMN="MODEL_DATA"
CB_NUM_WORKERS="0"
CB_ROW_CHUNKS="200000"
CB_MODEL_CHUNKS="125"
CB_MEMORY_FRACTION="0.3"
CB_DISTRIBUTED="1"
CB_NATIVE_INPUT_PATTERN="20????????????/*beam{beam:02d}*.20????????????.calB0.ms"
# note that B0 will get changed to G<whatever> in the run_crystalball_beams.sh script

# Dask cluster mode
# "local"  = spawn workers on the same node (original behaviour)
# "slurm"  = submit workers as separate SLURM jobs (queue-allocated)
CB_DASK_MODE="slurm"

# --- Local-mode settings (used when CB_DASK_MODE="local") ---
CB_DASK_LOCAL_NWORKERS="4"       # max workers on this node
CB_DASK_LOCAL_WORKER_CPUS="1"    # CPUs per worker
CB_DASK_LOCAL_WORKER_MEM="7G"    # memory per worker

# --- SLURM-mode settings (used when CB_DASK_MODE="slurm") ---
CB_DASK_SLURM_NWORKERS="24"       # max worker jobs to submit
CB_DASK_SLURM_WORKER_CPUS="1"      # CPUs per worker job
CB_DASK_SLURM_WORKER_MEM="16G"      # memory per worker job
CB_DASK_SLURM_WORKER_TIME="03:00:00" # walltime per worker job
CB_DASK_SLURM_ACCOUNT=""            # SLURM account (empty = inherit default)
CB_DASK_SLURM_PARTITION=""          # SLURM partition (empty = default queue)
CB_DASK_SLURM_TMP="5GB"             # local SSD per worker for spill

# Local directory for Dask worker scratch space (spill to disk)
CB_DASK_LOCAL_DIR="${SCRIPT_DIR}/dask-worker-space"

# UVSub
RUN_UVSUB="${SCRIPT_DIR}/scripts/slurm/run_uvsub_beams.sh"
UVSUB_SCRIPT="${SCRIPT_DIR}/src/casa/uvsub_ms_beams.py"
UVSUB_TIME="01:00:00"
UVSUB_OUT_PREFIX="uvsub"
UVSUB_CONCAT_INPUT_PATTERN="cont_combined/*beam{beam:02d}.avg.calB0.ms"
UVSUB_NATIVE_INPUT_PATTERN="20????????????/*beam{beam:02d}*.20????????????.calB0.ms"

# =============================================================================
# 9. Native Resolution Continuum Imaging
# =============================================================================
WSCLEAN_NATIVE_PATTERN="native_combined/*beam{beam:02d}*.calG6.uvsub.ms"
WSCLEAN_NATIVE_MEM="20G"
WSCLEAN_NATIVE_TIME="02:00:00"
WSCLEAN_NATIVE_OPTS="${WSCLEAN_NATIVE_OPTS:-"-data-column DATA -mgain 0.8 -niter 1 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 90 -auto-mask 10000"}"

# =============================================================================
# 10. FastDUCC Transient search
# =============================================================================
RUN_FASTDUCC="${SCRIPT_DIR}/scripts/slurm/run_fastducc_beams.sh"
RUN_FASTDUCC_AGG="${SCRIPT_DIR}/scripts/slurm/run_fastducc_aggregate_chunks.sh"
# Optional obs-level aggregation (leave unset/comment out to skip)
# RUN_FASTDUCC_OBSAGG="run_fastducc_aggregate_obs.sh"
FD_CPUS="1"
FD_MEM="4G" #mem request for main fastducc driver
FD_TIME="02:00:00"
AGG_TIME="00:15:00"
AGG_CPUS="1"
AGG_MEM="1G"

CHUNK_GLOB="202?*" #was 202?*
FD_CHUNK_GLOB="native_combined*" #was 202?*
KIND="boxcar"
FASTDUCC_INPUT_PATTERN="native_combined/*beam{beam:02d}*.calB0.uvsub.ms"

# options for fastducc search/no search/plot_cands/only
# set to 1 to enable the option; leave empty string to disable
FD_NO_VAR_SEARCH=""
FD_NO_BOX_SEARCH=""
FD_PLOT_CANDS_ONLY=""

# =============================================================================
# 11. DStools Extraction
# =============================================================================
RUN_EXTRACT_DS="${SCRIPT_DIR}/scripts/slurm/run_dstools_extract_cands.sh"
EXTRACT_SCRIPT="${SCRIPT_DIR}/src/dstools/extract_ds_orchestrator.py"
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
DS_MS_GLOB_TEMPLATE="*/cracoData*%s*uvsub.ms"
DS_DATACOLUMN="data"
DS_PRIMARY_BEAM=""
DS_NOFLAG="false"
DS_BASELINE_AVERAGE="true"
DS_MINUVDIST="0.0"
DS_VERBOSE="false"
DS_OVERWRITE="false"
DS_DRY_RUN="false"
DS_CATALOGUE=""
DS_SCAN_SCOPE="all"   # 'all' or 'catalogue'

# =============================================================================
# 12. Utilities
# =============================================================================
RUN_COPY_CONTINUUM="${SCRIPT_DIR}/scripts/slurm/run_copy_continuum.sh"
COPY_TIME="00:15:00"
COPY_CPUS="1"
COPY_MEM="2G"

RUN_CLEARCAL="${SCRIPT_DIR}/scripts/slurm/run_clearcal_beams.sh"

# -------------------- Sanity: required dirs --------------
mkdir -p "${OUT_ROOT}"
mkdir -p "${CONT_OUT_ROOT}"
mkdir -p "${NATIVE_OUT_ROOT}"
