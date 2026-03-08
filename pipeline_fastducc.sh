#!/bin/bash
set -euo pipefail

# -------------------- USER DEFAULTS (override via env or edit) --------------------
USER=$( whoami )
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/"${USER}"/data}
UVFITS_PATTERN=${UVFITS_PATTERN:-"20??*/*beam*.uvfits"}             # relative under DATA_ROOT/SBID
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}             # relative under DATA_ROOT/SBID
BIND_SRC=${BIND_SRC:-/fred/oz451}

FLINT_WSCLEAN_SIF=${FLINT_WSCLEAN_SIF:-/fred/oz451/containers/flint-containers_wsclean.sif}
FLINT_CASA_SIF=${FLINT_CASA_SIF:-/fred/oz451/containers/flint-containers_casa.sif}

IMPORT_SCRIPT=${IMPORT_SCRIPT:-import_array.py}
RUN_IMPORT=${RUN_IMPORT:-run_import.sh}
IMPORT_CPUS=${IMPORT_CPUS:-2}
IMPORT_MEM=${IMPORT_MEM:-1G}

FLAG_SCRIPT=${FLAG_SCRIPT:-flag.sh}
RUN_FLAG=${RUN_FLAG:-run_flag.sh}
FLAG_COLUMN="DATA"
FLAG_CPUS=${FLAG_CPUS:-4}
FLAG_MEM=${FLAG_MEM:-12G}
SCRIPT_DIR=${SCRIPT_DIR:-/fred/oz451/$USER/scripts/lotrun_processing}

AVERAGE_SCRIPT=${AVERAGE_SCRIPT:-average_ms_beams.py}
AVERAGE_PYTHON=${AVERAGE_PYTHON:-"apptainer exec --bind /fred/oz451:/fred/oz451 /fred/oz451/${USER}/containers/flint-containers_casa.sif python3"}
TIMEBIN=${TIMEBIN:-"9.90s"}
RUN_AVERAGE=${RUN_AVERAGE:-run_average_beams.sh}
AVERAGE_CPUS=${AVERAGE_CPUS:-4}
AVERAGE_MEM=${AVERAGE_MEM:-4G}

OUT_ROOT=${OUT_ROOT:-/fred/oz451/$USER/data}
PATTERN=${PATTERN:-"20??*/*beam{beam:02d}*.20????????????.avg.ms"}
CONCAT_PYTHON=${CONCAT_PYTHON:-"apptainer exec --bind /fred/oz451:/fred/oz451 /fred/oz451/$USER/containers/flint-containers_casa.sif python3"}
CONCAT_SCRIPT=${CONCAT_SCRIPT:-concat_ms_beams.py}
RUN_CONCAT=${RUN_CONCAT:-run_concat_beams.sh}
CONCAT_CPUS=${CONCAT_CPUS:-4}
CONCAT_MEM=${CONCAT_MEM:-16G}

# -------------------------------------------------------


RUN_FASTDUCC_AGG=${RUN_FASTDUCC_AGG:-run_fastducc_aggregate_chunks.sh}
RUN_FASTDUCC_OBSAGG=${RUN_FASTDUCC_OBSAGG:-run_fastducc_aggregate_obs.sh}
AGG_TIME=${AGG_TIME:-00:30:00}
AGG_CPUS=${AGG_CPUS:-1}
AGG_MEM=${AGG_MEM:-2G}
CHUNK_GLOB=${CHUNK_GLOB:-202?*}

KIND=${KIND:-boxcar}

# -------------------- dstools extract-ds orchestrator --------------------
# SLURM launcher that calls the orchestrator; same pattern as other RUN_* wrappers
RUN_EXTRACT_DS=${RUN_EXTRACT_DS:-run_dstools_extract_cands.sh}
# Resources for the launcher job (the orchestrator spawns Dask workers itself)
EXTRACT_TIME=${EXTRACT_TIME:-01:00:00}
EXTRACT_CPUS=${EXTRACT_CPUS:-1}
EXTRACT_MEM=${EXTRACT_MEM:-4G}
# Orchestrator/Dask worker sizing (forwarded to extract_ds_orchestrator via env)
DS_N_WORKERS=${N_WORKERS:-48}
DS_CPUS=${DS_CPUS:-1}
DS_MEM=${DS_MEM:-8GB}
DS_WALLTIME=${DS_WALLTIME:-01:00:00}
DS_QUEUE=${DS_QUEUE:-}        # optional SLURM partition
DS_PROJECT=${DS_PROJECT:-}    # optional SLURM account
# Orchestrator behavior
DS_MIN_SNR=${DS_MIN_SNR:-8.0}                 # e.g. 7.0 (blank to disable)
DS_BATCH_SIZE=${DS_BATCH_SIZE:-200}
DS_RETRIES=${DS_RETRIES:-1}
DS_SLEEP_BETWEEN_BATCHES=${DS_SLEEP_BETWEEN_BATCHES:-0}
DS_BEAM_SCOPE=${DS_BEAM_SCOPE:-union}       # ignored if max_snr_beam exists in catalogue
DS_MATCH_ARCSEC=${DS_MATCH_ARCSEC:-35.0}
DS_MS_GLOB_TEMPLATE=${DS_MS_GLOB_TEMPLATE:-**/cracoData*%s*uvsub.ms}
DS_DATACOLUMN=${DS_DATACOLUMN:-data}
DS_PRIMARY_BEAM=${DS_PRIMARY_BEAM:-}        # blank => no external PB image
DS_NOFLAG=${DS_NOFLAG:-false}
DS_BASELINE_AVERAGE=${DS_BASELINE_AVERAGE:-true}
DS_MINUVDIST=${DS_MINUVDIST:-0.0}
DS_VERBOSE=${DS_VERBOSE:-false}
DS_OVERWRITE=${DS_OVERWRITE:-false}
DS_DRY_RUN=${DS_DRY_RUN:-false}
# If want to pin the catalogue path instead of auto-discovery, set:
# DS_CATALOGUE="/fred/.../<field>.<SBID>_obs_${KIND}_super_summary.vot"
DS_CATALOGUE=${DS_CATALOGUE:-}

RUN_WSCLEAN=${RUN_WSCLEAN:-run_wsclean_beams.sh}
RUN_CB=${RUN_CB:-run_crystalball_beams.sh}
RUN_SELFCAL=${RUN_SELFCAL:-run_selfcal_beams.sh}
RUN_APPLYCAL=${RUN_APPLYCAL:-run_applycal_beams.sh}
RUN_BANDPASS=${RUN_BANDPASS:-run_applycal_beams.sh}
RUN_UVSUB=${RUN_UVSUB:-run_uvsub_beams.sh}
RUN_FASTDUCC=${RUN_FASTDUCC:-run_fastducc_beams.sh}
RUN_FLINT_MASK=${RUN_FLINT_MASK:-run_flintmask_beams.sh}
RUN_CLEARCAL=${RUN_CLEARCAL:-run_clearcal_beams.sh}

ARRAY_SPEC=${ARRAY_SPEC:-0-35}
BIGARRAY_SPEC=${BIGARRAY_SPEC:-0-500}
WSCLEAN_CPUS=${WSCLEAN_CPUS:-4}
WSCLEAN_MEM=${WSCLEAN_MEM:-16G}
SC_CPUS=${SC_CPUS:-8}
SC_MEM=${SC_MEM:-4G}
FM_CPUS=${FM_CPUS:-1}
FM_MEM=${FM_MEM:-1G}

n_chunks=$( ls -d ${DATA_ROOT}/${SBID}/${CHUNK_GLOB} 2>/dev/null | wc -l )
if (( n_chunks > 0 )); then
  CHUNK_ARRAY_SPEC="0-$((n_chunks-1))"
else
  CHUNK_ARRAY_SPEC="0-0"
fi


# Crystalball defaults
CB_TIME=${CB_TIME:-"03:15:00"}
CB_CPUS=${CB_CPUS:-32}
CB_MEM=${CB_MEM:-54G}
CB_OUTPUT_COLUMN=${CB_OUTPUT_COLUMN:-MODEL_DATA}
CB_NUM_WORKERS=${CB_NUM_WORKERS:-2048} #i have no clue why 2048 speeds up things despite only having 32 cpus but whtever
CB_ROW_CHUNKS=${CB_ROW_CHUNKS:-0}
CB_MODEL_CHUNKS=${CB_MODEL_CHUNKS:-0}
CB_MEMORY_FRACTION=${CB_MEMORY_FRACTION:-0.8}

# fastducc defaults
FD_TIME=${FD_TIME:-"06:00:00"}
FD_CPUS=${FD_CPUS:-1}
FD_MEM=${FD_MEM:-32G}

# options for fastducc search/no search/plot_cands/only
# set to 1 to enable the option; leave unset or set to empty string to disable
FD_NO_VAR_SEARCH=${FD_NO_VAR_SEARCH:-}
FD_NO_BOX_SEARCH=${FD_NO_BOX_SEARCH:-}
FD_PLOT_CANDS_ONLY=${FD_PLOT_CANDS_ONLY:-}

#flint_masking defaults
FLOOD_FILL_POSITIVE_SEED_CLIP=${FLOOD_FILL_POSITIVE_SEED_CLIP:-1.1}
FLOOD_FILL_POSITIVE_FLOOD_CLIP=${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-0.7}
FLOOD_FILL_MAC_BOX_SIZE=${FLOOD_FILL_MAC_BOX_SIZE:-350}
BEAM_SHAPE_ERODE_MIN_RESPONSE=${BEAM_SHAPE_ERODE_MIN_RESPONSE:-0.75}

# CASA self-cal defaults
SC_FIELD=${SC_FIELD:-""}
SC_SPW=${SC_SPW:-""}
SC_REFANT=${SC_REFANT:-""}
SC_COMBINE=${SC_COMBINE:-scan}
SC_MINSNR=${SC_MINSNR:-3.0}
SC_PARANG=${SC_PARANG:-""}          # set non-empty to enable
SC_APPLY_CALWT=${SC_APPLY_CALWT:-False} #was True

# -------------------- PIPELINE CONFIG (round-specific params) --------------------

UVSUB_OUT_PREFIX=${UVSUB_OUT_PREFIX:-"uvsub"}

# IMG_TAG per round (0 = initial pre-selfcal imaging; 1..4 are successive re-imaging passes
declare -a IMG_TAGS=("initial" "selfcal_1" "selfcal_2" "selfcal_3" "selfcal_4" "selfcal_5" "selfcal_6")

# WSClean options per round (round 0 can use a shallower set; others deepen progressively)
declare -a WSCLEAN_OPTS
WSCLEAN_OPTS[0]="${WSCLEAN_OPTS0:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 25000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 3 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[1]="${WSCLEAN_OPTS1:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 2 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 8.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 3.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 5.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"

# Self-cal rounds: index, mode, solint, caltable prefix (only rounds 1..4 have self-cal; 4 = amplitude+phase)
# declare -a SC_INDEX=(1 2 3 4)
# declare -a SC_CALMODE=("p" "p" "p" "ap")
# declare -a SC_SOLINT=("480s" "300s" "120s" "600s")
# declare -a SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_ap")

declare -a SC_INDEX=(1 2 3 4 5 6)
declare -a SC_CALMODE=("p" "p" "p" "p" "ap" "ap")
declare -a SC_SOLINT=("480s" "300s" "120s" "30s" "600s" "300s")
declare -a SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_p" "selfcal5_ap" "selfcal6_ap" )

# -------------------- HELPERS --------------------


submit_importuvfits() {
  local dep jid
  dep="${1:-}"
  jid=$(sbatch --array="${BIGARRAY_SPEC}" --job-name=importuvfits_array --time=00:10:00 --cpus-per-task="${IMPORT_CPUS}" --mem="${IMPORT_MEM}" --output=logs/importuvfits_%A_%a.out --error=logs/importuvfits_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",UVFITS_PATTERN="${UVFITS_PATTERN}",IMPORT_SCRIPT="${IMPORT_SCRIPT}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}" "${RUN_IMPORT}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}


submit_flag() {
  local dep jid pattern
  dep="${1:-}"
  jid=$(sbatch --array="${BIGARRAY_SPEC}" --job-name=aoflagger_array --time=00:30:00 --cpus-per-task="${FLAG_CPUS}" --mem="${FLAG_MEM}" --output=logs/aoflagger_%A_%a.out --error=logs/aoflagger_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",SCRIPT_DIR="${SCRIPT_DIR}",FLAG_SCRIPT="${FLAG_SCRIPT}",COLUMN="${FLAG_COLUMN}",RUN_FLAG="${RUN_FLAG}" "${RUN_FLAG}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}

submit_average() {
  local dep jid
  dep="${1:-}"
  jid=$(sbatch --array="${BIGARRAY_SPEC}" --job-name=average_array --time=01:00:00 --cpus-per-task="${AVERAGE_CPUS}" --mem="${AVERAGE_MEM}" --output=logs/average_%A_%a.out --error=logs/average_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",SCRIPT_DIR="${SCRIPT_DIR}",SCRIPT="${AVERAGE_SCRIPT}",PYTHON="${AVERAGE_PYTHON}",TIMEBIN="${TIMEBIN}" "${RUN_AVERAGE}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}

submit_concat() {
  local dep jid
  dep="${1:-}"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=concat_ms --time=01:00:00 --cpus-per-task="${CONCAT_CPUS}" --mem="${CONCAT_MEM}" --output=logs/concat_%A_%a.out --error=logs/concat_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",OUT_ROOT="${OUT_ROOT}",PATTERN="${PATTERN}",PYTHON="${CONCAT_PYTHON}",SCRIPT="${CONCAT_SCRIPT}" "${RUN_CONCAT}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}

submit_wsclean() {
  local dep img_tag opts jid idx fits_mask_tag
  dep="${1:-}"; img_tag="$2"; opts="$3"; idx="$4"; fits_mask_tag="${5:-}"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=wsclean_ms --time=04:00:00 --cpus-per-task="${WSCLEAN_CPUS}" --mem="${WSCLEAN_MEM}" --output=logs/wsclean_%A_%a.out --error=logs/wsclean_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}",IMG_TAG="${img_tag}",INDEX="${idx}",BIND_SRC="${BIND_SRC}",WSCLEAN_OPTS="${opts}",FITS_MASK_TAG="${fits_mask_tag}" "${RUN_WSCLEAN}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}


submit_flintmask() {
    local dep img_tag jid idx selfcal_flag
    dep="${1:-}"; img_tag="$2"; idx="$3"; selfcal_flag="$4";
    jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=flint_mask --time=00:30:00 --cpus-per-task="${FM_CPUS}" --mem="${FM_MEM}" --output=logs/flint_mask_%A_%a.out --error=logs/flint_mask_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SELFCAL="${selfcal_flag}",SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",IMG_TAG="${img_tag}",INDEX="${idx}",FLOOD_FILL_POSITIVE_SEED_CLIP="${FLOOD_FILL_POSITIVE_SEED_CLIP}",FLOOD_FILL_POSITIVE_FLOOD_CLIP="${FLOOD_FILL_POSITIVE_FLOOD_CLIP}",FLOOD_FILL_MAC_BOX_SIZE="${FLOOD_FILL_MAC_BOX_SIZE}",BEAM_SHAPE_ERODE_MIN_RESPONSE="${BEAM_SHAPE_ERODE_MIN_RESPONSE}" "${RUN_FLINT_MASK}" | awk '{print $4}' )
    echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}


submit_crystalball() {
    local dep img_tag jid idx selfcal_flag
    dep="${1:-}"; img_tag="$2"; idx="$3"; selfcal_flag="${4:-0}"
    jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=cb_predict --time="${CB_TIME}" --cpus-per-task="${CB_CPUS}" --mem="${CB_MEM}" --output=logs/crystalball_%A_%a.out --error=logs/crystalball_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SELFCAL="${selfcal_flag}",SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",IMG_TAG="${img_tag}",OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}",INDEX="${idx}",NUM_WORKERS="${CB_NUM_WORKERS}",ROW_CHUNKS="${CB_ROW_CHUNKS}",MODEL_CHUNKS="${CB_MODEL_CHUNKS}",MEMORY_FRACTION="${CB_MEMORY_FRACTION}" "${RUN_CB}" | awk '{print $4}')
    echo "${jid}"
    if [ -z "${jid}" ]; then
	echo "sbatch not successful. exiting"
	exit
    fi
}


submit_bandpass() {
  local dep cal_dir extension jid delete_previous
  dep="${1:-}"; cal_dir="$2"; extension="$3"; delete_previous="$4"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=bandpass_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/bandpass_%A_%a.out --error=logs/bandpass_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=applycal_ms_beams.py,CAL_DIR="${cal_dir}",EXTENSION="${extension}",DELETE_PREVIOUS="${delete_previous}" "${RUN_BANDPASS}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}


submit_applycal() {
  local dep cal_dir extension jid delete_previous
  dep="${1:-}"; cal_dir="$2"; extension="$3"; delete_previous="$4"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=applycal_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/applycal_%A_%a.out --error=logs/applycal_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=applycal_ms_beams.py,CAL_DIR="${cal_dir}",EXTENSION="${extension}",DELETE_PREVIOUS="${delete_previous}" "${RUN_APPLYCAL}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}

submit_selfcal() {
  local dep idx calmode solint prefix jid
  dep="${1:-}"; idx="$2"; calmode="$3"; solint="$4"; prefix="$5"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=selfcal_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/selfcal_%A_%a.out --error=logs/selfcal_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=selfcal_ms_beams.py,INDEX="${idx}",CALMODE="${calmode}",SOLINT="${solint}",FIELD="${SC_FIELD}",SPW="${SC_SPW}",REFANT="${SC_REFANT}",COMBINE="${SC_COMBINE}",MINSNR="${SC_MINSNR}",PARANG="${SC_PARANG}",CALTABLE_PREFIX="${prefix}",PLOT_DIR="plots",APPLY_CALWT="${SC_APPLY_CALWT}" "${RUN_SELFCAL}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}

submit_uvsub() {
  local dep idx out_prefix ext jid selfcal_flag
  dep="${1:-}"; idx="$2"; out_prefix="$3"; ext="$4"; selfcal_flag="$5";
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=uvsub_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/uvsub_%A_%a.out --error=logs/uvsub_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SELFCAL="${selfcal_flag}",SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=uvsub_ms_beams.py,INDEX="${idx}",EXTENSION="${ext}",OUT_PREFIX="${out_prefix}" "${RUN_UVSUB}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}

submit_fastducc() {
  local dep idx ext jid selfcal_flag
  dep="${1:-}"; idx="$2"; ext="$3"; selfcal_flag="$4";
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=fastducc_ms --time=06:00:00 --cpus-per-task="${FD_CPUS}" --mem="${FD_MEM}" --output=logs/fastducc_%A_%a.out --error=logs/fastducc_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SELFCAL="${selfcal_flag}",SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",BIND_SRC="${BIND_SRC}",INDEX="${idx}",EXTENSION="${ext}",FD_NO_VAR_SEARCH="${FD_NO_VAR_SEARCH}",FD_NO_BOX_SEARCH="${FD_NO_BOX_SEARCH}",FD_PLOT_CANDS_ONLY="${FD_PLOT_CANDS_ONLY}" "${RUN_FASTDUCC}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}


submit_fastducc_aggregate_chunks() {
  local dep jid
  dep="${1:-}"

  jid=$( sbatch --array="${CHUNK_ARRAY_SPEC}" --job-name=fastducc_agg --time="${AGG_TIME}" --cpus-per-task="${AGG_CPUS}" --mem="${AGG_MEM}" --output=logs/fastducc_agg_%A_%a.out --error=logs/fastducc_agg_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",CHUNK_GLOB="${CHUNK_GLOB}" "${RUN_FASTDUCC_AGG}" | awk '{print $4}'  )

  echo "${jid}"
  if [[ -z "${jid}" ]]; then
    echo "sbatch not successful. exiting"
    exit 1
  fi
}



submit_fastducc_aggregate_obs() {
  local dep jid
  dep="${1:-}"

  jid=$( sbatch  --job-name=fastducc_obsagg --time="${AGG_TIME}" --cpus-per-task="${AGG_CPUS}" --mem="${AGG_MEM}" --output=logs/fastducc_obsagg_%A_%a.out --error=logs/fastducc_obsagg_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}" "${RUN_FASTDUCC_OBSAGG}" | awk '{print $4}'  )

  echo "${jid}"
  if [[ -z "${jid}" ]]; then
    echo "sbatch not successful. exiting"
    exit 1
  fi
}


# -------------------- dstools extract-ds orchestrator --------------------
submit_extract_ds() {
  local dep jid
  dep="${1:-}"
  jid=$( sbatch --job-name=ds_extract \
                --time="${EXTRACT_TIME}" \
                --cpus-per-task="${EXTRACT_CPUS}" \
                --mem="${EXTRACT_MEM}" \
                --output=logs/dstools_extract_%A.out \
                --error=logs/dstools_extract_%A.err \
                ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",KIND="${KIND}",DS_N_WORKERS="${DS_N_WORKERS}",DS_CPUS="${DS_CPUS}",DS_MEM="${DS_MEM}",DS_WALLTIME="${DS_WALLTIME}",DS_MIN_SNR="${DS_MIN_SNR}",DS_QUEUE="${DS_QUEUE}",DS_PROJECT="${DS_PROJECT}",DS_BATCH_SIZE="${DS_BATCH_SIZE}",DS_RETRIES="${DS_RETRIES}",DS_SLEEP_BETWEEN_BATCHES="${DS_SLEEP_BETWEEN_BATCHES}",DS_BEAM_SCOPE="${DS_BEAM_SCOPE}",DS_MATCH_ARCSEC="${DS_MATCH_ARCSEC}",DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE}",DS_DATACOLUMN="${DS_DATACOLUMN}",DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM}",DS_NOFLAG="${DS_NOFLAG}",DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE}",DS_MINUVDIST="${DS_MINUVDIST}",DS_VERBOSE="${DS_VERBOSE}",DS_OVERWRITE="${DS_OVERWRITE}",DS_DRY_RUN="${DS_DRY_RUN}",DS_CATALOGUE="${DS_CATALOGUE:-}" \
		"${RUN_EXTRACT_DS}" | awk '{print $4}' )
  echo "${jid}"
  if [[ -z "${jid}" ]]; then
    echo "sbatch not successful. exiting"
    exit 1
  fi
}


submit_clearcal() {
  local dep extension jid
  dep="${1:-}";  extension="$2"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=clearcal_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/clearcal_%A_%a.out --error=logs/clearcal_%A_%a.err ${dep:+--dependency=afterok:${dep}} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=clearcal_ms_beams.py,EXTENSION="${extension}" "${RUN_CLEARCAL}" | awk '{print $4}')
  echo "${jid}"
  if [ -z "${jid}" ]; then
    echo "sbatch not successful. exiting"
    exit
  fi
}


# -------------------- PIPELINE EXECUTION --------------------
mkdir -p logs plots
n=$( ls -l ${DATA_ROOT}/${SBID}/202*/*.uvfits  | wc -l )
BIGARRAY_SPEC="0-$((n-1))"
###
#steps before selfcal, to add in here for automated processing
#1. symlink uvfits
####
./symlink_uvfits.sh "${SBID}"

#2. import uvfits
#jid_imp=$(submit_importuvfits "" )

#echo "submitted importuvfits ${jid_imp}"


jid_sb7=""

###
#now applycal selfcal onto highres visibilities, crystalball sky model, and uvsub
###
#step 2: apply selfcal to native res
jid_ac_old=$jid_sb7
PATTERN="20??*/*beam{beam:02d}*.20????????????.calB0.ms"    # relative under data-root/SBID
for i in "${SC_INDEX[@]}";
do
    if (( i > 1 ))
    then
	dp="--delete-previous"
    else
	dp=""
    fi
    #jid_ac=$(submit_applycal "${jid_ac_old}" "caltables" "G${i}" "${dp}")
    #echo "submitted craco applycal ${jid_ac}"
    #jid_ac_old=$jid_ac    
    PATTERN="20??*/*beam{beam:02d}*.20????????????.calG${i}.ms"    # relative under data-root/SBID
done
echo $PATTERN
#step : crystalball model from 2h continuyum beam onto native res beam
# jid_cb=$(submit_crystalball "${jid_ac_old}" "${IMG_TAGS[6]}" "$(( SC_INDEX[5] ))" "0" )
# echo "submitted craco crystalball ${jid_cb}"

# #step : uvsub craco
# jid_uvs=$(submit_uvsub "${jid_cb}" "${SC_INDEX[5]}" "${UVSUB_OUT_PREFIX}" "G6" "0" )
# echo "submitted craco uvsub ${jid_uvs}"

jid_uvs=""

PATTERN="20??*/*beam{beam:02d}*.20????????????.calB0.uvsub.ms"    # relative under data-root/SBID
#                               dep="${1:-}"; idx="$2"; ext="$3"; selfcal_flag="$4";
jid_fastducc=$( submit_fastducc "${jid_uvs}" "${SC_INDEX[5]}" "G6" "0" )

#jid_fastducc=""

jid_agg=$( submit_fastducc_aggregate_chunks "${jid_fastducc}" )
#echo "submitted fastducc aggregate chunks ${jid_agg}"
#jid_agg=""
jid_obs=$( submit_fastducc_aggregate_obs "${jid_agg}" )
echo "submitted fastducc aggregate chunks ${jid_obs}"

jid_prev="$jid_obs"
for kind in "boxcar" "variance"
do
    KIND="$kind"
    echo "doing ds extract for $KIND"
    jid_prev=$( submit_extract_ds "${jid_prev}" )
    echo "submitted dstools extract-ds ${jid_prev} for kind ${kind}"
done

jid_ds="$jid_prev"


echo "Pipeline submitted."

