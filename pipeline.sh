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

RUN_WSCLEAN=${RUN_WSCLEAN:-run_wsclean_beams.sh}
RUN_CB=${RUN_CB:-run_crystalball_beams.sh}
RUN_SELFCAL=${RUN_SELFCAL:-run_selfcal_beams.sh}
RUN_APPLYCAL=${RUN_APPLYCAL:-run_applycal_beams.sh}
RUN_BANDPASS=${RUN_BANDPASS:-run_applycal_beams.sh}
RUN_UVSUB=${RUN_UVSUB:-run_uvsub_beams.sh}
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


# Crystalball defaults
CB_TIME=${CB_TIME:-"03:15:00"}
CB_CPUS=${CB_CPUS:-32}
CB_MEM=${CB_MEM:-54G}
CB_OUTPUT_COLUMN=${CB_OUTPUT_COLUMN:-MODEL_DATA}
CB_NUM_WORKERS=${CB_NUM_WORKERS:-2048} #i have no clue why 2048 speeds up things despite only having 32 cpus but whtever
CB_ROW_CHUNKS=${CB_ROW_CHUNKS:-0}
CB_MODEL_CHUNKS=${CB_MODEL_CHUNKS:-0}
CB_MEMORY_FRACTION=${CB_MEMORY_FRACTION:-0.8}

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
jid_imp=$(submit_importuvfits "" )
echo "submitted importuvfits ${jid_imp}"

#3. run_flag.sh on ms (flag.sh)
PATTERN="20??*/*beam*.20????????????.ms"    # relative under data-root/SBID
jid_fl1=$(submit_flag "${jid_imp}" )
echo "submitted flag ${jid_fl1}"

PATTERN="20??*/*beam{beam:02d}*.20????????????.ms"    # relative under data-root/SBID
#apply bandpass to the native res
jid_ac1=$(submit_bandpass "${jid_fl1}" "cal" "B0" "--delete-previous" )
echo "submitted bandpass ${jid_ac1}"

#now flag bandpass calibrated data
PATTERN="20??*/*beam*.20????????????.calB0.ms"    # relative under data-root/SBID
jid_fl2=$(submit_flag "${jid_ac1}" )
echo "submitted flag ${jid_fl2}"
####

#6. run_average_beams.sh (average_ms_beams.py)
jid_av1=$(submit_average "${jid_fl2}" )
echo "submitted average ${jid_av1}"

PATTERN="20??*/*beam*.20????????????.avg.calB0.ms"    # relative under data-root/SBID
jid_fl3=$(submit_flag "${jid_av1}" )
echo "submitted flag ${jid_fl3}"

PATTERN="20??*/*beam{beam:02d}*.20????????????.avg.calB0.ms"    # relative under data-root/SBID
#7. run_concat_beams.sh (concat_ms_beams.py)
jid_cat=$(submit_concat "${jid_fl3}" )
echo "submitted concat ${jid_cat}"

PATTERN="*beam{beam:02d}.avg.calB0.ms"
echo ">>> Round 0: initial imaging -> predict -> self-cal"

#initial image to generate mask
jid_img_=$(submit_wsclean "${jid_cat}" "initial_scratch" "${WSCLEAN_OPTS[0]}" "$(( SC_INDEX[0]-1 ))")
echo "submitted initial img ${jid_img_}"
jid_fm_=$(submit_flintmask "${jid_img_}" "initial_scratch" "$(( SC_INDEX[0]-1 ))" 1 )
echo "submitted initial mask ${jid_fm_}"

jid_img0=$(submit_wsclean "${jid_fm_}" "${IMG_TAGS[0]}" "${WSCLEAN_OPTS[0]}" "$(( SC_INDEX[0]-1 ))" "initial_scratch")
echo "submitted round 0 selfcal img ${jid_img0}"
jid_fm0=$(submit_flintmask "${jid_img0}" "${IMG_TAGS[0]}" "$(( SC_INDEX[0]-1 ))" 1 )
echo "submitted round 0 selfcal mask ${jid_fm0}"

jid_cb0=$(submit_crystalball "${jid_fm0}" "${IMG_TAGS[0]}" "$(( SC_INDEX[0]-1 ))" 1)
echo "submitted round 0 selfcal crystalball ${jid_cb0}"
jid_sc1=$(submit_selfcal "${jid_cb0}" "${SC_INDEX[0]}" "${SC_CALMODE[0]}" "${SC_SOLINT[0]}" "${SC_PREFIX[0]}")
echo "submitted round 1 selfcal ${jid_sc1}"

jid_sc1=$jid_fm0
#after this step we should have a new measurement set called X.selfcal_1.ms
jid_img1=$(submit_wsclean "${jid_sc1}" "${IMG_TAGS[1]}" "${WSCLEAN_OPTS[1]}" "$(( SC_INDEX[1]-1 ))" "${IMG_TAGS[0]}")
echo "submitted round 1 selfcal img ${jid_img1}"
#jid_img1="
jid_fm1=$(submit_flintmask "${jid_img1}" "${IMG_TAGS[1]}" "$(( SC_INDEX[1]-1 ))" 1 )
echo "submitted round 1 selfcal mask ${jid_fm1}"

#echo ">>> Rounds 1..2..3: re-image deeper -> predict -> self-cal"
# round 1 (phase-only, 60s)
jid_cb1=$(submit_crystalball "${jid_img1}" "${IMG_TAGS[1]}" "$(( SC_INDEX[1]-1 ))" 1)
echo "submitted round 1 selfcal crystalball ${jid_cb1}"
jid_sc2=$(submit_selfcal "${jid_cb1}" "${SC_INDEX[1]}" "${SC_CALMODE[1]}" "${SC_SOLINT[1]}" "${SC_PREFIX[1]}")
echo "submitted round 2 selfcal ${jid_sc2}"
jid_img2=$(submit_wsclean "${jid_sc2}" "${IMG_TAGS[2]}" "${WSCLEAN_OPTS[2]}" "$(( SC_INDEX[2]-1 ))" "${IMG_TAGS[1]}")
echo "submitted round 2 selfcal img ${jid_img2}"
jid_fm2=$(submit_flintmask "${jid_img2}" "${IMG_TAGS[2]}" "$(( SC_INDEX[2]-1 ))" 1 )
echo "submitted round 2 selfcal mask ${jid_fm2}"

# round 2 (phase-only, 30s)
jid_cb2=$(submit_crystalball "${jid_fm2}" "${IMG_TAGS[2]}" "$(( SC_INDEX[2]-1 ))" 1)
echo "submitted round 2 selfcal crystalball ${jid_cb2}"
jid_sc3=$(submit_selfcal "${jid_cb2}" "${SC_INDEX[2]}" "${SC_CALMODE[2]}" "${SC_SOLINT[2]}" "${SC_PREFIX[2]}")
echo "submitted round 3 selfcal ${jid_sc3}"
jid_img3=$(submit_wsclean "${jid_sc3}" "${IMG_TAGS[3]}" "${WSCLEAN_OPTS[3]}" "$(( SC_INDEX[3]-1 ))" "${IMG_TAGS[2]}")
echo "submitted round 3 selfcal img ${jid_img3}"
jid_fm3=$(submit_flintmask "${jid_img3}" "${IMG_TAGS[3]}" "$(( SC_INDEX[3]-1 ))" 1 )
echo "submitted round 3 selfcal mask ${jid_fm3}"

# round 3 (phase-only, 30s)
jid_cb3=$(submit_crystalball "${jid_fm3}" "${IMG_TAGS[3]}" "$(( SC_INDEX[3]-1 ))" 1)
echo "submitted round 3 selfcal crystalball ${jid_cb3}"
jid_sc4=$(submit_selfcal "${jid_cb3}" "${SC_INDEX[3]}" "${SC_CALMODE[3]}" "${SC_SOLINT[3]}" "${SC_PREFIX[3]}")
echo "submitted round 4 selfcal ${jid_sc4}"
jid_img4=$(submit_wsclean "${jid_sc4}" "${IMG_TAGS[4]}" "${WSCLEAN_OPTS[4]}" "$(( SC_INDEX[4]-1 ))" "${IMG_TAGS[3]}")
echo "submitted round 4 selfcal img ${jid_img4}"
jid_fm4=$(submit_flintmask "${jid_img4}" "${IMG_TAGS[4]}" "$(( SC_INDEX[4]-1 ))" 1 )
echo "submitted round 4 selfcal mask ${jid_fm4}"

#round 4 (amp+phase, 600s)
jid_cb4=$(submit_crystalball "${jid_fm4}" "${IMG_TAGS[4]}" "$(( SC_INDEX[4]-1 ))" 1)
echo "submitted round 4 selfcal crystalball ${jid_cb4}"
jid_sc5=$(submit_selfcal "${jid_cb4}" "${SC_INDEX[4]}" "${SC_CALMODE[4]}" "${SC_SOLINT[4]}" "${SC_PREFIX[4]}")
echo "submitted round 5 selfcal ${jid_sc5}"
jid_img5=$(submit_wsclean "${jid_sc5}" "${IMG_TAGS[5]}" "${WSCLEAN_OPTS[5]}" "$(( SC_INDEX[5]-1 ))" "${IMG_TAGS[4]}")
echo "submitted round 5 selfcal img ${jid_img5}"
jid_fm5=$(submit_flintmask "${jid_img5}" "${IMG_TAGS[5]}" "$(( SC_INDEX[5]-1 ))" 1 )
echo "submitted round 5 selfcal mask ${jid_fm5}"

# #round 5 (amp+phase, 300s)
jid_cb5=$(submit_crystalball "${jid_fm5}" "${IMG_TAGS[5]}" "$(( SC_INDEX[5]-1 ))" 1)
echo "submitted round 5 selfcal crystalball ${jid_cb5}"
jid_sc6=$(submit_selfcal "${jid_cb5}" "${SC_INDEX[5]}" "${SC_CALMODE[5]}" "${SC_SOLINT[5]}" "${SC_PREFIX[5]}")
echo "submitted round 6 selfcal ${jid_sc6}"
# re-image after A+P self-cal
jid_img6=$(submit_wsclean "${jid_sc6}" "${IMG_TAGS[6]}" "${WSCLEAN_OPTS[6]}" "$(( SC_INDEX[5] ))" "${IMG_TAGS[5]}")
echo "submitted round 6 selfcal img ${jid_img6}"
jid_fm6=$(submit_flintmask "${jid_img6}" "${IMG_TAGS[6]}" "$(( SC_INDEX[5] ))" 1 )
echo "submitted final selfcal mask ${jid_fm6}"
#final predict from latest source list
jid_cb6=$(submit_crystalball "${jid_fm6}" "${IMG_TAGS[6]}" "$(( SC_INDEX[5] ))" 1)
echo "submitted round 6 selfcal crystalball ${jid_cb6}"

PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calG6.ms"}  # relative under data-root/SBID
jid_sb7=$(submit_uvsub "${jid_cb6}" "${SC_INDEX[5]}" "${UVSUB_OUT_PREFIX}" "B0" "1" )
echo "submitted continuum uvsub ${jid_sb7}"

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
    jid_ac_old=$jid_ac    
    PATTERN="20??*/*beam{beam:02d}*.20????????????.calG${i}.ms"    # relative under data-root/SBID
done
echo $PATTERN
#step : crystalball model from 2h continuyum beam onto native res beam
jid_cb=$(submit_crystalball "${jid_ac_old}" "${IMG_TAGS[6]}" "$(( SC_INDEX[5] ))" "0" )
echo "submitted craco crystalball ${jid_cb}"

#step : uvsub craco
jid_uvs=$(submit_uvsub "${jid_cb}" "${SC_INDEX[5]}" "${UVSUB_OUT_PREFIX}" "G6" "0" )
echo "submitted craco uvsub ${jid_uvs}"

echo "Pipeline submitted."

