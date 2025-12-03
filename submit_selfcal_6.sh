#!/bin/bash
set -euo pipefail

# -------------------- USER DEFAULTS (override via env or edit) --------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/azic/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}             # relative under DATA_ROOT/SBID
BIND_SRC=${BIND_SRC:-/fred/oz451}

FLINT_WSCLEAN_SIF=${FLINT_WSCLEAN_SIF:-/fred/oz451/azic/containers/flint-containers_wsclean.sif}
FLINT_CASA_SIF=${FLINT_CASA_SIF:-/fred/oz451/azic/containers/flint-containers_casa.sif}

RUN_WSCLEAN=${RUN_WSCLEAN:-run_wsclean_beams.sh}
RUN_CB=${RUN_CB:-run_crystalball_beams.sh}
RUN_SELFCAL=${RUN_SELFCAL:-run_selfcal_beams.sh}
RUN_UVSUB=${RUN_SELFCAL:-run_uvsub_beams.sh}

ARRAY_SPEC=${ARRAY_SPEC:-0-36}
WSCLEAN_CPUS=${WSCLEAN_CPUS:-4}
WSCLEAN_MEM=${WSCLEAN_MEM:-32G}
CB_CPUS=${CB_CPUS:-8}
CB_MEM=${CB_MEM:-24G}
SC_CPUS=${SC_CPUS:-8}
SC_MEM=${SC_MEM:-8G}

# Crystalball defaults
CB_OUTPUT_COLUMN=${CB_OUTPUT_COLUMN:-MODEL_DATA}
CB_NUM_WORKERS=${CB_NUM_WORKERS:-8}
CB_ROW_CHUNKS=${CB_ROW_CHUNKS:-0}
CB_MODEL_CHUNKS=${CB_MODEL_CHUNKS:-0}
CB_MEMORY_FRACTION=${CB_MEMORY_FRACTION:-0.8}

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
WSCLEAN_OPTS[2]="${WSCLEAN_OPTS2:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 1.0 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[3]="${WSCLEAN_OPTS3:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[4]="${WSCLEAN_OPTS4:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 15.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[5]="${WSCLEAN_OPTS5:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 2.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"
WSCLEAN_OPTS[6]="${WSCLEAN_OPTS6:-"-data-column DATA -save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 1536 1536 -auto-threshold 0.5 -auto-mask 2.0 -join-channels -channels-out 4 -fit-spectral-pol 3"}"

# Self-cal rounds: index, mode, solint, caltable prefix (only rounds 1..4 have self-cal; 4 = amplitude+phase)
# declare -a SC_INDEX=(1 2 3 4)
# declare -a SC_CALMODE=("p" "p" "p" "ap")
# declare -a SC_SOLINT=("480s" "300s" "120s" "600s")
# declare -a SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_ap")

declare -a SC_INDEX=(1 2 3 4 5 6)
declare -a SC_CALMODE=("p" "p" "p" "p" "ap" "ap")
declare -a SC_SOLINT=("480s" "300s" "120s" "30s" "600s" "300s" )
declare -a SC_PREFIX=("selfcal1_p" "selfcal2_p" "selfcal3_p" "selfcal4_p" "selfcal5_ap" "selfcal6_ap" )


# -------------------- HELPERS --------------------
submit_wsclean() {
  local dep img_tag opts jid idx
  dep="${1:-}"; img_tag="$2"; opts="$3"; idx="$4"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=wsclean_ms --time=24:00:00 --cpus-per-task="${WSCLEAN_CPUS}" --mem="${WSCLEAN_MEM}" --output=logs/wsclean_%A_%a.out --error=logs/wsclean_%A_%a.err ${dep:+--dependency=afterok:$dep} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_WSCLEAN_SIF="${FLINT_WSCLEAN_SIF}",IMG_TAG="${img_tag}",INDEX="${idx}",BIND_SRC="${BIND_SRC}",WSCLEAN_OPTS="${opts}" "${RUN_WSCLEAN}" | awk '{print $4}')
  echo "${jid}"
}

submit_crystalball() {
    local dep img_tag jid idx
    dep="${1:-}"; img_tag="$2"; idx="$3"
    jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=cb_predict --time=02:00:00 --cpus-per-task="${CB_CPUS}" --mem="${CB_MEM}" --output=logs/crystalball_%A_%a.out --error=logs/crystalball_%A_%a.err ${dep:+--dependency=afterok:$dep} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",IMG_TAG="${img_tag}",OUTPUT_COLUMN="${CB_OUTPUT_COLUMN}",INDEX="${idx}",,NUM_WORKERS="${CB_NUM_WORKERS}",ROW_CHUNKS="${CB_ROW_CHUNKS}",MODEL_CHUNKS="${CB_MODEL_CHUNKS}",MEMORY_FRACTION="${CB_MEMORY_FRACTION}" "${RUN_CB}" | awk '{print $4}')
    echo "${jid}"
}

submit_selfcal() {
  local dep idx calmode solint prefix jid
  dep="${1:-}"; idx="$2"; calmode="$3"; solint="$4"; prefix="$5"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=selfcal_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/selfcal_%A_%a.out --error=logs/selfcal_%A_%a.err ${dep:+--dependency=afterok:$dep} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=selfcal_ms_beams.py,INDEX="${idx}",CALMODE="${calmode}",SOLINT="${solint}",FIELD="${SC_FIELD}",SPW="${SC_SPW}",REFANT="${SC_REFANT}",COMBINE="${SC_COMBINE}",MINSNR="${SC_MINSNR}",PARANG="${SC_PARANG}",CALTABLE_PREFIX="${prefix}",PLOT_DIR="plots",APPLY_CALWT="${SC_APPLY_CALWT}" "${RUN_SELFCAL}" | awk '{print $4}')
  echo "${jid}"
}

submit_uvsub() {
  local dep idx out_prefix ext jid
  dep="${1:-}"; idx="$2"; out_prefix="$3"; ext="$4"
  jid=$(sbatch --array="${ARRAY_SPEC}" --job-name=uvsub_ms --time=02:00:00 --cpus-per-task="${SC_CPUS}" --mem="${SC_MEM}" --output=logs/uvsub_%A_%a.out --error=logs/uvsub_%A_%a.err ${dep:+--dependency=afterok:$dep} --export=ALL,SBID="${SBID}",DATA_ROOT="${DATA_ROOT}",PATTERN="${PATTERN}",FLINT_CASA_SIF="${FLINT_CASA_SIF}",BIND_SRC="${BIND_SRC}",SCRIPT=uvsub_ms_beams.py,INDEX="${idx}",EXTENSION="${ext}",OUT_PREFIX="${out_prefix}" "${RUN_UVSUB}" | awk '{print $4}')
  echo "${jid}"
}


# -------------------- PIPELINE EXECUTION --------------------

###
#steps before selfcal, to add in here for automated processing
#1. symlink uvfits
#2. import uvfits
#3. run_flag.sh on ms (flag.sh)
#4. run_bandpass_beams.sh (applycal_ms_beams.py)
#5. run_flag.sh on calB0.ms (flag.sh)
#6. run_average_beams.sh (average_ms_beams.py)
#7. run_concat_beams.sh (concat_ms_beams.py)

mkdir -p logs plots

echo ">>> Round 0: initial imaging -> predict -> self-cal (phase-only 120s)"
#jid_img0=$(submit_wsclean "" "${IMG_TAGS[0]}" "${WSCLEAN_OPTS[0]}" "$(( SC_INDEX[0]-1 ))")
jid_img0=""
#jid_cb0=$(submit_crystalball "${jid_img0}" "${IMG_TAGS[0]}" "$(( SC_INDEX[0]-1 ))")
jid_cb0=""
#jid_sc1=$(submit_selfcal "${jid_cb0}" "${SC_INDEX[0]}" "${SC_CALMODE[0]}" "${SC_SOLINT[0]}" "${SC_PREFIX[0]}")
jid_sc1=""
#after this step we should have a new measurement set called X.selfcal_1.ms
#jid_img1=$(submit_wsclean "${jid_sc1}" "${IMG_TAGS[1]}" "${WSCLEAN_OPTS[1]}" "$(( SC_INDEX[1]-1 ))")
jid_img1=""

echo ">>> Rounds 1..2..3: re-image deeper -> predict -> phase-only self-cal (60s, 30s)"
# round 1 (phase-only, 60s)
#jid_cb1=$(submit_crystalball "${jid_img1}" "${IMG_TAGS[1]}" "$(( SC_INDEX[1]-1 ))")
jid_cb1=""
jid_sc2=$(submit_selfcal "${jid_cb1}" "${SC_INDEX[1]}" "${SC_CALMODE[1]}" "${SC_SOLINT[1]}" "${SC_PREFIX[1]}")
jid_img2=$(submit_wsclean "${jid_sc2}" "${IMG_TAGS[2]}" "${WSCLEAN_OPTS[2]}" "$(( SC_INDEX[2]-1 ))")
#jid_cb1=""
#jid_sc2=""

# round 2 (phase-only, 30s)
jid_cb2=$(submit_crystalball "${jid_img2}" "${IMG_TAGS[2]}" "$(( SC_INDEX[2]-1 ))")
jid_sc3=$(submit_selfcal "${jid_cb2}" "${SC_INDEX[2]}" "${SC_CALMODE[2]}" "${SC_SOLINT[2]}" "${SC_PREFIX[2]}")
jid_img3=$(submit_wsclean "${jid_sc3}" "${IMG_TAGS[3]}" "${WSCLEAN_OPTS[3]}" "$(( SC_INDEX[3]-1 ))")

# round 3 (phase-only, 30s)
jid_cb3=$(submit_crystalball "${jid_img3}" "${IMG_TAGS[3]}" "$(( SC_INDEX[3]-1 ))")
jid_sc4=$(submit_selfcal "${jid_cb3}" "${SC_INDEX[3]}" "${SC_CALMODE[3]}" "${SC_SOLINT[3]}" "${SC_PREFIX[3]}")
jid_img4=$(submit_wsclean "${jid_sc4}" "${IMG_TAGS[4]}" "${WSCLEAN_OPTS[4]}" "$(( SC_INDEX[4]-1 ))")

#round 4 (amp+phase, 600s)
jid_cb4=$(submit_crystalball "${jid_img4}" "${IMG_TAGS[4]}" "$(( SC_INDEX[4]-1 ))")
jid_sc5=$(submit_selfcal "${jid_cb4}" "${SC_INDEX[4]}" "${SC_CALMODE[4]}" "${SC_SOLINT[4]}" "${SC_PREFIX[4]}")
jid_img5=$(submit_wsclean "${jid_sc5}" "${IMG_TAGS[5]}" "${WSCLEAN_OPTS[5]}" "$(( SC_INDEX[5]-1 ))")

#round 5 (amp+phase, 300s)
jid_cb5=$(submit_crystalball "${jid_img5}" "${IMG_TAGS[5]}" "$(( SC_INDEX[5]-1 ))")
jid_sc6=$(submit_selfcal "${jid_cb5}" "${SC_INDEX[5]}" "${SC_CALMODE[5]}" "${SC_SOLINT[5]}" "${SC_PREFIX[5]}")
# re-image after A+P self-cal
jid_img6=$(submit_wsclean "${jid_sc6}" "${IMG_TAGS[6]}" "${WSCLEAN_OPTS[6]}" "$(( SC_INDEX[6]-1 ))")
# final predict from latest source list
jid_cb6=$(submit_crystalball "${jid_img6}" "${IMG_TAGS[6]}" "$(( SC_INDEX[6]-1 ))")
#PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calG6.ms"}  # relative under data-root/SBID
jid_sb7=$(submit_uvsub "${jid_cb6}" "${SC_INDEX[5]}" "${UVSUB_OUT_PREFIX}" "B0" )


###
#now applycal selfcal onto highres visibilities, crystalball sky model, and uvsub
###
PATTERN=${PATTERN:-"20??*/*beam{beam:02d}*.20????????????.ms"}    # relative under data-root/SBID
#step 1: apply bandpass to the native res
#step 2: apply selfcal to native res
#step 3: crystalball model from 2h continuyum beam onto native res beam
#step 4: uvsub

echo "Pipeline submitted."
echo "Initial image JID: ${jid_img0}"
echo "Final JID: ${jid_sb7}"

