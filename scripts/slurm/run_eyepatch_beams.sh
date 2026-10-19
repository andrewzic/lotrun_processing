#!/bin/bash
#SBATCH --job-name=eyepatch_mask
#SBATCH --output=logs/eyepatch_%A_%a.out
#SBATCH --error=logs/eyepatch_%A_%a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}    # relative under data-root/SBID
USE_CONTAINER=${USE_CONTAINER:-False}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/casacore.sif}
EYEPATCH_SIF=${EYEPATCH_SIF:-${CRYSTALBALL_SIF}}
BIND_SRC=${BIND_SRC:-${USER_PATH:-/fred/oz451}}
IMG_TAG=${IMG_TAG:-"initial"}
INDEX=${INDEX:-0}
SELFCAL=${SELFCAL:-1}
IGNORE_QC_FAIL=${IGNORE_QC_FAIL:-0}

# Eye-patch masking options (all configurable via --export or config)
FLOOD_FILL=${FLOOD_FILL:-True}
FLOOD_FILL_POSITIVE_SEED_CLIP=${FLOOD_FILL_POSITIVE_SEED_CLIP:-${SEED_CLIP:-1.1}}
FLOOD_FILL_POSITIVE_FLOOD_CLIP=${FLOOD_FILL_POSITIVE_FLOOD_CLIP:-${FLOOD_CLIP:-0.7}}
FLOOD_FILL_USE_MAC=${FLOOD_FILL_USE_MAC:-True}
FLOOD_FILL_MAC_BOX_SIZE=${FLOOD_FILL_MAC_BOX_SIZE:-${MAC_BOX_SIZE:-350}}
FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR=${FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR:-${MAC_ADAPTIVE_STEP_FACTOR:-}}
FLOOD_FILL_MAC_ADAPTIVE_SKEW_DELTA=${FLOOD_FILL_MAC_ADAPTIVE_SKEW_DELTA:-}
FLOOD_FILL_MAC_ADAPTIVE_MAX_DEPTH=${FLOOD_FILL_MAC_ADAPTIVE_MAX_DEPTH:-}

BEAM_SHAPE_ERODE=${BEAM_SHAPE_ERODE:-True}
BEAM_SHAPE_ERODE_MIN_RESPONSE=${BEAM_SHAPE_ERODE_MIN_RESPONSE:-${BEAM_ERODE_MIN_RESPONSE:-0.75}}
BEAM_SHAPE_ERODE_SCALES=${BEAM_SHAPE_ERODE_SCALES:-${CLEAN_SCALES:-}}

GROW_LOW_SNR_ISLAND=${GROW_LOW_SNR_ISLAND:-False}
GROW_LOW_SNR_ISLAND_CLIP=${GROW_LOW_SNR_ISLAND_CLIP:-}
GROW_LOW_SNR_ISLAND_SIZE=${GROW_LOW_SNR_ISLAND_SIZE:-}

CONVOLVE_FIRST=${CONVOLVE_FIRST:-False}
RMS_FITS=${RMS_FITS:-}
BKG_FITS=${BKG_FITS:-}
SAVE_SIGNAL=${SAVE_SIGNAL:-False}
BASE_SNR_CLIP=${BASE_SNR_CLIP:-}
EXTRA_EYEPATCH_OPTS=${EXTRA_EYEPATCH_OPTS:-}

# ---------------------------------------------------------------------------

if [[ "${USE_CONTAINER}" == "True" && -n "${EYEPATCH_SIF}" ]]; then
    # container mode
    EYEPATCH=${EYEPATCH_BIN:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${EYEPATCH_SIF} eye-patch}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b 2>/dev/null || true
    unset PYTHONPATH
    if [[ -f "${CRYSTALBALL_ENV}/bin/activate" ]]; then
        source "${CRYSTALBALL_ENV}/bin/activate"
    fi
    EYEPATCH=${EYEPATCH_BIN:-${CRYSTALBALL_ENV}/bin/eye-patch}
fi

mkdir -p logs

job_id="${SLURM_JOB_ID:-LOCAL}"
task_id="${SLURM_ARRAY_TASK_ID:-0}"
echo "Job ${job_id}.${task_id} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$task_id PATTERN=$PATTERN"
echo "Environment: ${CRYSTALBALL_ENV}"

# Format beam index and glob pattern
beam="${task_id}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"
if (( SELFCAL == 1 )); then
    if (( INDEX > 0 )); then
        glob2="${glob/calB0/selfcal_${INDEX}}"
        glob2="${glob2/_averaged_cal.leakage/.selfcal_${INDEX}}" # catch all for continuum    
    else
        glob2="${glob}"
    fi
else
    if (( INDEX > 0 )); then
        glob2="${glob/calB0/calG${INDEX}}"
        glob2="${glob2/_averaged_cal.leakage/.selfcal_${INDEX}}" # catch all for continuum
    else
        glob2="${glob}"
    fi
fi
search_glob="${root}/${glob2}"

# Discover MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
    echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
    exit 0
fi

for ms in "${msnames[@]}"; do
    flag_file="$(dirname "${ms}")/beam${beam2}_selfcal_failed.flag"
    if [[ -f "${flag_file}" && "${IGNORE_QC_FAIL}" != "1" ]]; then
        echo "Bypassing eyepatch mask for beam ${beam2} (Selfcal failure detected)"
        continue
    fi

    # Derive the WSClean MFS image from earlier
    fits_image="${ms%.ms}.${IMG_TAG}_img-MFS-image.fits"
    if [[ ! -f "${fits_image}" ]]; then
        echo "ERROR: MFS fits image not found for MS '${ms}': expected '${fits_image}'"
        exit 1
    fi
    echo "Deriving fits mask from MFS fits image ${fits_image}"

    # Resolve scales for --beam-shape-erode-scales
    scales_clean=""
    if [[ -n "${BEAM_SHAPE_ERODE_SCALES}" && "${BEAM_SHAPE_ERODE_SCALES}" != "auto" ]]; then
        scales_clean=$(echo "${BEAM_SHAPE_ERODE_SCALES}" | tr "[],\"" " " | xargs)
    else
        # Attempt auto-detection from preceding WSClean log if scales were not explicitly provided
        wsclean_log=""
        if [[ -f "${ms%.ms}.${IMG_TAG}_img.wsclean.log" ]]; then
            wsclean_log="${ms%.ms}.${IMG_TAG}_img.wsclean.log"
        elif [[ -f "${fits_image%.fits}.wsclean.log" ]]; then
            wsclean_log="${fits_image%.fits}.wsclean.log"
        else
            # Search Slurm logs for the matching WSClean job
            candidate_logs=( $(ls -t logs/wsclean_${IMG_TAG}_*_${beam}.out logs/wsclean_${IMG_TAG}_*_${beam2}.out logs/wsclean_*_${beam}.out logs/wsclean_*_${beam2}.out 2>/dev/null || true) )
            if [[ ${#candidate_logs[@]} -gt 0 ]]; then
                wsclean_log="${candidate_logs[0]}"
            fi
        fi

        if [[ -n "${wsclean_log}" && -f "${wsclean_log}" ]]; then
            scales_clean=$(awk '/Scale info:/{flag=1; next} flag && /^- Scale/{gsub(/,/,"",$3); printf "%d ", int($3 + 0.5)} flag && (/RMS per scale/ || /^$/){exit}' "${wsclean_log}" | xargs)
            if [[ -n "${scales_clean}" ]]; then
                echo "Auto-detected multi-scale scales from '${wsclean_log}': ${scales_clean}"
            fi
        fi
    fi

    # Construct eye-patch mask invocation
    cmd=( ${EYEPATCH} mask "${fits_image}" )

    if [[ "${FLOOD_FILL}" == "True" || "${FLOOD_FILL}" == "1" ]]; then
        cmd+=( "--flood-fill" )
    fi
    if [[ -n "${FLOOD_FILL_POSITIVE_SEED_CLIP}" ]]; then
        cmd+=( "--flood-fill-positive-seed-clip" "${FLOOD_FILL_POSITIVE_SEED_CLIP}" )
    fi
    if [[ -n "${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" ]]; then
        cmd+=( "--flood-fill-positive-flood-clip" "${FLOOD_FILL_POSITIVE_FLOOD_CLIP}" )
    fi
    if [[ "${FLOOD_FILL_USE_MAC}" == "True" || "${FLOOD_FILL_USE_MAC}" == "1" ]]; then
        cmd+=( "--flood-fill-use-mac" )
    fi
    if [[ -n "${FLOOD_FILL_MAC_BOX_SIZE}" ]]; then
        cmd+=( "--flood-fill-use-mac-box-size" "${FLOOD_FILL_MAC_BOX_SIZE}" )
    fi
    if [[ -n "${FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR}" ]]; then
        cmd+=( "--flood-fill-use-mac-adaptive-step-factor" "${FLOOD_FILL_MAC_ADAPTIVE_STEP_FACTOR}" )
    fi
    if [[ -n "${FLOOD_FILL_MAC_ADAPTIVE_SKEW_DELTA}" ]]; then
        cmd+=( "--flood-fill-use-mac-adaptive-skew-delta" "${FLOOD_FILL_MAC_ADAPTIVE_SKEW_DELTA}" )
    fi
    if [[ -n "${FLOOD_FILL_MAC_ADAPTIVE_MAX_DEPTH}" ]]; then
        cmd+=( "--flood-fill-use-mac-adaptive-max-depth" "${FLOOD_FILL_MAC_ADAPTIVE_MAX_DEPTH}" )
    fi
    if [[ "${GROW_LOW_SNR_ISLAND}" == "True" || "${GROW_LOW_SNR_ISLAND}" == "1" ]]; then
        cmd+=( "--grow-low-snr-island" )
        if [[ -n "${GROW_LOW_SNR_ISLAND_CLIP}" ]]; then
            cmd+=( "--grow-low-snr-island-clip" "${GROW_LOW_SNR_ISLAND_CLIP}" )
        fi
        if [[ -n "${GROW_LOW_SNR_ISLAND_SIZE}" ]]; then
            cmd+=( "--grow-low-snr-island-size" "${GROW_LOW_SNR_ISLAND_SIZE}" )
        fi
    fi
    if [[ "${BEAM_SHAPE_ERODE}" == "True" || "${BEAM_SHAPE_ERODE}" == "1" ]]; then
        cmd+=( "--beam-shape-erode" )
        if [[ -n "${BEAM_SHAPE_ERODE_MIN_RESPONSE}" ]]; then
            cmd+=( "--beam-shape-erode-minimum-response" "${BEAM_SHAPE_ERODE_MIN_RESPONSE}" )
        fi
        if [[ -n "${scales_clean}" ]]; then
            cmd+=( "--beam-shape-erode-scales" ${scales_clean} )
        fi
    fi
    if [[ "${CONVOLVE_FIRST}" == "True" || "${CONVOLVE_FIRST}" == "1" ]]; then
        cmd+=( "--convolve-first" )
    fi
    if [[ -n "${RMS_FITS}" ]]; then
        cmd+=( "--rms-fits" "${RMS_FITS}" )
    fi
    if [[ -n "${BKG_FITS}" ]]; then
        cmd+=( "--bkg-fits" "${BKG_FITS}" )
    fi
    if [[ "${SAVE_SIGNAL}" == "True" || "${SAVE_SIGNAL}" == "1" ]]; then
        cmd+=( "--save-signal" )
    fi
    if [[ -n "${BASE_SNR_CLIP}" ]]; then
        cmd+=( "--base-snr-clip" "${BASE_SNR_CLIP}" )
    fi
    if [[ -n "${EXTRA_EYEPATCH_OPTS}" ]]; then
        cmd+=( ${EXTRA_EYEPATCH_OPTS} )
    fi

    echo "Running eye-patch mask:"
    echo "${cmd[*]}"
    "${cmd[@]}"

    expected_mask="${fits_image%.fits}.mask.fits"
    if [[ -f "${expected_mask}" ]]; then
        echo "Successfully generated mask: ${expected_mask}"
    else
        echo "WARN: Expected mask '${expected_mask}' not found after running eye-patch!"
    fi
done
