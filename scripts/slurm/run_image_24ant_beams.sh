#!/bin/bash
#SBATCH --job-name=image_24ant
#SBATCH --output=logs/image_24ant_%A_%a.out
#SBATCH --error=logs/image_24ant_%A_%a.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --array=0-35

set -euo pipefail

module load apptainer

# -------------------- User-configurable via --export --------------------
SBID=${SBID:-SB82418}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
INDEX=${INDEX:-6}
FLINT_WSCLEAN_SIF=${FLINT_WSCLEAN_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/flint-containers_wsclean.sif}
FLINT_CASA_SIF=${FLINT_CASA_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/flint-containers_casa.sif}
BIND_SRC=${BIND_SRC:-${USER_PATH:-/fred/oz451}}
WSCLEAN_CHANNELS_OUT=${WSCLEAN_CHANNELS_OUT:-4}
# -----------------------------------------------------------------------

mkdir -p logs

beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"

# Find the calibrated/selfcal MS for this beam
shopt -s nullglob
candidates=(
    "${root}/continuum_visibilities/"*"beam${beam2}"*".selfcal_${INDEX}.ms"
    "${root}/continuum_visibilities/"*"beam${beam2}"*".selfcal_"*".ms"
    "${root}/continuum_visibilities/"*"beam${beam2}"*"_averaged_cal.leakage.ms"
)
shopt -u nullglob

if [[ ${#candidates[@]} -eq 0 ]]; then
    echo "ERROR: No suitable continuum MS found for SBID=${SBID}, beam=${beam2} under ${root}/continuum_visibilities/"
    exit 1
fi

in_ms="${candidates[0]}"
echo "Selected input MS for 24-ant CRACO-matching imaging: ${in_ms}"

split_ms="${in_ms%.ms}.craco24ant.ms"
rm -rf "${split_ms}" "${split_ms}.flagversions"

echo "Extracting inner 24 antennas (0~23) into ${split_ms}..."
apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${FLINT_CASA_SIF}" python3 -c "
import os, sys, shutil
from casatasks import split
in_vis = '${in_ms}'
out_vis = '${split_ms}'
print(f'Splitting {in_vis} -> {out_vis} (antenna=0~23)')
split(vis=in_vis, outputvis=out_vis, antenna='0~23', datacolumn='DATA')
print('Split complete.')
"

outname="${in_ms%.ms}.craco24ant_img"
echo "Running WSClean on ${split_ms} with CRACO-matching parameters -> ${outname}"

apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${FLINT_WSCLEAN_SIF}" \
    wsclean \
    -name "${outname}" \
    -data-column DATA \
    -save-source-list \
    -mgain 0.8 \
    -multiscale \
    -multiscale-scale-bias 0.8 \
    -multiscale-max-scales 5 \
    -niter 50000 \
    -pol i \
    -weight briggs 0.5 \
    -scale 12asec \
    -size 1536 1536 \
    -auto-threshold 1.0 \
    -auto-mask 5.0 \
    -join-channels \
    -channels-out "${WSCLEAN_CHANNELS_OUT}" \
    -fit-spectral-pol 3 \
    -abs-mem 28 \
    -parallel-gridding 8 \
    "${split_ms}" 2>&1 | tee "${outname}.wsclean.log"

# Copy final MFS image to continuum_images/
dst_dir="${root}/continuum_images"
mkdir -p "${dst_dir}"
if [[ -f "${outname}-MFS-image.fits" ]]; then
    dst_img="${dst_dir}/$(basename "${outname}")-MFS-image.fits"
    echo "Copying ${outname}-MFS-image.fits -> ${dst_img}"
    cp -f "${outname}-MFS-image.fits" "${dst_img}"
fi

# Clean up temporary split MS to conserve space
echo "Cleaning up temporary 24-ant MS: ${split_ms}"
rm -rf "${split_ms}" "${split_ms}.flagversions"

# Clean up temporary intermediate images
rm -rf "${outname}"*-dirty.fits "${outname}"*-psf.fits "${outname}"*-residual.fits

echo "Successfully completed 24-ant CRACO-matching imaging for beam ${beam2}."
