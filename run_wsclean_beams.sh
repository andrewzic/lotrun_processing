#!/bin/bash
#SBATCH --job-name=wsclean_ms
#SBATCH --output=logs/wsclean_%A_%a.out
#SBATCH --error=logs/wsclean_%A_%a.err
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# -------------------- User-configurable via --export --------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}   # relative under data-root/SBID
FLINT_WSCLEAN_SIF=${FLINT_WSCLEAN_SIF:-/fred/oz451/${USER}/containers/flint-containers_wsclean.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}
# You can override WSCLEAN_OPTS to tune imaging parameters:
WSCLEAN_OPTS=${WSCLEAN_OPTS:-"-save-source-list -multiscale -multiscale-scale-bias 0.8 -niter 100000 -pol xx -weight briggs 0.5 -scale 12asec -size 856 856 -auto-threshold 3 -auto-mask 8 -join-channels -channels-out 2 -fit-spectral-pol 1"}
FITS_MASK_TAG=${FITS_MASK_TAG:-}
IMG_TAG=${IMG_TAG:-"initial"}
INDEX=${INDEX:-0}

# -----------------------------------------------------------------------

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Container: ${FLINT_WSCLEAN_SIF}; Bind: ${BIND_SRC}"

module load apptainer

# Resolve the beam-specific glob by formatting {beam:02d}
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"
if (( INDEX > 0 )); then
    glob2="${glob/calB0/selfcal_${INDEX}}"
else
    glob2="${glob}"
fi
search_glob="${root}/${glob2}"

# Expand glob to list of MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

if [[ ${#msnames[@]} -eq 0 ]]; then
    echo "WARN: No MS found for SBID=$SBID beam=${beam2} using pattern '${search_glob}'"
    exit 1
fi

if [[ -n "${FITS_MASK_TAG}" ]]; then
    
    mask_glob="${PATTERN//\{beam:02d\}/$beam2}"
    if (( INDEX > 1 )); then
	mask_glob2="${mask_glob/calB0/selfcal_$((INDEX-1))}"
    elif (( INDEX <= 1 )); then
	mask_glob2="${mask_glob}"
    fi
    
    mask_search_glob="${root}/${mask_glob2}"
    echo "will search for fits mask using $mask_search_glob"
    shopt -s nullglob
    mask_msnames=( ${mask_search_glob} )
    shopt -u nullglob
    echo "found ${#mask_msnames[@]} masks"
fi
# Run WSClean for each MS found for this beam

for i in "${!msnames[@]}"
do
    msname=${msnames[$i]}
    echo "found msname=$msname"
    if [[ -n "${FITS_MASK_TAG}" ]]; then    
	mask_msname=${mask_msnames[$i]}
	FITS_MASK="${mask_msname%.ms}.${FITS_MASK_TAG}_img-MFS-image.mask.fits"
	echo "using $FITS_MASK as fits mask"
	if [ ! -f ${FITS_MASK} ]; then
	    echo "fits mask ${FITS_MASK} does not exist. exiting."
	    exit 1
	fi
	NEW_WSCLEAN_OPTS="${WSCLEAN_OPTS} -fits-mask ${FITS_MASK}"
    else
	NEW_WSCLEAN_OPTS="${WSCLEAN_OPTS}"
    fi
    outname="${msname%.ms}.${IMG_TAG}_img"
    echo "outname is $outname"
    echo "Running WSClean: MS=${msname} -> name=${outname}"
    echo "apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${FLINT_WSCLEAN_SIF} wsclean -name ${outname} ${NEW_WSCLEAN_OPTS} ${msname}"
    apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${FLINT_WSCLEAN_SIF}" wsclean -name "${outname}" ${NEW_WSCLEAN_OPTS} "${msname}"
    rm -rf "${outname}*-00*.fits"
    rm -rf "${outname}-MFS-dirty.fits"
    rm -rf "${outname}-MFS-psf.fits"
    rm -rf "${outname}-MFS-residual.fits"
    #rm -rf "${outname}-MFS-model.fits"
done

# for msname in "${msnames[@]}"; do

	   
#     fi    

# done
