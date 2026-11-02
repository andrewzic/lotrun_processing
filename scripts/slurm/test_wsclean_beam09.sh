#!/bin/bash
#SBATCH --job-name=test_wsclean_b09
#SBATCH --output=logs/test_wsclean_b09_%j.out
#SBATCH --error=logs/test_wsclean_b09_%j.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --partition=skylake
#SBATCH --account=oz451

set -euo pipefail

module load apptainer

mkdir -p logs
OUTDIR="/fred/oz451/azic/data/SB82418/continuum_visibilities/test_beam09"
mkdir -p "${OUTDIR}"

MS="/fred/oz451/azic/data/SB82418/continuum_visibilities/scienceData.LTR_1812-2849.SB82418.LTR_1812-2849.beam09_averaged_cal.leakage.ms"
SIF="/fred/oz451/azic/containers/flint-containers_wsclean.sif"

echo "Starting test WSClean on ${MS}"
echo "Output prefix: ${OUTDIR}/test_b09"

apptainer exec --bind /fred/oz451:/fred/oz451 "${SIF}" \
    wsclean \
    -name "${OUTDIR}/test_b09" \
    -data-column DATA \
    -save-source-list \
    -mgain 0.8 \
    -multiscale \
    -multiscale-scale-bias 0.8 \
    -niter 5000 \
    -pol i \
    -weight briggs 0.5 \
    -scale 2.5asec \
    -size 4096 4096 \
    -auto-threshold 3 \
    -auto-mask 15.0 \
    -join-channels \
    -channels-out 4 \
    -fit-spectral-pol 3 \
    -abs-mem 28 \
    -parallel-gridding 8 \
    "${MS}"

echo "WSClean test completed successfully."
