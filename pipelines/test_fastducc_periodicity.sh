#!/bin/bash
#SBATCH --job-name=test_fastducc_periodicity
#SBATCH --output=logs/test_periodicity_%j.out
#SBATCH --error=logs/test_periodicity_%j.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
# =============================================================================
# Test script for fastducc periodicity search on single-scan MS
# Target: Candidate J173808.5-233126.2 (PSR J1738-2330, F0 ~ 0.5053 Hz, DM ~ 96.6)
# =============================================================================
set -euo pipefail

SBID="${SBID:-SB77974}"
BEAM="${BEAM:-01}"
SCAN_TIMESTAMP="${SCAN_TIMESTAMP:-20251015072402}"
DATA_ROOT="${DATA_ROOT:-/fred/oz451/${USER}/data}"

# Single scan MS path
MSNAME="${DATA_ROOT}/${SBID}/${SCAN_TIMESTAMP}/cracoData.LTR_1733-2344.${SBID}.beam${BEAM}.${SCAN_TIMESTAMP}.calG9.uvsub.ms"

if [[ ! -d "${MSNAME}" ]]; then
  echo "WARN: ${MSNAME} does not exist, trying fallback pattern..."
  MSNAME=$(ls -d ${DATA_ROOT}/${SBID}/${SCAN_TIMESTAMP}/*beam${BEAM}*.cal*.ms 2>/dev/null | head -n 1)
fi

echo "Selected Measurement Set for test: ${MSNAME}"

# DM0 search as requested
DM="${DM:-0.0}"
FMIN="${FMIN:-0.2}"
NPIX="${NPIX:-384}"
PIXSCALE_ARCSEC="${PIXSCALE_ARCSEC:-22.0}"
THRESHOLD_SIGMA="${THRESHOLD_SIGMA:-5.0}"
NHARM="${NHARM:-8}"
NTHREADS="${NTHREADS:-8}"
MAX_BLOCKS="${MAX_BLOCKS:-3}"
PARALLEL_MODE="${PARALLEL_MODE:-dask-slurm}"

module load python-scientific/3.11.5-foss-2023b 2>/dev/null || true
unset PYTHONPATH
source "/fred/oz451/${USER}/scripts/crystalball_nt/bin/activate"

echo "Launching fastducc periodicity search test (DM=${DM}, max_blocks=${MAX_BLOCKS}, parallel_mode=${PARALLEL_MODE})..."
fastducc periodicity \
  --msname "${MSNAME}" \
  --dm "${DM}" \
  --fmin "${FMIN}" \
  --npix "${NPIX}" \
  --pixscale-arcsec "${PIXSCALE_ARCSEC}" \
  --threshold-sigma "${THRESHOLD_SIGMA}" \
  --nharm "${NHARM}" \
  --nthreads "${NTHREADS}" \
  --max-blocks "${MAX_BLOCKS}" \
  --parallel-mode "${PARALLEL_MODE}" \
  --write-products \
  --detect

echo "Periodicity search test completed successfully."
