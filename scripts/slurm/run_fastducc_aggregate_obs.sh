#!/bin/bash
#SBATCH --job-name=fastducc_obsagg
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --output=logs/fastducc_obsagg_%A.out
#SBATCH --error=logs/fastducc_obsagg_%A.err

set -euo pipefail

USE_CONTAINER=${USE_CONTAINER:-False}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/casacore.sif}

SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}

# Optional overrides (CLI pattern / outdir); you can export these too if you want
KIND_LIST=${KIND_LIST:-"variance boxcar"}   # run both families by default
SKY_TOL_ARCSEC=${SKY_TOL_ARCSEC:-35.0}
OUTDIR_DEFAULT="${DATA_ROOT}/${SBID}/candidates"

# -------------------- Runtime environment ----------
if [[ "${USE_CONTAINER}" == "True" && -n "${CRYSTALBALL_SIF}" ]]; then
    # container mode
    FASTDUCC=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} fastducc}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    FASTDUCC=${CRYSTALBALL:-fastducc}
fi

# -------------------- Run obs-level aggregation for each kind ------------------------
obs_root="${DATA_ROOT}/${SBID}"
outdir="${OUTDIR_DEFAULT}"

mkdir -p "${outdir}"

for kind in ${KIND_LIST}; do
  echo "[ObsAgg] SBID=${SBID} kind=${kind} sky_tol=${SKY_TOL_ARCSEC} outdir=${outdir}"
  # use fastducc_run.py’s CLI entrypoint to dispatch into candidates.aggregate_observation_from_super_summaries
  ${FASTDUCC} aggregate_obs --obs-root "${obs_root}" --kind "${kind}" --sky-tol-arcsec "${SKY_TOL_ARCSEC}" --outdir "${outdir}"
done

echo "[ObsAgg] Done: outputs in ${outdir}"
