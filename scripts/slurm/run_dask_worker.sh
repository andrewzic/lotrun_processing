#!/bin/bash
#SBATCH --job-name=dask_worker
#SBATCH --output=logs/dask-worker-slurm_%j.out
#SBATCH --error=logs/dask-worker-slurm_%j.err
# Time, CPUs, mem, tmp, account, partition are set dynamically by the
# parent crystalball script via sbatch command-line overrides.

set -euo pipefail

# ---------------------- Required environment variables ----------------------
# These MUST be passed via --export from the parent crystalball job:
#   DASK_SCHEDULER_ADDRESS  - tcp://<host>:<port> of the scheduler
#   CRYSTALBALL_ENV         - path to the Python venv
#   CRYSTALBALL_SIF         - path to container SIF (empty = use venv)
#   BIND_SRC                - apptainer bind path (only needed in container mode)
#   CB_DASK_SLURM_WORKER_CPUS - CPUs per worker (nthreads)
#   CB_DASK_SLURM_WORKER_MEM  - memory limit per worker

DASK_SCHEDULER_ADDRESS="${DASK_SCHEDULER_ADDRESS:?ERROR: DASK_SCHEDULER_ADDRESS not set}"
CB_DASK_SLURM_WORKER_CPUS="${CB_DASK_SLURM_WORKER_CPUS:-1}"
CB_DASK_SLURM_WORKER_MEM="${CB_DASK_SLURM_WORKER_MEM:-4G}"

# ---------------------- Activate environment ----------------------
if [ -n "${CRYSTALBALL_SIF:-}" ]; then
    echo "[dask-worker] Container mode (SIF=${CRYSTALBALL_SIF})"
    DASK_WORKER_CMD="apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} dask-worker"
else
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    DASK_WORKER_CMD="dask-worker"
fi

# ---------------------- Thread pinning ----------------------
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

# ---------------------- Scratch directory ----------------------
# Prefer fast node-local SSD ($JOBFS) if available
if [[ -n "${JOBFS:-}" && -d "${JOBFS}" ]]; then
    WORKER_LOCAL_DIR="${JOBFS}/dask-worker-space"
else
    WORKER_LOCAL_DIR="/tmp/dask-worker-space-${SLURM_JOB_ID}"
fi
mkdir -p "${WORKER_LOCAL_DIR}"

# ---------------------- Start worker ----------------------
echo "[dask-worker] Connecting to scheduler: ${DASK_SCHEDULER_ADDRESS}"
echo "[dask-worker] nthreads=${CB_DASK_SLURM_WORKER_CPUS}, memory-limit=${CB_DASK_SLURM_WORKER_MEM}"
echo "[dask-worker] local-directory=${WORKER_LOCAL_DIR}"
echo "[dask-worker] hostname=$(hostname), SLURM_JOB_ID=${SLURM_JOB_ID}"

${DASK_WORKER_CMD} \
    "${DASK_SCHEDULER_ADDRESS}" \
    --nthreads "${CB_DASK_SLURM_WORKER_CPUS}" \
    --memory-limit "${CB_DASK_SLURM_WORKER_MEM}" \
    --local-directory "${WORKER_LOCAL_DIR}" \
    --no-dashboard
