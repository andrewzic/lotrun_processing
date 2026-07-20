#!/bin/bash
#SBATCH --job-name=cb_predict
#SBATCH --output=logs/crystalball_%A_%a.out
#SBATCH --error=logs/crystalball_%A_%a.err
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --tmp=20GB
#SBATCH --array=0-0
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=SB82418
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}    # relative under data-root/SBID
SOURCE_LIST_PATTERN=${SOURCE_LIST_PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}
BIND_SRC=${BIND_SRC:-${USER_PATH:-/fred/oz451}}
USE_CONTAINER=${USE_CONTAINER:-False}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-${USER_PATH:-/fred/oz451}/${USER}/containers/casacore.sif}
CB_SUBDIR="${CB_SUBDIR}"
CB_SRCLIST_SUBDIR=${CB_SRCLIST_SUBDIR:-"cont_combined"}
IMG_TAG=${IMG_TAG:-"initial"}
INDEX=${INDEX:-0}
SELFCAL=${SELFCAL:-1}

# Crystalball runtime options (all optional; tune as needed)
OUTPUT_COLUMN=${OUTPUT_COLUMN:-MODEL_DATA}          # crystalball -o
NUM_WORKERS=${NUM_WORKERS:-0}                       # crystalball -j
ROW_CHUNKS=${ROW_CHUNKS:-0}                         # crystalball -rc (0 = auto)
MODEL_CHUNKS=${MODEL_CHUNKS:-0}                     # crystalball -mc (0 = auto)
FIELD=${FIELD:-}                                     # crystalball -f (empty = auto)
MEMORY_FRACTION=${MEMORY_FRACTION:-0.3}             # crystalball -mf
REGION_FILE=${REGION_FILE:-}                         # crystalball -w (optional DS9 region)
PREDICT_ONLY=${PREDICT_ONLY:-}                       # crystalball -po (set to 1 to enable)
NUM_BRIGHTEST_SOURCES=${NUM_BRIGHTEST_SOURCES:-0}   # crystalball -ns (0 = all)

# ---------------- Distributed Dask settings ----------------

# Enable per-beam distributed Dask cluster
CB_DISTRIBUTED=${CB_DISTRIBUTED:-1}

# Dask cluster mode: "local" or "slurm"
CB_DASK_MODE=${CB_DASK_MODE:-local}

# --- Local-mode defaults ---
CB_DASK_LOCAL_NWORKERS=${CB_DASK_LOCAL_NWORKERS:-4}
CB_DASK_LOCAL_WORKER_CPUS=${CB_DASK_LOCAL_WORKER_CPUS:-1}
CB_DASK_LOCAL_WORKER_MEM=${CB_DASK_LOCAL_WORKER_MEM:-7G}

# --- SLURM-mode defaults ---
CB_DASK_SLURM_NWORKERS=${CB_DASK_SLURM_NWORKERS:-512}
CB_DASK_SLURM_WORKER_CPUS=${CB_DASK_SLURM_WORKER_CPUS:-1}
CB_DASK_SLURM_WORKER_MEM=${CB_DASK_SLURM_WORKER_MEM:-16G}
CB_DASK_SLURM_WORKER_TIME=${CB_DASK_SLURM_WORKER_TIME:-00:45:00}
CB_DASK_SLURM_ACCOUNT=${CB_DASK_SLURM_ACCOUNT:-}
CB_DASK_SLURM_PARTITION=${CB_DASK_SLURM_PARTITION:-}
CB_DASK_SLURM_TMP=${CB_DASK_SLURM_TMP:-5GB}

source /fred/oz451/azic/scripts/lotrun_processing/configs/config.sh
CRYSTALBALL_SIF=""
CB_DASK_SLURM_NWORKERS=512
CB_DASK_SLURM_WORKER_MEM=16G
INDEX=6
SELFCAL=0
PATTERN="*beam{beam:02d}*.calB0.ms"
IMG_TAG="selfcal_6"
MODEL_CHUNKS=125
ROW_CHUNKS=200000


# Path to the Dask worker helper script (for SLURM mode)
# Use config's SCRIPT_DIR since config.sh is now sourced
RUN_DASK_WORKER="${SCRIPT_DIR}/scripts/slurm/run_dask_worker.sh"
# ---------------------------------------------------------------------------
# ---------------- Start per-beam Dask cluster ----------------


if [[ "${USE_CONTAINER}" == "True" && -n "${CRYSTALBALL_SIF}" ]]; then
    # container mode
    CRYSTALBALL=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} crystalball}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    CRYSTALBALL=${CRYSTALBALL:-${CRYSTALBALL_ENV}/bin/crystalball}
fi


# Track SLURM worker job IDs for cleanup
SLURM_WORKER_JIDS=""

# Cleanup function: kill scheduler and cancel any SLURM worker jobs
cleanup_dask() {
    echo "Cleaning up Dask cluster for beam ${SLURM_ARRAY_TASK_ID:-?}..."
    if [[ -n "${DASK_SCHED_PID:-}" ]]; then
        kill "${DASK_SCHED_PID}" 2>/dev/null || true
    fi
    if [[ -n "${SLURM_WORKER_JIDS}" ]]; then
        echo "Cancelling SLURM worker jobs: ${SLURM_WORKER_JIDS}"
        for wjid in ${SLURM_WORKER_JIDS}; do
            scancel "${wjid}" 2>/dev/null || true
        done
    fi
}
trap cleanup_dask EXIT

if [[ "${CB_DISTRIBUTED}" == "1" ]]; then

    SCHED_LOG="logs/cb_dask-scheduler_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out"

    echo "Starting dask scheduler..."
    dask-scheduler \
    --host "$(hostname)" \
    --port 0 \
    > "${SCHED_LOG}" 2>&1 &

    DASK_SCHED_PID=$!

    sleep 10

    # Wait for scheduler to announce itself
    for i in {1..30}; do
    if grep -q "Scheduler at" "${SCHED_LOG}"; then
        DASK_SCHEDULER_ADDRESS=$(grep "Scheduler at" "${SCHED_LOG}" | awk '{print $NF}')
        export DASK_SCHEDULER_ADDRESS
        echo "Scheduler running at ${DASK_SCHEDULER_ADDRESS}"
        break
    fi
    sleep 1
    done

    if [[ -z "${DASK_SCHEDULER_ADDRESS:-}" ]]; then
    echo "ERROR: Scheduler failed to start"
    cat "${SCHED_LOG}"
    exit 1
    fi

    export OMP_NUM_THREADS=1
    export OPENBLAS_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export NUMEXPR_NUM_THREADS=1

    # ====================== LOCAL MODE ======================
    if [[ "${CB_DASK_MODE}" == "local" ]]; then

        echo "[DASK] Local mode: starting ${CB_DASK_LOCAL_NWORKERS} workers on $(hostname)"

        # Local directory for Dask worker scratch space (spill to disk)
        # Prefer fast node-local SSD ($JOBFS) if available
        if [[ -n "${JOBFS:-}" && -d "${JOBFS}" ]]; then
            CB_DASK_LOCAL_DIR="${JOBFS}/dask-worker-space"
        else
            CB_DASK_LOCAL_DIR=${CB_DASK_LOCAL_DIR:-"${SCRIPT_DIR:-/fred/oz451/azic/scripts/lotrun_processing}/dask-worker-space"}
        fi
        mkdir -p "${CB_DASK_LOCAL_DIR}"

        for ((i=0; i<CB_DASK_LOCAL_NWORKERS; i++)); do
            dask-worker \
                "${DASK_SCHEDULER_ADDRESS}" \
                --nthreads "${CB_DASK_LOCAL_WORKER_CPUS}" \
                --memory-limit "${CB_DASK_LOCAL_WORKER_MEM}" \
                --local-directory "${CB_DASK_LOCAL_DIR}" \
                > "logs/dask-worker_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${i}.out" 2>&1 &
        done

        # Give local workers time to register
        sleep 10

    # ====================== SLURM MODE ======================
    elif [[ "${CB_DASK_MODE}" == "slurm" ]]; then

        echo "[DASK] SLURM mode: submitting ${CB_DASK_SLURM_NWORKERS} worker jobs"

        # Build sbatch command for worker jobs
        worker_sbatch_args=(
            --job-name="dask_worker_b${SLURM_ARRAY_TASK_ID}"
            --time="${CB_DASK_SLURM_WORKER_TIME}"
            --cpus-per-task="${CB_DASK_SLURM_WORKER_CPUS}"
            --mem="${CB_DASK_SLURM_WORKER_MEM}"
            --tmp="${CB_DASK_SLURM_TMP}"
            --output="logs/dask-worker-slurm_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}_%j.out"
            --error="logs/dask-worker-slurm_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}_%j.err"
        )
        [[ -n "${CB_DASK_SLURM_ACCOUNT}" ]] && worker_sbatch_args+=(--account="${CB_DASK_SLURM_ACCOUNT}")
        [[ -n "${CB_DASK_SLURM_PARTITION}" ]] && worker_sbatch_args+=(--partition="${CB_DASK_SLURM_PARTITION}")

        # Export variables needed by the worker script
        worker_export="DASK_SCHEDULER_ADDRESS=${DASK_SCHEDULER_ADDRESS}"
        worker_export+=",CRYSTALBALL_ENV=${CRYSTALBALL_ENV}"
        worker_export+=",CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-}"
        worker_export+=",BIND_SRC=${BIND_SRC}"
        worker_export+=",CB_DASK_SLURM_WORKER_CPUS=${CB_DASK_SLURM_WORKER_CPUS}"
        worker_export+=",CB_DASK_SLURM_WORKER_MEM=${CB_DASK_SLURM_WORKER_MEM}"
        worker_export+=",USE_CONTAINER=${USE_CONTAINER}"

        # Submit worker jobs
        for ((i=0; i<CB_DASK_SLURM_NWORKERS; i++)); do
            wjid=$(sbatch "${worker_sbatch_args[@]}" \
                --export="${worker_export}" \
                "${RUN_DASK_WORKER}" | awk '{print $4}')
            if [[ -n "${wjid}" ]]; then
                SLURM_WORKER_JIDS+="${wjid} "
            fi
        done

        echo "[DASK] Submitted worker jobs: ${SLURM_WORKER_JIDS}"

        # Wait for at least 1 worker to connect (timeout 5 min)
        echo "[DASK] Waiting for workers to connect..."
        python3 "${SCRIPT_DIR}/scripts/wait_for_workers.py" \
            "${DASK_SCHEDULER_ADDRESS}" --timeout 86400 --interval 120
        if [[ $? -ne 0 ]]; then
            echo "ERROR: Timed out waiting for SLURM Dask workers"
            exit 1
        fi

    else
        echo "ERROR: Unknown CB_DASK_MODE='${CB_DASK_MODE}'. Must be 'local' or 'slurm'."
        exit 1
    fi
fi

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Environment: ${CRYSTALBALL_ENV}"

# Format beam index and glob pattern
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}/${CB_SUBDIR}"
root_srclist="${DATA_ROOT}/${SBID}/${CB_SRCLIST_SUBDIR}"
glob="${PATTERN//\{beam:02d\}/$beam2}"

if (( SELFCAL == 1 ))
then
    if (( INDEX > 0 )); then
	glob2="${glob/calB0/selfcal_${INDEX}}"
    glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" #catch all for continuum    
    else
	glob2="${glob}"
    fi
    #if in selfcal mode, match the source list to the measurement set
    source_list_glob="${glob2}"
else
    if (( INDEX > 0 )); then
	glob2="${glob/calB0/calG${INDEX}}"
    glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" #catch all for continuum
	source_list_glob_="${SOURCE_LIST_PATTERN//\{beam:02d\}/$beam2}"
	source_list_glob="${source_list_glob_/calB0/selfcal_${INDEX}}"
    else
	glob2="${glob}"
	source_list_glob="${SOURCE_LIST_PATTERN//\{beam:02d\}/$beam2}"	
    fi
fi
search_glob="${root}/${glob2}"
source_list_search_glob="${root_srclist}/${source_list_glob}"
# Discover MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob
shopt -s nullglob
source_list_msnames=( ${source_list_search_glob} )
shopt -u nullglob

echo "Looking for MS files with glob: ${search_glob}"
echo "Looking for source list MS files with glob: ${source_list_search_glob}"
echo "Found ${#msnames[@]} MS files and ${#source_list_msnames[@]} source list MS files for beam ${beam2}."
#assume there is only one source list relevant per beam
if [[ ${#source_list_msnames[@]} -eq 0 ]]; then
  echo "WARN: No source list MS found for SBID=$SBID beam=${beam2} using '${source_list_search_glob}'"
  exit 1
fi
source_list_msname=${source_list_msnames[0]}

if [[ ${#msnames[@]} -eq 0 ]]; then
  echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
  exit 1
fi

# Build a reusable option string for crystalball
cb_opts=( "-o" "${OUTPUT_COLUMN}" "-j" "${NUM_WORKERS}" "-mf" "${MEMORY_FRACTION}" )
[[ "${ROW_CHUNKS}" -gt 0 ]]   && cb_opts+=( "-rc" "${ROW_CHUNKS}" )
[[ "${MODEL_CHUNKS}" -gt 0 ]] && cb_opts+=( "-mc" "${MODEL_CHUNKS}" )
[[ -n "${FIELD}" ]]           && cb_opts+=( "-f"  "${FIELD}" )
[[ -n "${REGION_FILE}" ]]     && cb_opts+=( "-w"  "${REGION_FILE}" )
[[ -n "${PREDICT_ONLY}" ]]    && cb_opts+=( "-po" )
[[ "${NUM_BRIGHTEST_SOURCES}" -gt 0 ]] && cb_opts+=( "-ns" "${NUM_BRIGHTEST_SOURCES}" )
[[ "${CB_DISTRIBUTED}" == "1" ]]       && cb_opts+=( "--scheduler distributed" "--address ${DASK_SCHEDULER_ADDRESS}" )

echo "Crystalball argv:"
printf '  %q\n' "${cb_opts[@]}"

for ms in "${msnames[@]}"; do
    # Derive the WSClean source list path from the earlier -name "${ms%.ms}.img"
    
    # Discover MS files for this beam
    shopt -s nullglob
    msnames=( ${search_glob} )
    shopt -u nullglob

    if [[ ${#msnames[@]} -eq 0 ]]; then
	echo "WARN: No MS found for SBID=$SBID beam=${beam2} using '${search_glob}'"
	exit 1
    fi
    
  src_list="${source_list_msname%.ms}.${IMG_TAG}_img-sources.txt"
  if [[ ! -f "${src_list}" ]]; then
      echo "WARN: Source list not found for MS '${ms}': expected '${src_list}'"
      exit 1
      #continue
  fi
  echo "Predicting model -> MS=${ms}"
  echo "Using source list: ${src_list}"

  # Execute crystalball CLI inside environment
  echo "running:"
  echo "${CRYSTALBALL} ${ms} -sm ${src_list} ${cb_opts[@]}"
  $CRYSTALBALL ${ms} -sm ${src_list} ${cb_opts[@]}
  #apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${CRYSTALBALL_SIF}" \
  #  crystalball "${ms}" -sm "${src_list}" "${cb_opts[@]}"
done

# ---------------- Teardown Dask cluster ----------------
# cleanup_dask is called automatically via the EXIT trap set earlier.
# This explicit call provides a visible log message.
if [[ "${CB_DISTRIBUTED}" == "1" ]]; then
    echo "Stopping Dask cluster for beam ${beam2}"
fi
