#!/bin/bash
#SBATCH --job-name=cb_predict
#SBATCH --output=logs/crystalball_%A_%a.out
#SBATCH --error=logs/crystalball_%A_%a.err
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=24G
#SBATCH --array=0-36
# Optional: #SBATCH --partition=standard

set -euo pipefail

# ---------------------- User-configurable via --export ----------------------
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}    # relative under data-root/SBID
SOURCE_LIST_PATTERN=${SOURCE_LIST_PATTERN:-"*beam{beam:02d}*.avg.calB0.ms"}
#BIND_SRC=${BIND_SRC:-/fred/oz451}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-/fred/oz451/${USER}/scripts/crystalball_nt/}
IMG_TAG=${IMG_TAG:-"initial"}
INDEX=${INDEX:-0}
SELFCAL=${SELFCAL:-1}

# Crystalball runtime options (all optional; tune as needed)
OUTPUT_COLUMN=${OUTPUT_COLUMN:-MODEL_DATA}          # crystalball -o
NUM_WORKERS=${NUM_WORKERS:-0}                       # crystalball -j
ROW_CHUNKS=${ROW_CHUNKS:-0}                         # crystalball -rc (0 = auto)
MODEL_CHUNKS=${MODEL_CHUNKS:-0}                     # crystalball -mc (0 = auto)
FIELD=${FIELD:-}                                     # crystalball -f (empty = auto)
MEMORY_FRACTION=${MEMORY_FRACTION:-0.8}             # crystalball -mf
REGION_FILE=${REGION_FILE:-}                         # crystalball -w (optional DS9 region)
PREDICT_ONLY=${PREDICT_ONLY:-}                       # crystalball -po (set to 1 to enable)
NUM_BRIGHTEST_SOURCES=${NUM_BRIGHTEST_SOURCES:-0}   # crystalball -ns (0 = all)

# ---------------- Distributed Dask settings ----------------

# Enable per-beam distributed Dask cluster
CB_DISTRIBUTED=${CB_DISTRIBUTED:-1}

# Per-beam Dask cluster limits (VERY IMPORTANT)
# These control how hard ONE beam can hit the cluster
CB_DASK_NWORKERS=${CB_DASK_NWORKERS:-8}        # max workers for this beam
CB_DASK_WORKER_CPUS=${CB_DASK_WORKER_CPUS:-1}  # CPUs per worker
CB_DASK_WORKER_MEM=${CB_DASK_WORKER_MEM:-8G}   # Memory per worker

# Port offsets so array tasks don't collide
CB_DASK_SCHED_PORT_BASE=${CB_DASK_SCHED_PORT_BASE:-8786}

# ---------------------------------------------------------------------------
# ---------------- Start per-beam Dask cluster ----------------

if [[ "${CB_DISTRIBUTED}" == "1" ]]; then

    SCHED_LOG="logs/dask-scheduler_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out"

    echo "Starting dask scheduler..."
    dask-scheduler \
    --host "$(hostname)" \
    --port 0 \
    > "${SCHED_LOG}" 2>&1 &

    DASK_SCHED_PID=$!

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

    echo "Starting ${CB_DASK_NWORKERS} Dask workers for this beam"

    export OMP_NUM_THREADS=1
    export OPENBLAS_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export NUMEXPR_NUM_THREADS=1

    for ((i=0; i<CB_DASK_NWORKERS; i++)); do
        dask-worker \
            "${DASK_SCHEDULER_ADDRESS}" \
            --nthreads ${CB_DASK_WORKER_CPUS} \
            --memory-limit ${CB_DASK_WORKER_MEM} \
            > "logs/dask-worker_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}_${i}.out" 2>&1 &
    done

    # Give workers time to register
    sleep 10
fi


module load python-scientific/3.11.5-foss-2023b
unset PYTHONPATH
source ${CRYSTALBALL_ENV}/bin/activate

mkdir -p logs

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} on $(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"
echo "Environment: ${CRYSTALBALL_ENV}"

# Format beam index and glob pattern
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
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
source_list_search_glob="${root}/${source_list_glob}"
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
  echo "${CRYSTALBALL_ENV}/bin/crystalball ${ms} -sm ${src_list} ${cb_opts[@]}"
  ${CRYSTALBALL_ENV}/bin/crystalball ${ms} -sm ${src_list} ${cb_opts[@]}
  #apptainer exec --bind "${BIND_SRC}:${BIND_SRC}" "${CRYSTALBALL_SIF}" \
  #  crystalball "${ms}" -sm "${src_list}" "${cb_opts[@]}"
done

# ---------------- Teardown Dask cluster ----------------

if [[ "${CB_DISTRIBUTED}" == "1" ]]; then
    echo "Stopping Dask cluster for beam ${beam2}"
    kill ${DASK_SCHED_PID} || true
fi
