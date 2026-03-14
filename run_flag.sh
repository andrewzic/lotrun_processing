#!/bin/bash
#SBATCH --job-name=aoflagger_array
#SBATCH --output=logs/aoflagger_%A_%a.out
#SBATCH --error=logs/aoflagger_%A_%a.err
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=12G
#SBATCH --array=0-500 #usually about 350 craco ms per obs
# Optional: set your partition/queue
# #SBATCH --partition=standard
# Optional: limit concurrency to avoid filesystem contention
# #SBATCH --array=0-99%10

set -euo pipefail
# -------------------- USER CONFIG --------------------
# Path to the list of MS files (one per line)

SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"20??*/*beam*.20????????????*.ms"}   # relative under data-root/SBID
SCRIPT_DIR=${SCRIPT_DIR:-/fred/oz451/${USER}/scripts/lotrun_processing}
FLINT_AOFLAGGER_SIF=${FLINT_AOFLAGGER_SIF:-/fred/oz451/${USER}/containers/flint-containers_aoflagger.sif}
BIND_SRC=${BIND_SRC:-/fred/oz451}

# Path to flag.sh
#FLAG_SCRIPT=${FLAG_SCRIPT:-${SCRIPT_DIR}/flag.sh}
AOFLAGGER=${AOFLAGGER:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${FLINT_AOFLAGGER_SIF} aoflagger}

# Column to use in aoflagger ("DATA" default)
COLUMN=${COLUMN:-DATA}

# Directory that contains aoflagger strategies (expects ASKAP.lua inside)
# This must match what flag.sh expects: $script_dir/aoflagger/ASKAP.lua
export script_dir=${SCRIPT_DIR:-$PWD}

AOFLAGGER_OPTIONS="-column $COLUMN -strategy $script_dir/aoflagger/ASKAP.lua -v"

# -----------------------------------------------------

root="${DATA_ROOT}/${SBID}"
glob="${PATTERN}"
search_glob="${root}/${glob}"

# Expand glob to list of MS files for this beam
shopt -s nullglob
msnames=( ${search_glob} )
shopt -u nullglob

mkdir -p logs

# if [[ ! -x "$FLAG_SCRIPT" ]]; then
#     echo "ERROR: FLAG_SCRIPT '$FLAG_SCRIPT' not found or not executable." >&2
#     exit 1
# fi

# 1) Ensure SLURM_ARRAY_TASK_ID is set
if [[ -z "${SLURM_ARRAY_TASK_ID-}" ]]; then
    echo "ERROR: SLURM_ARRAY_TASK_ID is not set. Are you running this within an sbatch array job?" >&2
    exit 1
fi

# 2) Ensure files were found
if (( ${#msnames[@]} == 0 )); then
    echo "ERROR: No MS files found for glob: ${search_glob}" >&2
    exit 1
fi

# 3) Ensure index is in range
idx=${SLURM_ARRAY_TASK_ID}
if (( idx < 0 || idx >= ${#msnames[@]} )); then
    echo "ERROR: SLURM_ARRAY_TASK_ID=${idx} is out of range (0..$(( ${#msnames[@]} - 1 ))) for ${#msnames[@]} files." >&2
    exit 0
fi

MSFILE=${msnames[$idx]}

if [[ -z "${MSFILE:-}" ]]; then
    echo "ERROR: No entry in '$search_glob' for SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID" >&2
    exit 1
fi

echo "Job ${SLURM_JOB_ID}.${SLURM_ARRAY_TASK_ID} starting on $(hostname)"
echo "Using MS: $MSFILE"
echo "Column:   $COLUMN"
echo "script_dir: $script_dir"

#module load aoflagger
module load apptainer

# Run the flagging wrapped in a retry loop to handle transient errors (e.g., filesystem issues)
run_with_timeout_and_retry() {
  local attempts="$1"; shift
  local timeout_s="$1"; shift
  local try=1
  local rc=0
  set -o monitor
  while (( try <= attempts )); do
    echo "[INFO] Attempt ${try}/${attempts}: $(date -Is) :: $*"
    setsid timeout --preserve-status --signal=TERM --kill-after=30s "${timeout_s}" "$@" &
    local pid=$!
    wait "${pid}"
    rc=$?
    if (( rc == 0 )); then
      echo "[INFO] Success on attempt ${try}."
      return 0
    elif (( rc == 124 )); then
      echo "[WARN] Timeout after ${timeout_s}; attempt ${try}."
      pgid=$(ps -o pgid= -p "${pid}" | tr -d ' ')
      if [[ -n "${pgid}" ]]; then
        echo "[INFO] Killing leftover processes in PGID ${pgid}"
        kill -TERM -"${pgid}" 2>/dev/null || true
        sleep 2
        kill -KILL -"${pgid}" 2>/dev/null || true
      fi
    else
      echo "[ERROR] Command failed with rc=${rc} on attempt ${try} (non-timeout)."
      return "${rc}"
    fi
    sleep $(( (RANDOM % 10) + 5 ))s
    ((try++))
  done
  echo "[ERROR] Exhausted ${attempts} attempts; giving up."
  return 124
}

# Run AOFLAGGER with a 8-minute cap, up to 3 tries
run_with_timeout_and_retry 3 8m \
  srun --cpu-bind=cores --kill-on-bad-exit=1 \
       bash -lc '${AOFLAGGER} ${AOFLAGGER_OPTIONS} "$MSFILE" '


