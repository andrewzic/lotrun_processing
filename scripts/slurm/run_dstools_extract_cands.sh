#!/bin/bash
#SBATCH --job-name=ds_extract
#SBATCH --output=logs/dstools_extract_%j.out
#SBATCH --error=logs/dstools_extract_%j.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G

set -euo pipefail

# -------------------- User-tunable env (with defaults) --------------------
# environment variables
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-}

# Observation selection
export SBID="${SBID:-SB77974}"
export DATA_ROOT="${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
export KIND="${KIND:-boxcar}"                 # 'boxcar' or 'variance'
export DS_MIN_SNR="${DS_MIN_SNR:-8.0}"                 # e.g. 7.0 (blank to disable)
export DS_SOURCE_ID="${DS_SOURCE_ID:-}"             # restrict to one source_id (blank to disable)

# Dask/SLURM worker sizing (used by orchestrator's --scheduler slurm)
export DS_N_WORKERS="${DS_N_WORKERS:-48}"           # total workers to scale to
export DS_CPUS=${DS_CPUS:-1}
export DS_MEM=${DS_MEM:-8GB}
export DS_WALLTIME=${DS_WALLTIME:-01:00:00}
export DS_QUEUE="${DS_QUEUE:-}"                     # e.g. 'workq' or cluster-specific
export DS_PROJECT="${DS_PROJECT:-}"                 # SLURM account/project

# Orchestrator knobs
export DS_BATCH_SIZE="${DS_BATCH_SIZE:-200}"
export DS_RETRIES="${DS_RETRIES:-1}"
export DS_SLEEP_BETWEEN_BATCHES="${DS_SLEEP_BETWEEN_BATCHES:-0}"
export DS_BEAM_SCOPE="${DS_BEAM_SCOPE:-union}"      # 'union' or 'strict' (will be ignored if max_snr_beam is present)
export DS_MATCH_ARCSEC="${DS_MATCH_ARCSEC:-35.0}"
export DS_MS_GLOB_TEMPLATE="${DS_MS_GLOB_TEMPLATE:-**/cracoData*%s*uvsub.ms}"
export DS_DATACOLUMN="${DS_DATACOLUMN:-data}"
export DS_PRIMARY_BEAM="${DS_PRIMARY_BEAM:-}"       # blank => no PB correction from file
export DS_NOFLAG="${DS_NOFLAG:-false}"              # 'true' to drop flags, else false
export DS_BASELINE_AVERAGE="${DS_BASELINE_AVERAGE:-true}"
export DS_MINUVDIST="${DS_MINUVDIST:-0.0}"
export DS_VERBOSE="${DS_VERBOSE:-false}"
export DS_OVERWRITE="${DS_OVERWRITE:-false}"
export DS_DRY_RUN="${DS_DRY_RUN:-false}"
export DS_CATALOGUE="${DS_CATALOGUE:-}"         # blank => auto-discover summary catalogue from data root
export DS_SCAN_SCOPE="${DS_SCAN_SCOPE:-all}"

# If prefer to pin an explicit catalogue instead of auto-discovery, set:
# export CATALOGUE="/path/to/<field>.<SBID>_obs_${KIND}_super_summary.vot"

# -------------------- Paths & logs --------------------
root="${DATA_ROOT}/${SBID}"
mkdir -p "${root}/candidates" "logs" "dask-worker-space"

# -------------------- Runtime environment for the launcher --------------------
# This should mirror the orchestrator's default --job-prologue for workers.
if [ -n "${CRYSTALBALL_SIF}" ]; then
    # container mode
    PYTHON=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} python}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    PYTHON=${CRYSTALBALL:-python}
fi

# -------------------- Build orchestrator command --------------------
cmd=( ${PYTHON} src/extract/extract_ds_orchestrator.py
  --sbid "${SBID}"
  --data-root "${DATA_ROOT}"
  --kind "${KIND}"
  --scheduler slurm
  --n-workers "${DS_N_WORKERS}"
  --cores "${DS_CPUS}"
  --mem "${DS_MEM}"
  --walltime "${DS_WALLTIME}"
  --batch-size "${DS_BATCH_SIZE}"
  --retries "${DS_RETRIES}"
  --sleep-between-batches "${DS_SLEEP_BETWEEN_BATCHES}"
  --beam-scope "${DS_BEAM_SCOPE}"
  --scan-scope "${DS_SCAN_SCOPE}"
  --match-arcsec "${DS_MATCH_ARCSEC}"
  --ms-glob-template "${DS_MS_GLOB_TEMPLATE}"
  --datacolumn "${DS_DATACOLUMN}"
)

# Optional flags/args
if [[ -n "${DS_CATALOGUE:-}" ]]; then
  cmd+=( --catalogue "${DS_CATALOGUE}" )
fi
if [[ -n "${DS_MIN_SNR}" ]]; then
  cmd+=( --min-snr "${DS_MIN_SNR}" )
fi
if [[ -n "${DS_SOURCE_ID}" ]]; then
  cmd+=( --source-id "${DS_SOURCE_ID}" )
fi
if [[ -n "${DS_PRIMARY_BEAM}" ]]; then
  cmd+=( --primary-beam "${DS_PRIMARY_BEAM}" )
fi
[[ "${DS_NOFLAG}" == "true" ]] && cmd+=( --noflag )
[[ "${DS_BASELINE_AVERAGE}" == "true" ]] && true || cmd+=( --baseline-average False )
[[ "${DS_VERBOSE}" == "true" ]] && cmd+=( --verbose )
[[ "${DS_OVERWRITE}" == "true" ]] && cmd+=( --overwrite )
[[ "${DS_DRY_RUN}" == "true" ]] && cmd+=( --dry-run )

# SLURM queue/account customization for Dask workers
if [[ -n "${DS_QUEUE}" ]]; then
  cmd+=( --queue "${DS_QUEUE}" )
fi
if [[ -n "${DS_PROJECT}" ]]; then
  cmd+=( --project "${DS_PROJECT}" )
fi

# Pass the *same* module/venv steps to the Dask workers via job prologue
if [ -n "${CRYSTALBALL_SIF}" ]; then
  cmd+=( --job-prologue "" )
else
  cmd+=( --job-prologue "module load python-scientific/3.11.5-foss-2023b ; unset PYTHONPATH; source ${USER_PATH:-/fred/oz451}/${USER}/scripts/crystalball_nt/bin/activate" )
fi

# -------------------- Run --------------------
echo "[INFO] Running: ${cmd[*]}"
"${cmd[@]}"