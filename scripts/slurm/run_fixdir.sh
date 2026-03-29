#!/bin/bash
#SBATCH --job-name=fixdir_ms
#SBATCH --output=logs/fixdir_ms_%A_%a.out
#SBATCH --error=logs/fixdir_ms_%A_%a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --array=0-35
set -euo pipefail
module load apptainer
# -------- User-configurable via --export or edit defaults here --------
SBID=${SBID:-SB82418}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
PATTERN=${PATTERN:-"*beam{beam:02d}_averaged_cal*.ms"}
SCRIPT=${SCRIPT:-scripts/utils/fix_dir.py}
CRYSTALBALL_ENV=${CRYSTALBALL_ENV:-/fred/oz451/${USER}/scripts/crystalball_nt/}
CRYSTALBALL_SIF=${CRYSTALBALL_SIF:-}
INDEX=${INDEX:-0}
# ---------------------------------------------------------------------

mkdir -p logs
printf 'Job %s.%s on %s\n' "${SLURM_JOB_ID}" "${SLURM_ARRAY_TASK_ID}" "$(hostname)"
echo "SBID=$SBID DATA_ROOT=$DATA_ROOT BEAM=$SLURM_ARRAY_TASK_ID PATTERN=$PATTERN"

if [ -n "${CRYSTALBALL_SIF}" ]; then
    # container mode
    PYTHON=${CRYSTALBALL:-apptainer exec --bind ${BIND_SRC}:${BIND_SRC} ${CRYSTALBALL_SIF} python}
else
    # venv mode
    module load python-scientific/3.11.5-foss-2023b
    unset PYTHONPATH
    source "${CRYSTALBALL_ENV}/bin/activate"
    PYTHON=${CRYSTALBALL:-${CRYSTALBALL_ENV}/bin/python3}
fi



# Resolve the beam-specific glob by formatting {beam:02d}
beam="${SLURM_ARRAY_TASK_ID}"
printf -v beam2 "%02d" "${beam}"
root="${DATA_ROOT}/${SBID}"
glob="${PATTERN//\{beam:02d\}/$beam2}"
if (( INDEX > 0 )); then
    glob2="${glob/calB0/selfcal_${INDEX}}"
    glob2="${glob2/_averaged_cal.leakage/selfcal_${INDEX}}" #catch all for continuum
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

for i in "${!msnames[@]}"
do
    msname=${msnames[$i]}
    echo "found msname=$msname"
    python "$SCRIPT" "$msname"
done
