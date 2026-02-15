#!/bin/bash
#SBATCH --job-name=ds_extract
#SBATCH --output=logs/dstools_extract_%A_%a.out
#SBATCH --error=logs/dstools_extract_%A_%a.err
#SBATCH --time=00:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G

set -euo pipefail

# Env overrides
SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}
CHUNK_GLOB=${CHUNK_GLOB:-202?*}
KIND=${KIND:-"boxcar"}


root="${DATA_ROOT}/${SBID}"
cand_dir="${root}/candidates/"

cand_catalogues=( "${cand_dir}/"*.${SBID}_obs_${KIND}_super_summary.csv )
cand_catalogue=${cand_catalogues[0]}

# Discover chunk directories (sub-observations)
shopt -s nullglob
chunkdirs=("${root}"/${CHUNK_GLOB})
shopt -u nullglob

if [[ ${#chunkdirs[@]} -eq 0 ]]; then
  echo "WARN: No chunk directories found under '${root}' matching '${CHUNK_GLOB}'"
  exit 0
fi

idx=${SLURM_ARRAY_TASK_ID:-0}
if (( idx < 0 || idx >= ${#chunkdirs[@]} )); then
  echo "ERROR: SLURM_ARRAY_TASK_ID=${idx} out of range (0..$(( ${#chunkdirs[@]} - 1 )))"
  exit 2
fi

chunkdir="${chunkdirs[$idx]}"
echo "Aggregating candidates for chunkdir: ${chunkdir}"

# Ensure output directory exists
mkdir -p "${chunkdir}/candidates"

# Runtime environment
module load python-scientific/3.11.5-foss-2023b
unset PYTHONPATH
source /fred/oz451/azic/scripts/crystalball_nt/bin/activate

# Run both kinds

#for each chunk dir, for each candidate in cand_catalogue, run:
dstools-extract-ds -d data -p <RA> <DEC> <ms> <outfile>
#using the RA and DEC of the cand from the catalogue.
#Set the outfile to the 
fastducc aggregate --obs-root "${chunkdir}/" --kind boxcar --outdir "${chunkdir}/candidates/"
fastducc aggregate --obs-root "${chunkdir}/" --kind variance --outdir "${chunkdir}/candidates/"

echo "Done: ${chunkdir}"
