#!/bin/bash
#SBATCH --job-name=contcopy
#SBATCH --output=logs/contcopy_%A_%a.out
#SBATCH --error=logs/contcopy_%A_%a.err
#SBATCH --time=00:15:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G

set -euo pipefail
# -------------------- USER CONFIG --------------------
# Path to the list of MS files (one per line)

SBID=${SBID:-SB77974}
DATA_ROOT=${DATA_ROOT:-/fred/oz451/${USER}/data}

# -----------------------------------------------------

src_root="${DATA_ROOT}/${SBID}"

dst_dir="${src_root}/continuum_images"
mkdir -p "${dst_dir}"

shopt -s nullglob

for beam in $(seq 0 35); do
    printf -v beam2 "%02d" "${beam}"

    # Gather all candidate selfcal images for this beam
    candidates=( "${src_root}"/*"beam${beam2}"*selfcal_*_img-MFS-image.fits )

   if (( ${#candidates[@]} == 0 )); then
        echo "Beam ${beam2}: no selfcal images found, skipping."
        continue
    fi

    # Track the highest selfcal iteration (second number) and file path
    max_iter=-1
    best_file=""

    for f in "${candidates[@]}"; do
        # Match ...selfcal_X.selfcal_Y_img-MFS-image.fits
        if [[ $(basename -- "$f") =~ selfcal_([0-9]+)\.selfcal_([0-9]+)_img-MFS-image\.fits$ ]]; then
            iter2="${BASH_REMATCH[2]}"
            # Prefer the highest Y; if tie, optionally prefer highest X
            if (( iter2 > max_iter )); then
                max_iter="$iter2"
                best_file="$f"
            elif (( iter2 == max_iter )); then
                # Optional tie-breaker on the first number if needed
                iter1="${BASH_REMATCH[1]}"
                if [[ -n "${best_file}" ]] && [[ $(basename -- "$best_file") =~ selfcal_([0-9]+)\.selfcal_([0-9]+)_img-MFS-image\.fits$ ]]; then
                    best_iter1="${BASH_REMATCH[1]}"
                    if (( iter1 > best_iter1 )); then
                        best_file="$f"
                    fi
                fi
            fi
        fi
    done

    if [[ -z "${best_file}" ]]; then
        echo "Beam ${beam2}: no matching selfcal_*.*_img-MFS-image.fits after parsing, skipping."
        continue
    fi

    echo "Beam ${beam2}: copying $(basename -- "$best_file")"
    cp -f -- "${best_file}" "${dst_dir}/"
done

shopt -u nullglob
echo "Done copying continuum images to ${dst_dir}"