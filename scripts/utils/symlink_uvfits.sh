#!/bin/bash
set -euo pipefail

SBID="$1"
USER="${USER:-$(whoami)}"
DATA_SRC_ROOT="${DATA_SRC_ROOT:-${USER_PATH:-/fred/oz451}/data/craco}"
DATA_ROOT="${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"

# Strip trailing slashes for consistent path concatenation
DATA_SRC_ROOT="${DATA_SRC_ROOT%/}"
DATA_ROOT="${DATA_ROOT%/}"

# Validate source directory exists
if [ ! -d "${DATA_SRC_ROOT}/${SBID}" ]; then
    echo "ERROR: source directory does not exist: ${DATA_SRC_ROOT}/${SBID}"
    exit 1
fi

# Remove any broken symlinks in the destination before creating new ones
if [ -d "${DATA_ROOT}/${SBID}" ]; then
    find "${DATA_ROOT}/${SBID}" -xtype l -delete 2>/dev/null || true
fi

src_dir="${DATA_SRC_ROOT}/${SBID}"
dest_base="${DATA_ROOT}/${SBID}"
declare -A seen_dirs

# Stream find output and use pure bash string operations (zero subshells per file)
while IFS= read -r f; do
    bf="${f##*/}"
    scan_dir="${f%/*}"
    scanid="${scan_dir##*/}"

    target_dir="${dest_base}/${scanid}"
    if [[ -z "${seen_dirs[$target_dir]:-}" ]]; then
        mkdir -p "${target_dir}"
        seen_dirs["$target_dir"]=1
    fi

    dest_file="${target_dir}/${bf}"
    if [[ -L "${dest_file}" && -e "${dest_file}" ]]; then
        continue
    fi
    ln -sf "$f" "${dest_file}"
done < <(find "${src_dir}/" -name "*.uvfits")

# Calibration tables
cal_src=""
if [ -d "${src_dir}/CRACO-Calibration-Tables-${SBID}" ]; then
    cal_src="${src_dir}/CRACO-Calibration-Tables-${SBID}"
elif [ -d "${src_dir}/cal" ]; then
    cal_src="${src_dir}/cal"
fi

if [ -n "$cal_src" ]; then
    cal_dest="${dest_base}/CRACO-Calibration-Tables-${SBID}"
    mkdir -p "${cal_dest}"
    while IFS= read -r c; do
        bc="${c##*/}"
        dest_c="${cal_dest}/${bc}"
        if [[ -L "${dest_c}" && -e "${dest_c}" ]]; then
            continue
        fi
        ln -sf "$c" "${dest_c}"
    done < <(find "$cal_src" -name "*.B0")

    if [ ! -e "${dest_base}/cal" ] && [ ! -L "${dest_base}/cal" ]; then
        ln -s "CRACO-Calibration-Tables-${SBID}" "${dest_base}/cal"
    fi
else
    echo "cannot find cal or CRACO-Calibration-Tables directory for SBID ${SBID}"
    exit 1
fi

    
	       
