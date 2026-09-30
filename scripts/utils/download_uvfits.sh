#!/bin/bash
#SBATCH --job-name=download_uvfits
#SBATCH --output=logs/download_%x_%j.out
#SBATCH --error=logs/download_%x_%j.err
#SBATCH --partition=datamover
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=0

# Download CRACO visibilities and continuum products from CASDA for a given SBID,
# and organize the downloaded files into:
#   - CRACO scan uvfits: <DATA_ROOT>/<SBID>/<scanid>/
#   - Continuum visibilities: <DATA_ROOT>/<SBID>/continuum_visibilities/
#   - CRACO bandpass tables: <DATA_ROOT>/<SBID>/CRACO-Calibration-Tables-<SBID>/
#     (with backwards-compatible symlink <DATA_ROOT>/<SBID>/cal)

set -euo pipefail

SBID_INPUT="${1:-${SBID:-}}"
CASDA_USERNAME="${2:-${CASDA_USERNAME:-}}"
MAX_NWORKERS="${3:-${DOWNLOAD_WORKERS:-4}}"

if [ -z "${SBID_INPUT}" ]; then
    echo "Usage: $0 <SBID> [CASDA_USERNAME] [MAX_NWORKERS]"
    echo "       $0 <SBID> --organize-only"
    echo "Example: $0 82418 andrew.zic@csiro.au 4"
    exit 1
fi

# Normalize SBID (e.g. "82418" or "SB82418" -> sbid_num="82418", SBID="SB82418")
sbid_num="${SBID_INPUT#SB}"
SBID="SB${sbid_num}"

USER=$( whoami )
DATA_ROOT="${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data}"
DEST_DIR="${DATA_ROOT}/${SBID}"

mkdir -p "${DEST_DIR}"

# Remove any broken symlinks in the destination directory
find "${DEST_DIR}" -xtype l -delete 2>/dev/null || true

# If user specified --organize-only or ORGANIZE_ONLY=1, skip vis_download
ORGANIZE_ONLY="${ORGANIZE_ONLY:-0}"
if [ "${CASDA_USERNAME}" = "--organize-only" ]; then
    ORGANIZE_ONLY=1
fi

if [ "${ORGANIZE_ONLY}" -eq 0 ]; then
    if [ -z "${CASDA_USERNAME}" ]; then
        echo "ERROR: CASDA_USERNAME is required unless using --organize-only"
        echo "Usage: $0 <SBID> <CASDA_USERNAME> [MAX_NWORKERS]"
        exit 1
    fi

    # Ensure vis_download is available in PATH or use virtual environment / modules
    VENV_PATH="${VENV_PATH:-/fred/oz451/azic/scripts/crystalball_nt}"
    
    # Attempt module loading if running in an environment with Lmod
    if command -v module &>/dev/null || type module &>/dev/null; then
        module load python-scientific/3.11.5-foss-2023b 2>/dev/null || \
        module load Python/3.11.3-GCCcore-12.3.0 2>/dev/null || \
        module load Python 2>/dev/null || true
    fi

    VIS_CMD=()
    # 1. Test if vis_download in PATH works directly
    if command -v vis_download &>/dev/null && vis_download --help &>/dev/null; then
        VIS_CMD=(vis_download)
    # 2. Test if vis_download inside VENV works directly
    elif [ -x "${VENV_PATH}/bin/vis_download" ] && "${VENV_PATH}/bin/vis_download" --help &>/dev/null; then
        VIS_CMD=("${VENV_PATH}/bin/vis_download")
    # 3. Test if venv python works directly
    elif [ -x "${VENV_PATH}/bin/python" ] && "${VENV_PATH}/bin/python" -c "import vis_downloader" 2>/dev/null; then
        VIS_CMD=("${VENV_PATH}/bin/python" -m vis_downloader.async_download)
    elif [ -x "${VENV_PATH}/bin/python3" ] && "${VENV_PATH}/bin/python3" -c "import vis_downloader" 2>/dev/null; then
        VIS_CMD=("${VENV_PATH}/bin/python3" -m vis_downloader.async_download)
    else
        # 4. Fallback: On datamover or heterogenous nodes where venv symlink may point to a missing path,
        # find system or module python3 and add venv site-packages to PYTHONPATH
        for cand in \
            "$(command -v python3 2>/dev/null || true)" \
            /apps/modules/software/Python/3.11*/bin/python3 \
            /apps/modules/software/Python/3.10*/bin/python3 \
            /usr/bin/python3; do
            [ -x "${cand}" ] || continue
            if "${cand}" -c "import vis_downloader" 2>/dev/null; then
                VIS_CMD=("${cand}" -m vis_downloader.async_download)
                break
            elif PYTHONPATH="${VENV_PATH}/lib/python3.11/site-packages:${PYTHONPATH:-}" "${cand}" -c "import vis_downloader" 2>/dev/null; then
                export PYTHONPATH="${VENV_PATH}/lib/python3.11/site-packages:${PYTHONPATH:-}"
                VIS_CMD=("${cand}" -m vis_downloader.async_download)
                break
            fi
        done
    fi

    if [ ${#VIS_CMD[@]} -eq 0 ]; then
        echo "ERROR: vis_download / vis_downloader not found in PATH, ${VENV_PATH}/bin, or available Python modules"
        exit 1
    fi

    echo "Downloading data for ${SBID} (SBID=${sbid_num}) into ${DEST_DIR}"

    # Build vis_download arguments
    # Note: omit --vis-type by default to download all products (CRACO scans + continuum 10s visibilities)
    # Set VIS_TYPE (e.g. "craco" or "science") in the environment if filtering is desired.
    VIS_DOWNLOAD_ARGS=(
        --output-dir "${DEST_DIR}/"
        --username "${CASDA_USERNAME}"
        --max-workers "${MAX_NWORKERS}"
        --extract-tar
        --resume
    )

    if [ -n "${VIS_TYPE:-}" ]; then
        VIS_DOWNLOAD_ARGS+=(--vis-type "${VIS_TYPE}")
    fi

    "${VIS_CMD[@]}" "${VIS_DOWNLOAD_ARGS[@]}" "${sbid_num}"
fi

echo "Organizing files for ${SBID} in ${DEST_DIR}..."

# 1. Organize CRACO uvfits files into per-scan directories
for f in "${DEST_DIR}"/cracoData.*.uvfits; do
    [ -e "$f" ] || continue
    bf=$( basename "$f" )
    # Filename format: cracoData.<field>.<SBID>.<beam>.<scanid>.uvfits
    name="${bf%.uvfits}"
    scanid="${name##*.}"
    scan_dir="${DEST_DIR}/${scanid}"
    mkdir -p "${scan_dir}"
    echo "Organizing ${bf} -> ${scanid}/"
    mv "$f" "${scan_dir}/${bf}"
done

# Also move scan-specific CRACO metadata files (e.g. *.craco_metadata.xml) if present
for f in "${DEST_DIR}"/cracoData.*.craco_metadata.xml; do
    [ -e "$f" ] || continue
    bf=$( basename "$f" )
    name="${bf%.craco_metadata.xml}"
    scanid="${name##*.}"
    scan_dir="${DEST_DIR}/${scanid}"
    mkdir -p "${scan_dir}"
    echo "Organizing ${bf} -> ${scanid}/"
    mv "$f" "${scan_dir}/${bf}"
done

# 2. Organize continuum 10s visibilities (scienceData...) into continuum_visibilities/
CONT_DIR="${DEST_DIR}/continuum_visibilities"
for f in "${DEST_DIR}"/scienceData.*; do
    [ -e "$f" ] || continue
    mkdir -p "${CONT_DIR}"
    bf=$( basename "$f" )
    echo "Organizing ${bf} -> continuum_visibilities/"
    mv "$f" "${CONT_DIR}/${bf}"
done

# 3. Handle CRACO bandpass calibration tables and ensure backwards compatibility
cal_tables_dir="${DEST_DIR}/CRACO-Calibration-Tables-${SBID}"
if [ -d "${cal_tables_dir}" ]; then
    echo "Found bandpass calibration tables in ${cal_tables_dir}"
    # Create backwards-compatible 'cal' symlink if 'cal' doesn't exist
    if [ ! -e "${DEST_DIR}/cal" ] && [ ! -L "${DEST_DIR}/cal" ]; then
        ln -s "CRACO-Calibration-Tables-${SBID}" "${DEST_DIR}/cal"
        echo "Created backwards-compatible symlink: ${DEST_DIR}/cal -> CRACO-Calibration-Tables-${SBID}"
    fi
elif [ -d "${DEST_DIR}/cal" ]; then
    echo "Found legacy cal directory at ${DEST_DIR}/cal"
    if [ ! -e "${cal_tables_dir}" ] && [ ! -L "${cal_tables_dir}" ]; then
        ln -s "cal" "${cal_tables_dir}"
        echo "Created backwards-compatible symlink: ${cal_tables_dir} -> cal"
    fi
else
    echo "WARNING: Could not find bandpass calibration tables in ${DEST_DIR}"
fi

echo "Successfully organized ${SBID} in ${DEST_DIR}"
