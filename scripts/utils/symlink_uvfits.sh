#!/bin/bash

SBID=$1
USER=$( whoami )
DATA_SRC_ROOT=${DATA_SRC_ROOT:-${USER_PATH:-/fred/oz451}/data/craco/}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data/}

# Validate source directory exists
if [ ! -d "${DATA_SRC_ROOT}/${SBID}" ]; then
    echo "ERROR: source directory does not exist: ${DATA_SRC_ROOT}/${SBID}"
    exit 1
fi

# Remove any broken symlinks in the destination before creating new ones
if [ -d "${DATA_ROOT}/${SBID}" ]; then
    find "${DATA_ROOT}/${SBID}" -xtype l -delete 2>/dev/null
fi

for f in $( find "${DATA_SRC_ROOT}/${SBID}/" -name "*.uvfits" )
do
    bf=$( basename $f )
    ff=$( realpath $f )
    scanid=$( echo $ff | sed 's|'"${DATA_SRC_ROOT}"'||g' | awk -F'/' '{print $2}' )
    sbid=$( echo $ff | sed 's|'"${DATA_SRC_ROOT}"'||g' | awk -F'/' '{print $1}' )
    mkdir -p ${DATA_ROOT}/$sbid/$scanid/
    if [ -L "${DATA_ROOT}/$sbid/$scanid/$bf" ] && [ -e "${DATA_ROOT}/$sbid/$scanid/$bf" ]; then
	# Valid symlink already exists, skip
	continue
    fi
    ln -sf $ff ${DATA_ROOT}/$sbid/$scanid/
done

#don't forget the cal
if [ -d "${DATA_SRC_ROOT}${SBID}"/cal ]
then
    mkdir -p "${DATA_ROOT}/${SBID}/cal"
    for c in $( find "${DATA_SRC_ROOT}${SBID}/cal" -name "*.B0" )
    do
	bc=$( basename $c )
	if [ -L "${DATA_ROOT}/${SBID}/cal/$bc" ] && [ -e "${DATA_ROOT}/${SBID}/cal/$bc" ]; then
	    # Valid symlink already exists, skip
	    continue
	fi
	ln -sf $c "${DATA_ROOT}/${SBID}/cal/$bc"
    done
else
    echo "cannot find cal directory for SBID ${SBID}"
    exit 1
fi

    
	       
