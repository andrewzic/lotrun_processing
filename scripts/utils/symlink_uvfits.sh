#!/bin/bash

SBID=$1
USER=$( whoami )
DATA_SRC_ROOT=${DATA_SRC_ROOT:-${USER_PATH:-/fred/oz451}/data/craco/}
DATA_ROOT=${DATA_ROOT:-${USER_PATH:-/fred/oz451}/${USER}/data/}

for f in $( find "${DATA_SRC_ROOT}/${SBID}/" -name "*.uvfits" )
do
    bf=$( basename $f )
    ff=$( realpath $f )
    scanid=$( echo $ff | sed 's|'"${DATA_SRC_ROOT}"'||g' | awk -F'/' '{print $2}' )
    sbid=$( echo $ff | sed 's|'"${DATA_SRC_ROOT}"'||g' | awk -F'/' '{print $1}' )
    mkdir -p ${DATA_ROOT}/$sbid/$scanid/
    if [ ! -e ${DATA_ROOT}/$sbid/$scanid/$bf ];
    then
	ln -s $ff ${DATA_ROOT}/$sbid/$scanid/
    fi
done

#don't forget the cal
if [ -d "${DATA_SRC_ROOT}${SBID}"/cal ]
then
    mkdir -p "${DATA_ROOT}/${SBID}/cal"
    for c in $( find "${DATA_SRC_ROOT}${SBID}/cal" -name "*.B0" )
    do
	bc=$( basename $c )
	if [ ! -e "${DATA_ROOT}/${SBID}/cal/$bc" ]
	then
	    ln -s $c "${DATA_ROOT}/${SBID}/cal/$bc"
	fi
    done
else
    echo "cannot find cal directory for SBID ${SBID}"
    exit 1
fi

    
	       
