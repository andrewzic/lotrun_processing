#!/bin/bash

SBID=$1
USER=$( whoami )

for f in $( find /fred/oz451/data/craco/"${SBID}"/ -name "*.uvfits" )
do
    bf=$( basename $f )
    ff=$( realpath $f )
    scanid=$( echo $ff | sed 's|/fred/oz451/data/craco/||g' | awk -F'/' '{print $2}' )
    sbid=$( echo $ff | sed 's|/fred/oz451/data/craco/||g' | awk -F'/' '{print $1}' )
    echo $sbid $scanid $bf
    echo $ff
    echo $ff | sed 's|/fred/oz451/data/craco||g'
    mkdir -p fred/oz451/"${USER}"/data/$sbid/$scanid/
    if [ ! -e /fred/oz451/"${USER}"/data/$sbid/$scanid/$bf ];
    then
	ln -s $ff /fred/oz451/"${USER}"/data/$sbid/$scanid/
    fi
done
