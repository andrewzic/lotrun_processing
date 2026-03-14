#!/bin/bash

SBID=$1
USER=$( whoami )

for f in $( find /fred/oz451/data/craco/"${SBID}"/ -name "*.uvfits" )
do
    bf=$( basename $f )
    ff=$( realpath $f )
    scanid=$( echo $ff | sed 's|/fred/oz451/data/craco/||g' | awk -F'/' '{print $2}' )
    sbid=$( echo $ff | sed 's|/fred/oz451/data/craco/||g' | awk -F'/' '{print $1}' )
    mkdir -p /fred/oz451/"${USER}"/data/$sbid/$scanid/
    if [ ! -e /fred/oz451/"${USER}"/data/$sbid/$scanid/$bf ];
    then
	ln -s $ff /fred/oz451/"${USER}"/data/$sbid/$scanid/
    fi
done

#don't forget the cal
if [ -d /fred/oz451/data/craco/"${SBID}"/cal ]
then
    mkdir -p /fred/oz451/"${USER}"/data/"${SBID}"/cal
    for c in $( find /fred/oz451/data/craco/"${SBID}"/cal -name "*.B0" )
    do
	bc=$( basename $c )
	if [ ! -e /fred/oz451/"${USER}"/data/"${SBID}"/cal/"$bc" ]
	then
	    ln -s $c /fred/oz451/"${USER}"/data/"${SBID}"/cal/"$bc"
	fi
    done
else
    echo "cannot find cal directory for SBID ${SBID}"
    exit 1
fi

    
	       
