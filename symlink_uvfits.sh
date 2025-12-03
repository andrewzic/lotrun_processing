#!/bin/bash

for f in $( find /fred/oz451/data/craco/SB77974/ -name "*.uvfits" )
do
    bf=$( basename $f )
    ff=$( realpath $f )
    df=$( dirname $ff )
    scanid=$( echo $ff | sed 's|/fred/oz451/data/craco/||g' | awk -F'/' '{print $2}' )
    sbid=$( echo $ff | sed 's|/fred/oz451/data/craco/||g' | awk -F'/' '{print $1}' )
    echo $sbid $scanid $bf
    echo $ff
    echo $ff | sed 's|/fred/oz451/data/craco||g'
    if [ ! -e /fred/oz451/azic/data/$sbid/$scanid/$bf ];
    then
	ln -s $ff /fred/oz451/azic/data/$sbid/$scanid/
    fi
done
