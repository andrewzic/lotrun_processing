#!/bin/bash

msfile=$1
column=$2
script_dir=$3

if [ -z $column ]
then
    column="CORRECTED"
fi

aoflagger -column $column -strategy $script_dir/aoflagger/ASKAP.lua -v $msfile
