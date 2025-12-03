#!/bin/bash

sbatch --array=0-$((N-1)) --export=ALL,MS_LIST_FILE=mslist_noavg.txt,FLAG_SCRIPT=$( realpath flag.sh ),COLUMN=DATA,script_dir=$PWD run_flag.sh
