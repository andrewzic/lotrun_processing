#!/bin/bash

sbatch --array=0-36 --job-name=concat_ms --time=04:00:00 --cpus-per-task=4 --mem=16G --output=logs/concat_%A_%a.out --error=logs/concat_%A_%a.err --export=ALL,SBID=SB77974,DATA_ROOT=/fred/oz451/azic/data,OUT_ROOT=/fred/oz451/azic/data,SCRIPT=concat_ms_beams.py,PYTHON="apptainer exec --bind /fred/oz451:/fred/oz451 /fred/oz451/azic/containers/flint-containers_casa.sif python3" run_concat_beams.sh 
