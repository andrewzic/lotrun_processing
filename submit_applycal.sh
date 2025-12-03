#!/bin/bash

sbatch --array=0-36 --job-name=applycal_ms --cpus-per-task=4 --output=logs/applycal_%A_%a.out --error=logs/applycal_%A_%a.err --export=ALL,SBID=SB77974,DATA_ROOT=/fred/oz451/azic/data,PATTERN="*beam{beam:02d}.avg.ms",CAL_DIR=cal,SCRIPT=applycal_ms_beams.py,CASA_SIF=/fred/oz451/azic/containers/flint-containers_casa.sif,BIND_SRC=/fred/oz451,PYTHON="apptainer exec --bind /fred/oz451:/fred/oz451 /fred/oz451/azic/containers/flint-containers_casa.sif python3" run_applycal_beams.sh
