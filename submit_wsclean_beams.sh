#!/bin/bash

sbatch --array=0-36 --job-name=wsclean_ms --output=logs/wsclean_%A_%a.out --error=logs/wsclean_%A_%a.err --export=ALL,SBID=SB77974,DATA_ROOT=/fred/oz451/azic/data,PATTERN="*beam{beam:02d}*.avg.calB0.ms",IMG_TAG="initial",FLINT_WSCLEAN_SIF=/fred/oz451/azic/containers/flint-containers_wsclean.sif,BIND_SRC=/fred/oz451 run_wsclean_beams.sh

#WSCLEAN_OPTS="-save-source-list -multiscale -multiscale-scale-bias 0.7 -niter 100000 -pol xx -weight briggs 0.5 -scale 20asec -size 500 500 -auto-threshold 1 -auto-mask 5"
