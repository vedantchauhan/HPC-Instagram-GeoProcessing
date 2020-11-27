#!/bin/sh
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=01:00:00
#SBATCH --partition=physical
module load Java/1.8.0_71
module load mpj/0.44
mpjrun.sh -np 8 HPC_GeoProcessing