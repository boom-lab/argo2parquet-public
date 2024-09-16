#!/bin/bash
#SBATCH --job-name=argo_conv
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=enrico.milanese@whoi.edu
#SBATCH --nodes=1            # Number of nodes
#SBATCH --ntasks=18
#SBATCH --mem=100G
#SBATCH --time=01:00:00      # Walltime in hh:mm:ss or d-hh:mm:ss
#SBATCH --output=/vortexfs1/share/boom/users/enrico.milanese/nc2parquet/slurm-logs/slurm-%j.out
#SBATCH --error=/vortexfs1/share/boom/users/enrico.milanese/nc2parquet/slurm-logs/slurm-%j.err

date;hostname

echo ""

source /vortexfs1/share/boom/users/enrico.milanese/nc2parquet/venv/3.9/bin/activate

# -u to force unbuffered output
python3 -u /vortexfs1/share/boom/users/enrico.milanese/nc2parquet/main.py --db phy -d false

echo ""

date
