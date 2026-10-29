#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=06:00:00
#SBATCH --partition=normal
#SBATCH --output=.jupyterdask/%x-%j.out
##SBATCH --reservation=[reservation_name]
# The line above is commented out. To run with a reservation restore by removing the leading hash character and substituting
# the correct reservation name

# Jupyter + Dask on Spider for the Dask/ROOT blueprint, started with the jupyterdask CLI:
#   jupyterdask -i /path/to/ssh/key --template dask_root_blueprint/scripts/jupyterdask-spider.slurm --run <USER>@spider.surf.nl
# Based on scripts/jupyterdask-spider.slurm in the RS-DAT JDS_development_template.

# Image built by .github/workflows/build-dask-root-blueprint.yml: tag "latest" is pushed from main,
# other branches push their branch name (e.g. "dask-root-blueprint") for testing before a merge.
APPTAINER_IMAGE_TAG="latest"
APPTAINER_IMAGE="oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:${APPTAINER_IMAGE_TAG}"
# Recommended: pull the image once (on the UI node) to a local .sif. If that file exists it is used instead of
# the registry, so the Jupyter job and every Dask worker job start fast and don't each download from ghcr.io
# (a cold download takes longer than jupyterdask's default 120 s start-up timeout):
#   apptainer pull ~/dask-root-blueprint.sif oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:latest
# Re-run the pull (with --force) after a new image has been built.
LOCAL_IMAGE="${HOME}/dask-root-blueprint.sif"
if [ -f "${LOCAL_IMAGE}" ]; then
  APPTAINER_IMAGE="${LOCAL_IMAGE}"
fi

# Bind all paths from host system within the container
_DIRS=`/usr/bin/ls -1 / | /usr/bin/awk '!/dev/' | /usr/bin/sed 's/^/\//g' `
export APPTAINER_BIND=`echo ${_DIRS} | /usr/bin/sed 's/ /,/g' `

# Use Python interpreter from the container image
PYTHON="apptainer run ${APPTAINER_IMAGE} python"

# Configure default Dask cluster settings (used by SLURMCluster() in the notebook and by the Dask sidebar)
export DASK_DISTRIBUTED__DASHBOARD__LINK="/proxy/{port}/status"
export DASK_LABEXTENSION__FACTORY__MODULE="dask_jobqueue"
export DASK_LABEXTENSION__FACTORY__CLASS="SLURMCluster"
export DASK_JOBQUEUE__SLURM__DEATH_TIMEOUT=60
export DASK_JOBQUEUE__SLURM__PYTHON=${PYTHON}
# Worker logs: absolute path, because dask-jobqueue submits worker jobs from the notebook kernel's working
# directory (the notebook's folder), so a relative path would put them next to the notebook instead.
# run without reservation
export DASK_JOBQUEUE__SLURM__JOB_EXTRA_DIRECTIVES="['--output', '${HOME}/.jupyterdask/%x-%j.out']"
# run with reservation
#export DASK_JOBQUEUE__SLURM__JOB_EXTRA_DIRECTIVES="['--output', '${HOME}/.jupyterdask/%x-%j.out', '--reservation', '[reservation_name]']"
# Dask workers per Slurm job: distributed RDataFrame works best with single-threaded worker processes,
# so PROCESSES = CORES (here 4 workers of 1 thread and 4 GiB each per job).
export DASK_JOBQUEUE__SLURM__PROCESSES=4
export DASK_JOBQUEUE__SLURM__CORES=4
export DASK_JOBQUEUE__SLURM__MEMORY="16GiB"
export DASK_JOBQUEUE__SLURM__WALLTIME="02:00:00"
export DASK_JOBQUEUE__SLURM__QUEUE="normal"
export DASK_JOBQUEUE__SLURM__LOCAL_DIRECTORY="\$TMPDIR"

# Configure dCache access (only needed to read data from dCache with dcachefs)
export FSSPEC_DCACHE_API_URL="https://dcacheview.grid.surfsara.nl:22880/api/v1"
export FSSPEC_DCACHE_WEBDAV_URL="https://webdav.grid.surfsara.nl:2880"
# Do not commit a real token: set it in a local copy of this script.
export FSSPEC_DCACHE_TOKEN="<MACAROON>"

# Start the Jupyter server
${PYTHON} \
  -m jupyterlab \
  --no-browser \
  --port=`shuf -i 8400-9400 -n 1` \
  --ip=`hostname -s`
