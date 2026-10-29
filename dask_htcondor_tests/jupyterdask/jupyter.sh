#!/bin/bash

APPTAINER_IMAGE=/scratch/hpcdat/containers/dask_htcondor.sif

export DASK_DISTRIBUTED__DASHBOARD__LINK="/proxy/{port}/status"
export DASK_LABEXTENSION__FACTORY__MODULE="dask_jobqueue"
export DASK_LABEXTENSION__FACTORY__CLASS="HTCondorCluster"

export DASK_JOBQUEUE__HTCONDOR__DEATH_TIMEOUT=60
export DASK_JOBQUEUE__HTCONDOR__PYTHON="python"
export DASK_JOBQUEUE__HTCONDOR__PROCESSES=1
export DASK_JOBQUEUE__HTCONDOR__CORES=1
export DASK_JOBQUEUE__HTCONDOR__MEMORY="1GB"
export DASK_JOBQUEUE__HTCONDOR__DISK="1GB"
export DASK_JOBQUEUE__HTCONDOR__WALLTIME="01:00:00"
export DASK_JOBQUEUE__HTCONDOR__LOCAL_DIRECTORY="/tmp"
export DASK_JOBQUEUE__HTCONDOR__LOG_DIRECTORY="/tmp"
export DASK_JOBQUEUE__HTCONDOR__JOB_EXTRA_DIRECTIVES="{'+SingularityImage': '\"${APPTAINER_IMAGE}\"', 'Requirements': 'HasSingularity'}"

# Cannot bind all dirs because of the condor libraries in dask_htcondor.sif, and this is a bit cleaner
export APPTAINER_BIND="/etc/condor"


apptainer run ${APPTAINER_IMAGE} python3 -m jupyterlab \
  --no-browser \
  --port=8888 \
  --ip=0.0.0.0
