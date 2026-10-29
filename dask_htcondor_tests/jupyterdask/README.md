# JupyterLab + Dask

Runs JupyterLab inside the `dask_htcondor.sif` container on the submit node.
The Dask labextension is pre-configured to create `HTCondorCluster` instances,
so you can start and scale a Dask cluster directly from the notebook interface.

## Prerequisites

- Apptainer on the submit node
- `dask_htcondor.sif` built and placed at `/scratch/hpcdat/containers/dask_htcondor.sif`
  (see [`../containers/`](../containers/))
- HTCondor cluster accessible from the submit node

## Run

```bash
bash jupyter.sh
```

JupyterLab starts on port 8888. Access it at `http://<submit-node>:8888/` or via an
SSH tunnel if connecting from outside the cluster:

```bash
ssh -L 8888:localhost:8888 <submit-node>
```

## What the script configures

| Variable | Value | Effect |
| --- | --- | --- |
| `DASK_LABEXTENSION__FACTORY__MODULE` | `dask_jobqueue` | Dask panel uses dask-jobqueue |
| `DASK_LABEXTENSION__FACTORY__CLASS` | `HTCondorCluster` | New clusters are HTCondor clusters |
| `DASK_DISTRIBUTED__DASHBOARD__LINK` | `/proxy/{port}/status` | Dashboard URL works through a Jupyter proxy |
| `APPTAINER_BIND` | `/etc/condor` | HTCondor config inside the container |
| `DASK_JOBQUEUE__HTCONDOR__*` | see script | Default worker resources (1 core, 1 GB) |

## Modifying default worker resources

Edit `jupyter.sh` and adjust the `DASK_JOBQUEUE__HTCONDOR__*` environment variables
before launching. These values become the defaults shown in the Dask labextension panel
and can be overridden per-cluster in your notebook.
