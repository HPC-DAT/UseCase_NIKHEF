# Container images

Apptainer container definitions for Dask HTCondor jobs.
Build once, copy to a shared filesystem path accessible from all compute nodes.

## Images

### `dask_hello_world.def` → `dask_hello_world.sif`

Minimal worker image based on `python:3.12-slim`.
Contains only Dask, dask-jobqueue, and NumPy — no HTCondor tools or JupyterLab.
Use this for the [`hello_world`](../hello_world/) example.

### `dask_htcondor.def` → `dask_htcondor.sif`

Full image based on AlmaLinux 9 + Miniforge.
Contains:

- Python 3.12 (conda-forge, GLIBC 2.17 compatible)
- Dask + dask-jobqueue + dask-labextension
- HTCondor tools (`condor_submit`, `condor_rm`, `condor_chirp`)
- JupyterLab

Use this for [`htcondor_scheduler`](../htcondor_scheduler/) and [`jupyterdask`](../jupyterdask/).

> **GLIBC compatibility** — conda-forge packages target GLIBC 2.17, so the container
> runs on execute nodes with older system libraries without issues.

## Building

```bash
# From the containers/ directory
apptainer build dask_hello_world.sif dask_hello_world.def
apptainer build dask_htcondor.sif dask_htcondor.def
```

Then copy to the shared location expected by the examples:

```bash
mkdir -p /scratch/hpcdat/containers/
cp dask_hello_world.sif dask_htcondor.sif /scratch/hpcdat/containers/
```

If you use a different path, update `CONTAINER_IMAGE` in each example's Python script and the `+SingularityImage` line in `dask.sub` / `interactive.sub`.
