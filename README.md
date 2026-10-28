# UseCase_NIKHEF

Dask distributed computing on an HTCondor cluster, developed for the NIKHEF (High Energy Physics) use case.

## Repository layout

```text
dask_htcondor_tests/
├── containers/          # Apptainer container definitions
├── hello_world/         # Simplest example: scheduler on submit node, workers on HTCondor
├── htcondor_scheduler/  # Scheduler on submit node, workers submitted via HTCondorCluster
├── jupyterdask/         # JupyterLab + Dask labextension inside the container
└── debug/               # Interactive HTCondor job for debugging inside the container
```

## Examples

| Example | Where scheduler runs | How workers start |
| --- | --- | --- |
| [`hello_world`](dask_htcondor_tests/hello_world/) | Submit node | HTCondorCluster (direct) |
| [`htcondor_scheduler`](dask_htcondor_tests/htcondor_scheduler/) | Submit node | HTCondorCluster (separate client) |
| [`jupyterdask`](dask_htcondor_tests/jupyterdask/) | Submit node (JupyterLab) | HTCondorCluster (via Dask labextension) |

## Container images

Container definitions live in [`containers/`](dask_htcondor_tests/containers/). Build once and copy to a shared path (e.g. `/scratch/hpcdat/containers/`) accessible from all nodes.

| Image | Purpose |
| --- | --- |
| `dask_hello_world.sif` | Lightweight worker image (Dask only) |
| `dask_htcondor.sif` | Full image: Dask + HTCondor tools + JupyterLab |

## Code generation

Developed with code assistance from Claude Sonnet 4.6, Opus 4.7 and 4.8.
