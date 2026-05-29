# Dask HTCondor Hello World

Estimates π using a Monte Carlo method distributed across HTCondor workers via `dask-jobqueue`.
Workers run inside an Apptainer container so the environment is self-contained and independent of the compute nodes.

## Prerequisites

- Python 3.8+ on the submit node
- Apptainer on the submit node (to build the image)
- Access to an HTCondor cluster with Apptainer support on the worker nodes
- A shared filesystem (e.g. `/scratch`) accessible from both the submit node and worker nodes

## 1. Build the container image

```bash
apptainer build dask_hello_world.sif dask_hello_world.def
```

Copy the image to a location reachable by all worker nodes, then update `CONTAINER_IMAGE` in `dask_hello_world.py` to match:

```{bash}
mkdir -p /scratch/hpcdat/containers/
cp dask_hello_world.sif /scratch/hpcdat/containers/
```

```python
CONTAINER_IMAGE = "/scratch/hpcdat/containers/dask_hello_world.sif"
```

## 2. Set up the submit-node environment

The venv is only needed to run the submit-side script (scheduler + client). Workers use the container.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Run

```bash
python dask_hello_world.py
```

The script will:

1. Start a Dask scheduler on the submit node bound to its hostname
2. Submit 4 HTCondor jobs — each worker runs inside `dask_hello_world.sif`
3. Distribute 20 tasks across those workers (1 million random samples each)
4. Print the π estimate and error when all tasks complete

Example output:
```
Dashboard: http://194.171.96.60:8787/status
Waiting for workers...
Connected workers: 4

Total samples : 20,000,000
Pi estimate   : 3.141732
Reference     : 3.141593
Error         : 0.000139
```

## Notes

- The Dask dashboard is available at the URL printed at startup while the script is running.
- Workers are automatically shut down when the `with` block exits.
- To use adaptive scaling instead of a fixed 4 workers, replace `cluster.scale(4)` with `cluster.adapt(minimum=1, maximum=10)` in `dask_hello_world.py`.

## Deactivate the submit-node environment

```bash
deactivate
```
