# Dask HTCondor Hello World

Estimates π using a Monte Carlo method distributed across HTCondor workers via `dask-jobqueue`.

## Prerequisites

- Python 3.8+
- Access to an HTCondor submit node

## Setup

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Run

```bash
python dask_hello_world.py
```

The script will:
1. Submit 4 HTCondor jobs as Dask workers (`vanilla` universe, 1 core, 2 GB RAM each)
2. Distribute 20 tasks across those workers (1 million random samples each)
3. Print the π estimate and error when all tasks complete

Example output:
```
Dashboard: http://localhost:8787/status
Waiting for workers...
Connected workers: 4

Total samples : 20,000,000
Pi estimate   : 3.141732
Reference     : 3.141593
Error         : 0.000139
```

## Notes

- The Dask dashboard is available at `http://localhost:8787` while the script is running.
- Workers are automatically shut down when the `with` block exits.
- To use adaptive scaling instead of a fixed 4 workers, replace `cluster.scale(4)` with `cluster.adapt(minimum=1, maximum=10)` in `dask_hello_world.py`.

## Deactivate the environment

```bash
deactivate
```
