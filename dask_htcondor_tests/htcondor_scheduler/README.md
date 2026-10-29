# Dask HTCondor — Scheduler on Submit Node

The Dask **scheduler** runs directly on the submit node inside the Apptainer container.
It uses `HTCondorCluster` to submit **worker** jobs to the cluster.
A separate `client.py` connects to the scheduler and submits the computation.

```text
Submit node                              Compute nodes
┌──────────────────────────────────┐    ┌──────────────────┐
│  python scheduler_job.py         │───▶│  Dask worker     │
│  → starts scheduler              │    │  (inside .sif)   │
│  → submits workers via           │    └──────────────────┘
│    HTCondorCluster               │
│  → prints scheduler address      │
│                                  │
│  python client.py tcp://…:port   │
│  → connects to scheduler         │
│  → submits tasks                 │
└──────────────────────────────────┘
```

## Prerequisites

- Python 3.8+ on the submit node
- Apptainer on the submit node
- HTCondor cluster with Apptainer support on compute nodes
- `condor_submit` available on the submit node
- A shared filesystem (e.g. `/scratch`) accessible from all nodes

## 1. Build the container image

Container definitions are in [`../containers/`](../containers/).

```bash
cd ../containers
apptainer build dask_htcondor.sif dask_htcondor.def
mkdir -p /scratch/hpcdat/containers/
cp dask_htcondor.sif /scratch/hpcdat/containers/
```

If you use a different path, update `CONTAINER_IMAGE` in `scheduler_job.py`:

```python
CONTAINER_IMAGE = "/scratch/hpcdat/containers/dask_htcondor.sif"
```

## 2. Set up the submit-node environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Run

**Terminal 1** — start the scheduler (runs inside the container on the submit node):

```bash
source .venv/bin/activate
apptainer run /scratch/hpcdat/containers/dask_htcondor.sif python scheduler_job.py
```

The scheduler prints its address once it is ready:

```text
Scheduler : tcp://submit.example.org:8786
Dashboard : http://submit.example.org:8787/status
Running — send condor_rm to shut down.
```

**Terminal 2** — connect and run the computation:

```bash
source .venv/bin/activate
python client.py tcp://submit.example.org:8786
```

Example output:

```text
Scheduler address: tcp://submit.example.org:8786
Connected workers: 4
Dashboard:         http://submit.example.org:8787/status

Total samples : 20,000,000
Pi estimate   : 3.141732
Reference     : 3.141593
Error         : 0.000139
```

Stop the scheduler when done:

```bash
# Press Ctrl-C in Terminal 1, or send SIGTERM
```

## Running the scheduler as an HTCondor job (experimental)

`dask.sub` submits `scheduler_job.py` as an HTCondor job so the scheduler runs on a
compute node instead of the submit node. This requires the container to have access
to HTCondor config files and binaries via bind mounts, which is not yet supported on
this cluster.

When it becomes available, the workflow will be:

```bash
# Submit the scheduler job
condor_submit dask.sub

# Wait for the address to appear in the log
grep "Scheduler :" scheduler.out

# Connect the client
python client.py tcp://<address>
```

## Deactivate the environment

```bash
deactivate
```
