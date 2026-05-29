# Dask HTCondor — Scheduler on HTCondor

The Dask **scheduler** runs as an HTCondor job inside the Apptainer container.
Once the scheduler is up, it submits the **worker** jobs itself — the submit node
only needs to act as a lightweight Dask client.

```text
Submit node            Compute node A              Compute nodes B–E
┌─────────────┐        ┌──────────────────────┐    ┌──────────────────┐
│  dask.sub   │──────▶ │  scheduler_job.py    │───▶│  worker_job.py   │
│             │condor  │  1. start scheduler  │    │  (Dask workers)  │
│  client.py  │◀──┐    │  2. condor_submit    │    └──────────────────┘
│  (client)   │   │    │     workers          │
└─────────────┘   └────│  3. write address    │
                       └──────────────────────┘
                         /scratch/.../dask-scheduler.json
```

## Prerequisites

Same as `hello_world`:

- Apptainer on the submit node
- HTCondor cluster with Apptainer support on compute nodes
- `condor_submit` accessible inside the container (see note below)
- Shared filesystem (`/scratch`) accessible by all nodes

> **condor_submit inside the container**
> Most clusters bind-mount `/usr` into Apptainer containers automatically, making
> `condor_submit` available. If yours does not, uncomment and adjust the
> `+SingularityBindPath` line in `dask.sub`.

## 1. Build / reuse the container image

Reuses the image from `hello_world`. If you haven't built it yet:

```bash
apptainer build ../hello_world/dask_hello_world.sif ../hello_world/dask_hello_world.def
mkdir -p /scratch/hpcdat/containers/
cp ../hello_world/dask_hello_world.sif /scratch/hpcdat/containers/
```

## 2. Edit `dask.sub`

Set `initialdir` to the absolute path of this directory on your cluster:

```ini
initialdir = /scratch/hpcdat/dask_htcondor_tests/htcondor_scheduler
```

Also verify `CONTAINER_IMAGE` and `SCHEDULER_FILE` in `scheduler_job.py` match your paths.

## 3. Set up the submit-node environment

```bash
# Make the wrapper scripts executable (only needed once after cloning)
chmod +x run_scheduler.sh run_worker.sh

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Run

In one terminal, submit the scheduler (which will submit workers itself):

```bash
mkdir -p logs
condor_submit dask.sub
```

In a second terminal, run the client once the scheduler is up:

```bash
source .venv/bin/activate
python client.py
```

`client.py` polls for the scheduler address file and connects automatically.

Example output:

```text
Waiting for scheduler address file (/scratch/hpcdat/dask-scheduler.json)...
Scheduler address: tcp://10.0.0.42:8786
Connected workers: 4
Dashboard:         http://10.0.0.42:8787/status

Total samples : 20,000,000
Pi estimate   : 3.141732
Reference     : 3.141593
Error         : 0.000139
```

HTCondor logs are written to `logs/`.

## Key difference from previous example

| | `htcondor_scheduler` (previous) | `htcondor_scheduler` (this) |
| --- | --- | --- |
| Who submits workers | `submit.py` on the submit node | `scheduler_job.py` on the compute node |
| Submit node role | Submits scheduler + workers + client | Submits scheduler only + client |
| Entry point | `python submit.py` | `condor_submit dask.sub` + `python client.py` |

## Deactivate the environment

```bash
deactivate
```
