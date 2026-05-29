"""
Dask scheduler — runs inside the Apptainer container on a compute node.

After the scheduler is up it submits the worker jobs to HTCondor directly,
so the submit node only needs to run client.py.
"""
import asyncio
import pathlib
import socket
import subprocess
import tempfile

from distributed import Scheduler

# --- configuration -----------------------------------------------------------

CONTAINER_IMAGE  = "/scratch/hpcdat/containers/dask_hello_world.sif"
CONTAINER_PYTHON = "/usr/local/bin/python3"
SCHEDULER_FILE   = "/scratch/hpcdat/dask-scheduler.json"
N_WORKERS        = 4

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent

# -----------------------------------------------------------------------------


def submit_workers(scheduler_address: str) -> None:
    submit_text = f"""\
universe          = vanilla
executable        = {CONTAINER_PYTHON}
arguments         = {SCRIPT_DIR}/worker_job.py {scheduler_address}
+SingularityImage = "{CONTAINER_IMAGE}"
request_cpus      = 1
request_memory    = 2GB
request_disk      = 1GB
initialdir        = {SCRIPT_DIR}
log               = logs/worker.log
output            = logs/worker-$(Process).out
error             = logs/worker-$(Process).err
queue {N_WORKERS}
"""
    with tempfile.NamedTemporaryFile("w", suffix=".sub", delete=False) as f:
        f.write(submit_text)
        path = f.name

    result = subprocess.run(
        ["condor_submit", path], capture_output=True, text=True, check=True
    )
    print(result.stdout.strip(), flush=True)


async def run() -> None:
    async with Scheduler(host=socket.gethostname(), port=8786) as scheduler:
        pathlib.Path(SCHEDULER_FILE).write_text(scheduler.address)
        print(f"Scheduler listening at {scheduler.address}", flush=True)

        print(f"Submitting {N_WORKERS} worker jobs...", flush=True)
        submit_workers(scheduler.address)

        await scheduler.finished()


if __name__ == "__main__":
    asyncio.run(run())
