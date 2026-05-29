"""
Dask scheduler — runs inside the Apptainer container on a compute node.

Uses HTCondorCluster to submit and manage worker jobs, so no manual
condor_submit calls are needed. Writes the scheduler address to a shared
file for client.py to read, then waits until client.py signals it to stop.
"""
import pathlib
import socket
import sys
import time

from dask_jobqueue import HTCondorCluster

# --- configuration -----------------------------------------------------------

CONTAINER_IMAGE = "/scratch/hpcdat/containers/dask_hello_world.sif"
SCHEDULER_FILE  = "/scratch/hpcdat/dask-scheduler.json"
STOP_FILE       = "/scratch/hpcdat/dask-stop"
N_WORKERS       = 4

# -----------------------------------------------------------------------------


def main() -> None:
    cluster = HTCondorCluster(
        cores=1,
        memory="2GB",
        disk="1GB",
        death_timeout=60,
        # sys.executable is the container's Python — workers run the same image
        python=sys.executable,
        scheduler_options={"host": socket.gethostname()},
        job_extra_directives={
            "+SingularityImage": f'"{CONTAINER_IMAGE}"',
            "request_cpus": "1",
        },
        worker_extra_args=["--nthreads", "1"],
    )
    cluster.scale(N_WORKERS)

    pathlib.Path(SCHEDULER_FILE).write_text(cluster.scheduler_address)
    print(f"Scheduler : {cluster.scheduler_address}", flush=True)
    print(f"Dashboard : {cluster.dashboard_link}", flush=True)
    print(f"Waiting for stop signal ({STOP_FILE})...", flush=True)

    stop = pathlib.Path(STOP_FILE)
    stop.unlink(missing_ok=True)
    try:
        while not stop.exists():
            time.sleep(5)
    finally:
        cluster.close()
        pathlib.Path(SCHEDULER_FILE).unlink(missing_ok=True)
        stop.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
