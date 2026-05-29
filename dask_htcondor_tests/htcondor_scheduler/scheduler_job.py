"""
Dask scheduler — runs inside the Apptainer container on a compute node.

Uses HTCondorCluster to submit and manage worker jobs. No shared filesystem
is required: HTCondorCluster's generated worker script is transferred to each
execute node via HTCondor file transfer (should_transfer_files = yes).

Reports the scheduler address back to the submit node via condor_chirp so
that client.py can connect without a shared filesystem.
"""
import signal
import socket
import subprocess
import sys
import time

from dask_jobqueue import HTCondorCluster

# --- configuration -----------------------------------------------------------

CONTAINER_IMAGE = "/scratch/hpcdat/containers/dask_hello_world.sif"
N_WORKERS       = 4

# -----------------------------------------------------------------------------


def main() -> None:
    cluster = HTCondorCluster(
        cores=1,
        memory="2GB",
        disk="1GB",
        death_timeout=60,
        # sys.executable is the container's Python; workers run the same image
        python=sys.executable,
        scheduler_options={"host": socket.gethostname()},
        job_extra_directives={
            "+SingularityImage": f'"{CONTAINER_IMAGE}"',
            "request_cpus": "1",
            # Transfer the generated worker script to each execute node so no
            # shared filesystem is needed between the scheduler and workers.
            "should_transfer_files": "yes",
            "when_to_transfer_output": "on_exit",
        },
        worker_extra_args=["--nthreads", "1"],
    )
    cluster.scale(N_WORKERS)

    # Write address into the job's ClassAd so client.py can read it with
    # condor_q <cluster_id> -json without a shared filesystem.
    subprocess.run(
        ["condor_chirp", "set_job_attr",
         "DaskSchedulerAddress", f'"{cluster.scheduler_address}"'],
        check=True,
    )
    print(f"Scheduler : {cluster.scheduler_address}", flush=True)
    print(f"Dashboard : {cluster.dashboard_link}", flush=True)
    print("Running — send condor_rm to shut down.", flush=True)

    stop = False

    def _on_sigterm(*_: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        while not stop:
            time.sleep(5)
    finally:
        cluster.close()


if __name__ == "__main__":
    main()
