"""
Dask client — runs on the submit node.

Usage: python client.py <scheduler_cluster_id>

Polls condor_q for the DaskSchedulerAddress ClassAd attribute written by the
scheduler job via condor_chirp. Runs the computation, then calls condor_rm to
shut the scheduler (and its workers) down.
"""
import json
import subprocess
import sys
import time

import numpy as np
from distributed import Client


def get_scheduler_address(cluster_id: str) -> str:
    print(f"Waiting for scheduler address (cluster {cluster_id})...")
    for _ in range(120):
        result = subprocess.run(
            ["condor_q", cluster_id, "-json"],
            capture_output=True, text=True,
        )
        jobs = json.loads(result.stdout or "[]")
        if jobs and "DaskSchedulerAddress" in jobs[0]:
            return jobs[0]["DaskSchedulerAddress"].strip('"')
        time.sleep(2)
    raise RuntimeError(
        f"Scheduler (cluster {cluster_id}) did not report its address within 120 s"
    )


def estimate_pi(n_samples: int, seed: int) -> float:
    rng = np.random.default_rng(seed)
    x, y = rng.uniform(-1, 1, n_samples), rng.uniform(-1, 1, n_samples)
    return float(np.sum(x**2 + y**2 < 1.0))


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <scheduler_cluster_id>")
        sys.exit(1)

    cluster_id = sys.argv[1]
    scheduler_address = get_scheduler_address(cluster_id)
    print(f"Scheduler address: {scheduler_address}")

    with Client(scheduler_address) as client:
        client.wait_for_workers(n_workers=1, timeout=120)
        print(f"Connected workers: {len(client.scheduler_info()['workers'])}")
        print(f"Dashboard:         {client.dashboard_link}\n")

        n_tasks = 20
        samples_per_task = 1_000_000

        futures = [
            client.submit(estimate_pi, samples_per_task, seed=i)
            for i in range(n_tasks)
        ]
        hits = client.gather(futures)

    total_hits = sum(hits)
    total_samples = n_tasks * samples_per_task
    pi_estimate = 4.0 * total_hits / total_samples

    print(f"Total samples : {total_samples:,}")
    print(f"Pi estimate   : {pi_estimate:.6f}")
    print(f"Reference     : {np.pi:.6f}")
    print(f"Error         : {abs(pi_estimate - np.pi):.6f}")

    print(f"\nShutting down scheduler job {cluster_id}...")
    subprocess.run(["condor_rm", cluster_id], check=True)


if __name__ == "__main__":
    main()
