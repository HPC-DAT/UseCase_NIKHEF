"""
Dask client — runs on the submit node.

Waits for the scheduler (started via `condor_submit dask.sub`) to write its
address to SCHEDULER_FILE, connects, runs the computation, then writes
STOP_FILE to signal the scheduler job to shut down.
"""
import pathlib
import time

import numpy as np
from distributed import Client

SCHEDULER_FILE = "/scratch/hpcdat/dask-scheduler.json"
STOP_FILE      = "/scratch/hpcdat/dask-stop"


def estimate_pi(n_samples: int, seed: int) -> float:
    rng = np.random.default_rng(seed)
    x, y = rng.uniform(-1, 1, n_samples), rng.uniform(-1, 1, n_samples)
    return float(np.sum(x**2 + y**2 < 1.0))


def main() -> None:
    scheduler_path = pathlib.Path(SCHEDULER_FILE)

    print(f"Waiting for scheduler address file ({SCHEDULER_FILE})...")
    for _ in range(120):
        if scheduler_path.exists():
            break
        time.sleep(1)
    else:
        raise RuntimeError("Scheduler did not appear within 120 s — check logs/scheduler.err")

    scheduler_address = scheduler_path.read_text().strip()
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

    # Signal the scheduler job to shut down
    pathlib.Path(STOP_FILE).touch()


if __name__ == "__main__":
    main()
