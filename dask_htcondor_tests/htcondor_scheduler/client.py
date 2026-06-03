"""
Dask client — runs on the submit node.

Usage: python client.py <scheduler_address>
Example: python client.py tcp://submit.example.org:8786

Connects to a running Dask scheduler, submits the pi estimation tasks,
and prints results. The scheduler address is printed by scheduler_job.py
at startup.
"""
import subprocess
import sys

import numpy as np
from distributed import Client


def estimate_pi(n_samples: int, seed: int) -> float:
    rng = np.random.default_rng(seed)
    x, y = rng.uniform(-1, 1, n_samples), rng.uniform(-1, 1, n_samples)
    return float(np.sum(x**2 + y**2 < 1.0))


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <scheduler_address>, e.g., 'tcp://127.0.1.1:24065'")
        sys.exit(1)

    scheduler_address = sys.argv[1]
    # scheduler_address = "tcp://127.0.1.1:24065"
    print(f"Scheduler address: {scheduler_address}")

    with Client(scheduler_address) as client:
        # client.wait_for_workers(n_workers=1, timeout=120)
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


if __name__ == "__main__":
    main()
