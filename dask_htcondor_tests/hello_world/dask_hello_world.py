"""
Simple Dask workflow using HTCondorCluster.

Runs a Monte Carlo pi estimation distributed across HTCondor workers.
"""
import dask
import dask.array as da
import numpy as np
from dask_jobqueue import HTCondorCluster


def estimate_pi(n_samples: int, seed: int) -> float:
    rng = np.random.default_rng(seed)
    x, y = rng.uniform(-1, 1, n_samples), rng.uniform(-1, 1, n_samples)
    return float(np.sum(x**2 + y**2 < 1.0))


def main():
    cluster = HTCondorCluster(
        cores=1,
        memory="2GB",
        disk="1GB",
        # Worker lifetime: kill if scheduler unreachable for 60s
        death_timeout=60,
        job_extra_directives={
            "universe": "vanilla",
            "request_cpus": "1",
        },
        worker_extra_args=["--nthreads", "1"],
    )

    # Scale to 4 workers; adapt if needed: cluster.adapt(minimum=1, maximum=10)
    cluster.scale(4)

    print(f"Dashboard: {cluster.dashboard_link}")
    print("Waiting for workers...")

    with cluster, dask.distributed.Client(cluster) as client:
        client.wait_for_workers(n_workers=1, timeout=120)
        print(f"Connected workers: {len(client.scheduler_info()['workers'])}")

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

        print(f"\nTotal samples : {total_samples:,}")
        print(f"Pi estimate   : {pi_estimate:.6f}")
        print(f"Reference     : {np.pi:.6f}")
        print(f"Error         : {abs(pi_estimate - np.pi):.6f}")


if __name__ == "__main__":
    import dask.distributed  # noqa: F401 — ensure distributed is imported

    main()
