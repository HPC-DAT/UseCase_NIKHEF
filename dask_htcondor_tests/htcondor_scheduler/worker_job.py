"""Dask worker — runs inside the Apptainer container on a compute node."""
import asyncio
import sys

from distributed import Worker


async def run() -> None:
    scheduler_address = sys.argv[1]
    async with Worker(scheduler_address, nthreads=1) as worker:
        print(f"Worker {worker.address} connected to {scheduler_address}", flush=True)
        await worker.finished()


if __name__ == "__main__":
    asyncio.run(run())
