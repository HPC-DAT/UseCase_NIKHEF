"""Generate notebooks/02_coffea_dask.ipynb (kept as a script so the notebook stays reviewable).

  python tools/build_coffea_notebook.py [OUTPUT]   (default: notebooks/02_coffea_dask.ipynb next to this folder)
"""
import sys
from pathlib import Path
import nbformat as nbf

md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell
cells = [
md("""# AGC ttbar analysis with coffea on Dask

This notebook runs the CMS ttbar analysis of the [Analysis Grand Challenge](https://agc.readthedocs.io) (AGC)
with **coffea**, the Python HEP stack built on [uproot](https://uproot.readthedocs.io) (reading ROOT files without
ROOT), [awkward](https://awkward-array.org) (arrays of variable-length lists, like jets per event) and
[hist](https://hist.readthedocs.io), distributed over a **Dask** cluster. It is the same analysis as
`01_rdataframe_dask.ipynb`, with the same inputs, histograms and validation, so you can compare the two approaches.
The same notebook runs

- **on Spider or Snellius with RS-DAT** (the default), where the Dask workers are Slurm jobs and the input files are
  read directly from the AGC storage at CERN; and
- **locally**, on a Dask `LocalCluster` and a small skim of one input file, to develop and test.

Only the cluster and the input list change between the two; the analysis code is the same.

**What the analysis does.** Events with exactly one tight electron or muon and at least four jets are split into two
regions: exactly one b-tagged jet (`4j1b`, histogram of $H_T$) and at least two (`4j2b`, histogram of the mass of the
trijet with the highest $p_T$). For nominal samples, jet energy scale and resolution variations and b-tagging weight
variations are computed in the same pass over the data.

**How it is distributed.** The analysis is a coffea *processor* (`AGCProcessor` in `../agc/coffea_analysis.py`):
its `process` method turns one chunk of events into histograms, written with NanoEvents (`events.Jet.pt`,
`events.Electron`, ...) and awkward array operations. A coffea `Runner` splits the input files into chunks of
`CHUNKSIZE` events, runs the processor on every chunk as a Dask task, and adds up the histograms."""),
md("""## 1. Settings

The defaults run on the Slurm cluster (RS-DAT) and read the first AGC file of every sample from CERN EOS.
For local development, set `RUN_ON = "local"` and `DATA = "skim"`."""),
code("""import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))  # the agc/ package next to notebooks/

RUN_ON = "rsdat"  # "rsdat": Dask workers as Slurm jobs (RS-DAT) | "local": Dask LocalCluster on this machine
N_WORKERS = 4      # local: single-threaded worker processes on this machine
N_JOBS = 2         # rsdat: Slurm jobs; each runs several workers (see scripts/jupyterdask-spider.slurm)

DATA = "remote"    # "remote": AGC files read over the network | "skim": one local file you downloaded
SKIM_PATH = Path("~/dev/data/hpc-dat/agc_ttbar_skim_100k.root").expanduser()  # used when DATA == "skim"
N_FILES_PER_SAMPLE = 1  # used when DATA == "remote": first N files of each of the 9 samples
PREFIX = "eos"     # "eos" (root://, CERN EOS public mirror) | "unl" (HTTPS, UNL) | URL prefix of your own copy

CHUNKSIZE = 200_000  # events per chunk; every chunk is one Dask task
OUTPUT = Path("histograms_coffea.root")"""),
md("""## 2. Start a Dask cluster

The same cluster setup as in the RDataFrame notebook.

**Local:** a `LocalCluster` with single-threaded worker processes.

**RS-DAT:** the Jupyter server runs as a Slurm job started with `jupyterdask` and
`scripts/jupyterdask-spider.slurm`. That script sets the `DASK_JOBQUEUE__SLURM__*` defaults (cores, memory,
walltime, and the container image for the workers), which `SLURMCluster` picks up. You can also create the cluster
from the Dask tab in JupyterLab's sidebar and connect to it with `Client("tcp://...")` instead."""),
code("""from distributed import Client

if RUN_ON == "local":
    from distributed import LocalCluster
    cluster = LocalCluster(n_workers=N_WORKERS, threads_per_worker=1, processes=True)
    n_expected = N_WORKERS
elif RUN_ON == "rsdat":
    import dask
    from dask_jobqueue import SLURMCluster
    slurm = dask.config.get("jobqueue.slurm")  # the defaults from the Slurm script
    cluster = SLURMCluster(cores=slurm["cores"], processes=slurm["processes"], memory=slurm["memory"])
    cluster.scale(jobs=N_JOBS)
    n_expected = N_JOBS * (slurm["processes"] or 1)
else:
    raise ValueError(f"unknown RUN_ON: {RUN_ON}")

client = Client(cluster)
client.wait_for_workers(n_expected, timeout=900)  # Slurm jobs can take a while to start
print("Dashboard:", client.dashboard_link)
client"""),
md("""## 3. Choose the inputs

The AGC file list (`agc/data/nanoaod_inputs.json`) has 9 samples: ttbar (nominal and 4 systematic variations),
three single-top channels and W+jets. 691 files, ~3.5 TB in total; the first files of all 9 samples together are ~30 GB.
uproot reads only the branches the processor uses, so far less than that goes over the network.

A local skim is treated as one ttbar sample and normalised by its own number of events. (A skim made with
`tools/make_agc_skim.py --minimal` works too: it keeps the event ID and lepton fields coffea's NanoAOD schema needs.)"""),
code("""from agc.inputs import local_inputs, remote_inputs
from agc.coffea_analysis import fileset

if DATA == "skim":
    inputs = local_inputs(SKIM_PATH)
elif DATA == "remote":
    inputs = remote_inputs(N_FILES_PER_SAMPLE, prefix=PREFIX)
else:
    raise ValueError(f"unknown DATA: {DATA}")

for inp in inputs:
    print(f"{inp.process:18s} {inp.variation:10s} {len(inp.paths):3d} file(s) {inp.nevents:>12,} events")

# coffea describes the inputs as a "fileset": one dataset per sample, with metadata the processor uses
next(iter(fileset(inputs).items()))"""),
md("""## 4. Run the analysis

`run_analysis` hands the fileset and the processor to a coffea `Runner` with a `DaskExecutor`: the files are split
into chunks, each chunk is processed on a worker, and the histograms are added up. Watch the Dask dashboard while it
runs. (The workers only need the container image: the `agc` code is sent along with the processor.)"""),
code("""import time
from agc.coffea_analysis import run_analysis, save_histos

start = time.time()
histos = run_analysis(inputs, client=client, chunksize=CHUNKSIZE)
elapsed = time.time() - start
nevents = sum(inp.nevents for inp in inputs)
print(f"{len(histos)} histograms from {nevents:,} events in {elapsed:.1f} s ({nevents / elapsed:,.0f} events/s)")

save_histos(histos, OUTPUT)
print("Saved to", OUTPUT.resolve())"""),
md("""## 5. Look at the results

The histograms are `hist.Hist` objects: you can plot, slice and combine them directly, or use the ROOT file."""),
code("""from agc.validation import plot_regions

fig = plot_regions(OUTPUT)"""),
code("""for name in ["4j1b_ttbar", "4j2b_ttbar"]:
    print(f"{name:12s} weighted events {histos[name].sum().value:10.1f}")
histos["4j2b_ttbar"]"""),
md("""## 6. Validate against the AGC reference

The AGC publishes reference histograms for the first N files per sample. A run with `DATA = "remote"` and
`N_FILES_PER_SAMPLE` in (1, 2, 5, 10) can be compared directly. A skim cannot: it has fewer events and a different
normalisation. The coffea and RDataFrame versions give identical histograms on the same input, so both notebooks can
be checked against the same reference."""),
code("""from agc.inputs import reference_file
from agc.validation import compared, validate

if DATA == "remote":
    ref = reference_file(N_FILES_PER_SAMPLE)
    errors = validate(OUTPUT, ref)
    print(f"Compared {len(compared(OUTPUT, ref))} histograms with {ref.name}")
    print("All good!" if not errors else "\\n".join(f"{k}: {v}" for k, v in errors.items()))
else:
    print("Skipped: validation needs DATA = 'remote'.")"""),
md("""## 7. Clean up

Close the client and the cluster. On RS-DAT this cancels the worker jobs; the Jupyter job keeps running until you
stop it or its walltime ends."""),
code("""client.close()
cluster.close()"""),
]

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
out = sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parents[1] / "notebooks" / "02_coffea_dask.ipynb"
nbf.write(nb, str(out))
