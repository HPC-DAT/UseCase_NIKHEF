"""Generate notebooks/01_rdataframe_dask.ipynb (kept as a script so the notebook stays reviewable).

  python tools/build_notebook.py [OUTPUT]   (default: notebooks/01_rdataframe_dask.ipynb next to this folder)
"""
import sys
from pathlib import Path
import nbformat as nbf

md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell
cells = [
md("""# AGC ttbar analysis with ROOT RDataFrame on Dask

This notebook runs the CMS ttbar analysis of the [Analysis Grand Challenge](https://agc.readthedocs.io) (AGC)
with **ROOT RDataFrame**, distributed over a **Dask** cluster. The same notebook runs

- **locally**, on a Dask `LocalCluster` and a small skim of one input file, to develop and test; and
- **on Spider or Snellius with RS-DAT**, where the Dask workers are Slurm jobs and the input files are read
  directly from the AGC storage.

Only the cluster and the input list change between the two; the analysis code is the same.

**What the analysis does.** Events with exactly one tight electron or muon and at least four jets are split into two
regions: exactly one b-tagged jet (`4j1b`, histogram of $H_T$) and at least two (`4j2b`, histogram of the mass of the
trijet with the highest $p_T$). For nominal samples, jet energy scale and resolution variations and b-tagging weight
variations are computed in the same pass over the data, with RDataFrame's `Vary`.

**How it is distributed.** RDataFrame builds a computation graph per sample; nothing is read until the graphs run.
With an `executor=client` argument, each graph is split into partitions (ranges of entries), every partition becomes
a Dask task, and the partial histograms are merged back in this notebook. The analysis code lives in `../agc/`."""),
md("""## 1. Settings

Change these to switch between local development and a run on the cluster."""),
code("""import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))  # the agc/ package next to notebooks/

RUN_ON = "local"  # "local": Dask LocalCluster on this machine | "rsdat": Dask workers as Slurm jobs (RS-DAT)
N_WORKERS = 4      # local: single-threaded worker processes on this machine
N_JOBS = 2         # rsdat: Slurm jobs; each runs several workers (see scripts/jupyterdask-spider.slurm)

DATA = "skim"      # "skim": one local file | "remote": AGC files read over the network
SKIM_PATH = Path("~/dev/data/hpc-dat/agc_ttbar_skim_100k.root").expanduser()  # used when DATA == "skim"
N_FILES_PER_SAMPLE = 1  # used when DATA == "remote": first N files of each of the 9 samples
PREFIX = "unl"     # "unl" (HTTPS, UNL) or "eos" (root://, CERN EOS), or a URL prefix of your own copy

NPARTITIONS = None  # tasks per sample; None lets ROOT choose (one per worker core)
OUTPUT = Path("histograms.root")"""),
md("""## 2. Start a Dask cluster

**Local:** a `LocalCluster` with single-threaded worker processes (distributed RDataFrame runs one task per worker thread, and works best with one thread per process).

**RS-DAT:** the Jupyter server runs as a Slurm job started with `jupyterdask` and
`scripts/jupyterdask-spider.slurm`. That script sets the `DASK_JOBQUEUE__SLURM__*` defaults (cores, memory,
walltime, and the container image for the workers), so `SLURMCluster()` needs no arguments here.
You can also create the cluster from the Dask tab in JupyterLab's sidebar and connect to it with
`Client("tcp://...")` instead."""),
code("""from distributed import Client

if RUN_ON == "local":
    from distributed import LocalCluster
    cluster = LocalCluster(n_workers=N_WORKERS, threads_per_worker=1, processes=True)
    n_expected = N_WORKERS
elif RUN_ON == "rsdat":
    import dask
    from dask_jobqueue import SLURMCluster
    cluster = SLURMCluster()
    cluster.scale(jobs=N_JOBS)
    n_expected = N_JOBS * (dask.config.get("jobqueue.slurm.processes") or 1)
else:
    raise ValueError(f"unknown RUN_ON: {RUN_ON}")

client = Client(cluster)
client.wait_for_workers(n_expected, timeout=900)  # Slurm jobs can take a while to start
print("Dashboard:", client.dashboard_link)
client"""),
md("""## 3. Choose the inputs

The AGC file list (`agc/data/nanoaod_inputs.json`) has 9 samples: ttbar (nominal and 4 systematic variations),
three single-top channels and W+jets. 691 files, ~3.5 TB in total; the first file of each sample is ~30 GB.
RDataFrame reads only the ~20 branches the analysis needs, so far less than that goes over the network.

A local skim is treated as one ttbar sample and normalised by its own number of events."""),
code("""from agc.inputs import local_inputs, remote_inputs

if DATA == "skim":
    inputs = local_inputs(SKIM_PATH)
elif DATA == "remote":
    inputs = remote_inputs(N_FILES_PER_SAMPLE, prefix=PREFIX)
else:
    raise ValueError(f"unknown DATA: {DATA}")

for inp in inputs:
    print(f"{inp.process:18s} {inp.variation:10s} {len(inp.paths):3d} file(s) {inp.nevents:>12,} events")"""),
md("""## 4. Run the analysis

`run_analysis` books the histograms for every sample, asks for the systematic variations, and then runs all
computation graphs together with `RunGraphs`. Watch the Dask dashboard while it runs."""),
code("""import time
import ROOT
from agc.rdf_analysis import run_analysis, save_histos

ROOT.gROOT.SetBatch(True)
ROOT.TH1.AddDirectory(False)

start = time.time()
histos = run_analysis(inputs, client=client, npartitions=NPARTITIONS)
elapsed = time.time() - start
nevents = sum(inp.nevents for inp in inputs)
print(f"{len(histos)} histograms from {nevents:,} events in {elapsed:.1f} s ({nevents / elapsed:,.0f} events/s)")

save_histos(histos, OUTPUT)
print("Saved to", OUTPUT.resolve())"""),
md("""## 5. Look at the results"""),
code("""from agc.validation import plot_regions

fig = plot_regions(OUTPUT)"""),
code("""for name in ["4j1b_ttbar", "4j2b_ttbar"]:
    h = histos[name]
    print(f"{name:12s} weighted events {h.Integral():10.1f}   raw entries {int(h.GetEntries()):7d}")"""),
md("""## 6. Validate against the AGC reference

The AGC publishes reference histograms for the first N files per sample. A run with `DATA = "remote"` and
`N_FILES_PER_SAMPLE` in (1, 2, 5, 10) can be compared directly. A skim cannot: it has fewer events and a different
normalisation. (The blueprint was checked locally on a skim of the complete first ttbar file: all 22 ttbar histograms
match the 1-file reference.)"""),
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
out = sys.argv[1] if len(sys.argv) > 1 else Path(__file__).parents[1] / "notebooks" / "01_rdataframe_dask.ipynb"
nbf.write(nb, str(out))
