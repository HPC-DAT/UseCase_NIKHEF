# Dask/ROOT blueprint: AGC ttbar analysis with RDataFrame and coffea on Dask

A worked example of a HEP analysis on Dask that runs the same way on a laptop and on SURF's Spider or Snellius
with the [RS-DAT](https://github.com/RS-DAT) tools (Jupyter + Dask on Slurm, in an Apptainer container).

The analysis is the CMS ttbar analysis of the [Analysis Grand Challenge](https://agc.readthedocs.io) (AGC), on public
CMS 2015 Open Data in NanoAOD format, distributed over Dask, in two versions that give identical histograms:

- **ROOT RDataFrame** (`notebooks/01_rdataframe_dask.ipynb`), adapted from the
  [ROOT team's AGC implementation](https://github.com/root-project/analysis-grand-challenge);
- **coffea** (uproot + awkward + hist, no ROOT needed; `notebooks/02_coffea_dask.ipynb`), adapted from the
  [IRIS-HEP AGC coffea implementation](https://github.com/iris-hep/analysis-grand-challenge).

Both are validated against the AGC reference histograms, and run from the same container image.

Audience: HEP researchers who want to move an analysis from a single machine to a Dask cluster on Slurm.

## Contents

```text
dask_root_blueprint/
├── notebooks/
│   ├── 01_rdataframe_dask.ipynb         # the blueprint with RDataFrame: settings → Dask cluster → inputs → run → plots → validation
│   └── 02_coffea_dask.ipynb             # the same with coffea (same sections, same inputs and validation)
├── agc/
│   ├── inputs.py                        # AGC file list, cross sections, normalisation, local skims
│   ├── rdf_analysis.py                  # event selection, systematics and histograms with RDataFrame
│   ├── coffea_analysis.py               # the same with coffea: an AGCProcessor run by a coffea Runner on Dask
│   ├── helpers.h                        # C++ helpers used in the RDataFrame expressions (from the AGC)
│   ├── validation.py                    # comparison with AGC reference histograms, plots (uproot, no ROOT needed)
│   └── data/                            # nanoaod_inputs.json (691 files) and reference histograms (1, 2, 5, 10 files)
├── environment.yml                      # conda environment: ROOT, Dask, uproot/awkward/coffea, plotting
├── apptainer/                           # image definition and Jupyter/Dask defaults (RS-DAT template)
├── scripts/jupyterdask-spider.slurm     # Slurm job for jupyterdask: Jupyter server + Dask worker settings
└── tools/
    ├── make_agc_skim.py                 # make a small local copy of an AGC file for development
    ├── build_notebook.py                # regenerates notebook 01 (kept as code for review)
    └── build_coffea_notebook.py         # regenerates notebook 02
```

The image workflow lives at the repository root: `.github/workflows/build-dask-root-blueprint.yml`.

## How it works

RDataFrame describes the analysis as a lazy computation graph per sample: filters, new columns, systematic
variations (`Vary`) and histograms. Nothing is read until the graphs run. When the dataframe is created with
`ROOT.RDataFrame("Events", files, executor=client)`, the graph is split into partitions (ranges of entries), each
partition becomes a Dask task, and the partial histograms are merged back in the notebook. All samples run together
with `RunGraphs`, so the cluster is kept busy across samples.

**coffea** works chunk by chunk instead: a *processor* (`AGCProcessor`) turns one chunk of events into histograms,
using NanoEvents (`events.Jet`, `events.Electron`, ...) and awkward array operations. A coffea `Runner` with a
`DaskExecutor` splits the files into chunks of `CHUNKSIZE` events, runs the processor on every chunk as a Dask task,
and adds up the histograms. Jet pT variations re-run the selection; weight variations reuse it.

| | RDataFrame (notebook 01) | coffea (notebook 02) |
|---|---|---|
| Analysis written as | lazy dataframe graph (C++ expressions, JIT-compiled) | Python function on awkward arrays per chunk |
| Unit of work on Dask | partition (range of entries) per sample | chunk of `CHUNKSIZE` events per file |
| Systematic variations | `Vary` + `VariationsFor` | loops over jet pT variations and weight variations |
| Reads ROOT files with | ROOT (TTree, XRootD/Davix built in) | uproot (+ fsspec-xrootd for `root://`) |
| Code on the workers | C++ helpers sent with `DistributeCppCode` | `agc` package sent with the processor (cloudpickle by value) |

Switching between laptop and cluster changes only two settings in either notebook:

| | Local development | RS-DAT on Spider/Snellius |
|---|---|---|
| `RUN_ON` | `"local"`: `LocalCluster` | `"rsdat"`: `SLURMCluster()`, workers are Slurm jobs |
| `DATA` | `"skim"`: one local file | `"remote"`: AGC files read over HTTPS (UNL) or XRootD (EOS) |
| Environment | conda environment from `environment.yml` | Apptainer image built by GitHub Actions |

## Decisions

Decided by the project (Tim Kok, 28 Oct 2026):

| Decision | Why / notes |
|---|---|
| Base the blueprint on the AGC CMS ttbar analysis | Public data, a well-known HEP workload, and published reference histograms to validate against |
| RDataFrame first; a coffea/uproot notebook later | Both stacks are common in HEP; start with one and keep the same inputs and validation for the second (coffea notebook added 29 Oct 2026) |
| One "fat" container image with both stacks, separate notebooks per approach | One image to build and maintain on Spider/Snellius; researchers pick the notebook that matches their stack |
| Notebook-based, connecting to the Dask cluster that RS-DAT provides | RS-DAT runs Jupyter and the Dask scheduler as a Slurm job; users scale the cluster from Jupyter and work in notebooks |
| Build the image with the RS-DAT JDS_development_template scaffolding (GitHub Actions → Apptainer image on GHCR) | Standard RS-DAT way of providing a Python environment on Spider |
| An Open OnDemand variant with modules/CVMFS is out of scope for now | Later concern |
| Audience: HEP researchers, as an example of using Dask and RS-DAT | |
| Lives in `UseCase_NIKHEF`, in its own folder, on branch `dask-root-blueprint` (off `main`) | Next to the existing Dask-on-HTCondor tests |
| Develop locally on a skim of one AGC file; validate on Spider with the full files | The AGC storage (EOS, UNL) is reachable from a laptop and from Spider workers, but the first files of all samples are ~30 GB together |
| Notebook defaults: Slurm cluster (`RUN_ON = "rsdat"`), remote data (`DATA = "remote"`) from CERN EOS (`PREFIX = "eos"`) (29 Oct 2026) | The blueprint's main use is a run on Spider/Snellius; EOS is CERN's public mirror and has complete files (one UNL file is not) |

Implementation choices made while building it (open for review):

| Choice | Why / alternative |
|---|---|
| ROOT 6.40 from conda-forge, with the stable distributed API (`ROOT.RDataFrame(..., executor=client)`) | Current ROOT; the AGC v1.0.0 code needs ROOT 6.28 and the older experimental API. Pinning an older ROOT is possible if Spider needs it |
| Current coffea (2026.x) in the image, not the AGC's coffea 0.7 | coffea 0.7 needs awkward 1 and an old Dask, which conflict with the rest of the image |
| Leave out the AGC's ML inference and statistical fit | Keeps the example about distributing the analysis; both can be added later |
| Analysis code in a small `agc` package; the notebook only configures and calls it | Keeps the notebook readable and lets the coffea notebook reuse inputs and validation |
| coffea notebook in the classic processor style (`ProcessorABC` + `Runner` + `DaskExecutor`) | The API of the coffea 2026.x in the image (it no longer uses dask-awkward); the same structure as the AGC coffea implementation |
| coffea code reproduces the RDataFrame numerics (float32 factors, float64 weights, same pseudo-random jet smearing) | Both notebooks give bin-for-bin identical histograms, so one reference and one validation serve both |
| C++ helpers sent to workers with `DistributeCppCode` | Workers then need no access to the repository, only the image |
| Dask workers: 4 single-threaded worker processes with 4 GiB each per Slurm job (template default: 1 worker, 2 cores, 24 GiB) | ROOT recommends one thread per worker process for distributed RDataFrame |
| Image `oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint`, tag `latest` from `main` and the branch name otherwise | One repository can hold several images; branches can be tested without touching `latest` |
| Workflow logs in to GHCR as the triggering user (`github.actor`) instead of a `GHCR_USERNAME` secret, and builds on the runner's `/mnt` disk | No extra secret needed; the ~4.4 GB environment does not fit on the runner's root disk |
| Skims written as classic NanoAOD TTrees (not RNTuple, uproot's default since 5.7) | Readable by ROOT 6.28+ and coffea alike |
| Notebook generated from `tools/build_notebook.py` | Reviewable as code; the committed notebook has no outputs |
| Reference histograms for 1, 2, 5 and 10 files per sample included | Enough for test runs; larger references are in the AGC repositories |

## Run it locally

1. Create the environment (conda, mamba or micromamba). The image merges `environment.yml` with
   `apptainer/environment_default.yml`; locally the first one plus JupyterLab is enough:

   ```bash
   mamba env create -f dask_root_blueprint/environment.yml
   mamba activate dask_root_blueprint
   mamba install jupyterlab dask-labextension
   ```

2. Get a local skim. Download one AGC file (3.3 GB) and keep the first 100k events (67 MB):

   ```bash
   curl -L -o ttbar_0000.root "https://xrootd-local.unl.edu:1094//store/user/AGC/nanoAOD/TT_TuneCUETP8M1_13TeV-powheg-pythia8/cmsopendata2015_ttbar_19980_PU25nsData2015v1_76X_mcRun2_asymptotic_v12_ext3-v1_00000_0000.root"
   python dask_root_blueprint/tools/make_agc_skim.py ttbar_0000.root agc_ttbar_skim_100k.root --events 100000
   ```

   `--minimal` keeps only the branches the analysis reads, plus the event IDs and lepton fields coffea's NanoAOD
   schema needs (the analysis branches of all 1.33M events of this file are 180 MB; the extra fields add a little).

3. Open `notebooks/01_rdataframe_dask.ipynb` or `notebooks/02_coffea_dask.ipynb`, set `RUN_ON = "local"`,
   `DATA = "skim"` and `SKIM_PATH`, and run all cells. (The defaults are for a run on the Slurm cluster.)

## Run it on Spider with RS-DAT

1. **Image.** Pushing a change to `environment.yml` or `apptainer/` on `main` or `dask-root-blueprint` builds the
   image and pushes it to the GitHub Container Registry (Actions tab → "Build dask-root-blueprint image"; it can also
   be started by hand there):
   - `oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:latest` from `main`
   - `oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:dask-root-blueprint` from the branch

   New packages under an organisation are private. Either make the package public (package settings on GitHub), or
   log in on Spider first: `apptainer registry login -u <github-user> oras://ghcr.io` with a token that can read
   packages.

2. **Slurm script.** Check `scripts/jupyterdask-spider.slurm`: the image tag (`APPTAINER_IMAGE_TAG`), partition and
   walltime, and the worker settings (`DASK_JOBQUEUE__SLURM__*`). The defaults give each Dask job 4 single-threaded
   workers with 4 GiB each. Put a dCache macaroon in a local copy only, never in the repository.

   Pull the image once to `~/dask-root-blueprint.sif` on the Spider login node; the script then uses that file instead
   of downloading the ~1.3 GB image at every job start (a cold download can take longer than jupyterdask's 120 s
   start-up timeout):

   ```bash
   apptainer pull ~/dask-root-blueprint.sif oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:latest
   ```

   Repeat with `--force` after a new image build. Jupyter and Dask worker logs are written to `~/.jupyterdask/`.

3. **Start Jupyter** with the [jupyterdask](https://github.com/RS-DAT/JupyterDaskOnSLURM) CLI, from a clone of this
   repository on your machine:

   ```bash
   jupyterdask -i ~/.ssh/<key> --template dask_root_blueprint/scripts/jupyterdask-spider.slurm --run <user>@spider.surf.nl
   ```

4. **Run the notebook.** In JupyterLab, clone this repository (git extension) or open an existing clone on Spider, then
   open `01_rdataframe_dask.ipynb`. The defaults already run on the Slurm cluster with remote data from CERN EOS and one
   file per sample (9 files); adjust `N_JOBS` if needed and run all cells. With one file per sample the validation
   cell compares all histograms with the AGC reference. `N_FILES_PER_SAMPLE = None` processes all 691 files.

Spider worker nodes read the files directly from CERN EOS over `root://`. The message
`TLS: Unable to use cert+key file …/.proxy; does not exist` is harmless: XRootD looks for a grid proxy first and then
reads `eospublic` anonymously. `PREFIX = "unl"` reads the same files over HTTPS from UNL, but the first
`single_top_s_chan` file there has only 100 of its 632000 events (found 29 Oct 2026), so that sample does not match the
reference. If TLS verification fails for HTTPS, set `X509_CERT_DIR=/etc/grid-security/certificates` (the Slurm script
binds the host directories into the container).

## Validation status (29 Oct 2026)

| Check | Where | Result |
|---|---|---|
| Environment solves (ROOT 6.40.04, Dask 2026.8, coffea 2026.9, Python 3.12) | conda-forge, linux-64 | OK, ~4.4 GB unpacked |
| Notebook end to end, `LocalCluster`, 100k-event skim | laptop-class machine, 2 workers | OK |
| Local and distributed runs give identical histograms | 10k skim | OK |
| AGC reference, 1 file per sample, ttbar nominal (22 histograms) | full first ttbar file, `LocalCluster` | all match |
| Image build in GitHub Actions, pull on Spider | GHCR, tag `dask-root-blueprint` | OK (29 Oct) |
| `SLURMCluster` on Spider, remote data (UNL), 1 file per sample, all 9 samples | Spider, 2 jobs × 4 workers | 104 of 124 histograms match; all 20 `single_top_s_chan` histograms empty because of the incomplete UNL file (29 Oct) |
| Notebook 01 from scratch with the new defaults (Slurm, EOS, 1 file per sample, all 9 samples) | Spider, `SLURMCluster` | all 124 histograms match the AGC reference (29 Oct) |
| coffea and RDataFrame give identical histograms (all regions and variations, nominal + non-nominal samples) | synthetic NanoAOD-like data, 300k events, in-process and on a Dask `LocalCluster` | identical (50/50) |
| coffea with Dask workers that cannot import the `agc` package (as Slurm workers) | separate `dask scheduler`/`dask worker` | OK |
| Both notebooks end to end, `LocalCluster`, `--minimal` skim | workspace with the image's environment (ROOT 6.40.04, coffea 2026.9.0) | OK, same totals |
| Notebook 02 (coffea) with the same defaults | Spider, `SLURMCluster` | all 124 histograms match the AGC reference (29 Oct) |

The 4j1b/4j2b histograms that depend on the pseudo-random jet resolution smearing (`pt_res_up`) are excluded from the
comparison, as in the AGC's own validation.

## Notes for developers

- **ROOT version.** The code uses the stable distributed API of ROOT ≥ 6.36 (`ROOT.RDataFrame(..., executor=client)`,
  `ROOT.RDF.Distributed.*`). The AGC v1.0.0 code uses the older `ROOT.RDF.Experimental.Distributed.Dask.RDataFrame`.
- **`SLURMCluster` arguments (ROOT 6.40).** Distributed RDataFrame reads the number of cores from the arguments a
  dask-jobqueue cluster was created with (`cluster.worker_spec[...]["options"]["cores"]`). With `SLURMCluster()` and the
  values only in the Dask config this fails with *Could not retrieve the provided worker specification*, even when
  `npartitions` is set. The notebook therefore passes `cores`, `processes` and `memory` explicitly (from the Dask config).
  Worth reporting to root-project/root.
- **Errors in distributed `RunGraphs`** are not re-raised. If results are missing, `run_analysis` re-runs one failed
  graph on its own so the real exception appears in the notebook.
- **dask-jobqueue import (ROOT 6.40).** Distributed `RunGraphs` imports `dask_jobqueue` from a background thread, and
  `dask_jobqueue` sets a signal handler on import, which fails outside the main thread. `RunGraphs` swallows the error
  and returns no results. `agc/rdf_analysis.py` imports `dask_jobqueue` up front and checks that results came back.
- **Result lifetime.** Varied histograms belong to RDataFrame's result objects; `run_analysis` returns clones so they
  stay valid after the call.
- **C++ helpers** are sent to the workers with `ROOT.RDF.Distributed.DistributeCppCode`, so workers need no access to
  the repository.
- **Not included** from the AGC: the ML inference step and the statistical fit.
- **coffea notebook.** The processor (`agc/coffea_analysis.py`) mirrors `rdf_analysis.py` step by step; keep them in
  sync when changing the analysis. coffea's `NanoAODSchema` needs `run`, `luminosityBlock`, `event` and the lepton
  `phi`/`mass`/`charge` branches, even though the analysis does not use all of them. For Dask workers without the
  repository (Slurm jobs), `run_analysis` registers the `agc` package with `cloudpickle.register_pickle_by_value`.
- **Image tag.** `scripts/jupyterdask-spider.slurm` uses `latest` (built from `main`). To test a branch before merging,
  set `APPTAINER_IMAGE_TAG` to the branch name.

## References

Publications:

- A. Held, E. Kauffman, O. Shadura, A. Wightman, *Physics analysis for the HL-LHC: Concepts and pipelines in practice with the Analysis Grand Challenge*, EPJ Web of Conferences 295, 06016 (2024), [doi:10.1051/epjconf/202429506016](https://doi.org/10.1051/epjconf/202429506016) — the Analysis Grand Challenge and the CMS ttbar analysis this blueprint implements.
- V. E. Padulano, E. Guiraud, A. Falko, E. Gazzarrini, E. Garcia Garcia, D. Gosein, *First implementation and results of the Analysis Grand Challenge with a fully Pythonic RDataFrame*, EPJ Web of Conferences 295, 06011 (2024), [doi:10.1051/epjconf/202429506011](https://doi.org/10.1051/epjconf/202429506011) — the RDataFrame implementation that notebook 01 is adapted from.

Code and data:

- [iris-hep/analysis-grand-challenge](https://github.com/iris-hep/analysis-grand-challenge) (AGC: analysis definition, coffea implementation, file list and reference histograms; `analyses/cms-open-data-ttbar`); notebook 02 is adapted from its coffea implementation.
- [root-project/analysis-grand-challenge](https://github.com/root-project/analysis-grand-challenge) (RDataFrame implementation); notebook 01 is adapted from it.
- [CMS Open Data](https://opendata.cern.ch): the CMS 2015 simulated samples behind the AGC NanoAOD files (see the [AGC documentation](https://agc.readthedocs.io) for the datasets).
- [AGC documentation](https://agc.readthedocs.io).
- Tools: [ROOT RDataFrame](https://root.cern/doc/master/classROOT_1_1RDataFrame.html),
  [coffea](https://coffea-hep.readthedocs.io), [uproot](https://uproot.readthedocs.io),
  [awkward](https://awkward-array.org), [hist](https://hist.readthedocs.io), [Dask](https://docs.dask.org),
  [dask-jobqueue](https://jobqueue.dask.org).

## Credits

Analysis and reference histograms from the IRIS-HEP Analysis Grand Challenge
([iris-hep/analysis-grand-challenge](https://github.com/iris-hep/analysis-grand-challenge), coffea implementation)
and the ROOT team's RDataFrame implementation
([root-project/analysis-grand-challenge](https://github.com/root-project/analysis-grand-challenge)), both MIT licence.
Container and Slurm scaffolding from [RS-DAT/JDS_development_template](https://github.com/RS-DAT/JDS_development_template),
Jupyter on Slurm with [RS-DAT/JupyterDaskOnSLURM](https://github.com/RS-DAT/JupyterDaskOnSLURM).
