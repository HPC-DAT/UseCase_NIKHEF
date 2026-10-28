# Dask/ROOT blueprint: AGC ttbar analysis with RDataFrame on Dask

A worked example of a HEP analysis on Dask that runs the same way on a laptop and on SURF's Spider or Snellius
with the [RS-DAT](https://github.com/RS-DAT) tools (Jupyter + Dask on Slurm, in an Apptainer container).

The analysis is the CMS ttbar analysis of the [Analysis Grand Challenge](https://agc.readthedocs.io) (AGC), on public
CMS 2015 Open Data in NanoAOD format, implemented with ROOT RDataFrame and distributed over Dask. It is adapted from
the [ROOT team's AGC implementation](https://github.com/root-project/analysis-grand-challenge) and validated against
the AGC reference histograms.

Audience: HEP researchers who want to move an analysis from a single machine to a Dask cluster on Slurm.

## Contents

```text
dask_root_blueprint/
├── notebooks/01_rdataframe_dask.ipynb   # the blueprint: settings → Dask cluster → inputs → run → plots → validation
├── agc/
│   ├── inputs.py                        # AGC file list, cross sections, normalisation, local skims
│   ├── rdf_analysis.py                  # event selection, systematics and histograms with RDataFrame
│   ├── helpers.h                        # C++ helpers used in the RDataFrame expressions (from the AGC)
│   ├── validation.py                    # comparison with AGC reference histograms, plots (uproot, no ROOT needed)
│   └── data/                            # nanoaod_inputs.json (691 files) and reference histograms (1, 2, 5, 10 files)
├── environment.yml                      # conda environment: ROOT, Dask, uproot/awkward/coffea, plotting
├── apptainer/                           # image definition and Jupyter/Dask defaults (RS-DAT template)
├── scripts/jupyterdask-spider.slurm     # Slurm job for jupyterdask: Jupyter server + Dask worker settings
└── tools/
    ├── make_agc_skim.py                 # make a small local copy of an AGC file for development
    └── build_notebook.py                # regenerates the notebook (kept as code for review)
```

The image workflow lives at the repository root: `.github/workflows/build-dask-root-blueprint.yml`.

## How it works

RDataFrame describes the analysis as a lazy computation graph per sample: filters, new columns, systematic
variations (`Vary`) and histograms. Nothing is read until the graphs run. When the dataframe is created with
`ROOT.RDataFrame("Events", files, executor=client)`, the graph is split into partitions (ranges of entries), each
partition becomes a Dask task, and the partial histograms are merged back in the notebook. All samples run together
with `RunGraphs`, so the cluster is kept busy across samples.

Switching between laptop and cluster changes only two settings in the notebook:

| | Local development | RS-DAT on Spider/Snellius |
|---|---|---|
| `RUN_ON` | `"local"`: `LocalCluster` | `"rsdat"`: `SLURMCluster()`, workers are Slurm jobs |
| `DATA` | `"skim"`: one local file | `"remote"`: AGC files read over HTTPS (UNL) or XRootD (EOS) |
| Environment | conda environment from `environment.yml` | Apptainer image built by GitHub Actions |

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

   `--minimal` keeps only the branches the analysis reads (all 1.33M events of this file fit in 180 MB).

3. Open `notebooks/01_rdataframe_dask.ipynb`, set `SKIM_PATH`, and run all cells.

## Run it on Spider with RS-DAT

1. **Image.** Pushing a change to `environment.yml` or `apptainer/` on `main` or `dask-root-blueprint` builds the
   image and pushes it to the GitHub Container Registry (Actions tab → "Build dask-root-blueprint image"; it can also
   be started by hand there):
   - `oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:latest` from `main`
   - `oras://ghcr.io/hpc-dat/usecase_nikhef/dask-root-blueprint:dask-root-blueprint` from the branch

   New packages under an organisation are private. Either make the package public (package settings on GitHub), or
   log in on Spider first: `apptainer registry login -u <github-user> oras://ghcr.io` with a token that can read
   packages.

2. **Slurm script.** Check `scripts/jupyterdask-spider.slurm`: the image tag (`APPTAINER_IMAGE`), partition and
   walltime, and the worker settings (`DASK_JOBQUEUE__SLURM__*`). The defaults give each Dask job 4 single-threaded
   workers with 4 GiB each. Put a dCache macaroon in a local copy only, never in the repository.

3. **Start Jupyter** with the [jupyterdask](https://github.com/RS-DAT/JupyterDaskOnSLURM) CLI, from a clone of this
   repository on your machine:

   ```bash
   jupyterdask -i ~/.ssh/<key> --template dask_root_blueprint/scripts/jupyterdask-spider.slurm --run <user>@spider.surf.nl
   ```

4. **Run the notebook.** In JupyterLab, clone this repository (git extension) or open an existing clone on Spider, then
   in `01_rdataframe_dask.ipynb` set `RUN_ON = "rsdat"`, `N_JOBS`, `DATA = "remote"` and `N_FILES_PER_SAMPLE = 1`.
   With one file per sample (9 files) the validation cell compares all histograms with the AGC reference.
   `N_FILES_PER_SAMPLE = None` processes all 691 files.

Spider worker nodes can reach the UNL endpoint directly (checked from the login node; worker nodes have no outbound
restrictions). If TLS verification fails there, set `X509_CERT_DIR=/etc/grid-security/certificates` (the Slurm script
binds the host directories into the container), or use `PREFIX = "eos"`.

## Validation status (28 Oct 2026)

| Check | Where | Result |
|---|---|---|
| Environment solves (ROOT 6.40.04, Dask 2026.8, coffea 2026.9, Python 3.12) | conda-forge, linux-64 | OK, ~4.4 GB unpacked |
| Notebook end to end, `LocalCluster`, 100k-event skim | laptop-class machine, 2 workers | OK |
| Local and distributed runs give identical histograms | 10k skim | OK |
| AGC reference, 1 file per sample, ttbar nominal (22 histograms) | full first ttbar file, `LocalCluster` | all match |
| Image build in GitHub Actions | | not yet run |
| `SLURMCluster` on Spider, remote data, all 9 samples | | not yet run |

The 4j1b/4j2b histograms that depend on the pseudo-random jet resolution smearing (`pt_res_up`) are excluded from the
comparison, as in the AGC's own validation.

## Notes for developers

- **ROOT version.** The code uses the stable distributed API of ROOT ≥ 6.36 (`ROOT.RDataFrame(..., executor=client)`,
  `ROOT.RDF.Distributed.*`). The AGC v1.0.0 code uses the older `ROOT.RDF.Experimental.Distributed.Dask.RDataFrame`.
- **dask-jobqueue import (ROOT 6.40).** Distributed `RunGraphs` imports `dask_jobqueue` from a background thread, and
  `dask_jobqueue` sets a signal handler on import, which fails outside the main thread. `RunGraphs` swallows the error
  and returns no results. `agc/rdf_analysis.py` imports `dask_jobqueue` up front and checks that results came back.
- **Result lifetime.** Varied histograms belong to RDataFrame's result objects; `run_analysis` returns clones so they
  stay valid after the call.
- **C++ helpers** are sent to the workers with `ROOT.RDF.Distributed.DistributeCppCode`, so workers need no access to
  the repository.
- **Not included** from the AGC: the ML inference step and the statistical fit.
- **Next:** a coffea/uproot notebook on the same inputs and validation, in the same image.

## Credits

Analysis and reference histograms from the IRIS-HEP Analysis Grand Challenge
([iris-hep/analysis-grand-challenge](https://github.com/iris-hep/analysis-grand-challenge)) and the ROOT team's
RDataFrame implementation ([root-project/analysis-grand-challenge](https://github.com/root-project/analysis-grand-challenge),
MIT licence). Container and Slurm scaffolding from [RS-DAT/JDS_development_template](https://github.com/RS-DAT/JDS_development_template).
