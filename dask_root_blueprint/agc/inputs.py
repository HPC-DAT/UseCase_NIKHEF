"""Input handling for the AGC CMS ttbar analysis (shared by the RDataFrame and coffea notebooks).

The file list comes from the Analysis Grand Challenge (`data/nanoaod_inputs.json`): CMS 2015 Open Data
converted to NanoAOD, 5 processes with systematic variations, 691 files in total.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
INPUTS_JSON = DATA_DIR / "nanoaod_inputs.json"
REFERENCE_DIR = DATA_DIR / "reference"

# Prefix used in nanoaod_inputs.json, and known mirrors of the same files.
UNL_HTTPS = "https://xrootd-local.unl.edu:1094//store/user/AGC"
PREFIXES = {
    "eos": "root://eospublic.cern.ch//eos/root-eos/AGC",  # CERN EOS public mirror (default)
    # HTTPS via XRootD at UNL. Note (29 Oct 2026): the first single_top_s_chan file there has only 100 of its
    # 632000 events, so the AGC reference comparison fails for that sample; EOS has the complete file.
    "unl": UNL_HTTPS,
}

# Cross sections in pb, as in the AGC reference implementations.
XSEC_INFO = {
    "ttbar": 396.87 + 332.97,  # nonallhad + allhad, same x-sec for all ttbar variations
    "single_top_s_chan": 2.0268 + 1.2676,
    "single_top_t_chan": (36.993 + 22.175) / 0.252,  # scale from lepton filter to inclusive
    "single_top_tW": 37.936 + 37.906,
    "wjets": 61457 * 0.252,  # e/mu+nu final states
}
LUMI = 3378  # /pb


@dataclass
class AGCInput:
    paths: list[str]  # local paths, https:// or root:// URLs
    process: str
    variation: str
    nevents: int  # total number of events in these files, used for normalisation

    @property
    def xsec_weight(self) -> float:
        return XSEC_INFO[self.process] * LUMI / self.nevents


def remote_inputs(
    n_files_per_sample: int | None = 1,
    prefix: str = "eos",
    processes: list[str] | None = None,
    variations: list[str] | None = None,
) -> list[AGCInput]:
    """Inputs read directly from the AGC storage (CERN EOS or UNL).

    n_files_per_sample: first N files of every process/variation (None = all 691 files, ~3.5 TB).
    prefix: "eos" (default), "unl", or any replacement for the UNL prefix (e.g. a dCache WebDAV URL of a copy).
    """
    new_prefix = PREFIXES.get(prefix, prefix)
    spec = json.loads(INPUTS_JSON.read_text())
    inputs = []
    for process, per_variation in spec.items():
        if process == "data" or (processes and process not in processes):
            continue
        for variation, info in per_variation.items():
            if variations and variation not in variations:
                continue
            files = info["files"][:n_files_per_sample]
            paths = [f["path"].replace(UNL_HTTPS, new_prefix) for f in files]
            inputs.append(AGCInput(paths, process, variation, sum(f["nevts"] for f in files)))
    return inputs


def local_inputs(path: str | Path, process: str = "ttbar", variation: str = "nominal") -> list[AGCInput]:
    """A single local file (e.g. a skim made with tools/make_agc_skim.py), normalised by its own event count."""
    import uproot  # local import: only needed here

    path = str(Path(path).expanduser())
    with uproot.open(path) as f:
        nevents = f["Events"].num_entries
    return [AGCInput([path], process, variation, nevents)]


def reference_file(n_files_per_sample: int) -> Path:
    """Reference histograms published by the AGC for N files per process (1, 2, 5, 10 are included here)."""
    p = REFERENCE_DIR / f"histos_{n_files_per_sample}_file_per_process.json"
    if not p.exists():
        raise FileNotFoundError(f"No reference for {n_files_per_sample} files per process in {REFERENCE_DIR}")
    return p
