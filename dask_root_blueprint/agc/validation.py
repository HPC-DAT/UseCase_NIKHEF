"""Compare output histograms with the AGC reference histograms, and plot them.

Pure Python (uproot + numpy + matplotlib), so the coffea notebook can use it too.
The comparison follows the AGC's validate_histograms.py
(https://github.com/iris-hep/analysis-grand-challenge, MIT licence): bins must agree, except
histograms that depend on the pseudo-random jet resolution smearing (pt_res_up), and small bin migrations.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import uproot


def histos_as_dict(path: str | Path) -> dict[str, dict]:
    """{name: {"edges": [...], "contents": [... incl. under/overflow]}} for every TH1 in a ROOT file."""
    out = {}
    with uproot.open(path) as f:
        for name in {k.rsplit(";", 1)[0] for k in f.keys()}:
            h = f[name]
            out[name] = {"edges": h.axis().edges().tolist(), "contents": h.values(flow=True).tolist()}
    return out


def _bins_ok(got: np.ndarray, ref: np.ndarray) -> bool:
    close = np.isclose(got, ref)
    if close.all():
        return True
    # Groups of adjacent differing bins are accepted if their sum matches (bin migration from float rounding).
    bad = np.where(~close)[0]
    for group in np.split(bad, np.where(np.diff(bad) > 1)[0] + 1):
        if not np.allclose(got[group], ref[group], atol=2.0) or not np.isclose(got[group].sum(), ref[group].sum()):
            return False
    return True


def validate(histos_file: str | Path, reference_json: str | Path, only_present: bool = False) -> dict[str, str]:
    """Return {histogram name: problem} for every mismatch with the reference (empty dict = all good).

    only_present: skip reference histograms that are not in the output (e.g. when only ttbar was run).
    """
    got = histos_as_dict(histos_file)
    ref = json.loads(Path(reference_json).read_text())
    errors = {}
    for name, r in ref.items():
        if name not in got:
            if not (only_present or name.endswith("pseudodata")):
                errors[name] = "missing"
            continue
        g = got[name]
        if not np.allclose(g["edges"], r["edges"]):
            errors[name] = "edges differ"
        elif "pt_res_up" not in name and not _bins_ok(np.array(g["contents"]), np.array(r["contents"])):
            errors[name] = f"contents differ: sum {sum(g['contents']):.1f} vs reference {sum(r['contents']):.1f}"
    return errors


def compared(histos_file: str | Path, reference_json: str | Path) -> list[str]:
    """Names of reference histograms that are present in the output and were therefore checked."""
    got = histos_as_dict(histos_file).keys()
    return sorted(n for n in json.loads(Path(reference_json).read_text()) if n in got)


PROCESSES = ["ttbar", "wjets", "single_top_t_chan", "single_top_tW", "single_top_s_chan"]
LABELS = {"4j1b": r"$H_T$ [GeV]  (4j1b)", "4j2b": r"$m_{bjj}$ [GeV]  (4j2b)"}


def plot_regions(histos_file: str | Path, processes: list[str] | None = None):
    """Stacked nominal distributions per region, like the AGC plots."""
    import matplotlib.pyplot as plt

    processes = processes or PROCESSES
    with uproot.open(histos_file) as f:
        names = {k.rsplit(";", 1)[0] for k in f.keys()}
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
        for ax, region in zip(axes, LABELS):
            present = [p for p in processes if f"{region}_{p}" in names]
            if not present:
                continue
            edges = f[f"{region}_{present[0]}"].axis().edges()
            values = [f[f"{region}_{p}"].values() for p in present]
            ax.hist([edges[:-1]] * len(values), bins=edges, weights=values, stacked=True, label=present, histtype="stepfilled")
            ax.set_xlabel(LABELS[region])
            ax.set_ylabel("Events")
            ax.legend(frameon=False)
        fig.tight_layout()
    return fig
