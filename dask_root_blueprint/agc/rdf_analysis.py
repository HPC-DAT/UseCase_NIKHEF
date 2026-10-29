"""AGC CMS ttbar analysis with ROOT RDataFrame, local or distributed on Dask.

Adapted from the ROOT team's AGC implementation
(https://github.com/root-project/analysis-grand-challenge, analyses/cms-open-data-ttbar/analysis.py, MIT licence),
updated to the stable distributed RDataFrame API of ROOT >= 6.36 (`ROOT.RDataFrame(..., executor=client)`).
The ML inference and statistical fit steps of the AGC are left out.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ROOT

from .inputs import AGCInput

try:
    # Workaround (ROOT 6.40, dask-jobqueue 0.9): distributed RDataFrame imports dask_jobqueue lazily from a
    # worker thread inside RunGraphs, and dask_jobqueue installs a signal handler at import time, which only
    # works in the main thread. RunGraphs swallows that error and returns without results.
    # Importing it here, in the main thread, avoids the problem.
    import dask_jobqueue  # noqa: F401
except ImportError:
    pass

HELPERS = Path(__file__).parent / "helpers.h"
REGIONS = {"4j1b": "HT", "4j2b": "Trijet_mass"}
BINNING = (25, 50, 550)  # bins, low, high (GeV)


@dataclass
class Booked:
    result: Any  # RResultPtr (or its distributed proxy), replaced by a ResultMap when varied
    region: str
    process: str
    variation: str
    vary: bool  # True for nominal samples: systematic variations are produced with Vary()


def declare_helpers(distributed: bool) -> None:
    """Make the C++ helpers in helpers.h available, locally and (if distributed) on every Dask worker."""
    code = HELPERS.read_text()
    if distributed:
        # Declares the code locally and on each worker before it processes data.
        ROOT.RDF.Distributed.DistributeCppCode(code)
    else:
        ROOT.gInterpreter.Declare(code)


def make_dataframe(inp: AGCInput, client=None, npartitions: int | None = None):
    """An RDataFrame over the input files; distributed over the Dask cluster if a client is given."""
    if client is None:
        return ROOT.RDataFrame("Events", inp.paths)
    kwargs = {"executor": client}
    if npartitions:
        kwargs["npartitions"] = npartitions
    return ROOT.RDataFrame("Events", inp.paths, **kwargs)


def _trijet_mass(df):
    """Mass of the highest-pT trijet with at least one b-tagged jet, in events with >= 2 b-tagged jets."""
    df = df.Filter("Sum(Jet_btagCSVV2_cut > 0.5) > 1")
    df = df.Define("Jet_p4", "ConstructP4(Jet_pt_cut, Jet_eta_cut, Jet_phi_cut, Jet_mass_cut)")
    df = df.Define("Trijet_idx", "Combinations(Jet_pt_cut, 3)")
    df = df.Define(
        "Trijet_btag",
        """
        auto J1 = Take(Jet_btagCSVV2_cut, Trijet_idx[0]);
        auto J2 = Take(Jet_btagCSVV2_cut, Trijet_idx[1]);
        auto J3 = Take(Jet_btagCSVV2_cut, Trijet_idx[2]);
        return J1 > 0.5 || J2 > 0.5 || J3 > 0.5;
        """,
    )
    df = df.Define(
        "Trijet_p4",
        """
        auto J1 = Take(Jet_p4, Trijet_idx[0]);
        auto J2 = Take(Jet_p4, Trijet_idx[1]);
        auto J3 = Take(Jet_p4, Trijet_idx[2]);
        return (J1 + J2 + J3)[Trijet_btag];
        """,
    )
    df = df.Define("Trijet_pt", "return Map(Trijet_p4, [](const ROOT::Math::PxPyPzMVector &v) { return v.Pt(); })")
    return df.Define("Trijet_mass", "Trijet_p4[ArgMax(Trijet_pt)].M()")


def book_histos(df, inp: AGCInput) -> list[Booked]:
    """Book the HT (4j1b) and trijet mass (4j2b) histograms for one sample. Nothing runs yet: RDataFrame is lazy."""
    nominal = inp.variation == "nominal"
    df = df.Define("Weights", str(inp.xsec_weight))

    if nominal:
        # jet energy scale and resolution variations
        df = df.Vary(
            "Jet_pt",
            "ROOT::RVec<ROOT::RVecF>{Jet_pt*pt_scale_up(), Jet_pt*jet_pt_resolution(Jet_pt)}",
            ["pt_scale_up", "pt_res_up"],
        )
        if inp.process == "wjets":
            df = df.Vary("Weights", "Weights*flat_variation()", ["scale_var_up", "scale_var_down"])

    # exactly one tight lepton, at least four jets with pT > 30 GeV
    df = (
        df.Define(
            "Electron_mask",
            "Electron_pt > 30 && abs(Electron_eta) < 2.1 && Electron_sip3d < 4 && Electron_cutBased == 4",
        )
        .Define(
            "Muon_mask",
            "Muon_pt > 30 && abs(Muon_eta) < 2.1 && Muon_sip3d < 4 && Muon_tightId && Muon_pfRelIso04_all < 0.15",
        )
        .Filter("Sum(Electron_mask) + Sum(Muon_mask) == 1")
        .Define("Jet_mask", "Jet_pt > 30 && abs(Jet_eta) < 2.4 && Jet_jetId == 6")
        .Filter("Sum(Jet_mask) >= 4")
    )
    for var in ["pt", "btagCSVV2", "eta", "phi", "mass"]:
        df = df.Define(f"Jet_{var}_cut", f"Jet_{var}[Jet_mask]")

    if nominal:
        # b-tagging weight variations
        df = df.Vary(
            "Weights",
            "ROOT::RVecD{Weights*btag_weight_variation(Jet_pt_cut)}",
            [f"btag_var_{i}_{d}" for i in range(4) for d in ["up", "down"]],
        )

    regions = {
        "4j1b": df.Filter("Sum(Jet_btagCSVV2_cut > 0.5) == 1").Define("HT", "Sum(Jet_pt_cut)"),
        "4j2b": _trijet_mass(df),
    }
    booked = []
    for region, rdf in regions.items():
        model = ROOT.RDF.TH1DModel(f"{region}_{inp.process}_{inp.variation}", inp.process, *BINNING)
        booked.append(Booked(rdf.Histo1D(model, REGIONS[region], "Weights"), region, inp.process, inp.variation, nominal))
    return booked


def run_analysis(inputs: list[AGCInput], client=None, npartitions: int | None = None) -> dict[str, "ROOT.TH1D"]:
    """Build the computation graphs for all samples, run them in one go, and return the histograms by name.

    client: a dask.distributed Client (LocalCluster, or the RS-DAT cluster on Slurm). None = run in this process.
    npartitions: number of tasks per sample on Dask (default: ROOT decides from the cluster size).
    """
    distributed = client is not None
    rdf_dist = ROOT.RDF.Distributed if distributed else None
    declare_helpers(distributed)

    booked: list[Booked] = []
    for inp in inputs:
        booked += book_histos(make_dataframe(inp, client, npartitions), inp)

    variations_for = rdf_dist.VariationsFor if distributed else ROOT.RDF.Experimental.VariationsFor
    for b in booked:
        if b.vary:
            b.result = variations_for(b.result)

    # All event loops run here: one pass over each sample's files fills every histogram and variation.
    (rdf_dist.RunGraphs if distributed else ROOT.RDF.RunGraphs)([b.result for b in booked])
    if distributed:
        failed = [b for b in booked if b.result.proxied_node.value is None]
        if failed:
            # Distributed RunGraphs does not re-raise errors from its submission threads, so nothing shows up
            # in the notebook or the worker logs. Re-run one failed graph on its own, in this (main) thread,
            # so the real exception is raised here.
            b = failed[0]
            try:
                (b.result.GetKeys() if b.vary else b.result.GetValue())
            except Exception as err:
                raise RuntimeError(
                    f"Distributed RunGraphs failed for {b.region}_{b.process}_{b.variation} "
                    f"({len(failed)} of {len(booked)} results missing): {err}"
                ) from err
            raise RuntimeError(
                f"Distributed RunGraphs returned without results for {len(failed)} of {len(booked)} histograms, "
                "and re-running one graph alone did not fail; check the Dask worker logs."
            )
    return _collect(booked)


def _collect(booked: list[Booked]) -> dict[str, "ROOT.TH1D"]:
    """Turn results into named TH1Ds, following the AGC naming: <region>_<process>[_<variation>]."""
    out = {}
    for b in booked:
        base = f"{b.region}_{b.process}"
        if not b.vary:
            h = b.result.GetValue()
            name = base if b.variation == "nominal" else f"{base}_{b.variation}"
            out[name] = h
            continue
        for key in b.result.GetKeys():
            key = str(key)
            h = b.result[key]
            variation = key.split(":")[-1]
            out[base if variation == "nominal" else f"{base}_{variation}"] = h
    # Clone: varied histograms are owned by RDataFrame's result objects, which are freed once `booked` goes away.
    ROOT.TH1.AddDirectory(False)
    return {name: h.Clone(name) for name, h in out.items()}


def save_histos(histos: dict[str, "ROOT.TH1D"], path: str | Path) -> Path:
    """Write the histograms to a ROOT file, plus the AGC pseudodata when all needed samples are present."""
    path = Path(path)
    with ROOT.TFile.Open(str(path), "RECREATE") as f:
        for name, h in histos.items():
            f.WriteObject(h, name)
        for region in REGIONS:
            needed = [f"{region}_wjets", f"{region}_ttbar_PS_var", f"{region}_ttbar_ME_var"]
            if all(n in histos for n in needed):
                pseudo = histos[needed[0]].Clone(f"{region}_pseudodata")
                pseudo.Add(histos[needed[1]], 0.5)
                pseudo.Add(histos[needed[2]], 0.5)
                f.WriteObject(pseudo, pseudo.GetName())
    return path
