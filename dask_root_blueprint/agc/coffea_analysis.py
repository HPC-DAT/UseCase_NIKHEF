"""AGC CMS ttbar analysis with coffea (NanoEvents + awkward + hist), local or distributed on Dask.

The same analysis as rdf_analysis.py: same selection, regions, binning, systematic variations and histogram names,
so both notebooks can be validated against the same AGC reference. Written in the coffea "processor" style:
a processor turns one chunk of events into histograms, and a Runner splits the files into chunks, runs the processor
on every chunk (on the Dask workers) and adds up the results.

Adapted from the IRIS-HEP AGC coffea implementation
(https://github.com/iris-hep/analysis-grand-challenge, analyses/cms-open-data-ttbar, MIT licence).
The ML inference and statistical fit steps of the AGC are left out.
"""

from __future__ import annotations

import sys
from pathlib import Path

import awkward as ak
import cloudpickle
import hist
import numpy as np
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from scipy.special import ndtri

from .inputs import AGCInput

REGIONS = {"4j1b": "HT", "4j2b": "Trijet_mass"}
BINNING = (25, 50, 550)  # bins, low, high (GeV)
PT_SCALE_UP = np.float32(1.03)


# --- systematic variations, same definitions as helpers.h -------------------------------------------------------

def jet_pt_resolution(pt: ak.Array) -> ak.Array:
    """Pseudo-random jet pT smearing factors (mean 1, sigma 5%), reproducible: derived from the pT values themselves."""
    flat = ak.to_numpy(ak.flatten(pt))
    u = 0.001 * ((flat * np.float32(1000)).astype(np.int64) % 1000) + 0.0005
    res = (1 + 0.05 * ndtri(u)).astype(np.float32)
    return ak.unflatten(res, ak.num(pt))


def btag_weight_variations(jet_pt: ak.Array) -> dict[str, np.ndarray]:
    """Weight factors per event for b-tagging variations: one up/down pair per leading jet (7.5% x pT / 50 GeV)."""
    out = {}
    for i in range(4):
        pt_i = ak.to_numpy(jet_pt[:, i]).astype(np.float64)
        out[f"btag_var_{i}_up"] = (1 + 0.075 * pt_i / 50).astype(np.float32)
        out[f"btag_var_{i}_down"] = (1 - 0.075 * pt_i / 50).astype(np.float32)
    return out


# --- the analysis --------------------------------------------------------------------------------------------------

def select(events, jet_pt: ak.Array):
    """Events with exactly one tight lepton and at least four jets (pT > 30 GeV); returns those events' jets."""
    electrons, muons = events.Electron, events.Muon
    tight_el = (electrons.pt > 30) & (abs(electrons.eta) < 2.1) & (electrons.sip3d < 4) & (electrons.cutBased == 4)
    tight_mu = (
        (muons.pt > 30) & (abs(muons.eta) < 2.1) & (muons.sip3d < 4) & muons.tightId & (muons.pfRelIso04_all < 0.15)
    )
    jets = ak.with_field(events.Jet, jet_pt, "pt")  # nominal or varied jet pT
    jets = jets[(jets.pt > 30) & (abs(jets.eta) < 2.4) & (jets.jetId == 6)]

    selected = (ak.sum(tight_el, axis=1) + ak.sum(tight_mu, axis=1) == 1) & (ak.num(jets) >= 4)
    return selected, jets[selected]


def observables(jets) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """Per region: (mask over the selected events, observable value for the events in the region)."""
    n_btag = ak.sum(jets.btagCSVV2 > 0.5, axis=1)

    in_4j1b = ak.to_numpy(n_btag == 1)
    ht = ak.to_numpy(ak.sum(jets.pt[in_4j1b], axis=1))

    # 4j2b: mass of the trijet with the highest pT, among trijets with at least one b-tagged jet
    in_4j2b = ak.to_numpy(n_btag >= 2)
    j1, j2, j3 = ak.unzip(ak.combinations(jets[in_4j2b], 3, fields=["j1", "j2", "j3"]))
    trijet = j1 + j2 + j3
    has_btag = (j1.btagCSVV2 > 0.5) | (j2.btagCSVV2 > 0.5) | (j3.btagCSVV2 > 0.5)
    trijet = trijet[has_btag]
    best = ak.argmax(trijet.pt, axis=1, keepdims=True)
    trijet_mass = ak.to_numpy(ak.flatten(trijet[best].mass))

    return {"4j1b": (in_4j1b, ht), "4j2b": (in_4j2b, trijet_mass)}


def new_histogram(name: str, label: str) -> hist.Hist:
    return hist.Hist.new.Reg(*BINNING, name="observable", label=label).Weight(name=name)


def histogram_name(region: str, process: str, variation: str) -> str:
    """AGC naming: <region>_<process> for the nominal histogram, <region>_<process>_<variation> otherwise."""
    return f"{region}_{process}" if variation == "nominal" else f"{region}_{process}_{variation}"


class AGCProcessor(processor.ProcessorABC):
    """Turns one chunk of events into histograms for every region and variation."""

    def process(self, events) -> dict[str, hist.Hist]:
        meta = events.metadata
        process, sample_variation, weight = meta["process"], meta["variation"], meta["xsec_weight"]
        nominal_sample = sample_variation == "nominal"

        # Jet pT variations change which events and jets are selected, so the selection runs once per variation.
        jet_pt = events.Jet.pt
        jet_variations = {"nominal": jet_pt}
        if nominal_sample:
            jet_variations["pt_scale_up"] = jet_pt * PT_SCALE_UP
            jet_variations["pt_res_up"] = jet_pt * jet_pt_resolution(jet_pt)

        histos = {}
        for jet_variation, pt in jet_variations.items():
            selected, jets = select(events, pt)
            n = len(jets)

            # Weight variations only change the event weights, on top of the nominal selection.
            # (Weights in double precision, factors in single precision, as in the RDataFrame version.)
            nominal_weight = np.full(n, weight, dtype=np.float64)
            weights = {"nominal": nominal_weight}
            if nominal_sample and jet_variation == "nominal":
                weights |= {k: nominal_weight * v.astype(np.float64) for k, v in btag_weight_variations(jets.pt).items()}
                if process == "wjets":
                    weights |= {
                        "scale_var_up": nominal_weight * np.float64(np.float32(1.025)),
                        "scale_var_down": nominal_weight * np.float64(np.float32(0.975)),
                    }

            for region, (mask, values) in observables(jets).items():
                for weight_variation, w in weights.items():
                    variation = jet_variation if weight_variation == "nominal" else weight_variation
                    if not nominal_sample:
                        variation = sample_variation
                    name = histogram_name(region, process, variation)
                    h = new_histogram(name, REGIONS[region])
                    h.fill(observable=values, weight=w[mask])
                    histos[name] = h
        return histos

    def postprocess(self, accumulator):
        return accumulator


def fileset(inputs: list[AGCInput]) -> dict:
    """coffea fileset: one dataset per sample, with the metadata the processor needs."""
    return {
        f"{inp.process}__{inp.variation}": {
            "files": {path: "Events" for path in inp.paths},
            "metadata": {"process": inp.process, "variation": inp.variation, "xsec_weight": inp.xsec_weight},
        }
        for inp in inputs
    }


def run_analysis(
    inputs: list[AGCInput], client=None, chunksize: int = 100_000, workers: int = 4
) -> dict[str, hist.Hist]:
    """Run the processor over all samples and return the histograms by name.

    client: a dask.distributed Client (LocalCluster, or the RS-DAT cluster on Slurm); every chunk becomes a Dask task.
            None = run in local processes (workers of them), without Dask.
    chunksize: events per chunk (= per task).
    """
    if client is not None:
        # Send this package's code to the workers together with the processor (pickled by value), so Dask workers
        # in Slurm jobs only need the container image, not a copy of the repository on their Python path.
        cloudpickle.register_pickle_by_value(sys.modules[__package__])
        executor = processor.DaskExecutor(client=client, status=False)
    else:
        executor = processor.FuturesExecutor(workers=workers, status=False)
    runner = processor.Runner(executor=executor, schema=NanoAODSchema, chunksize=chunksize)
    return runner(fileset(inputs), processor_instance=AGCProcessor())


def save_histos(histos: dict[str, hist.Hist], path: str | Path) -> Path:
    """Write the histograms to a ROOT file (with uproot), plus the AGC pseudodata when the needed samples are present."""
    import uproot

    path = Path(path)
    with uproot.recreate(path) as f:
        for name, h in sorted(histos.items()):
            f[name] = h
        for region in REGIONS:
            needed = [f"{region}_wjets", f"{region}_ttbar_PS_var", f"{region}_ttbar_ME_var"]
            if all(n in histos for n in needed):
                f[f"{region}_pseudodata"] = histos[needed[0]] + 0.5 * histos[needed[1]] + 0.5 * histos[needed[2]]
    return path
