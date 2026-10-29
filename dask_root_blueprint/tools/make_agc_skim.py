"""Make a small local copy (skim) of an AGC NanoAOD file for development.

Keeps the first N events and a subset of branches, written as a classic NanoAOD-style TTree
(nJet, Jet_pt, ...) that ROOT RDataFrame and coffea can both read.

  python make_agc_skim.py INPUT OUTPUT [--events N] [--minimal]

  --events N   keep the first N events (default: all)
  --minimal    keep only the branches the AGC ttbar analysis reads, plus the event IDs and lepton kinematics that
               coffea's NanoAOD schema needs (smallest file);
               default keeps the full Jet, Muon and Electron collections plus MET, PV, flags and a few triggers.

Needs uproot >= 5 and awkward >= 2. INPUT can be a local path or an https:// / root:// URL.
"""

import argparse
import time

import awkward as ak
import uproot

COLLECTIONS = ["Jet", "Muon", "Electron"]
# coffea's NanoAODSchema also needs phi, mass and charge for leptons, and run/luminosityBlock/event.
MINIMAL = {
    "Jet": ["pt", "eta", "phi", "mass", "btagCSVV2", "jetId"],
    "Muon": ["pt", "eta", "phi", "mass", "charge", "sip3d", "tightId", "pfRelIso04_all"],
    "Electron": ["pt", "eta", "phi", "mass", "charge", "sip3d", "cutBased"],
}
MINIMAL_SCALARS = ["run", "luminosityBlock", "event"]
EVENT_BRANCHES = {"run", "luminosityBlock", "event", "genWeight", "PV_npvs", "PV_npvsGood", "fixedGridRhoFastjetAll"}
TRIGGERS = {"HLT_IsoMu24", "HLT_IsoMu27", "HLT_Ele27_WPTight_Gsf", "HLT_Ele32_WPTight_Gsf"}
STEP = 100_000  # events per chunk


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("input")
    p.add_argument("output")
    p.add_argument("--events", type=int, default=None)
    p.add_argument("--minimal", action="store_true")
    args = p.parse_args()

    src = uproot.open(args.input)
    tree = src["Events"]
    n = min(args.events or tree.num_entries, tree.num_entries)
    keys = tree.keys()

    if args.minimal:
        scalars = MINIMAL_SCALARS
        fields = {c: [f"{c}_{f}" for f in fs] for c, fs in MINIMAL.items()}
    else:
        scalars = [k for k in keys if k in EVENT_BRANCHES | TRIGGERS or k.startswith(("MET_", "Flag_"))]
        fields = {c: [k for k in keys if k.startswith(c + "_")] for c in COLLECTIONS}

    out = uproot.recreate(args.output, compression=uproot.ZLIB(4))
    t0 = time.time()
    for start in range(0, n, STEP):
        stop = min(start + STEP, n)
        a = tree.arrays(scalars + sum(fields.values(), []), entry_start=start, entry_stop=stop)
        chunk = {k: a[k] for k in scalars}
        for c, branches in fields.items():
            # zipping a collection's branches gives the NanoAOD layout on write: nJet + Jet_pt, Jet_eta, ...
            chunk[c] = ak.zip({b[len(c) + 1 :]: a[b] for b in branches})
        if start == 0:
            # mktree writes a TTree; assigning out["Events"] = ... would write an RNTuple in uproot >= 5.7,
            # which older ROOT and coffea versions cannot read.
            out.mktree("Events", {k: v.type if isinstance(v, ak.Array) else v.dtype for k, v in chunk.items()})
        out["Events"].extend(chunk)
        print(f"{stop:>9} / {n} events  {time.time() - t0:5.0f}s", flush=True)

    if "Runs" in src and not args.minimal:
        # per-run bookkeeping (genEventCount, genEventSumw, ...), useful for normalisation
        runs = src["Runs"]
        rd = {k: runs[k].array(library="np") for k in runs.keys()
              if runs[k].typename in ("uint32_t", "int32_t", "int64_t", "uint64_t", "float", "double")}
        out.mktree("Runs", {k: v.dtype for k, v in rd.items()})
        out["Runs"].extend(rd)
    out.close()


if __name__ == "__main__":
    main()
