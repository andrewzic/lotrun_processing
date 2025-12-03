#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime

def parse_args():
    p = argparse.ArgumentParser(description="Phase-only self-calibration loop in CASA.")
    p.add_argument("--ms", required=True, help="Path to the measurement set.")
    p.add_argument("--solint", required=True, help="Comma-separated solution intervals, e.g. 'inf,300s,120s,60s'.")
    p.add_argument("--index", required=True, type=int, help="Self-cal index for book keeping")
    p.add_argument("--calmode", type=str, default="p", help="calibration mode: either 'p' or 'ap'")
    p.add_argument("--field", default="", help="Field selection (CASA syntax).")
    p.add_argument("--spw", default="", help="SPW/chans selection (CASA syntax).")
    p.add_argument("--refant", default="AK06", help="Reference antenna name(s).") #to do  : work out which antenna is the best
    p.add_argument("--combine", default="", help="Axes to combine in solve, e.g. 'scan,spw'.")
    p.add_argument("--minsnr", type=float, default=3.0, help="Minimum SNR for valid solutions.")
    p.add_argument("--parang", action="store_true", help="Apply parallactic angle in gaincal/applycal.")
    p.add_argument("--caltable-prefix", default="selfcal_p", help="Prefix for output cal tables.")
    p.add_argument("--plot-dir", default="plots", help="Directory to store diagnostic plots.")
    p.add_argument("--apply-calwt", type=str, default="False", help="applycal calwt flag (True/False).")
    return p.parse_args()

def has_model_column(ms):
    # Use casatools.table to check columns
    try:
        from casatools import table
        tb = table()
        tb.open(ms)
        cols = tb.colnames()
        tb.close()
        return "MODEL_DATA" in cols
    except Exception:
        return False

def solve_gain_phase(ms, caltable, solint, args):
    from casatasks import gaincal
    print(f"[{datetime.now().isoformat()}] gaincal: vis={ms}, caltable={caltable}, solint={solint}, calmode='{args.calmode}'")
    gaincal(
        vis=ms,
        caltable=caltable,
        field=args.field,
        spw=args.spw,
        solint=solint,
        combine=args.combine,
        refant=args.refant,
        minsnr=args.minsnr,
        gaintype="G",
        calmode=args.calmode,
        parang=args.parang
    )


def plot_solutions(caltable: str, figfile_base: str):
    """
    Plot calibration solutions using plotms (recommended in modern CASA).
    Saves phase-vs-time and amplitude-vs-time PNGs next to your caltable.
    """
    try:
        from casaplotms import plotms  # CASA 6 plotms interface
    except Exception as e:
        # Some packaged CASA expose plotms via casatasks; try that path too
        try:
            from casatasks import plotms  # fallback
        except Exception:
            raise RuntimeError(f"plotms is not available in this CASA build: {e}")

    # 1) Phase vs Time
    phase_png = f"{figfile_base}.phase.png"
    plotms(
        vis=caltable,          # plotms accepts calibration tables as 'vis'
        xaxis="time",
        yaxis="phase",
        coloraxis="antenna",
        showgui=False,
        plotfile=phase_png,
        overwrite=True
    )

    # 2) Amplitude vs Time (optional; comment out if you only want phase)
    amp_png = f"{figfile_base}.amp.png"
    plotms(
        vis=caltable,
        xaxis="time",
        yaxis="amp",
        coloraxis="antenna",
        showgui=False,
        plotfile=amp_png,
        overwrite=True
    )

    print(f"Saved plots:\n  {phase_png}\n  {amp_png}")

    
# def plot_solutions(caltable, figfile):
#     # Preferred: plotms (CASA 6) for cal tables; fallback to plotcal if available
#     try:
#         from casaplotms import plotms
#         print(f"[{datetime.now().isoformat()}] plotms: caltable={caltable} -> {figfile}")
#         plotms(
#             vis=caltable,
#             xaxis="time",
#             yaxis="phase",
#             coloraxis="antenna",
#             showgui=False,
#             plotfile=figfile,
#             overwrite=True
#         )
#     except Exception:
#         try:
#             from casatasks import plotcal
#             print(f"[{datetime.now().isoformat()}] plotcal (fallback): caltable={caltable} -> {figfile}")
#             plotcal(
#                 caltable=caltable,
#                 xaxis="time",
#                 yaxis="phase",
#                 iteration="antenna",
#                 showgui=False,
#                 figfile=figfile
#             )
#         except Exception as e:
#             print(f"WARNING: Unable to plot calibration table '{caltable}': {e}", file=sys.stderr)

def apply_gain(old_ms, new_ms, gaintables, args):
    from casatasks import applycal, split
    print(f"[{datetime.now().isoformat()}] applycal: vis={old_ms}, gaintable={gaintables}")
    applycal(
        vis=old_ms,
        field=args.field,
        spw=args.spw,
        gaintable=gaintables,
        gainfield=[""] * len(gaintables),
        interp=["linear,nearest"] * len(gaintables),
        calwt=[args.apply_calwt.lower() == "true"] * len(gaintables),
        parang=args.parang,
        flagbackup=True
    )
    print(f"[{datetime.now().isoformat()}] split: vis={old_ms}, outputvis={new_ms}, datcolumn='corrected'")
    split(vis=old_ms, outputvis=new_ms, datacolumn="corrected")
    
    
def main():
    args = parse_args()
    ms = args.ms
    index = args.index
    
    solint = args.solint
    print(f"[{datetime.now().isoformat()}] RUNNING SELF CALIBRATION: ms={ms}; INDEX={index}; solint={solint}")
    if args.index == 1:
        old_ms = args.ms
    elif args.index > 1:
        old_ms = args.ms.replace(".calB0.ms", f".selfcal_{args.index-1}.ms")
    else:
        raise ValueError(f"{args.index} make nossensens")
    
    new_ms = args.ms.replace(".calB0.ms", f".selfcal_{args.index}.ms")

    print(f"[{datetime.now().isoformat()}] SELF CALIBRATION WILL BE DERIVED FROM {old_ms}, CORRECTED DATA WILL BE SAVED TO {new_ms}")
    
    real_plotdir=os.path.join(os.path.dirname(ms), args.plot_dir)
    os.makedirs(real_plotdir, exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(ms), 'caltables'), exist_ok=True)
    
    if not has_model_column(old_ms):
        raise ValueErorr("ERROR: MODEL_DATA column not found; gaincal divides DATA by MODEL. Ensure you have predicted a model (e.g., via crystalball) before self-cal.")
    
    caltable = os.path.join(os.path.dirname(ms), "caltables", f"{os.path.basename(ms).replace('.calB0.ms', '')}_{args.caltable_prefix}.sol{index}_{solint}.G{index}")
    # CASA table names must be directory-like; ensure no forbidden chars:
    #caltable = caltable.replace(":", "").replace("/", "_")
    solve_gain_phase(old_ms, caltable, solint, args)
    #os.makedirs(os.path.join(os.path.dirname(ms), 'caltables'))
    
    figfile = os.path.join(real_plotdir, os.path.basename(caltable) + ".selfcal.png")
    #plot_solutions(caltable, figfile)
    #produced_tables.append(caltable)

    caltables = [caltable]
    apply_gain(old_ms, new_ms, caltables, args)
    print("Self-cal complete. Solutions applied to CORRECTED_DATA. You can image that column.")
    print(f"Produced caltables: {caltable}")
    #for t in produced_tables:
    #print(f"  - {t}")

if __name__ == "__main__":
    main()
