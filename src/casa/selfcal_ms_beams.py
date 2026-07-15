#!/usr/bin/env python3
import argparse
import os
import shutil
from datetime import datetime
try:
    from casaconfig import config
    config.nologfile = True
    config.logfile = "/dev/null"
except Exception:
    try:
        import casaconfig
        casaconfig.logfile = "/dev/null"
    except Exception:
        pass

from ms_tools import has_model_column, solve_gain_phase, apply_gain, split_to_nspws, apply_gain_and_combine

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
    p.add_argument("--nspws", type=int, default=16, help="Number of spectral windows to split into for calibration.")
    return p.parse_args()

def main():
    args = parse_args()
    ms = args.ms
    index = args.index
    
    solint = args.solint
    print(f"[{datetime.now().isoformat()}] RUNNING SELF CALIBRATION: ms={ms}; INDEX={index}; solint={solint}; nspws={args.nspws}")
    if args.index == 1:
        old_ms = args.ms
    elif args.index > 1:
        if "cracoData" in args.ms:
            # craco data
            old_ms = args.ms.replace(".calB0.ms", f".selfcal_{args.index-1}.ms")
        elif "scienceData" in args.ms:
            # continuum data is already bp calibrated and ends with _averaged_cal.leakage.ms
            old_ms = args.ms.replace("_averaged_cal.leakage.ms", f".selfcal_{args.index-1}.ms")
    else:
        raise ValueError(f"{args.index} make nossensens")
    
    if "cracoData" in args.ms:
        # craco data
        new_ms = args.ms.replace(".calB0.ms", f".selfcal_{args.index}.ms")
    elif "scienceData" in args.ms:
        # continuum data is already bp calibrated and ends with _averaged_cal.leakage.ms
        new_ms = args.ms.replace("_averaged_cal.leakage.ms", f".selfcal_{args.index}.ms")

    print(f"[{datetime.now().isoformat()}] SELF CALIBRATION WILL BE DERIVED FROM {old_ms}, CORRECTED DATA WILL BE SAVED TO {new_ms}")
    
    real_plotdir=os.path.join(os.path.dirname(ms), args.plot_dir)
    os.makedirs(real_plotdir, exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(ms), 'caltables'), exist_ok=True)
    
    if not has_model_column(old_ms):
        raise ValueError("ERROR: MODEL_DATA column not found; gaincal divides DATA by MODEL. Ensure you have predicted a model (e.g., via crystalball) before self-cal.")
    
    if "cracoData" in args.ms:
        # craco data
        caltable = os.path.join(os.path.dirname(ms), "caltables", f"{os.path.basename(ms).replace('.calB0.ms', '')}_{args.caltable_prefix}.sol{index}_{solint}.G{index}")
    elif "scienceData" in args.ms:
        # continuum data is already bp calibrated and ends with _averaged_cal.leakage.ms
        caltable = os.path.join(os.path.dirname(ms), "caltables", f"{os.path.basename(ms).replace('_averaged_cal.leakage.ms', '')}_{args.caltable_prefix}.sol{index}_{solint}.G{index}")

    if args.nspws > 1:
        # Multi-SPW workflow
        old_ms_multispw = split_to_nspws(old_ms, args.nspws)

        # Solve for gains on the split multi-SPW MS
        solve_gain_phase(old_ms_multispw, caltable, solint, args)

        # Apply calibration to the split multi-SPW MS and combine back
        apply_gain_and_combine(old_ms_multispw, old_ms, new_ms, caltable, args)

    else:
        # Standard single-SPW workflow
        solve_gain_phase(old_ms, caltable, solint, args)
        caltables = [caltable]
        apply_gain(old_ms, new_ms, caltables, args)

    print("Self-cal complete. Solutions applied to CORRECTED_DATA. You can image that column.")
    print(f"Produced caltables: {caltable}")

if __name__ == "__main__":
    main()
