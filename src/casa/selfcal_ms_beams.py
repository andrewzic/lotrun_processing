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

from ms_tools import has_model_column, solve_gain_phase, apply_gain, split_to_nspws, apply_gain_and_combine, check_caltable_quality, remove_ms_safely
import re

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
    # Construct quality flag path based on beam ID
    beam_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if not beam_id:
        from ms_tools import parse_ms_filename
        info = parse_ms_filename(args.ms)
        beam_match = re.search(r'\d+', info.get("beam", ""))
        beam_id = beam_match.group(0) if beam_match else "0"
    
    flag_file = os.path.join(os.path.dirname(ms), f"beam{int(beam_id):02d}_selfcal_failed.flag")
    
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

    # Check if a selfcal round has already failed
    if os.path.exists(flag_file):
        print(f"[{datetime.now().isoformat()}] Flag file {flag_file} detected. Bypassing calibration and copying data.")
        
        # Mirror copy to logs directory if missing
        local_flag_file = f"logs/beam{int(beam_id):02d}_selfcal_failed.flag"
        if not os.path.exists(local_flag_file):
            try:
                os.makedirs(os.path.dirname(os.path.abspath(local_flag_file)), exist_ok=True)
                shutil.copy2(flag_file, local_flag_file)
                print(f"[{datetime.now().isoformat()}] Restored flag file to logs directory: {local_flag_file}")
            except Exception as e:
                print(f"Warning: Failed to copy flag file to {local_flag_file}: {e}")

        remove_ms_safely(new_ms)
        shutil.copytree(old_ms, new_ms)
        print(f"[{datetime.now().isoformat()}] Bypass complete. Copied {old_ms} -> {new_ms}")
        return

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

    qc_passed = True
    qc_report_path = ms.replace(".ms", ".selfcal_qc_report.log")

    if args.nspws > 1:
        # Multi-SPW workflow
        old_ms_multispw = split_to_nspws(old_ms, args.nspws)

        # Solve for gains on the split multi-SPW MS
        solve_gain_phase(old_ms_multispw, caltable, solint, args)

        # Check caltable quality
        if os.path.exists(caltable):
            qc_passed = check_caltable_quality(caltable, qc_report_path)
        else:
            print(f"[{datetime.now().isoformat()}] Warning: Caltable {caltable} was not created by gaincal.")
            qc_passed = False

        if qc_passed:
            # Apply calibration to the split multi-SPW MS and combine back
            apply_gain_and_combine(old_ms_multispw, old_ms, new_ms, caltable, args)
        else:
            remove_ms_safely(old_ms_multispw, ignore_errors=True)

    else:
        # Standard single-SPW workflow
        solve_gain_phase(old_ms, caltable, solint, args)
        
        # Check caltable quality
        if os.path.exists(caltable):
            qc_passed = check_caltable_quality(caltable, qc_report_path)
        else:
            print(f"[{datetime.now().isoformat()}] Warning: Caltable {caltable} was not created by gaincal.")
            qc_passed = False

        if qc_passed:
            caltables = [caltable]
            apply_gain(old_ms, new_ms, caltables, args)

    if not qc_passed:
        print(f"[{datetime.now().isoformat()}] selfcal failed quality control check! Writing flag file {flag_file} and copying MS.")
        
        # Move the bad caltable to a 'bad' subdirectory
        if os.path.exists(caltable):
            bad_dir = os.path.join(os.path.dirname(caltable), "bad")
            try:
                os.makedirs(bad_dir, exist_ok=True)
                dest = os.path.join(bad_dir, os.path.basename(caltable))
                if os.path.exists(dest):
                    remove_ms_safely(dest, ignore_errors=True)
                shutil.move(caltable, dest)
                print(f"[{datetime.now().isoformat()}] Moved bad caltable {caltable} -> {dest}")
            except Exception as e:
                print(f"Warning: Failed to move bad caltable to bad/ directory: {e}. Nuking the bad caltable instead.")
                remove_ms_safely(caltable, ignore_errors=True)
                
        last_tag = f"selfcal_{index-1}" if index > 1 else "initial_scratch"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(flag_file)), exist_ok=True)
            with open(flag_file, "w") as f:
                f.write(f"LAST_SUCCESSFUL_INDEX={index-1}\n")
                f.write(f"LAST_SUCCESSFUL_TAG={last_tag}\n\n")
                f.write("=== QUALITY CONTROL REPORT ===\n")
                if os.path.exists(qc_report_path):
                    with open(qc_report_path, "r") as rf:
                        f.write(rf.read())
                else:
                    f.write("Failed to generate QC report log.\n")
            print(f"[{datetime.now().isoformat()}] Flag file written successfully to {flag_file}.")
        except Exception as e:
            print(f"Error writing flag file {flag_file}: {e}")

        # Also write a copy to the logs directory in the workspace root for quick inspection
        local_flag_file = f"logs/beam{int(beam_id):02d}_selfcal_failed.flag"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(local_flag_file)), exist_ok=True)
            shutil.copy2(flag_file, local_flag_file)
            print(f"[{datetime.now().isoformat()}] Copied flag file to logs directory: {local_flag_file}")
        except Exception as e:
            print(f"Warning: Failed to copy flag file to {local_flag_file}: {e}")

        remove_ms_safely(new_ms)
        shutil.copytree(old_ms, new_ms)
        print(f"[{datetime.now().isoformat()}] Copy complete: {old_ms} -> {new_ms}")
        return

    print("Self-cal complete. Solutions applied to CORRECTED_DATA. You can image that column.")
    print(f"Produced caltables: {caltable}")

if __name__ == "__main__":
    main()
