#!/usr/bin/env python3
import argparse
import glob
import os
import sys
import re
from casatools import table
from casatasks import applycal, split

def parse_args():
    parser = argparse.ArgumentParser(description="Run CASA applycal on MS files for specified beams (SBID-aware).")
    parser.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g., SB77974")
    parser.add_argument("--data-root", default="data", help="Root directory containing data/<SBID>")
    parser.add_argument("--pattern", default="*beam{beam:02d}*.avg.ms", help="Relative glob under data-root/SBID (format string with {beam:02d})")
    parser.add_argument("--cal-dir", required=True, help="Directory containing calibration tables under data-root/SBID (expects *beamXX*.B0)")
    parser.add_argument("--beam", type=int, help="Single beam index to process (0..36)")
    parser.add_argument("--beams", default="all", help='Comma-separated list (e.g., "0,5,12") or "all" for 0..36')
    parser.add_argument("--extension", default="B0", help='Gain table extension (e.g. "B0", "G5" etc.) to specify which calibrationt table to apply for beams')
    parser.add_argument("--dry-run", action="store_true", help="List planned operations without running applycal")
    parser.add_argument("--delete-previous", action="store_true", help="Delete previous generation ms split to save filesystem errors")
    return parser.parse_args()

def ensure_casa_applycal() -> bool:
    try:
        from casatasks import applycal  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.applycal not available: {e}", file=sys.stderr)
        return False


def _ms_nrows(ms_path: str) -> int:
    """Return the number of rows in the main table of a Measurement Set."""
    tb = table()
    try:
        tb.open(ms_path)
        n = tb.nrows()
    finally:
        try:
            tb.close()
        except Exception:
            pass
    return int(n)
    
def find_ms_for_beam(data_root: str, sbid: str, pattern: str, beam: int) -> list:
    root = os.path.join(data_root, sbid)
    pat = os.path.join(root, pattern.format(beam=beam))
    return sorted(glob.glob(pat))

def find_caltable(data_root: str, sbid: str, cal_dir: str, beam: int, extension: str="B0") -> str:
    root = os.path.join(data_root, sbid, cal_dir)
    matches = sorted(glob.glob(os.path.join(root, f"*beam{beam:02d}*.{extension}")))
    if not matches:
        raise FileNotFoundError(f"No cal table found in '{root}' for beam {beam:02d} matching '*beam{beam:02d}*.{extension}'")
    return matches[0]

def validate_and_clean_ms(msname: str, outputvis: str, delete_previous: bool=True) -> bool:
    # Basic existence check
    if not os.path.isdir(outputvis):
        raise RuntimeError(f"Split did not produce output MS: {outputvis}")

    # Validate row counts (must match)
    old_rows = _ms_nrows(msname)
    new_rows = _ms_nrows(outputvis)
    print(f"Row count check: old={old_rows} new={new_rows}")

    if new_rows != old_rows:
        raise RuntimeError(
            f"Row count mismatch after split: {msname} has {old_rows}, {outputvis} has {new_rows}"
        )

    # Optional additional size heuristic (can be enabled if desired)
    # total_size_bytes = _dir_size_bytes(outputvis)  # implement if you want size checks

    # Delete previous generation MS only after successful validation
    if delete_previous:
        try:
            print(f"Removing previous MS: {msname}")
            shutil.rmtree(msname)
        except Exception as e:
            # fail the whole task if deletion has filesystem hiccups
            raise RuntimeError(f"ERROR: Failed to remove {msname}: {e}")

    return True


def run_applycal(msname: str, caltable: str, extension: str = "B0", delete_previous: bool = False) -> str:
    """
    Apply a calibration table to 'msname' and split the corrected data to a new MS
    labeled with '.cal{extension}.ms'. If validation succeeds, delete the previous
    generation MS ('msname') to control disk usage.

    Returns:
        The path to the newly created output MS.
    """
    print(f"Applying cal: {caltable} -> {msname}")
    
    time_interp = "nearest" if extension == "B0" else "linear"
    freq_interp = "linear"
    
    applycal(vis=msname, gaintable=[caltable], interp=[time_interp, freq_interp])
    #replace e.g. .calG1.ms with .calG6
    if "cal" in msname:
        outputvis = re.sub(r'\.cal(?:B0|G\d+)\.ms', f".cal{extension}.ms", msname)
    else:
        outputvis = msname.replace(".ms", f".cal{extension}.ms")
    if outputvis == msname:
        raise ValueError(f"Output measurement set name {outputvis} matches input {msname} ya nong.")
    
    split(vis=msname, outputvis=outputvis, datacolumn="corrected")    
    success = validate_and_clean_ms(msname, outputvis, delete_previous=delete_previous)

    print(f"Completed applycal+split: {outputvis}")
    return outputvis

def run_clearcal(msname: str):
    from casatasks import clearcal
    print(f"Applying cal: {caltable} -> {msname}")
    # Interpolation list as per your example; adjust if you have multiple gaintables
    clearcal(vis=msname)

def main():
    args = parse_args()

    if args.beam is not None:
        beams = [args.beam]
    else:
        beams = list(range(0, 36)) if args.beams == "all" else [int(x) for x in args.beams.split(",")]

    if not args.dry_run and not ensure_casa_applycal():
        sys.exit(1)

    exit_code = 0
    for beam in beams:
        try:
            ms_list = find_ms_for_beam(args.data_root, args.sbid, args.pattern, beam)
            if not ms_list:
                print(f"WARN: No MS found under '{args.data_root}/{args.sbid}' for beam {beam:02d} with pattern '{args.pattern}'")
                continue
            caltable = find_caltable(args.data_root, args.sbid, args.cal_dir, beam, extension=args.extension)
            print(f"Beam {beam:02d}: {len(ms_list)} MS found; using caltable: {caltable}")
            for msname in ms_list:
                print(f"  MS: {msname}")
                if not args.dry_run:
                    run_applycal(msname, caltable, extension=args.extension, delete_previous=args.delete_previous)
        except Exception as e:
            print(f"ERROR: Beam {beam:02d} failed: {e}", file=sys.stderr)
            exit_code = 2

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
