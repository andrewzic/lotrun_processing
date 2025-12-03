#!/usr/bin/env python3
import argparse
import glob
import os
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Run CASA uvsub on MS files for specified beams (SBID-aware).") 
    parser.add_argument("--ms", required=True, help="Path to the measurement set.")
    parser.add_argument("--index", required=True, type=int, help="selfcal index to use for book-keeping purposes")
    # parser.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g., SB77974")
    # parser.add_argument("--data-root", default="data", help="Root directory containing data/<SBID>")
    # parser.add_argument("--pattern", default="*beam{beam:02d}*.avg.ms", help="Relative glob under data-root/SBID (format string with {beam:02d})")
    # parser.add_argument("--beam", type=int, help="Single beam index to process (0..36)")
    # parser.add_argument("--beams", default="all", help='Comma-separated list (e.g., "0,5,12") or "all" for 0..36')
    parser.add_argument("--out-prefix", default="uvsub", help="label for uvsub file")
    parser.add_argument("--dry-run", action="store_true", help="List planned operations without running uvsub")
    return parser.parse_args()

def ensure_casatasks() -> bool:
    try:
        from casatasks import applycal, uvsub, split  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.applycal, uvsub, split not available: {e}", file=sys.stderr)
        return False

def find_ms_for_beam(data_root: str, sbid: str, pattern: str, beam: int) -> list:
    root = os.path.join(data_root, sbid)
    pat = os.path.join(root, pattern.format(beam=beam))
    return sorted(glob.glob(pat))

def find_caltable(data_root: str, sbid: str, cal_dir: str, beam: int) -> str:
    root = os.path.join(data_root, sbid, cal_dir)
    matches = sorted(glob.glob(os.path.join(root, f"*beam{beam:02d}*.B0")))
    if not matches:
        raise FileNotFoundError(f"No cal table found in '{root}' for beam {beam:02d} matching '*beam{beam:02d}*.B0'")
    return matches[0]

def run_applycal(msname: str, caltable: str):
    from casatasks import applycal, split
    print(f"Applying cal: {caltable} -> {msname}")
    # Interpolation list as per your example; adjust if you have multiple gaintables
    applycal(vis=msname, gaintable=[caltable], interp=['nearest', 'linear'])
    outputvis = msname.replace('.ms', '.calB0.ms')
    split(vis=msname, outputvis=outputvis, datacolumn="corrected")

def run_clearcal(msname: str):
    from casatasks import clearcal
    print(f"Applying cal: {caltable} -> {msname}")
    # Interpolation list as per your example; adjust if you have multiple gaintables
    clearcal(vis=msname)

def run_uvsub(msname: str, out_prefix: str = "uvsub") -> None:
    from casatasks import uvsub, split
    outputvis = msname.replace(".ms", f".{out_prefix}.ms")
    print(f"Running uvsub: {msname} -> {outputvis}")
    uvsub(vis=msname)
    split(vis=msname, outputvis=outputvis, datacolumn="corrected")
    
def main():
    args = parse_args()
    ms = args.ms
    idx = args.index
    out_prefix = args.out_prefix
    
    # if args.index == 1:
    #     old_ms = args.ms
    # elif args.index > 1:
    #     old_ms = args.ms.replace(".calB0.ms", f".selfcal_{args.index-1}.ms")
    # else:
    #     raise ValueError(f"{args.index} make nossensens")
    
    # new_ms = old_ms.replace(".calB0.ms", f".selfcal_{args.index}.ms")
    
    if not args.dry_run and not ensure_casatasks():
        sys.exit(1)

    exit_code = 0

    try:
        if not args.dry_run:
            run_uvsub(ms, out_prefix=out_prefix)
    except Exception as e:
        print(f"ERROR: Beam {beam:02d} failed: {e}", file=sys.stderr)
        exit_code = 2

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
