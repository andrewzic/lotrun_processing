#!/usr/bin/env python3
import argparse
import glob
import os
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Run CASA applycal on MS files for specified beams (SBID-aware).")
    parser.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g., SB77974")
    parser.add_argument("--data-root", default="data", help="Root directory containing data/<SBID>")
    parser.add_argument("--pattern", default="*/*beam{beam:02d}*.avg.ms", help="Relative glob under data-root/SBID (format string with {beam:02d})")
    parser.add_argument("--beam", type=int, help="Single beam index to process (0..36)")
    parser.add_argument("--beams", default="all", help='Comma-separated list (e.g., "0,5,12") or "all" for 0..36')
    parser.add_argument("--dry-run", action="store_true", help="List planned operations without running applycal")
    return parser.parse_args()

def ensure_casa_applycal() -> bool:
    try:
        from casatasks import applycal  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.applycal not available: {e}", file=sys.stderr)
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
    from casatasks import applycal
    print(f"Applying cal: {caltable} -> {msname}")
    # Interpolation list as per your example; adjust if you have multiple gaintables
    applycal(vis=msname, gaintable=[caltable], interp=['nearest', 'linear'])

def run_clearcal(msname: str):
    from casatasks import clearcal
    print(f"Clearing cal -> {msname}")
    # Interpolation list as per your example; adjust if you have multiple gaintables
    clearcal(vis=msname)

def main():
    args = parse_args()

    if args.beam is not None:
        beams = [args.beam]
    else:
        beams = list(range(0, 37)) if args.beams == "all" else [int(x) for x in args.beams.split(",")]

    if not args.dry_run and not ensure_casa_applycal():
        sys.exit(1)

    exit_code = 0
    for beam in beams:
        try:
            ms_list = find_ms_for_beam(args.data_root, args.sbid, args.pattern, beam)
            if not ms_list:
                print(f"WARN: No MS found under '{args.data_root}/{args.sbid}' for beam {beam:02d} with pattern '{args.pattern}'")
                continue
            for msname in ms_list:
                print(f"  MS: {msname}")
                if not args.dry_run:
                    run_clearcal(msname)
        except Exception as e:
            print(f"ERROR: Beam {beam:02d} failed: {e}", file=sys.stderr)
            exit_code = 2

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
