#!/usr/bin/env python3
import argparse
import sys
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

from ms_tools import ensure_casa_flagdata, find_ms_files, run_flag_outer_antennas

def parse_args():
    p = argparse.ArgumentParser(description="CASA: flagdata(mode='manual', antenna='24~35') on MS files per beam (SBID-aware)")
    p.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g., SB82418")
    p.add_argument("--data-root", default="data", help="Root directory containing data/<SBID>")
    p.add_argument("--pattern", default="*/scienceData*beam{beam:02d}*.ms", help="Relative glob under data-root/SBID (format string with {beam:02d})")
    p.add_argument("--beam", type=int, help="Single beam index to process (0..36)")
    p.add_argument("--beams", default="all", help='Comma-separated list (e.g., "0,5,12") or "all" for 0..36')
    p.add_argument("--dry-run", action="store_true", help="List planned operations without running flagdata")
    return p.parse_args()

def main():
    args = parse_args()
    if args.beam is not None:
        beams = [args.beam]
    else:
        beams = list(range(0, 37)) if args.beams == "all" else [int(x) for x in args.beams.split(",")]
    if not args.dry_run and not ensure_casa_flagdata():
        sys.exit(1)
    exit_code = 0
    for beam in beams:
        # try:
        ms_list = find_ms_files(args.data_root, args.sbid, args.pattern, beam)
        if not ms_list:
            print(f"WARN: No MS found under '{args.data_root}/{args.sbid}' for beam {beam:02d} with pattern '{args.pattern}'")
            continue
        for msname in ms_list:
            # If we see a .ms.tar, skip with a warning (expect extraction done beforehand)
            if msname.endswith('.ms.tar'):
                print(f"SKIP: {msname} is a .ms.tar archive (extract before running)")
                continue
            print(f" MS: {msname}")
            if not args.dry_run:
                run_flag_outer_antennas(msname)
        # except Exception as e:
        #     print(f"ERROR: Beam {beam:02d} failed: {e}", file=sys.stderr)
        #     exit_code = 2
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
