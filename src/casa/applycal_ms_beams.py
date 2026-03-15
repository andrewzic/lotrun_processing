#!/usr/bin/env python3
import casaconfig
casaconfig.logfile = "/dev/null"
import argparse
import sys
from ms_tools import ensure_casa_applycal, find_ms_for_beam, find_caltables, run_applycal


def parse_args():
    parser = argparse.ArgumentParser(description="Run CASA applycal on MS files for specified beams (SBID-aware).")
    parser.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g., SB77974")
    parser.add_argument("--data-root", default="data", help="Root directory containing data/<SBID>")
    parser.add_argument("--pattern", default="*beam{beam:02d}*.avg.ms", help="Relative glob under data-root/SBID (format string with {beam:02d})")
    parser.add_argument("--cal-dir", required=True, help="Directory containing calibration tables under data-root/SBID (expects *beamXX*.B0)")
    parser.add_argument("--beam", type=int, help="Single beam index to process (0..36)")
    parser.add_argument("--beams", default="all", help='Comma-separated list (e.g., "0,5,12") or "all" for 0..36')
    parser.add_argument("--extension", default="B0", help='Gain table extension (e.g. "B0", "G5" etc.) to specify which calibrationt table to apply for beams. Use a wildcard like "G*" to automatically select the highest numbered Gaintable extension available for each beam (e.g. if G1, G2, G3 are present, it will apply G3). Default is "B0" which applies the initial calibration table without selfcal solutions.')
    parser.add_argument("--dry-run", action="store_true", help="List planned operations without running applycal")
    parser.add_argument("--delete-previous", action="store_true", help="Delete previous generation ms split to save filesystem errors")
    return parser.parse_args()


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
        # try:
        ms_list = find_ms_for_beam(args.data_root, args.sbid, args.pattern, beam)
        if not ms_list:
            print(f"WARN: No MS found under '{args.data_root}/{args.sbid}' for beam {beam:02d} with pattern '{args.pattern}'")
            continue
        caltables = find_caltables(args.data_root, args.sbid, args.cal_dir, beam, extension=args.extension)
        print(f"Beam {beam:02d}: {len(ms_list)} MS found; using caltables: {caltables}")
        for msname in ms_list:
            print(f"running applycal on  MS: {msname}")
            if not args.dry_run:
                run_applycal(msname, caltables, delete_previous=args.delete_previous)
        # except Exception as e:
        #     print(f"ERROR: Beam {beam:02d} failed: {e}", file=sys.stderr)
        #     exit_code = 2

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
