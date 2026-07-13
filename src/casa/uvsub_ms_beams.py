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

from ms_tools import ensure_casatasks, run_uvsub

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

    if not args.dry_run:
        run_uvsub(ms, out_prefix=out_prefix)

    # try:
    #     if not args.dry_run:
    #         run_uvsub(ms, out_prefix=out_prefix)
    # except Exception as e:
    #     print(f"ERROR: uvsub failed: {e}", file=sys.stderr)
    #     exit_code = 2

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
