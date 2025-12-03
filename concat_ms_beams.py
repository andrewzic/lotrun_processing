#!/usr/bin/env python3
import argparse
import glob
import os
import sys
import re

def find_ms_files(data_root: str, sbid: str, beam: int, pattern: str) -> list:
    """
    Find measurement sets using a format string pattern under data_root/sbid.
    The pattern may include {beam:02d}.
    """
    root = os.path.join(data_root, sbid)
    pat = os.path.join(root, pattern.format(beam=beam))
    print(pat)
    return sorted(glob.glob(pat))

def ensure_casa_concat():
    try:
        from casatasks import concat  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.concat not available: {e}", file=sys.stderr)
        return False

def strip_scanid_from_path(ms_path: str, sbid: str, out_root: str) -> str:
    """
    From an input path like:
      data/SB77974/20251015072402/cracoData.LTR_1733-2344.SB77974.beam17.20251015072402.avg.ms
    produce:
      <out_root>/SB77974/cracoData.LTR_1733-2344.SB77974.beam17.avg.ms
    """
    # Split into directory and filename
    d, fname = os.path.split(ms_path)

    # Remove the immediate scanid directory if it is a 14-digit datetime
    parts = d.split(os.sep)
    if len(parts) >= 2 and re.fullmatch(r"\d{14}", parts[-1]):
        d_without_scanid = os.sep.join(parts[:-1])
    else:
        d_without_scanid = d

    # Remove .<14digits>. from filename (e.g., ".20251015072402.")
    fname_clean = re.sub(r"\.(\d{14})\.", ".", fname)

    # Place output under out_root/SBID
    out_dir = os.path.join(out_root, sbid)
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, fname_clean)

    
def do_concat(msnames: list, output_path: str):
    from casatasks import concat
    print(f"Concatenating {len(msnames)} MS -> {output_path}")
    concat(msnames, concatvis=output_path, timesort=True)

def parse_args():
    parser = argparse.ArgumentParser(description="Concatenate MS per beam for a given SBID.")
    parser.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g., SB77974")
    parser.add_argument("--data-root", default="data", help="Root directory containing data/<SBID>")
    parser.add_argument("--out-root", default=None, help="Output root (default: same as --data-root)")
    parser.add_argument(
        "--pattern",
        default="*/*beam{beam:02d}*.avg.ms",
        help="Relative glob pattern under data-root/SBID (format string with {beam:02d})"
    )
    parser.add_argument("--beam", type=int, help="Single beam index to process (0..36)")
    parser.add_argument(
        "--beams",
        default="all",
        help='Comma-separated beam list (e.g., "0,1,2") or "all" for 0..36'
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List planned operations without running concat"
    )
    return parser.parse_args()

def main():
    args = parse_args()

    if args.beam is not None:
        beams = [args.beam]
    else:
        beams = list(range(0, 36)) if args.beams == "all" else [int(x) for x in args.beams.split(",")]

    out_root = args.out_root or args.data_root
    out_dir = os.path.join(out_root, args.sbid)
    os.makedirs(out_dir, exist_ok=True)

    if not args.dry_run and not ensure_casa_concat():
        sys.exit(1)

    any_work = False
    for beam in beams:
        msnames = find_ms_files(args.data_root, args.sbid, beam, args.pattern)
        if not msnames:
            print(f"WARN: No MS found for SBID={args.sbid} beam={beam:02d} using pattern '{args.pattern}'")
            continue

        output_msname = strip_scanid_from_path(msnames[0], args.sbid, out_root)
        print(f"Beam {beam:02d}: {len(msnames)} inputs")
        for m in msnames:
            print(f"  - {m}")
        print(f"  -> {output_msname}")

        if not args.dry_run:
            any_work = True
            do_concat(msnames, output_msname)

    if not any_work and not args.dry_run:
        print("No concatenations performed (no inputs found).", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
