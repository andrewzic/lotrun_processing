#!/usr/bin/env python3
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
import argparse
import sys
from ms_tools import ensure_casa_applycal, find_ms_files, find_caltables, run_applycal


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

    import os
    import shutil
    import re
    from ms_tools import remove_ms_safely

    last_index = os.environ.get("LAST_INDEX")
    if last_index and args.extension == "G*":
        target_ext = f"G{last_index}"
    else:
        target_ext = None

    exit_code = 0
    for beam in beams:
        ms_list = find_ms_files(args.data_root, args.sbid, args.pattern, beam)
        if not ms_list:
            print(f"WARN: No MS found under '{args.data_root}/{args.sbid}' for beam {beam:02d} with pattern '{args.pattern}'")
            continue
            
        # Dynamically infer the target extension from existing selfcal MS files in the directory
        beam_target_ext = target_ext
        if not beam_target_ext and args.extension == "G*":
            import glob
            beam_dir = os.path.dirname(ms_list[0])
            sfiles = glob.glob(os.path.join(beam_dir, f"*beam{beam:02d}*.selfcal_*.ms"))
            max_idx = 0
            for sf in sfiles:
                match = re.search(r'selfcal_(\d+)\.ms', sf)
                if match:
                    idx = int(match.group(1))
                    if idx > max_idx:
                        max_idx = idx
            if max_idx > 0:
                beam_target_ext = f"G{max_idx}"
                print(f"Auto-detected target extension: {beam_target_ext}")

        try:
            caltables = find_caltables(args.data_root, args.sbid, args.cal_dir, beam, extension=args.extension)
            print(f"Beam {beam:02d}: {len(ms_list)} MS found; using caltables: {caltables}")
            for msname in ms_list:
                print(f"running applycal on  MS: {msname}")
                if not args.dry_run:
                    run_applycal(msname, caltables, delete_previous=args.delete_previous, output_extension=beam_target_ext)
        except FileNotFoundError as e:
            # If no caltables are found (e.g. selfcal failed at stage 1 or all G* tables deleted/moved)
            print(f"WARN: No calibration tables found for beam {beam:02d}: {e}")
            print(f"Bypassing applycal and copying MS files forward to expected output name.")
            for msname in ms_list:
                ext = beam_target_ext if beam_target_ext else "G1"
                output_extension_str = f".cal{ext}.ms"
                if "cal" in msname:
                    outputvis = re.sub(r'\.cal(?:B0|G\d+)\.ms', output_extension_str, msname)
                else:
                    outputvis = msname.replace(".ms", output_extension_str)
                
                print(f"Copying {msname} -> {outputvis}")
                if not args.dry_run:
                    remove_ms_safely(outputvis)
                    shutil.copytree(msname, outputvis)
                    if args.delete_previous:
                        remove_ms_safely(msname)
        # except Exception as e:
        #     print(f"ERROR: Beam {beam:02d} failed: {e}", file=sys.stderr)
        #     exit_code = 2

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
