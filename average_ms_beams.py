#!/usr/bin/env python3
import argparse
import glob
import os
import sys
import re

def parse_args():
    p = argparse.ArgumentParser(description="Phase-only self-calibration loop in CASA.")
    p.add_argument("--ms", required=True, help="Path to the measurement set.")
    p.add_argument("--timebin", default="9.90s", help="average time bin.")

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

def do_average(msname: str, outputvis: str, timebin: str='9.90s'):
    from casatasks import mstransform
    print(f"averaging {msname} -> {outputvis}")
    mstransform(vis=msname, outputvis=new_msname, timeaverage=True, timebin=timebin, datacolumn='all')
    

def main():
    args = parse_args()
    ms = args.ms
    timebin = args.timebin
    if 'cal' in msname:
        new_msname = msname.replace('.cal', '.avg.cal')
    else:
        new_msname = msname.replace('.ms', '.avg.ms')    
    do_average(ms, new_msname, timebin=timebin)

if __name__ == "__main__":
    main()
