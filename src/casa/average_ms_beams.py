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
from ms_tools import do_average

def parse_args():
    p = argparse.ArgumentParser(description="Phase-only self-calibration loop in CASA.")
    p.add_argument("--ms", required=True, help="Path to the measurement set.")
    p.add_argument("--timebin", default="9.90s", help="average time bin.")
    return p.parse_args()    

def main():
    args = parse_args()
    msname = args.ms
    timebin = args.timebin
    if 'cal' in msname:
        new_msname = msname.replace('.cal', '.avg.cal')
    else:
        new_msname = msname.replace('.ms', '.avg.ms')    
    do_average(msname, new_msname, timebin=timebin)

if __name__ == "__main__":
    main()
