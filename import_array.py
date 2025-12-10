#!/usr/bin/env python3

import argparse
import os
import sys
import shutil
from casatasks import importuvfits
import casaconfig
casaconfig.logfile = "/dev/null"

def main():
    parser = argparse.ArgumentParser(
        description="Import a selected UVFITS file into a Measurement Set."
    )

    parser.add_argument(
        "-i", "--index",
        type=int,
        required=True,
        help="Index of the UVFITS file to process."
    )

    parser.add_argument(
        "-f", "--files",
        nargs="+",
        required=True,
        help="List of UVFITS files to process (space-separated)."
    )
    parser.add_argument(
        "--no-clobber",
        action="store_true",
        help="Do not clobber (overwrite) existing uvfits files"
    )

    args = parser.parse_args()
    uvfitsfiles = sorted(args.files)
    print(len(uvfitsfiles))

    try:
        uvfile = uvfitsfiles[args.index]
    except IndexError:
        print(f"Error: index {args.index} is out of range for {len(uvfitsfiles)} files.")
        sys.exit(1)

    msfile = uvfile.replace(".uvfits", ".ms")

    if os.path.exists(msfile):
        if not args.no_clobber:
            shutil.rmtree(msfile)
            importuvfits(fitsfile=uvfile, vis=msfile)
        else:
            raise RuntimeError(f"no_clobber is set to {args.no_clobber} but {msfile} already exists")
    else:
        importuvfits(fitsfile=uvfile, vis=msfile)

if __name__ == "__main__":
    main()
