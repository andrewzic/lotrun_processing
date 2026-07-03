#!/usr/bin/env python3
import casaconfig
casaconfig.logfile = "/dev/null"
import argparse
import os
import sys
import shutil
from casatasks import importuvfits


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
        "--clobber",
        action="store_true",
        help="Clobber (overwrite) existing ms files"
    )

    args = parser.parse_args()
    uvfitsfiles = sorted(args.files)
    print(len(uvfitsfiles))

    try:
        uvfile = uvfitsfiles[args.index]
    except IndexError:
        print(f"IndexError: index {args.index} is out of range for {len(uvfitsfiles)} files.")
        sys.exit(1)

    msfile = uvfile.replace(".uvfits", ".ms")

    if os.path.exists(msfile):
        if args.clobber:
            print(f"INFO: clobber is set to {args.clobber} and {msfile} exists. Overwriting {msfile}")
            shutil.rmtree(msfile)
            importuvfits(fitsfile=uvfile, vis=msfile)
        else:
            print(f"INFO: clobber is set to {args.clobber} and {msfile} exists. Skipping {msfile}")
    else:
        importuvfits(fitsfile=uvfile, vis=msfile)

if __name__ == "__main__":
    main()
