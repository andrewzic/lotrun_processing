#!/usr/bin/env python3

import argparse
import os
import sys
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

    args = parser.parse_args()
    uvfitsfiles = sorted(args.files)
    print(len(uvfitsfiles))

    try:
        uvfile = uvfitsfiles[args.index]
    except IndexError:
        print(f"Error: index {args.index} is out of range for {len(uvfitsfiles)} files.")
        sys.exit(1)

    msfile = uvfile.replace(".uvfits", ".ms")

    if not os.path.exists(msfile):
        importuvfits(fitsfile=uvfile, vis=msfile)
    else:
        print(f"MS already exists: {msfile}")

if __name__ == "__main__":
    main()
