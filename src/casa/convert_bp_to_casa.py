#!/usr/bin/env python3
"""
convert_bp_to_casa.py
=====================
Converts ASKAP continuum bandpass calibration tables (calparameters.*_bp.*.tab)
from CASDA / ASKAPsoft into CASA B Jones calibration tables (contCal.<sbid>.beamXX.B0).

Key features:
  - Extracts full per-channel (288 x 1 MHz) bandpass solutions for each beam.
  - Combines dual-polarization X and Y gains into total-intensity Stokes I gains.
  - Flags antennas/channels that are invalid (e.g. BANDPASS_VALID=False or non-finite).
  - Can convert a single beam or batch-convert all 36 beams in one fast pass (<2s).
  - Dual-backend support: runs seamlessly under casacore (bare-metal) or casatools (CASA container).
  - Auto-discovers calibration tables and templates if paths are not explicitly passed.
"""

import os
import sys
import glob
import shutil
import argparse
from typing import List, Tuple, Optional
import numpy as np

# Detect available table library
BACKEND = None
try:
    import casacore.tables as ct
    BACKEND = "casacore"
except ImportError:
    try:
        from casatools import table
        BACKEND = "casatools"
    except ImportError:
        pass


def find_continuum_bandpass_table(data_root: str, sbid: str, cal_dir: Optional[str] = None) -> Optional[str]:
    """
    Search standard locations for the ASKAP continuum bandpass table.
    Supports data_root being either the parent folder or the SBID directory itself.
    """
    search_dirs = [
        os.path.join(data_root, sbid),
        data_root,
    ]
    candidates = []
    for base in search_dirs:
        if cal_dir:
            candidates.append(os.path.join(base, cal_dir, "calparameters.*_bp.*.tab"))
            candidates.append(os.path.join(base, cal_dir, "calparameters.*.tab"))
        candidates.extend([
            os.path.join(base, "CalibrationTables", "calparameters.*_bp.*.tab"),
            os.path.join(base, "casda", "CalibrationTables", "calparameters.*_bp.*.tab"),
            os.path.join(base, "cal_cont", "calparameters.*_bp.*.tab"),
            os.path.join(base, "calparameters.*_bp.*.tab"),
            os.path.join(base, "CalibrationTables", "calparameters.*.tab"),
        ])

    # Also check DATA_SRC_ROOT if defined in environment
    data_src_root = os.environ.get("DATA_SRC_ROOT")
    if data_src_root:
        for base in [os.path.join(data_src_root, sbid), data_src_root]:
            candidates.extend([
                os.path.join(base, "CalibrationTables", "calparameters.*_bp.*.tab"),
                os.path.join(base, "calparameters.*_bp.*.tab"),
            ])

    for pattern in candidates:
        matches = sorted(glob.glob(pattern))
        if matches:
            return matches[0]

    # Recursive fallback under search dirs
    for base in search_dirs:
        if os.path.isdir(base):
            for root, dirs, files in os.walk(base):
                for d in dirs:
                    if d.startswith("calparameters.") and (d.endswith("_bp.tab") or "_bp." in d):
                        return os.path.join(root, d)

    return None


def find_template_b0(data_root: str, sbid: str, beam: int = 0, cal_dir: Optional[str] = None) -> Optional[str]:
    """
    Find an existing CASA B0 table (e.g. from CRACO or previous conversion)
    to use as a metadata template.
    """
    search_dirs = [
        os.path.join(data_root, sbid),
        data_root,
    ]
    candidates = []
    for base in search_dirs:
        if cal_dir:
            candidates.extend([
                os.path.join(base, cal_dir, f"*beam{beam:02d}*.B0"),
                os.path.join(base, cal_dir, "*.B0"),
            ])
        candidates.extend([
            os.path.join(base, f"CRACO-Calibration-Tables-{sbid}", f"*beam{beam:02d}*.B0"),
            os.path.join(base, f"CRACO-Calibration-Tables-{sbid}", "*.B0"),
            os.path.join(base, "cal", f"*beam{beam:02d}*.B0"),
            os.path.join(base, "cal", "*.B0"),
            os.path.join(base, "casda", f"CRACO-Calibration-Tables-{sbid}", f"*beam{beam:02d}*.B0"),
            os.path.join(base, "casda", f"CRACO-Calibration-Tables-{sbid}", "*.B0"),
            os.path.join(base, "CalibrationTables", "*.B0"),
            os.path.join(base, "cal_cont", "*.B0"),
        ])

    data_src_root = os.environ.get("DATA_SRC_ROOT")
    if data_src_root:
        for base in [os.path.join(data_src_root, sbid), data_src_root]:
            if cal_dir:
                candidates.extend([
                    os.path.join(base, cal_dir, f"*beam{beam:02d}*.B0"),
                    os.path.join(base, cal_dir, "*.B0"),
                ])
            candidates.extend([
                os.path.join(base, f"CRACO-Calibration-Tables-{sbid}", f"*beam{beam:02d}*.B0"),
                os.path.join(base, f"CRACO-Calibration-Tables-{sbid}", "*.B0"),
                os.path.join(base, "cal", "*.B0"),
            ])

    for pattern in candidates:
        matches = sorted(glob.glob(pattern))
        if matches:
            return matches[0]

    return None


def read_bandpass_cube(cont_bp_tab_path: str):
    """
    Read the full BANDPASS and BANDPASS_VALID arrays from the table.
    Returns:
        bp_cube, val_cube, backend
    """
    if BACKEND is None:
        raise ImportError("Neither casacore.tables nor casatools.table is available in this Python environment.")

    if BACKEND == "casacore":
        tb = ct.table(cont_bp_tab_path, readonly=True)
        bp_cube = tb.getcol("BANDPASS")
        val_cube = tb.getcol("BANDPASS_VALID")
        tb.close()
    else:
        tb = table()
        tb.open(cont_bp_tab_path)
        bp_cube = tb.getcol("BANDPASS")
        val_cube = tb.getcol("BANDPASS_VALID")
        tb.close()

    return bp_cube, val_cube, BACKEND


def extract_beam_gains(bp_cube, val_cube, beam_idx: int, backend: str) -> Tuple[np.ndarray, np.ndarray]:
    """
    Extract total intensity Stokes I complex gains and flags for a given beam.

    Returns:
        gi: np.ndarray shape (n_ant, n_chan), complex128
        flag_i: np.ndarray shape (n_ant, n_chan), bool (True = flagged/bad)
    """
    if backend == "casacore":
        # Shape: (1, n_beams, n_ant, n_elem) where n_elem = n_chan * 2
        b_bp = bp_cube[0, beam_idx]  # (n_ant, n_elem)
        b_val = val_cube[0, beam_idx]  # (n_ant, n_elem)
        n_ant, n_elem = b_bp.shape
        n_chan = n_elem // 2
        b_bp = b_bp.reshape(n_ant, n_chan, 2)
        b_val = b_val.reshape(n_ant, n_chan, 2)
        gx = b_bp[:, :, 0]
        gy = b_bp[:, :, 1]
        vx = b_val[:, :, 0]
        vy = b_val[:, :, 1]
    else:
        # casatools Shape: (n_elem, n_ant, n_beams, 1)
        b_bp = bp_cube[:, :, beam_idx, 0]  # (n_elem, n_ant)
        b_val = val_cube[:, :, beam_idx, 0]  # (n_elem, n_ant)
        n_elem, n_ant = b_bp.shape
        n_chan = n_elem // 2
        b_bp_reshaped = b_bp.reshape(n_chan, 2, n_ant)
        b_val_reshaped = b_val.reshape(n_chan, 2, n_ant)
        gx = b_bp_reshaped[:, 0, :].T  # (n_ant, n_chan)
        gy = b_bp_reshaped[:, 1, :].T  # (n_ant, n_chan)
        vx = b_val_reshaped[:, 0, :].T
        vy = b_val_reshaped[:, 1, :].T

    # Combine X and Y into total intensity gain g_I = (g_X + g_Y) / 2
    both_valid = vx & vy & np.isfinite(gx) & np.isfinite(gy) & (np.abs(gx) > 0) & (np.abs(gy) > 0)
    only_x_valid = vx & ~vy & np.isfinite(gx) & (np.abs(gx) > 0)
    only_y_valid = ~vx & vy & np.isfinite(gy) & (np.abs(gy) > 0)

    gi = np.ones((n_ant, n_chan), dtype=np.complex128)
    flag_i = np.ones((n_ant, n_chan), dtype=bool)

    # Average for both valid
    gi[both_valid] = (gx[both_valid] + gy[both_valid]) / 2.0
    flag_i[both_valid] = False

    # Fallback to single pol if only one valid
    gi[only_x_valid] = gx[only_x_valid]
    flag_i[only_x_valid] = False

    gi[only_y_valid] = gy[only_y_valid]
    flag_i[only_y_valid] = False

    return gi, flag_i


def write_b0_table(output_b0_path: str, template_b0_path: str, gi: np.ndarray, flag_i: np.ndarray, backend: str):
    """
    Write CPARAM and FLAG into output CASA B0 table copied from template.
    Uses atomic write to prevent race conditions or partial tables.
    """
    tmp_out = f"{output_b0_path}.tmp.{os.getpid()}"
    if os.path.exists(tmp_out):
        shutil.rmtree(tmp_out)

    shutil.copytree(template_b0_path, tmp_out)

    n_ant, n_chan = gi.shape

    if backend == "casacore":
        # casacore putcol expects (nrow=36, nchan=288, npol=2)
        cparam = np.zeros((n_ant, n_chan, 2), dtype=np.complex128)
        cparam[:, :, 0] = gi
        cparam[:, :, 1] = gi

        flag = np.zeros((n_ant, n_chan, 2), dtype=bool)
        flag[:, :, 0] = flag_i
        flag[:, :, 1] = flag_i

        tb_out = ct.table(tmp_out, readonly=False)
        tb_out.putcol("CPARAM", cparam)
        tb_out.putcol("FLAG", flag)
        tb_out.flush()
        tb_out.close()
    else:
        # casatools putcol expects (npol=2, nchan=288, nrow=36)
        cparam = np.zeros((2, n_chan, n_ant), dtype=np.complex128)
        cparam[0, :, :] = gi.T
        cparam[1, :, :] = gi.T

        flag = np.zeros((2, n_chan, n_ant), dtype=bool)
        flag[0, :, :] = flag_i.T
        flag[1, :, :] = flag_i.T

        tb_out = table()
        tb_out.open(tmp_out, nomodify=False)
        tb_out.putcol("CPARAM", cparam)
        tb_out.putcol("FLAG", flag)
        tb_out.flush()
        tb_out.close()

    if os.path.exists(output_b0_path):
        shutil.rmtree(output_b0_path)
    os.rename(tmp_out, output_b0_path)


def convert_continuum_bp_to_casa(
    cont_bp_tab_path: str,
    template_b0_path: str,
    output_b0_path: str,
    beam: int = 0,
    clobber: bool = False,
    verbose: bool = True
) -> str:
    """
    Convert continuum bandpass solutions for a single beam to a CASA B0 table.
    """
    if os.path.exists(output_b0_path) and not clobber:
        if verbose:
            print(f"Output table already exists (skipping conversion): {output_b0_path}")
        return output_b0_path

    if verbose:
        print(f"Converting beam {beam:02d} bandpass: {cont_bp_tab_path} -> {output_b0_path}")

    bp_cube, val_cube, backend = read_bandpass_cube(cont_bp_tab_path)
    gi, flag_i = extract_beam_gains(bp_cube, val_cube, beam, backend)

    if verbose:
        valid_pts = np.sum(~flag_i)
        total_pts = flag_i.size
        print(f"Beam {beam:02d}: valid solutions = {valid_pts}/{total_pts} ({valid_pts/total_pts:.1%})")

    os.makedirs(os.path.dirname(os.path.abspath(output_b0_path)), exist_ok=True)
    write_b0_table(output_b0_path, template_b0_path, gi, flag_i, backend)

    if verbose:
        print(f"Successfully generated CASA B0 table: {output_b0_path}")
    return output_b0_path


def convert_all_beams(
    cont_bp_tab_path: str,
    template_b0_path: str,
    output_dir: str,
    sbid: str,
    beams: Optional[List[int]] = None,
    clobber: bool = False,
    verbose: bool = True
) -> List[str]:
    """
    Convert continuum bandpass solutions for multiple beams in one efficient pass.
    """
    os.makedirs(output_dir, exist_ok=True)
    bp_cube, val_cube, backend = read_bandpass_cube(cont_bp_tab_path)

    if backend == "casacore":
        n_beams = bp_cube.shape[1]
    else:
        n_beams = bp_cube.shape[2]

    if beams is None:
        beams = list(range(n_beams))

    output_tables = []
    if verbose:
        print(f"Batch converting {len(beams)} beams for {sbid} using {backend} backend...")
        print(f"Source bandpass table: {cont_bp_tab_path}")
        print(f"Template table: {template_b0_path}")
        print(f"Output directory: {output_dir}")

    for beam in beams:
        if beam < 0 or beam >= n_beams:
            print(f"WARN: Beam index {beam} out of range (0..{n_beams-1}); skipping.")
            continue

        out_name = f"contCal.{sbid}.beam{beam:02d}.B0"
        out_path = os.path.join(output_dir, out_name)

        if os.path.exists(out_path) and not clobber:
            if verbose:
                print(f"  Beam {beam:02d}: {out_name} already exists (skipping).")
            output_tables.append(out_path)
            continue

        gi, flag_i = extract_beam_gains(bp_cube, val_cube, beam, backend)
        write_b0_table(out_path, template_b0_path, gi, flag_i, backend)

        if verbose:
            valid_pct = np.mean(~flag_i) * 100.0
            print(f"  Beam {beam:02d}: valid={valid_pct:.1f}% -> {out_name}")

        output_tables.append(out_path)

    if verbose:
        print(f"Completed conversion of {len(output_tables)} tables in {output_dir}")

    return output_tables


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert ASKAP continuum bandpass table (calparameters.*_bp.*.tab) to CASA B0 calibration tables."
    )
    parser.add_argument("--sbid", required=True, help="Scheduling Block ID, e.g. SB82418")
    parser.add_argument("--data-root", default=os.environ.get("DATA_ROOT", "data"),
                        help="Root directory containing data/<SBID>")
    parser.add_argument("--cont-tab", default=None,
                        help="Path to ASKAP continuum bandpass table (auto-discovered if omitted)")
    parser.add_argument("--template-b0", default=None,
                        help="Path to template CASA B0 table (auto-discovered if omitted)")
    parser.add_argument("--output-dir", default=None,
                        help="Directory to save converted CASA B0 tables (default: <data-root>/<SBID>/CalibrationTables)")
    parser.add_argument("--beam", type=int, default=None,
                        help="Single beam index to convert (0..35)")
    parser.add_argument("--beams", default="all",
                        help='Comma-separated list (e.g. "0,9,16") or "all" (default: "all")')
    parser.add_argument("--clobber", action="store_true",
                        help="Overwrite existing output tables")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress detailed console logging")
    return parser.parse_args()


def main():
    args = parse_args()
    verbose = not args.quiet

    # 1. Locate continuum bandpass table
    cont_tab = args.cont_tab
    if not cont_tab:
        cont_tab = find_continuum_bandpass_table(args.data_root, args.sbid)
        if not cont_tab:
            print(f"ERROR: No continuum bandpass table found for {args.sbid} under {args.data_root}", file=sys.stderr)
            sys.exit(1)

    if verbose:
        print(f"Continuum bandpass table: {cont_tab}")

    # 2. Determine output directory
    output_dir = args.output_dir
    if not output_dir:
        output_dir = os.path.join(args.data_root, args.sbid, "CalibrationTables")

    # 3. Locate template B0 table
    ref_beam = args.beam if args.beam is not None else 0
    template_b0 = args.template_b0
    if not template_b0:
        template_b0 = find_template_b0(args.data_root, args.sbid, beam=ref_beam)
        if not template_b0:
            print(f"ERROR: No template CASA B0 table found for {args.sbid} under {args.data_root}", file=sys.stderr)
            sys.exit(1)

    if verbose:
        print(f"Template B0 table: {template_b0}")

    # 4. Determine beam(s) to process
    if args.beam is not None:
        beams = [args.beam]
    elif args.beams == "all":
        beams = None  # None indicates all available beams in cube
    else:
        beams = [int(b.strip()) for b in args.beams.split(",") if b.strip()]

    # 5. Run conversion
    if beams is not None and len(beams) == 1:
        beam = beams[0]
        out_table = os.path.join(output_dir, f"contCal.{args.sbid}.beam{beam:02d}.B0")
        convert_continuum_bp_to_casa(
            cont_bp_tab_path=cont_tab,
            template_b0_path=template_b0,
            output_b0_path=out_table,
            beam=beam,
            clobber=args.clobber,
            verbose=verbose
        )
    else:
        convert_all_beams(
            cont_bp_tab_path=cont_tab,
            template_b0_path=template_b0,
            output_dir=output_dir,
            sbid=args.sbid,
            beams=beams,
            clobber=args.clobber,
            verbose=verbose
        )


if __name__ == "__main__":
    main()
