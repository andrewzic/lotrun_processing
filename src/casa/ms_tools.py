import glob
import os
import shutil
import sys
import re
from typing import List
from datetime import datetime
try:
    import casaconfig
    casaconfig.logfile = "/dev/null"
except Exception:
    pass

# Common CASA ensure functions
def ensure_casa_applycal() -> bool:
    try:
        from casatasks import applycal  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.applycal not available: {e}", file=sys.stderr)
        return False

def ensure_casa_flagdata() -> bool:
    try:
        from casatasks import flagdata  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.flagdata not available: {e}", file=sys.stderr)
        return False

def ensure_casa_concat():
    try:
        from casatasks import concat  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.concat not available: {e}", file=sys.stderr)
        return False

def ensure_casatasks() -> bool:
    try:
        from casatasks import applycal, uvsub, split  # noqa: F401
        return True
    except Exception as e:
        print(f"ERROR: casatasks.applycal, uvsub, split not available: {e}", file=sys.stderr)
        return False

# Common utilities
def _ms_nrows(ms_path: str) -> int:
    """Return the number of rows in the main table of a Measurement Set."""
    from casatools import table
    tb = table()
    try:
        tb.open(ms_path)
        n = tb.nrows()
    finally:
        try:
            tb.close()
        except Exception:
            pass
    return int(n)

def find_ms_files(data_root: str, sbid: str, pattern: str, beam: int) -> list:
    """
    Find measurement sets using a format string pattern under data_root/sbid.
    The pattern may include {beam:02d}.
    """
    root = os.path.join(data_root, sbid)
    print(f"Searching for MS files in '{root}' with pattern '{pattern}', beam='{beam}'")
    pat = os.path.join(root, pattern.format(beam=beam))
    print(f"Found the following ms files:\n {glob.glob(pat)}")
    return sorted(glob.glob(pat))

# Specific to applycal
def find_caltables(data_root: str, sbid: str, cal_dir: str, beam: int, extension: str="B0") -> str:
    root = os.path.join(data_root, sbid, cal_dir)
    matches = sorted(glob.glob(os.path.join(root, f"*beam{beam:02d}*.{extension}")))
    if not matches:
        raise FileNotFoundError(f"No cal table found in '{root}' for beam {beam:02d} matching '*beam{beam:02d}*.{extension}'")
    return matches

def validate_and_clean_ms(msname: str, outputvis: str, delete_previous: bool=True) -> bool:
    # Basic existence check
    if not os.path.isdir(outputvis):
        raise RuntimeError(f"Split did not produce output MS: {outputvis}")

    # Validate row counts (must match)
    old_rows = _ms_nrows(msname)
    new_rows = _ms_nrows(outputvis)
    print(f"Row count check: old={old_rows} new={new_rows}")

    if new_rows != old_rows:
        raise RuntimeError(
            f"Row count mismatch after split: {msname} has {old_rows}, {outputvis} has {new_rows}"
        )

    # Optional additional size heuristic (can be enabled if desired)
    # total_size_bytes = _dir_size_bytes(outputvis)  # implement if you want size checks

    # Delete previous generation MS only after successful validation
    if delete_previous:
        try:
            print(f"Removing previous MS: {msname}")
            shutil.rmtree(msname)
        except Exception as e:
            # fail the whole task if deletion has filesystem hiccups
            raise RuntimeError(f"ERROR: Failed to remove {msname}: {e}")

    return True

def determine_highest_extension(caltables):
    """Determine the highest numbered gaintable extension from the list of caltables."""
    extensions = []
    for cal in caltables:
        basename = os.path.basename(cal)
        parts = basename.split('.')
        if len(parts) > 1:
            ext = parts[-1]
            extensions.append(ext)
    
    max_num = -1
    max_ext = None
    for ext in extensions:
        if len(ext) > 1 and ext[0].isalpha() and ext[1:].isdigit():
            num = int(ext[1:])
            if num > max_num:
                max_num = num
                max_ext = ext
    return max_ext if max_ext else "B0"

def run_applycal(msname: str, caltables: List[str], delete_previous: bool = False) -> str:
    """
    Apply a calibration table to 'msname' and split the corrected data to a new MS
    labeled with '.cal{extension}.ms' where extension is determined from the highest
    numbered gaintable applied. If validation succeeds, delete the previous
    generation MS ('msname') to control disk usage.

    Returns:
        The path to the newly created output MS.
    """
    print(f"Applying cal: {caltables} -> {msname}")
    
    # determine extension based on highest numbered gaintable applied, e.g. .calG6 if G6 is highest, or .calB0 if only B0 applied
    extension = determine_highest_extension(caltables)
    
    time_interp = "nearest" if extension == "B0" else "linear"
    freq_interp = "linear"
    from casatasks import applycal, split
    print(f"applying caltables {caltables} to ms {msname}")    
    applycal(vis=msname, gaintable=caltables, interp=[time_interp, freq_interp]*len(caltables))
    
    output_extension = f".cal{extension}.ms"
    if "cal" in msname:
        outputvis = re.sub(r'\.cal(?:B0|G\d+)\.ms', output_extension, msname)
    else:
        outputvis = msname.replace(".ms", output_extension)
    if outputvis == msname:
        raise ValueError(f"Output measurement set name {outputvis} matches input {msname} ya nong.")

    if os.path.isdir(outputvis):
        print(f"found existing copy of {outputvis}. removing prior to split")
        shutil.rmtree(outputvis)
    print(f"splitting corrected data from ms {msname} to {outputvis}")
    split(vis=msname, outputvis=outputvis, datacolumn="corrected")    
    success = validate_and_clean_ms(msname, outputvis, delete_previous=delete_previous)

    print(f"Completed applycal+split: {outputvis}")
    return outputvis

def run_clearcal(msname: str):
    from casatasks import clearcal
    print(f"Applying cal: {caltable} -> {msname}")
    # Interpolation list as per your example; adjust if you have multiple gaintables
    clearcal(vis=msname)

# From unflag
def run_unflag(msname: str):
    from casatasks import flagdata
    print(f"Unflagging all flags -> {msname}")
    flagdata(vis=msname, mode='unflag', action='apply', flagbackup=False)

def run_quack(msname: str, quackinterval: float = 20.0):
    from casatasks import flagdata
    print(f"Quack flagging all flags -> {msname}")

    # Flag first 20 seconds of each scan
    flagdata(
        vis=msname,
        mode='quack',
        quackinterval=20.0,
        quackmode='beg'
    )

    # Flag last 20 seconds of each scan
    flagdata(
        vis=msname,
        mode='quack',
        quackinterval=20.0,
        quackmode='end'
    )

# From flagouter
def run_flag_outer_antennas(msname: str):
    from casatasks import flagdata
    print(f"Flagging outer antennas (id > 23) -> {msname}")
    # Flag antennas 24 through 35 (assuming 36-element array with antennas 0-35)
    flagdata(vis=msname, mode='manual', antenna='24~35', action='apply', flagbackup=False)

# From average
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
    mstransform(vis=msname, outputvis=outputvis, timeaverage=True, timebin=timebin, datacolumn='all')

# From selfcal
def has_model_column(ms):
    # Use casatools.table to check columns
    try:
        from casatools import table
        tb = table()
        tb.open(ms)
        cols = tb.colnames()
        tb.close()
        return "MODEL_DATA" in cols
    except Exception:
        return False

def solve_gain_phase(ms, caltable, solint, args):
    from casatasks import gaincal
    print(f"[{datetime.now().isoformat()}] gaincal: vis={ms}, caltable={caltable}, solint={solint}, calmode='{args.calmode}'")
    gaincal(
        vis=ms,
        caltable=caltable,
        field=args.field,
        spw=args.spw,
        solint=solint,
        combine=args.combine,
        refant=args.refant,
        minsnr=args.minsnr,
        gaintype="G",
        calmode=args.calmode,
        parang=args.parang
    )


def plot_solutions(caltable: str, figfile_base: str):
    """
    Plot calibration solutions using plotms (recommended in modern CASA).
    Saves phase-vs-time and amplitude-vs-time PNGs next to your caltable.
    """
    try:
        from casaplotms import plotms  # CASA 6 plotms interface
    except Exception as e:
        # Some packaged CASA expose plotms via casatasks; try that path too
        try:
            from casatasks import plotms  # fallback
        except Exception:
            raise RuntimeError(f"plotms is not available in this CASA build: {e}")

    # 1) Phase vs Time
    phase_png = f"{figfile_base}.phase.png"
    plotms(
        vis=caltable,          # plotms accepts calibration tables as 'vis'
        xaxis="time",
        yaxis="phase",
        coloraxis="antenna",
        showgui=False,
        plotfile=phase_png,
        overwrite=True
    )

    # 2) Amplitude vs Time (optional; comment out if you only want phase)
    amp_png = f"{figfile_base}.amp.png"
    plotms(
        vis=caltable,
        xaxis="time",
        yaxis="amp",
        coloraxis="antenna",
        showgui=False,
        plotfile=amp_png,
        overwrite=True
    )

    print(f"Saved plots:\n  {phase_png}\n  {amp_png}")

    
def apply_gain(old_ms, new_ms, gaintables, args):
    from casatasks import applycal, split
    print(f"[{datetime.now().isoformat()}] applycal: vis={old_ms}, gaintable={gaintables}")
    applycal(
        vis=old_ms,
        field=args.field,
        spw=args.spw,
        gaintable=gaintables,
        gainfield=[""] * len(gaintables),
        interp=["linear,nearest"] * len(gaintables),
        calwt=[args.apply_calwt.lower() == "true"] * len(gaintables),
        parang=args.parang,
        flagbackup=True
    )
    print(f"[{datetime.now().isoformat()}] split: vis={old_ms}, outputvis={new_ms}, datcolumn='corrected'")
    split(vis=old_ms, outputvis=new_ms, datacolumn="corrected")

def run_uvsub(msname: str, out_prefix: str = "uvsub") -> None:
    from casatasks import uvsub, split
    outputvis = msname.replace(".ms", f".{out_prefix}.ms")
    print(f"Running uvsub: {msname} -> {outputvis}")
    uvsub(vis=msname)
    split(vis=msname, outputvis=outputvis, datacolumn="corrected")