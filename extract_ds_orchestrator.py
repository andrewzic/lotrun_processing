#!/usr/bin/env python3
"""
extract_ds_orchestrator.py

Launch dstools-extract-ds over all (source, scan, beam) combinations
from an observation-level super-summary votable catalogue outta fastducc aggregate_obs.

Requires the dstools package (radio-dstools) on all workers.  The extraction logic mirrors
extract_ds.py's CLI entrypoint.

Two beam-selection modes are supported:
  - union (default): use the union beam list in the obs-level catalogue (column 'beams_all').
  - strict: optionally refine per-scan beam sets by reading each per-scan super-summary VOT
            and matching sky position (within --match-arcsec) to choose only beams listed
            for that source in that scan. (Requires astropy.)

Schedulers:
  - local: LocalCluster (process-based by default)
  - slurm: SLURMCluster (requires dask_jobqueue)
  - existing: connect to an existing scheduler via --scheduler-address

Example:
  python extract_ds_orchestrator.py \
    --sbid SB77974 \
    --data-root /fred/oz451/$USER/data \
    --catalogue /fred/oz451/$USER/data/SB77974/candidates/LTR_1733-2344.SB77974_obs_boxcar_super_summary.vot \
    --out-subdir extract_ds \
    --scheduler slurm --n-workers 48 --cores 1 --mem 4GB --walltime 00:30:00 \
    --job-prologue "module load python-scientific/3.11.5-foss-2023b; source /fred/oz451/azic/scripts/crystalball_nt/bin/activate" \
    --ms-glob-template "**/*%s*.ms" --overwrite
"""
from __future__ import annotations
import argparse
import math
import os
import sys
import glob
import time
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Dict, Tuple, Optional, Set

# ---- dstools imports -----------------------------------------
import logging
import warnings
from importlib.metadata import version
import h5py
import numpy as np
from astropy.wcs import FITSFixedWarning
warnings.filterwarnings("ignore", category=FITSFixedWarning, append=True)

from dstools.logger import setupLogger
from dstools.ms import MeasurementSet, combine_spws
from dstools.imaging import get_pb_correction
from dstools.utils import parse_coordinates
from dstools.ms import extract_baselines

# ---- Dask -------------------------------------------------------------------
from dask.distributed import Client, as_completed
try:
    from dask.distributed import LocalCluster
except Exception:
    LocalCluster = None
try:
    from dask_jobqueue import SLURMCluster
except Exception:
    SLURMCluster = None

# Optional Astropy for strict beam mapping
try:
    from astropy.table import Table
    import astropy.units as u
    from astropy.coordinates import SkyCoord
except Exception:
    Table = None
    SkyCoord = None

@dataclass
class ExtractTask:
    source_id: str
    srcname: str
    ra_deg: float
    dec_deg: float
    scan_id: str
    beam_id: str
    ms_path: Path
    out_file: Path

# ---------------- CLI args ---------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Orchestrate dstools-extract-ds via Python API over all relevant (source,scan,beam) combos.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Inputs
    p.add_argument('--sbid', required=True, help='SBID, e.g. SB77974')
    p.add_argument('--data-root', required=True, help='Root directory holding SBIDs')
    p.add_argument('--catalogue', default='', help='Obs-level super-summary VOTable; if empty, auto-discover by KIND')
    p.add_argument('--kind', choices=['boxcar','variance'], default='boxcar', help='Used only for auto-discovery if --catalogue not provided')

    # Beam mapping mode
    p.add_argument('--beam-scope', choices=['union','strict'], default='union',
                   help="Use obs-level union beams (union) or derive per-scan beams by matching per-scan super-summary VOTs (strict)")
    p.add_argument('--match-arcsec', type=float, default=35.0, help='Sky match radius when --beam-scope=strict')

    # File discovery & outputs
    p.add_argument('--ms-glob-template', default='**/cracoData*%s*uvsub.ms', help='Glob relative to <SBID>/<scan> to find MS for beam (%s -> beam id)')
    p.add_argument('--overwrite', action='store_true', help='Overwrite existing .ds outputs')
    p.add_argument('--dry-run', action='store_true', help='Only print actions without running')

    # Filtering
    p.add_argument('--min-snr', type=float, default=None, help='Optional min S/N threshold (filter rows)')
    p.add_argument('--source-id', type=str, default='', help='Only run for this source_id (useful for debugging)')

    # dstools extraction options (mirrors extract_ds.py)
    p.add_argument('--datacolumn', choices=['data','corrected','model'], default='data')
    p.add_argument('--primary-beam', default='', help='Path to PB image; provide non-existent path to compute separately (requires phasecentre)')
    p.add_argument('--noflag', action='store_true', help='Remove flagging mask')
    p.add_argument('--baseline-average', action='store_true', default=True, help='Average over baseline axis')
    p.add_argument('--minuvdist', type=float, default=0.0, help='Minimum UV distance in meters if averaging over baselines')
    p.add_argument('--verbose', action='store_true', help='Verbose dstools logging')

    # Dask scheduler choices
    p.add_argument('--scheduler', choices=['local','slurm','existing'], default='local')
    p.add_argument('--scheduler-address', default='', help='Connect to an existing scheduler (when --scheduler=existing)')

    # Dask common sizing
    p.add_argument('--n-workers', type=int, default=8, help='Number of workers (local or slurm)')
    p.add_argument('--cores', type=int, default=1, help='Cores per worker')
    p.add_argument('--threads-per-worker', type=int, default=None, help='Threads per worker (local only; default=cores)')
    p.add_argument('--mem', default='4GB', help='Memory per worker (slurm: e.g. 4GB; local: ignored)')
    p.add_argument('--walltime', default='01:00:00', help='Walltime per worker (slurm only)')

    # SLURM specifics
    p.add_argument('--queue', default='', help='SLURM partition/queue name (slurm only)')
    p.add_argument('--project', default='', help='SLURM account/project (slurm only)')
    p.add_argument('--job-extra', default='', help='Extra #SBATCH lines (semicolon-separated)')
    p.add_argument('--job-prologue', default='module load python-scientific/3.11.5-foss-2023b ; unset PYTHONPATH; source /fred/oz451/azic/scripts/crystalball_nt/bin/activate', help='Commands to run in SLURM job before worker starts (semicolon-separated)')

    # Execution tuning
    p.add_argument('--batch-size', type=int, default=200, help='Submit tasks in batches to limit scheduler pressure')
    p.add_argument('--retries', type=int, default=1, help='Number of retries per task on failure')
    p.add_argument('--sleep-between-batches', type=float, default=0.0, help='Seconds to sleep between batches')

    return p.parse_args(argv)

# ---------------- Helpers ----------------------------------------------------

def wrap_ra_0_360(ra_deg: float) -> float:
    if not math.isfinite(ra_deg):
        return ra_deg
    ra = ra_deg % 360.0
    if ra < 0.0:
        ra += 360.0
    return ra


def split_list(s: str) -> List[str]:
    if not s:
        return []
    s = s.strip().strip('"')
    if not s:
        return []
    out = []
    for tok in s.split(','):
        tok = tok.strip()
        if not tok:
            continue
        out.extend(t for t in tok.split() if t)
    return out

def load_obs_catalogue(vot_path: Path, min_snr: Optional[float] = None, only_source_id: str = '') -> List[Dict[str, str]]:
    """Load obs-level super-summary from VOTable and return normalized rows.
    Expected columns (best-effort):
      source_id, srcname, ra_deg, dec_deg, max_snr, scan_ids or scan_ids_all, beams_all
    """
    t = Table.read(vot_path, format='votable')
    rows: List[Dict[str, str]] = []
    # Column fallbacks
    col_sid = 'source_id' if 'source_id' in t.colnames else None
    col_name = 'srcname' if 'srcname' in t.colnames else None
    col_ra = 'ra_deg' if 'ra_deg' in t.colnames else None
    col_dec = 'dec_deg' if 'dec_deg' in t.colnames else None
    col_snr = 'max_snr' if 'max_snr' in t.colnames else None
    col_scans = 'scan_ids' if 'scan_ids' in t.colnames else ('scan_ids_all' if 'scan_ids_all' in t.colnames else None)
    col_beams = 'beams_all' if 'beams_all' in t.colnames else None

    if not (col_sid and col_ra and col_dec and col_scans):
        raise ValueError('VOTable missing required columns (need at least source_id, ra_deg, dec_deg, scan_ids[_all])')

    for r in t:
        try:
            sid = str(r[col_sid])
            if only_source_id and sid != only_source_id:
                continue
            name = str(r[col_name]) if col_name else f'SRC_{sid}'
            ra = float(r[col_ra])
            dec = float(r[col_dec])
            ra = wrap_ra_0_360(ra)
            scans = str(r[col_scans]) if col_scans else ''
            beams = str(r[col_beams]) if col_beams else ''
            snr_val = float(r[col_snr]) if col_snr else float('nan')
            if (min_snr is not None) and (not math.isnan(snr_val)) and (snr_val < min_snr):
                continue
            scan_ids = split_list(scans)
            beams_all = split_list(beams)
            if not scan_ids:
                continue
            rows.append({
                'source_id': sid,
                'srcname': name,
                'ra_deg': f"{ra:.9f}",
                'dec_deg': f"{dec:.9f}",
                'scan_ids': scan_ids,
                'beams_all': beams_all,
            })
        except Exception as e:
            print(f"[WARN] Skipping VOT row due to parse error: {e}")
    return rows


def discover_catalogue(sbid_dir: Path, sbid: str, kind: str) -> Path:
    cand_dir = sbid_dir / 'candidates'
    # prefer VOTable
    pats = list(cand_dir.glob(f"*.{sbid}_obs_{kind}_super_summary.vot"))
    if not pats:
        pats = list(cand_dir.glob(f"*.{sbid}_obs_{kind}_super_summary.xml"))
    if not pats:
        raise FileNotFoundError(f"No VOTable found under {cand_dir} for kind={kind}")
    return pats[0]


def find_ms_for_scan_beam(scan_dir: Path, beam_id: str, ms_glob_template: str) -> List[Path]:
    pat = ms_glob_template % beam_id
    cur = os.getcwd()
    try:
        os.chdir(scan_dir)
        paths = glob.glob(pat, recursive=True)
    finally:
        os.chdir(cur)
    return [scan_dir / p for p in paths]


def per_scan_beams_strict(sbid_dir: Path, scan_id: str, ra_deg: float, dec_deg: float,
                          kind: str, match_arcsec: float) -> Set[str]:
    cand_dir = sbid_dir / scan_id / 'candidates'
    vot_files = list(cand_dir.glob(f"*.*_{scan_id}_{kind}_super_summary.vot"))
    if not vot_files:
        vot_files = list(cand_dir.glob(f"*.*_{scan_id}_{kind}_super_summary.xml"))
    if not vot_files:
        return set()
    try:
        t = Table.read(vot_files[0], format='votable')
    except Exception:
        return set()
    if ('ra_deg' not in t.colnames) or ('dec_deg' not in t.colnames):
        return set()
    try:
        coords_tab = SkyCoord(ra=[float(x) for x in t['ra_deg']] * u.deg,
                              dec=[float(x) for x in t['dec_deg']] * u.deg,
                              frame='icrs')
        target = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg, frame='icrs')
        idx, sep2d, _ = target.match_to_catalog_sky(coords_tab)
        if sep2d.arcsec <= match_arcsec:
            beams = t[idx]['beams_all'] if 'beams_all' in t.colnames else ''
            return set(split_list(str(beams)))
    except Exception:
        return set()
    return set()

# --- naming helpers ---

def parse_field_from_catalogue_path(cat_path: Path) -> str:
    base = cat_path.name
    m = re.match(r'^(?P<field>[^.]+)\.SB\d{5,}_obs_(?:boxcar|variance)_super_summary\.(?:vot|xml)$', base, re.IGNORECASE)
    return m.group('field') if m else ''



def safe_name(s: str) -> str:
    """Sanitise a srcname for filenames (allow [A-Za-z0-9._+-], replace others with '_')."""
    t = re.sub(r'[^A-Za-z0-9._+-]+', '_', s.strip())
    t = re.sub(r'_+', '_', t).strip('_')
    return t or 'SRC'

# ---------------- dstools API worker ----------------------------------------

def extract_single_ms(ms_path: Path, out_file: Path, *,
                          datacolumn: str='data',
                          phasecentre: Optional[Tuple[float,float]]=None,
                          primary_beam: Optional[str]=None,
                          noflag: bool=False,
                          baseline_average: bool=True,
                          minuvdist: float=0.0,
                          verbose: bool=False) -> None:
    """Pythonic equivalent of extract_ds.py main() for a single MS.
    - ms_path: path to .ms
    - out_file: path to output .ds (HDF5)
    - phasecentre: (ra_deg, dec_deg) or None
    """
    setupLogger(verbose=verbose)
    columns = {
        'data': 'DATA',
        'corrected': 'CORRECTED_DATA',
        'model': 'MODEL_DATA',
    }
    if datacolumn not in columns:
        raise ValueError(f"Unsupported datacolumn: {datacolumn}")
    datacolumn_name = columns[datacolumn]

    ms = MeasurementSet(ms_path)
    if not ms.column_exists(datacolumn_name):
        raise RuntimeError(f"{ms} does not contain {datacolumn_name} column")

    # Combine SPWs (mirrors CLI)
    ms = combine_spws(ms)

    # Optionally rotate phasecentre
    pos = None
    if phasecentre is not None:
        ra_deg, dec_deg = phasecentre
        pos = parse_coordinates((str(ra_deg), str(dec_deg)))
        ms = ms.rotate_phasecentre(pos)

    # Primary beam correction scale
    if (primary_beam is not None) and (phasecentre is not None):
        pb_scale = get_pb_correction(ms, pos, Path(primary_beam))
    else:
        pb_scale = 1

    # Header with obs properties
    header = ms.header(datacolumn=datacolumn_name, pb_scale=pb_scale)

    # Baseline average if requested
    if baseline_average:
        ms = ms.average_baselines(minuvdist)

    # Output arrays
    visibilities = np.full(ms.dimensions, np.nan, dtype=complex)
    flags = np.full(ms.dimensions, np.nan, dtype=bool)
    uvdist = np.full(ms.nbaselines, np.nan)

    # Build cubes per baseline
    results = extract_baselines(ms, datacolumn_name)
    for baseline in results:
        baseline_idx, data_idx = baseline['baseline'], baseline['data_idx']
        visibilities[baseline_idx, data_idx] = baseline['data']
        flags[baseline_idx, data_idx] = baseline['flags']
        uvdist[baseline_idx] = baseline['uvdist']

    # Apply flags
    if not noflag:
        visibilities[flags] = np.nan

    # Apply PB correction
    visibilities /= header['pb_scale']

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(out_file, 'w', track_order=True) as f:
        f.attrs['dstools_version'] = version('radio-dstools')
        for attr in header:
            f.attrs[attr] = header[attr]
        f.create_dataset('time', data=ms.times)
        f.create_dataset('frequency', data=ms.channels)
        f.create_dataset('uvdist', data=uvdist)
        f.create_dataset('flux', data=visibilities)

    # Clean up temp files (mirrors CLI best-effort)
    os.system(f"rm -r {ms.path.parent}/*dstools-temp*.*ms 2>/dev/null")

# ---------------- Task builder ----------------------------------------------

def build_tasks(
    rows: List[Dict[str,str]],
    sbid_dir: Path,
    ms_glob_template: str,
    beam_scope: str,
    kind: str,
    match_arcsec: float,
    *,
    fieldname: str,
    sbid: str,
) -> List[ExtractTask]:
    tasks: List[ExtractTask] = []
    for row in rows:
        sid = row['source_id']
        name_raw = row['srcname']
        name = safe_name(name_raw)
        ra = float(row['ra_deg']); dec = float(row['dec_deg'])
        scan_ids = row['scan_ids']
        union_beams = row['beams_all']
        for sc in scan_ids:
            scan_dir = sbid_dir / sc
            if not scan_dir.is_dir():
                print(f"[WARN] Missing scan dir: {scan_dir}")
                continue
            if beam_scope == 'strict':
                beams = per_scan_beams_strict(sbid_dir, sc, ra, dec, kind, match_arcsec) or set(union_beams)
            else:
                beams = set(union_beams)
            for b in sorted(beams):
                ms_list = find_ms_for_scan_beam(scan_dir, b, ms_glob_template)
                if not ms_list:
                    print(f"[INFO] No MS for scan={sc} beam={b}; skipping")
                    continue
                # Output as <SBID>/<scan>/candidates/<fieldname>.<SBID>.beam<beam>.<scan>_cand_<srcname>.ds
                cand_dir = scan_dir / 'candidates'
                cand_dir.mkdir(parents=True, exist_ok=True)
                out_name = f"{fieldname}.{sbid}.{b}.{sc}_cand_{name}.ds"
                out_file = cand_dir / out_name
                for ms in ms_list:
                    tasks.append(ExtractTask(sid, name, ra, dec, sc, b, ms, out_file))
    return tasks

# ---------------- Dask submission -------------------------------------------

def run_task(task: ExtractTask, *, overwrite: bool, dry_run: bool,
             datacolumn: str, primary_beam: str, noflag: bool,
             baseline_average: bool, minuvdist: float, verbose: bool) -> Tuple[str, str, int]:
    """Run one extraction task via the Python API."""
    if (not overwrite) and task.out_file.exists() and task.out_file.stat().st_size > 0:
        return (str(task.out_file), 'exists', 0)
    if dry_run:
        cmd = f"dstools-extract-ds -d {datacolumn} -p {task.ra_deg:.9f} {task.dec_deg:.9f} {task.ms_path} {task.out_file}"
        return (str(task.out_file), 'dry-run: ' + cmd, 0)
    try:
        extract_single_ms(
            task.ms_path, task.out_file,
            datacolumn=datacolumn,
            phasecentre=(task.ra_deg, task.dec_deg),
            primary_beam=primary_beam or None,
            noflag=noflag,
            baseline_average=baseline_average,
            minuvdist=minuvdist,
            verbose=verbose,
        )
        return (str(task.out_file), 'ok', 0)
    except Exception as e:
        return (str(task.out_file), f'exception: {e}', 1)


def make_client(args: argparse.Namespace) -> Client:
    if args.scheduler == 'existing':
        if not args.scheduler_address:
            raise RuntimeError('--scheduler-address required for existing scheduler')
        return Client(args.scheduler_address)
    elif args.scheduler == 'local':
        if LocalCluster is None:
            raise RuntimeError('LocalCluster not available')
        tpw = args.threads_per_worker if args.threads_per_worker is not None else args.cores
        cluster = LocalCluster(n_workers=args.n_workers, threads_per_worker=tpw, processes=True)
        return Client(cluster)
    elif args.scheduler == 'slurm':
        if SLURMCluster is None:
            raise RuntimeError('dask_jobqueue.SLURMCluster not available')
        jb_extra = []
        if args.queue:
            jb_extra.append(f"-p {args.queue}")
        if args.project:
            jb_extra.append(f"-A {args.project}")
        if args.job_extra.strip():
            for tok in args.job_extra.split(';'):
                tok = tok.strip()
                if tok:
                    jb_extra.append(tok)
        prologue = []
        if args.job_prologue.strip():
            prologue = [line.strip() for line in args.job_prologue.split(';') if line.strip()]
        cluster = SLURMCluster(
            cores=args.cores,
            memory=args.mem,
            walltime=args.walltime,
            job_extra=jb_extra,
            job_script_prologue=prologue,
            local_directory=str(Path.cwd() / 'dask-worker-space'),
        )
        cluster.scale(args.n_workers)
        return Client(cluster)
    else:
        raise RuntimeError(f"Unsupported scheduler: {args.scheduler}")

# ---------------- Main -------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    sbid_dir = Path(args.data_root) / args.sbid

    # Catalogue
    cat_path = Path(args.catalogue) if args.catalogue else discover_catalogue(sbid_dir, args.sbid, args.kind)
    print(f"[INFO] Catalogue: {cat_path}")

    rows = load_obs_catalogue(cat_path, min_snr=args.min_snr, only_source_id=args.source_id)
    if not rows:
        print('[INFO] No rows to process after filtering; exiting.')
        return 0

    # Parse fieldname from catalogue filename for output naming
    fieldname = parse_field_from_catalogue_path(cat_path) or 'field'

    # Build tasks with required naming components
    tasks = build_tasks(
        rows, sbid_dir, args.ms_glob_template,
        args.beam_scope, args.kind, args.match_arcsec,
        fieldname=fieldname, sbid=args.sbid,
    )
    if not tasks:
        print('[INFO] No tasks to run (no MS found for requested combos).')
        return 0
    print(f"[INFO] Built {len(tasks)} tasks")

    client = make_client(args)
    print(f"[INFO] Connected to scheduler: {client}")

    batch = []
    completed = 0
    failures = 0

    def submit_batch(batch_tasks: List[ExtractTask]):
        nonlocal completed, failures
        if not batch_tasks:
            return
        futs = []
        for t in batch_tasks:
            fut = client.submit(
                run_task, t,
                overwrite=args.overwrite, dry_run=args.dry_run,
                datacolumn=args.datacolumn, primary_beam=args.primary_beam,
                noflag=args.noflag, baseline_average=args.baseline_average,
                minuvdist=args.minuvdist, verbose=args.verbose,
                retries=args.retries,
            )
            futs.append(fut)
        for f in as_completed(futs):
            out_file, msg, rc = f.result()
            status = 'OK' if rc == 0 else 'FAIL'
            if rc != 0:
                failures += 1
            completed += 1
            print(f"[{status}] {out_file}: {msg[:200]}")

    for t in tasks:
        batch.append(t)
        if len(batch) >= args.batch_size:
            submit_batch(batch)
            batch = []
            if args.sleep_between_batches > 0:
                time.sleep(args.sleep_between_batches)
    submit_batch(batch)

    print(f"[DONE] completed={completed} failures={failures}")
    return 0 if failures == 0 else 1

if __name__ == '__main__':
    raise SystemExit(main())
