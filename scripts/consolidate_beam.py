#!/usr/bin/env python3
import os, glob, re, sys
from fastducc import candidates

def parse_beams(args):
    if not args:
        return [19, 20, 21]
    beams = []
    for arg in args:
        for part in arg.split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                start, end = part.split("-", 1)
                beams.extend(range(int(start), int(end) + 1))
            else:
                beams.append(int(part))
    return sorted(list(set(beams)))

def consolidate_beam(beam_id: int, cand_dir: str):
    prefix = f"cracoData.LTR_1812-2849.SB82418.beam{beam_id:02d}.calG7.uvsub"

    # Find all DMs that have remaining chunk CSVs
    dm_set = set()
    with os.scandir(cand_dir) as it:
        for entry in it:
            if entry.is_file() and entry.name.startswith(prefix) and "chunk" in entry.name and entry.name.endswith(".csv"):
                m = re.search(r"_dm([0-9]+\.[0-9]+)_", entry.name)
                if m:
                    dm_set.add(float(m.group(1)))

    sorted_dms = sorted(dm_set)
    if not sorted_dms:
        print(f"No chunk files found for Beam {beam_id:02d} with prefix '{prefix}'. Skipping.", flush=True)
        return

    print(f"\n=== Consolidating {len(sorted_dms)} DMs for Beam {beam_id:02d}: {sorted_dms} ===", flush=True)

    for i, dm_val in enumerate(sorted_dms, 1):
        dm_tag = f"_dm{dm_val:06.2f}"
        ms_base_tag = f"{prefix}{dm_tag}"
        var_pattern = os.path.join(cand_dir, f"{prefix}*{dm_tag}*_var_candidates.csv")
        box_pattern = os.path.join(cand_dir, f"{prefix}*{dm_tag}*_chunk_*_boxcar_candidates.csv")
        print(f"[{i}/{len(sorted_dms)}] Beam {beam_id:02d} | DM={dm_val:.2f}...", flush=True)
        candidates.consolidate_chunk_catalogues(
            ms_base=ms_base_tag,
            out_dir=cand_dir,
            var_csv_pattern=var_pattern,
            box_csv_pattern=box_pattern,
            remove_chunk_catalogues=True
        )

    print(f"Consolidation for Beam {beam_id:02d} complete!", flush=True)

if __name__ == "__main__":
    cand_dir = "/fred/oz451/azic/data/SB82418/native_combined/candidates"
    beams = parse_beams(sys.argv[1:])
    print(f"Consolidating candidates for beam(s): {beams}", flush=True)
    for beam in beams:
        consolidate_beam(beam, cand_dir)
    print("\nAll requested beam consolidations complete!", flush=True)
