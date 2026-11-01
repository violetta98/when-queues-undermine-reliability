#!/usr/bin/env python3
"""Lay the per-message records out for Zenodo and checksum them.

    python3 tools/package_dataset.py --out ../zenodo-dataset
    python3 tools/package_dataset.py --out ../zenodo-dataset --link

Produces

    runs/<testbed>/<config>/<run_id>/<run_id>.timeline.csv
                                    /<run_id>.messages.csv.gz
    SHA256SUMS
    MANIFEST.csv

from the flat runs-aws/ and runs/ directories the harness writes. The flat
layout is what the analysis scripts expect, so it stays; this is the shape the
dataset is published in.

--link hardlinks instead of copying, which is instant and costs no extra disk,
but only works when the output is on the same filesystem.
"""

import argparse
import csv
import hashlib
import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTBEDS = [("ec2", ROOT / "runs-aws", ROOT / "data" / "ec2-runs.csv"),
            ("laptop", ROOT / "runs", ROOT / "data" / "laptop-runs.csv")]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--link", action="store_true",
                    help="hardlink instead of copy (same filesystem only)")
    ap.add_argument("--with-prejoin", action="store_true",
                    help="also ship the per-process CSVs (adds ~2.3 GB)")
    args = ap.parse_args()

    place = os.link if args.link else shutil.copy2
    sums, manifest = [], [["testbed", "config", "run_id", "file", "bytes"]]
    missing = []

    for testbed, flat, summary in TESTBEDS:
        if not summary.exists():
            print(f"no summary for {testbed} at {summary}, skipping")
            continue
        for row in csv.DictReader(open(summary)):
            run_id, config = row["run_id"], row["config"]
            dest = args.out / "runs" / testbed / config / run_id
            dest.mkdir(parents=True, exist_ok=True)
            for name in (f"{run_id}.timeline.csv", f"{run_id}.messages.csv.gz"):
                src = flat / name
                if not src.exists():
                    missing.append(f"{testbed}/{name}")
                    continue
                target = dest / name
                if not target.exists():
                    place(src, target)
                rel = target.relative_to(args.out)
                sums.append((sha256(target), rel))
                manifest.append([testbed, config, run_id, str(rel),
                                 target.stat().st_size])
            print(f"  {testbed}/{config}/{run_id}")

    # work/ holds two different things. The broker log, the metric samples and
    # the event files are small and carry numbers that appear in the paper --
    # the alarm episode count, the queue depth at each alarm and at each purge,
    # the Kafka probe behind Figure 5 -- so they ship.
    #
    # The four per-process CSVs (producer, consumer and the two neighbour
    # sides) are the pre-join form of the published messages file: every one of
    # their columns appears in it, renamed. Shipping them doubles the dataset
    # and adds no information, so they are left out unless asked for. Pass
    # --with-prejoin if you want someone to be able to redo the join itself
    # rather than trust postrun.py.
    skip = () if args.with_prejoin else (".csv.gz",)
    for flat_name in ("work-aws", "work"):
        src = ROOT / flat_name
        if not src.is_dir():
            continue
        for f in sorted(src.rglob("*")):
            if not f.is_file() or f.name.endswith(skip):
                continue
            dst = args.out / flat_name / f.relative_to(src)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                place(f, dst)
        print(f"  {flat_name}/ "
              f"({'with' if args.with_prejoin else 'without'} per-process CSVs)")

    with open(args.out / "SHA256SUMS", "w") as fh:
        for digest, rel in sorted(sums, key=lambda x: str(x[1])):
            fh.write(f"{digest}  {rel}\n")
    with open(args.out / "MANIFEST.csv", "w", newline="") as fh:
        csv.writer(fh).writerows(manifest)

    total = sum(int(r[4]) for r in manifest[1:])
    print(f"\n{len(manifest) - 1} files, {total / 2**30:.2f} GiB")
    print(f"SHA256SUMS and MANIFEST.csv written to {args.out}")
    if missing:
        print(f"\nmissing {len(missing)} file(s):")
        for m in missing[:10]:
            print("   " + m)


if __name__ == "__main__":
    main()
