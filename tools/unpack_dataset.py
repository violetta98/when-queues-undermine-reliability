#!/usr/bin/env python3
"""Flatten the published dataset into the layout the analysis scripts expect.

    python3 tools/unpack_dataset.py dataset            # hardlink, default
    python3 tools/unpack_dataset.py dataset --copy     # across filesystems

Zenodo gets <testbed>/runs/<config>/<run_id>/..., which is what you want when
browsing the record in a web page. The harness writes, and every script here
reads, a flat runs-aws/<run_id>.timeline.csv. This bridges the two without
duplicating 2.3 GB: hardlinks cost nothing as long as the dataset and the
repository are on the same filesystem.

work/ and work-aws/ are copied across as-is when the dataset ships them.
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTBED_DIR = {"ec2": "runs-aws", "laptop": "runs"}
WORK_DIR = {"ec2": "work-aws", "laptop": "work"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dataset", type=Path)
    ap.add_argument("--copy", action="store_true",
                    help="copy instead of hardlink")
    args = ap.parse_args()

    if not (args.dataset / "ec2").is_dir() and not (args.dataset / "laptop").is_dir():
        sys.exit(f"no ec2/ or laptop/ under {args.dataset} - unzip the dataset first")

    place = shutil.copy2 if args.copy else os.link
    n = skipped = 0
    for testbed, flat_name in TESTBED_DIR.items():
        src = args.dataset / testbed / "runs"
        if not src.is_dir():
            continue
        flat = ROOT / flat_name
        flat.mkdir(exist_ok=True)
        for f in sorted(src.rglob("*")):
            if not f.is_file():
                continue
            target = flat / f.name
            if target.exists():
                skipped += 1
                continue
            try:
                place(f, target)
            except OSError as e:
                sys.exit(f"{e}\nDifferent filesystems? Re-run with --copy.")
            n += 1
        print(f"{testbed}: {flat_name}/ ready")

    for testbed, work_name in WORK_DIR.items():
        src = args.dataset / testbed / "work"
        if src.is_dir() and not (ROOT / work_name).exists():
            shutil.copytree(src, ROOT / work_name)
            print(f"{testbed}: {work_name}/ restored")

    print(f"\n{n} files linked{' (copied)' if args.copy else ''}"
          f"{f', {skipped} already present' if skipped else ''}")
    print("Now: python3 analysis/paper_tables.py --print")


if __name__ == "__main__":
    main()
