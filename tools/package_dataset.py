#!/usr/bin/env python3
"""Lay the per-message records out for Zenodo and checksum them.

    python3 tools/package_dataset.py --out ../zenodo-dataset
    python3 tools/package_dataset.py --out ../zenodo-dataset --link

Produces

    <testbed>/runs/<config>/<run_id>/<run_id>.timeline.csv
                                    /<run_id>.messages.csv.gz
    <testbed>/work/<run_id>/...
    SHA256SUMS
    MANIFEST.csv

Testbed first, so that someone who wants only the cloud runs downloads one
folder and gets both the published pairs and the working files that go with
them.

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


DATASET_README = """# When Queues Undermine Reliability - per-message records

Raw results for 99 experimental runs measuring how RabbitMQ and Apache Kafka
behave when a consumer slows down for minutes instead of failing.

## The two testbeds

`ec2/` - 48 runs, the primary set, the one the paper reports. An AWS EC2
m6i.4xlarge instance: Intel Xeon Platinum 8375C at 2.90 GHz, 16 vCPU, 61.8 GiB,
Ubuntu 24.04.5 LTS (kernel 7.0.0-1014-aws), Docker 29.1.3, overlayfs, cgroups
v2. Broker data on a separate 100 GiB gp3 volume (ext4); provisioned IOPS and
throughput were not recorded, and the gp3 defaults are 3000 IOPS and 125 MiB/s.

`laptop/` - 51 runs, the second testbed, used to check that the conclusions
transfer. An Intel Core i9-9880H machine, 16 GiB RAM, NVMe SSD, macOS 14.1.1,
with the brokers inside Docker Desktop's Linux VM: Docker Engine 24.0.6, 12 vCPU
and 7.8 GiB given to the VM, shared with the host OS, no CPU pinning.

On both testbeds the brokers, the load generator and the neighbour queue share
one machine. RabbitMQ 4.3.6 on Erlang/OTP 27.3.4.19 with 4 CPUs and 1 GiB;
Kafka 4.3.1 in KRaft mode with 4 CPUs, 2 GiB, six partitions, replication factor
1 and acks=all. Images pinned by digest; every run manifest records the digest
it used, so the exact images can be recovered from the data alone.

Code, analysis scripts and the full documentation are in the companion
repository; this record holds only the data it reads.

    https://github.com/violetta98/when-queues-undermine-reliability

## Layout

Zenodo allows 100 files in a record and this dataset has over seventeen
hundred, so each testbed is split into two archives. Unzip the ones you need
into the same directory; they share a common tree.

    ec2-runs.zip       1.2 GiB   ec2/runs/<configuration>/<run_id>/
    ec2-work.zip       3 MiB     ec2/work/<run_id>/
    laptop-runs.zip    1.3 GiB   laptop/runs/<configuration>/<run_id>/
    laptop-work.zip    4 MiB     laptop/work/<run_id>/
    SHA256SUMS                   one line per file, paths as unzipped
    MANIFEST.csv                 testbed, configuration, run, file, bytes
    README.md

Reproducing the paper's tables needs `ec2-runs.zip`, and Table 5 also needs
`laptop-runs.zip`. The two small `work` archives carry the broker logs, the
one-second metric samples and the orchestrator events; they are the only source
for the alarm episode counts, the queue depth at each purge and the Kafka
offsets behind Figure 5.

Each run is two files:

    <run_id>.timeline.csv       commented provenance header, one row per second
    <run_id>.messages.csv.gz    one row per publish attempt

`timeline.csv` has a commented provenance header and one row per second.
`messages.csv.gz` has one row per publish attempt, for both the queue under
test and a second "neighbour" queue - the `queue` column separates them, and
every per-message figure in the paper is computed over `main` only.

`work/` holds the broker log, the one-second metric samples and the
orchestrator events. Several numbers in the paper come only from there: the
alarm episode counts, the queue depth at each alarm and at each operator purge,
and the Kafka offsets behind Figure 5.

Run identifiers decode as
`<configuration>_f<slowdown factor>_s<slow seconds>_<scenario>_r<replicate>`;
scenario A is the long slowdown, B the short peak.

## Configuration names

Directories carry the names the configurations were given as the experiment
grew, which is why the numbering has gaps. The paper uses short labels.

| Paper | Directory | Runs |
|---|---|---|
| R1 | `R-C1-unbounded` | both testbeds |
| R2 | `R-C2-maxlen-drophead` | both |
| R3 | `R-C3-reject-publish` | both |
| R4 | `R-C4-ttl` | both |
| R5 | `R-C5-combined` | both |
| R9 | `R-C9-wide-headroom` | both |
| R11 | `R-C11-reject-correct` | both |
| R12 | `R-C12-maxlen-correct-wide` | both |
| K1 | `K-C1-long-retention` | both |
| K2 | `K-C2-short-retention` | both |
| K4 | `K-C4-consumer-deadline` | both |
| K6 | `K-C6-retention-below-D` | both |
| K3 | `K-C3-lag-backpressure` | laptop only, run but not discussed in the paper |
| K7 | `K-C7-retention-bytes` | laptop only, run but not discussed in the paper |

R6, R7, R8, R10 and K5 appear in the paper as planned configurations that were
never run; there is no data for them here or anywhere.

## Using it

    unzip 'ec2-*.zip'            # and laptop-*.zip for Table 5
    sha256sum -c SHA256SUMS      # verifies the unzipped files
    # then, from a clone of the repository:
    python3 tools/unpack_dataset.py <path to this dataset>
    python3 analysis/paper_tables.py --print

`unpack_dataset.py` hardlinks the files into the flat layout the analysis
scripts expect, so nothing is copied twice.

## Licence

CC BY 4.0.
"""


def write_readme(out):
    (out / "README.md").write_text(DATASET_README)
    print("  README.md")


def make_archives(out):
    """Zenodo allows 100 files per record and the dataset has well over a
    thousand, so each testbed/kind becomes one archive. Splitting by kind as
    well as by testbed means the 38 MB of broker logs and metric samples can be
    fetched without the 2.6 GB of per-message records."""
    import subprocess
    made = []
    for testbed in ("ec2", "laptop"):
        for kind in ("runs", "work"):
            src = out / testbed / kind
            if not src.is_dir():
                continue
            name = f"{testbed}-{kind}.zip"
            # -1: the per-message records are already gzip, only the timelines
            # and the broker logs have anything left to squeeze.
            subprocess.run(["zip", "-r", "-q", "-1", name, f"{testbed}/{kind}"],
                           cwd=str(out), check=True)
            size = (out / name).stat().st_size / 2**30
            print(f"  {name}  {size:.2f} GiB")
            made.append(name)
            shutil.rmtree(src)
        d = out / testbed
        if d.is_dir() and not any(d.iterdir()):
            d.rmdir()
    return made


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--link", action="store_true",
                    help="hardlink instead of copy (same filesystem only)")
    ap.add_argument("--with-prejoin", action="store_true",
                    help="also ship the per-process CSVs (adds ~2.3 GB)")
    ap.add_argument("--archive", action="store_true",
                    help="zip each testbed/kind; Zenodo caps a record at 100 files")
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
            dest = args.out / testbed / "runs" / config / run_id
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
    for testbed, flat_name in (("ec2", "work-aws"), ("laptop", "work")):
        src = ROOT / flat_name
        if not src.is_dir():
            continue
        for f in sorted(src.rglob("*")):
            if not f.is_file() or f.name.endswith(skip):
                continue
            dst = args.out / testbed / "work" / f.relative_to(src)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                place(f, dst)
                rel = dst.relative_to(args.out)
                sums.append((sha256(dst), rel))
                manifest.append([testbed, "", "", str(rel), dst.stat().st_size])
        print(f"  {testbed}/work/ "
              f"({'with' if args.with_prejoin else 'without'} per-process CSVs)")

    write_readme(args.out)

    with open(args.out / "SHA256SUMS", "w") as fh:
        for digest, rel in sorted(sums, key=lambda x: str(x[1])):
            fh.write(f"{digest}  {rel}\n")
    with open(args.out / "MANIFEST.csv", "w", newline="") as fh:
        csv.writer(fh).writerows(manifest)

    if args.archive:
        make_archives(args.out)

    total = sum(int(r[4]) for r in manifest[1:])
    print(f"\n{len(manifest) - 1} files, {total / 2**30:.2f} GiB")
    print(f"SHA256SUMS and MANIFEST.csv written to {args.out}")
    if missing:
        print(f"\nmissing {len(missing)} file(s):")
        for m in missing[:10]:
            print("   " + m)


if __name__ == "__main__":
    main()
