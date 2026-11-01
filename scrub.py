#!/usr/bin/env python3
"""Remove identifying details from the artifact without touching measurements.

Three things leaked into files that are meant to be handed to reviewers: the
machine's host name, the SSD serial number and volume UUIDs, and the absolute
path of the author's home directory. None of them is needed to reproduce or
check anything, and under blind review all three identify the author.

What is deliberately kept: the disk model, every version string, every image
digest, and all timings. The point is anonymity, not vagueness.

Idempotent — running it twice changes nothing the second time.
"""

import gzip
import platform
import re
import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# The machine name is taken from the running system rather than written down
# here: hard-coding it would leave the very string this script exists to remove
# inside the script. Pass extra names as arguments when scrubbing an artifact
# produced on a different machine.
HOSTS = {h.split(".")[0] for h in (platform.node(), socket.gethostname())
         if h} | {a for a in sys.argv[1:] if not a.startswith("-")}

SUBS = [(re.compile(re.escape(h)), "host") for h in sorted(HOSTS, key=len,
                                                           reverse=True) if h]
SUBS += [
    # absolute paths under the author's home directory
    (re.compile(r"/Users/[A-Za-z0-9._-]+/Desktop/Article expriments"),
     "<artifact-root>"),
    (re.compile(r"/Users/[A-Za-z0-9._-]+/"), "<home>/"),
    # EC2 default hostnames are derived from the private IP, so they appear in
    # every kernel string and manifest from a cloud run. The hostname set above
    # only covers the machine the scrub runs on, which is not the machine the
    # cloud runs came from.
    (re.compile(r"\bip-\d{1,3}-\d{1,3}-\d{1,3}-\d{1,3}\b"), "ip-<redacted>"),
    # hardware identifiers
    (re.compile(r"Serial Number: [A-Z0-9]+"), "Serial Number: <redacted>"),
    (re.compile(r"Volume UUID: [0-9A-Fa-f-]{36}"), "Volume UUID: <redacted>"),
    (re.compile(r"Hardware UUID: [0-9A-Fa-f-]{36}"), "Hardware UUID: <redacted>"),
]

SKIP_SUFFIX = {".png", ".svg", ".pyc"}
SKIP_DIRS = {"__pycache__", ".git"}


def scrub_text(s):
    n = 0
    for pat, rep in SUBS:
        s, k = pat.subn(rep, s)
        n += k
    return s, n


def main():
    dry = "--dry-run" in sys.argv
    touched = total = 0
    for p in sorted(ROOT.rglob("*")):
        if not p.is_file() or p.name == Path(__file__).name:
            continue
        if p.suffix in SKIP_SUFFIX or set(p.parts) & SKIP_DIRS:
            continue
        gz = p.suffixes[-1:] == [".gz"]
        try:
            raw = (gzip.open(p, "rt", errors="surrogateescape") if gz
                   else p.open(errors="surrogateescape")).read()
        except (OSError, UnicodeDecodeError):
            continue
        out, n = scrub_text(raw)
        if not n:
            continue
        touched += 1
        total += n
        print(f"{p.relative_to(ROOT)}: {n}")
        if dry:
            continue
        if gz:
            with gzip.open(p, "wt") as fh:
                fh.write(out)
        else:
            p.write_text(out)
    print(f"{'would change' if dry else 'changed'} {touched} files, "
          f"{total} occurrences")
    return 0


if __name__ == "__main__":
    sys.exit(main())
