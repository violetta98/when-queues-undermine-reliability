#!/usr/bin/env python3
"""Compare the same arms measured on two testbeds.

    python3 cloud/compare_testbeds.py analysis/out/runs.csv <other>/runs.csv

The point of a second testbed is not to reproduce digits. It is to show that
the quantities the conclusions rest on are ratios and shares that transfer,
while the quantities that do not transfer are absolute times and rates which no
conclusion uses. So the two groups are reported separately, with the tolerance
stated in advance rather than chosen after seeing the numbers.
"""

import csv
import statistics as st
import sys

# Conclusion-bearing: a claim in the paper breaks if one of these moves a lot.
# Tolerance is in percentage points for shares, relative for the rest.
TRANSFERS = [
    ("useful_loss_ratio", "useful loss", "pp", 3.0),
    ("dead_work_ratio", "dead work", "pp", 5.0),
    ("on_time", "delivered on time", "pp", 5.0),
    ("purges", "operator purges per run", "abs", 1.0),
]

# Stand-bound: expected to differ, listed so the difference is visible and
# cannot be mistaken for a surprise.
STAND_BOUND = [
    ("time_to_alarm_s", "time to alarm, s"),
    ("backlog_at_slow_end", "backlog at end of slowdown"),
    ("measured_drain_s", "drain, s"),
    ("nbr_confirm_p99_baseline_ms", "neighbour baseline, ms"),
    ("nbr_confirm_p99_slow_ms", "neighbour during slowdown, ms"),
]


def load(path):
    """Group by (config, factor). The slowdown factor has to be part of the key:
    the AWS set carries a second operating point at x10 for one arm, and folding
    it into the same median as the x5 runs invents a disagreement between the
    testbeds that is really a difference between two experiments."""
    out = {}
    for r in csv.DictReader(open(path)):
        if r["scenario"] != "A":
            continue
        out.setdefault((r["config"], f"{float(r['factor']):g}"), []).append(r)
    return out


def med(rows, col):
    vals = []
    for r in rows:
        if col == "on_time":
            v = 100 * int(r["processed_useful"]) / int(r["produced"])
        elif col.endswith("_ratio"):
            v = 100 * float(r[col])
        else:
            if not r.get(col):
                continue
            v = float(r[col])
        vals.append(v)
    return st.median(vals) if vals else None


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    a, b = load(sys.argv[1]), load(sys.argv[2])
    shared = sorted(set(a) & set(b))
    if not shared:
        print("no shared configurations")
        return 1
    print(f"shared configurations: {len(shared)}\n")

    print("CONCLUSION-BEARING QUANTITIES - must agree within tolerance")
    bad = 0
    worst = []
    for col, label, kind, tol in TRANSFERS:
        print(f"\n  {label}")
        for c in shared:
            x, y = med(a[c], col), med(b[c], col)
            if x is None or y is None:
                continue
            d = y - x
            ok = abs(d) <= tol if kind in ("pp", "abs") else abs(d) <= tol * max(x, 1e-9)
            if not ok:
                bad += 1
                worst.append((label, c[0], x, y, d))
            unit = "pp" if kind == "pp" else ""
            print(f"    {c[0]:24s} {x:8.2f} -> {y:8.2f}  ({d:+.2f} {unit}) "
                  f"{'ok' if ok else 'OUT OF TOLERANCE'}")
    print(f"\n  out of tolerance: {bad}")
    for lb, c, x, y, d in worst:
        print(f"    {c} / {lb}: {x:.2f} -> {y:.2f} ({d:+.2f})")

    print("\nTESTBED-BOUND - differences expected, no conclusion rests on them")
    for col, label in STAND_BOUND:
        print(f"\n  {label}")
        for c in shared:
            x, y = med(a[c], col), med(b[c], col)
            if x is None or y is None:
                continue
            ratio = f"x{y / x:.2f}" if x else "—"
            print(f"    {c[0]:24s} {x:10.1f} -> {y:10.1f}  {ratio}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
