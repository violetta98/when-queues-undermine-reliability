#!/usr/bin/env python3
"""Rebuild Tables 3-6 of the paper from the run data.

    python3 analysis/paper_tables.py            # writes data/tables/*.csv
    python3 analysis/paper_tables.py --print    # and dumps them to stdout

Tables 1, 2 and 7 are not here: they list configurations and rules rather than
measurements, so there is nothing to recompute.

Table 5 and the removal-age columns of Table 4 need the per-message records,
which live in runs-aws/ and runs/ and are distributed through Zenodo rather
than through git. Without them the script still produces Tables 3 and 6, which
only need the per-run summaries, and says what it skipped.
"""

import argparse
import csv
import gzip
import statistics as st
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "tables"

# Paper name -> directory name. The internal names grew with the experiment and
# carry the hypothesis each arm was added for; the paper uses short labels.
ARMS = [
    ("R1", "R-C1-unbounded"),
    ("R2", "R-C2-maxlen-drophead"),
    ("R3", "R-C3-reject-publish"),
    ("R4", "R-C4-ttl"),
    ("R5", "R-C5-combined"),
    ("R9", "R-C9-wide-headroom"),
    ("R11", "R-C11-reject-correct"),
    ("R12", "R-C12-maxlen-correct-wide"),
    ("K1", "K-C1-long-retention"),
    ("K2", "K-C2-short-retention"),
    ("K4", "K-C4-consumer-deadline"),
    ("K6", "K-C6-retention-below-D"),
    # laptop only, run but not discussed in the paper
    ("K3", "K-C3-lag-backpressure"),
    ("K7", "K-C7-retention-bytes"),
]
TO_PAPER = {v: k for k, v in ARMS}

PER_RUN = 660_000  # publish attempts in the long slowdown


def load(path):
    rows = list(csv.DictReader(open(path)))
    for r in rows:
        r["factor"] = float(r["factor"])
    return rows


def pick(rows, config, scenario="A", factor=5.0):
    return [r for r in rows
            if r["config"] == config and r["scenario"] == scenario
            and r["factor"] == factor]


def pct(rows, col):
    return st.median(100 * float(r[col]) for r in rows)


def share_of_attempts(rows, col):
    return st.median(100 * int(r[col]) / int(r["produced"]) for r in rows)


def num(rows, col, default=None):
    vals = [float(r[col]) for r in rows if r.get(col) not in (None, "", "-1")]
    return st.median(vals) if vals else default


def table3(ec2):
    """Outcomes of the long slowdown, EC2, median of three runs."""
    out = [["arm", "on_time_pct", "useful_loss_pct", "dead_work_pct",
            "consumer_discard_pct", "shed_pct", "refused_pct",
            "backlog_msgs", "time_to_alarm_s", "purges", "drain_s"]]
    for paper, cfg in ARMS:
        rows = pick(ec2, cfg)
        if not rows:
            continue
        out.append([
            paper,
            f"{share_of_attempts(rows, 'processed_useful'):.2f}",
            f"{pct(rows, 'useful_loss_ratio'):.2f}",
            f"{pct(rows, 'dead_work_ratio'):.2f}",
            f"{share_of_attempts(rows, 'consumer_expired'):.2f}",
            f"{share_of_attempts(rows, 'shed'):.2f}",
            f"{share_of_attempts(rows, 'nacked'):.2f}",
            f"{num(rows, 'backlog_at_slow_end'):.0f}",
            "" if num(rows, "time_to_alarm_s") is None
            else f"{num(rows, 'time_to_alarm_s'):.1f}",
            f"{num(rows, 'purges', 0):.0f}",
            f"{num(rows, 'measured_drain_s'):.0f}",
        ])
    return out


def removal_ages(run_dir, run_id):
    """Age at removal for every message the broker dropped, main queue only."""
    ages, left = [], []
    path = run_dir / f"{run_id}.messages.csv.gz"
    if not path.exists():
        return None
    with gzip.open(path, "rt") as fh:
        for r in csv.DictReader(fh):
            if r["queue"] != "main":
                continue
            t_rm = r["t_removed_ns"]
            if t_rm in ("", "-1", "0"):
                continue
            age = (int(t_rm) - int(r["t_create_ns"])) / 1e9
            ages.append(age)
            left.append((int(r["deadline_ns"]) - int(t_rm)) / 1e9)
    return ages, left


def table4(ec2, run_dir):
    """Moment of removal. Removed is a total over the three runs; the rest are
    medians over the pooled messages, which is what the paper reports."""
    out = [["arm", "removed_msgs", "age_at_removal_s", "life_left_s",
            "spread_p10_p90_s"]]
    missing = []
    for paper, cfg in ARMS:
        rows = pick(ec2, cfg)
        if not rows:
            continue
        ages, left = [], []
        for r in rows:
            got = removal_ages(run_dir, r["run_id"])
            if got is None:
                missing.append(paper)
                break
            ages += got[0]
            left += got[1]
        else:
            if not ages:
                continue
            ages.sort()
            p10, p90 = ages[len(ages) // 10], ages[9 * len(ages) // 10]
            out.append([paper, len(ages), f"{st.median(ages):.2f}",
                        f"{st.median(left):+.2f}", f"{p90 - p10:.2f}"])
    return out, sorted(set(missing))


def slack(run_dir, run_id):
    """Slack to the deadline at completion, main queue only."""
    path = run_dir / f"{run_id}.messages.csv.gz"
    if not path.exists():
        return None
    vals = []
    with gzip.open(path, "rt") as fh:
        for r in csv.DictReader(fh):
            if r["queue"] != "main":
                continue
            done = r["t_done_ns"]
            if done in ("", "-1", "0"):
                continue
            vals.append((int(r["deadline_ns"]) - int(done)) / 1e9)
    return vals


def table5(ec2, laptop):
    """Worst-case slack per testbed: median over runs of the per-run minimum,
    with the range over runs. The per-run minimum is the statistic the sizing
    argument uses, so it must not be pooled before taking the median."""
    out = [["arm", "testbed", "worst_slack_median_s", "worst_slack_min_s",
            "worst_slack_max_s", "deliveries"]]
    missing = []
    for rows, run_dir, name in ((ec2, ROOT / "runs-aws", "ec2"),
                                (laptop, ROOT / "runs", "laptop")):
        for paper, cfg in (("R2", "R-C2-maxlen-drophead"),
                           ("R3", "R-C3-reject-publish"),
                           ("R11", "R-C11-reject-correct"),
                           ("R12", "R-C12-maxlen-correct-wide")):
            per_run, total = [], 0
            for r in pick(rows, cfg):
                vals = slack(run_dir, r["run_id"])
                if vals is None:
                    missing.append(name)
                    break
                per_run.append(min(vals))
                total += len(vals)
            else:
                if per_run:
                    out.append([paper, name, f"{st.median(per_run):+.3f}",
                                f"{min(per_run):+.3f}", f"{max(per_run):+.3f}",
                                total])
    return out, sorted(set(missing))


def table6(ec2):
    """Short slowdown, scenario B."""
    out = [["arm", "refused_pct", "shed_pct", "useful_loss_pct",
            "neighbour_loss_min", "neighbour_loss_max"]]
    for paper, cfg in (("R1", "R-C1-unbounded"),
                       ("R2", "R-C2-maxlen-drophead"),
                       ("R3", "R-C3-reject-publish")):
        rows = pick(ec2, cfg, scenario="B")
        if not rows:
            continue
        nbr = [int(float(r["nbr_publish_failures_slow"])) for r in rows
               if r.get("nbr_publish_failures_slow") not in (None, "")]
        out.append([
            paper,
            f"{share_of_attempts(rows, 'nacked'):.2f}",
            f"{share_of_attempts(rows, 'shed'):.2f}",
            f"{pct(rows, 'useful_loss_ratio'):.2f}",
            min(nbr) if nbr else "", max(nbr) if nbr else "",
        ])
    return out


def write(name, rows, show):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / name, "w", newline="") as fh:
        csv.writer(fh).writerows(rows)
    print(f"wrote {OUT / name} ({len(rows) - 1} rows)")
    if show:
        for r in rows:
            print("   " + "  ".join(str(x).rjust(9) for x in r))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true", dest="show")
    args = ap.parse_args()

    ec2 = load(ROOT / "data" / "ec2-runs.csv")
    laptop = load(ROOT / "data" / "laptop-runs.csv")

    write("table3_outcomes.csv", table3(ec2), args.show)
    write("table6_short_slowdown.csv", table6(ec2), args.show)

    t4, miss4 = table4(ec2, ROOT / "runs-aws")
    if len(t4) > 1:
        write("table4_removal_moment.csv", t4, args.show)
    if miss4:
        print(f"skipped in table 4 (no per-message records): {', '.join(miss4)}")

    t5, miss5 = table5(ec2, laptop)
    if len(t5) > 1:
        write("table5_slack.csv", t5, args.show)
    if miss5:
        print(f"table 5 incomplete, per-message records missing for: "
              f"{', '.join(miss5)}")
    if miss4 or miss5:
        print("Download the per-message records from Zenodo and unpack them "
              "into runs-aws/ and runs/ to rebuild these two tables.")


if __name__ == "__main__":
    main()
