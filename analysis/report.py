#!/usr/bin/env python3
"""Turn the per-run summaries into the tables and figures used in the paper.

Reads runs/*/summary.json (written by analyze.py) and writes everything into
analysis/out/. Every table reports the median across replicates with the
observed minimum and maximum, because three replicates are too few for a
standard deviation to mean much.

    python3 analysis/analyze.py && python3 analysis/report.py
"""

import csv
import json
import statistics
import sys
from collections import defaultdict
import gzip
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze  # noqa: E402

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
# Same switch as analysis/verify.py: default to the laptop set so existing
# invocations are unchanged, and set QLAB_RUNS / QLAB_OUT to build the figures
# for the AWS set without moving any directory.
RUNS = ROOT / os.environ.get("QLAB_RUNS", "runs")
OUT = ROOT / os.environ.get("QLAB_OUT", "analysis/out")
FIG = OUT / "figures"
NS = 1_000_000_000

ARM_ORDER = ["R-C1-unbounded", "R-C2-maxlen-drophead", "R-C3-reject-publish",
             "R-C4-ttl", "R-C5-combined",
             "K-C1-long-retention", "K-C2-short-retention",
             "K-C3-lag-backpressure", "K-C4-consumer-deadline",
             "K-C6-retention-below-D",
             "R-C9-wide-headroom", "K-C7-retention-bytes",
             "R-C11-reject-correct", "R-C12-maxlen-correct-wide"]
SHORT = {
    "R-C1-unbounded": "R1 unbounded",
    "R-C2-maxlen-drophead": "R2 L=24k drop-head",
    "R-C3-reject-publish": "R3 L=24k reject",
    "R-C4-ttl": "R4 TTL=0.9D",
    "R-C5-combined": "R5 TTL+reject+check",
    "K-C1-long-retention": "K1 retention 7 d",
    "K-C2-short-retention": "K2 retention 120 s",
    "K-C3-lag-backpressure": "K3 lag backpressure",
    "K-C4-consumer-deadline": "K4 consumer check",
    "K-C6-retention-below-D": "K6 retention 30 s",
    "R-C9-wide-headroom": "R9 unbounded, 2x headroom",
    "K-C7-retention-bytes": "K7 byte bound (retention.bytes)",
    "R-C11-reject-correct": "R11 L=23 700 reject",
    "R-C12-maxlen-correct-wide": "R12 L=59 750 drop-head",
}
# Colour-blind safe, distinguishable in greyscale print.
PALETTE = {"useful": "#4878a8", "dead": "#e8a33d", "loss_useful": "#b5422e",
           "loss_expired": "#9c9c9c", "rejected": "#5c9e6e", "remaining": "#d6d6d6"}


def load(include_pilot=False):
    """Summaries, read from the SUMMARY_JSON line in each timeline header."""
    return analyze.load_summaries(include_pilot)


def agg(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {"median": statistics.median(vals), "min": min(vals),
            "max": max(vals), "n": len(vals)}


def fmt(a, digits=1, scale=1.0, dash="--"):
    if a is None:
        return dash
    m, lo, hi = a["median"] * scale, a["min"] * scale, a["max"] * scale
    if digits == 0:
        return f"{m:.0f} [{lo:.0f}--{hi:.0f}]"
    return f"{m:.{digits}f} [{lo:.{digits}f}--{hi:.{digits}f}]"


# ---------------------------------------------------------------------------
def read_metrics(run_id):
    """Per-second rows of a run's timeline, with queue depth under a stable name."""
    p = RUNS / f"{run_id}.timeline.csv"
    if not p.exists():
        return []
    out = []
    for r in analyze.read_timeline_csv(p):
        r["main_depth"] = r.get("queue_depth", "")
        out.append(r)
    return out


def read_events(run_id):
    p = RUNS / f"{run_id}.timeline.csv"
    if not p.exists():
        return []
    try:
        return analyze.header_json(p, "EVENTS_JSON") or []
    except Exception:
        return []


def fig_backlog(summaries):
    """Depth over time for the RabbitMQ arms, with alarms and purges marked."""
    picks = [("R-C1-unbounded", "unbounded"),
             ("R-C2-maxlen-drophead", "max-length 24 000, drop-head"),
             ("R-C12-maxlen-correct-wide", "max-length 59 750, drop-head"),
             ("R-C4-ttl", "TTL = 0.9 D")]
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    plotted = 0
    for cfg, label in picks:
        rid = next((s["run_id"] for s in summaries
                    if s["config"] == cfg and s["scenario"] == "A"
                    and s["factor"] == 5 and s["rep"] == 1), None)
        if not rid:
            continue
        rows = read_metrics(rid)
        xs, ys = [], []
        for r in rows:
            d = r.get("main_depth")
            if d in (None, "") or float(d) < 0:
                continue
            xs.append(float(r["elapsed_s"]))
            ys.append(int(float(d)))
        if not xs:
            continue
        line, = ax.plot(xs, ys, label=label, linewidth=1.4)
        plotted += 1
        for e in read_events(rid):
            if e.get("event") == "operator.purge":
                ax.plot([e["elapsed_s"]], [e.get("depth_before", 0)], marker="v",
                        color=line.get_color(), markersize=7, linestyle="none")
            if e.get("event") == "broker.alarm_on":
                ax.axvline(e["elapsed_s"], color=line.get_color(), linestyle=":", alpha=0.5)
    if not plotted:
        plt.close(fig)
        return None
    ax.axvspan(60, 420, color="#000000", alpha=0.05)
    # headroom above the tallest bound so the band label does not sit on a line
    ax.set_ylim(top=ax.get_ylim()[1] * 1.12)
    ax.annotate("downstream 5x slower", xy=(240, ax.get_ylim()[1] * 0.96),
                ha="center", fontsize=8, color="#555555")
    ax.set_xlabel("time since start of run (s)")
    ax.set_ylabel("messages in queue")
    ax.legend(fontsize=8, frameon=False)
    ax.set_title("Backlog under a 360 s downstream slowdown\n"
                 "dotted line: resource alarm, triangle: operator purge", fontsize=9)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"fig1_backlog.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return "fig1_backlog"


def fig_outcomes(summaries):
    """Where every published message ended up, per configuration."""
    by = defaultdict(list)
    for s in summaries:
        if s["scenario"] == "A" and s["factor"] == 5:
            by[s["config"]].append(s)
    cfgs = [c for c in ARM_ORDER if c in by]
    if not cfgs:
        return None
    keys = [("processed_useful", "processed in time", PALETTE["useful"]),
            ("processed_dead", "processed after deadline (dead work)", PALETTE["dead"]),
            ("lost_useful", "removed while still useful", PALETTE["loss_useful"]),
            ("lost_expired", "removed after deadline", PALETTE["loss_expired"]),
            ("refused", "refused at publish", PALETTE["rejected"]),
            ("remaining", "still queued at end", PALETTE["remaining"])]
    fig, ax = plt.subplots(figsize=(8.4, 4.4))
    bottoms = [0.0] * len(cfgs)
    for key, label, colour in keys:
        vals = []
        for c in cfgs:
            rs = by[c]
            if key == "refused":
                v = [(s["states"]["nacked"] + s["states"]["rejected_local_bp"]
                      + s["states"]["shed"]) / max(s["conservation"]["produced_rows"], 1)
                     for s in rs]
            else:
                v = [s["states"][key] / max(s["conservation"]["produced_rows"], 1) for s in rs]
            vals.append(100 * statistics.median(v))
        ax.bar(range(len(cfgs)), vals, bottom=bottoms, label=label, color=colour,
               edgecolor="white", linewidth=0.5)
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    ax.set_xticks(range(len(cfgs)))
    ax.set_xticklabels([SHORT[c] for c in cfgs], rotation=35, ha="right", fontsize=8)
    ax.set_ylabel("share of published messages (%)")
    ax.legend(fontsize=8, frameon=False, loc="lower center",
              bbox_to_anchor=(0.5, 1.01), ncol=3)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"fig2_outcomes.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return "fig2_outcomes"


def fig_neighbour(summaries):
    """What the backlog does to an unrelated queue on the same broker."""
    by = defaultdict(list)
    for s in summaries:
        if s["scenario"] == "A" and s["factor"] == 5:
            by[s["config"]].append(s)
    cfgs = [c for c in ARM_ORDER if c in by]
    if not cfgs:
        return None
    base, slow = [], []
    for c in cfgs:
        b = [s["neighbour"]["confirm_latency"]["baseline"].get("p99_ms") for s in by[c]]
        s_ = [s["neighbour"]["confirm_latency"]["slow"].get("p99_ms") for s in by[c]]
        b = [x for x in b if x is not None] or [0]
        s_ = [x for x in s_ if x is not None] or [0]
        base.append(statistics.median(b))
        slow.append(statistics.median(s_))
    x = range(len(cfgs))
    fig, ax = plt.subplots(figsize=(7.6, 3.8))
    ax.bar([i - 0.2 for i in x], base, width=0.4, label="before the slowdown",
           color="#9bb7d4")
    ax.bar([i + 0.2 for i in x], slow, width=0.4, label="during the slowdown",
           color="#b5422e")
    ax.set_yscale("log")
    ax.set_xticks(list(x))
    ax.set_xticklabels([SHORT[c] for c in cfgs], rotation=30, ha="right", fontsize=8)
    ax.set_ylabel("neighbour publish-confirm p99 (ms, log scale)")
    ax.set_title("Publish-confirm latency of an unrelated queue on the same broker", fontsize=9)
    ax.legend(fontsize=8, frameon=False)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"fig3_neighbour.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return "fig3_neighbour"


def fig_drain(summaries):
    xs = [s["model_drain_s"] for s in summaries if s["model_drain_s"] and s["measured_drain_s"]]
    ys = [s["measured_drain_s"] for s in summaries if s["model_drain_s"] and s["measured_drain_s"]]
    if len(xs) < 3:
        return None
    fig, ax = plt.subplots(figsize=(4.2, 4.0))
    ax.scatter(xs, ys, s=22, color="#4878a8", alpha=0.8)
    hi = max(max(xs), max(ys)) * 1.08
    ax.plot([0, hi], [0, hi], linestyle="--", color="#888888", linewidth=1)
    ax.set_xlabel("model prediction B / (mu - lambda)  (s)")
    ax.set_ylabel("measured drain time (s)")
    ax.set_title("Drain time: model against measurement", fontsize=9)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"fig4_drain.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return "fig4_drain"


def fig_kafka_offsets(summaries):
    """Silent deletion in Kafka: the log start offset overtaking the group."""
    rid = next((s["run_id"] for s in summaries
                if s["config"] in ("K-C6-retention-below-D", "K-C2-short-retention")
                and s["scenario"] == "A" and s["rep"] == 1), None)
    if not rid:
        return None
    rows = read_metrics(rid)
    pts = [(float(r["elapsed_s"]), r.get("kafka_log_start_p0"),
            r.get("kafka_end_p0"), r.get("kafka_committed_p0")) for r in rows]
    pts = [(x, a, b, c) for x, a, b, c in pts if a not in (None, "") and b not in (None, "")]
    if len(pts) < 10:
        return None
    xs = [p[0] for p in pts]
    start = [int(float(p[1])) for p in pts]
    end = [int(float(p[2])) for p in pts]
    com = [int(float(p[3])) if p[3] not in (None, "") else -1 for p in pts]
    fig, ax = plt.subplots(figsize=(7.0, 3.4))
    ax.plot(xs, end, label="end offset (produced)", color="#4878a8", linewidth=1.4)
    ax.plot(xs, com, label="committed offset (consumed)", color="#e8a33d", linewidth=1.4)
    ax.plot(xs, start, label="log start offset (retained)", color="#b5422e", linewidth=1.4)
    ax.fill_between(xs, com, start, where=[s > c for s, c in zip(start, com)],
                    color="#b5422e", alpha=0.2, label="deleted before being read")
    ax.set_xlabel("time since start of run (s)")
    ax.set_ylabel("offset in partition 0")
    ax.legend(fontsize=8, frameon=False)
    ax.set_title("Kafka with retention shorter than the backlog: deletion without an error",
                 fontsize=9)
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"fig5_kafka_offsets.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return "fig5_kafka_offsets"


def fig_slack(summaries):
    """Distribution of slack to the deadline for the four length-bound arms.

    The paper's main negative result is that two candidate sizing rules both
    land within a fraction of a percent of the deadline, one just inside and
    one just outside. A table of worst case and p1 states that; the shape of
    the distribution is what shows it is not an outlier effect, so the x axis
    is clipped to the decisive region around zero.
    """
    # R2 is deliberately absent: its worst slack is +35.49 s, two orders of
    # magnitude outside the region that decides the other three, and including
    # it would compress exactly the detail the figure exists to show.
    picks = [("R-C3-reject-publish", "R3  L=24 000 reject"),
             ("R-C11-reject-correct", "R11 L=23 700 reject"),
             ("R-C12-maxlen-correct-wide", "R12 L=59 750 drop-head")]
    fig, ax = plt.subplots(figsize=(7.2, 3.8))
    plotted = 0
    for cfg, label in picks:
        vals = []
        for s in summaries:
            if s["config"] != cfg or s["scenario"] != "A" or s["factor"] != 5:
                continue
            p = RUNS / f"{s['run_id']}.messages.csv.gz"
            if not p.exists():
                continue
            with gzip.open(p, "rt") as fh:
                for r in csv.DictReader(fh):
                    # Main queue only. The neighbour queue is never slowed and
                    # never bounded, so its deliveries all sit at a large
                    # positive slack; including them would silently change the
                    # denominator away from the one the slack table uses.
                    if r.get("queue") != "main":
                        continue
                    d, done = r.get("deadline_ns"), r.get("t_done_ns")
                    if d and done and done not in ("", "-1", "0"):
                        vals.append((int(d) - int(done)) / 1e9)
        if not vals:
            continue
        vals.sort()
        n = len(vals)
        ax.plot(vals, [100 * i / n for i in range(n)], label=label, linewidth=1.6)
        plotted += 1
    if not plotted:
        plt.close(fig)
        return None
    ax.axvline(0, color="#b22222", linewidth=1.0, linestyle="--")
    ax.annotate("deadline", xy=(0, 0), xytext=(0.02, 22.4), fontsize=8, color="#b22222")
    ax.annotate("R2, L = 24 000: +35.49 s, off scale", xy=(0, 0), xytext=(-0.86, 20.6),
                fontsize=8, color="#555555")
    ax.set_xlim(-0.9, 0.45)
    ax.set_ylim(0, 24)
    ax.set_xlabel("slack to the deadline at completion (s)")
    ax.set_ylabel("share of deliveries at or below (%)")
    ax.legend(fontsize=8, frameon=False, loc="lower right")
    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(FIG / f"fig6_slack.{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return "fig6_slack"


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    summaries = load("--include-pilot" in sys.argv)
    if not summaries:
        print("no summaries found; run analyze.py first", file=sys.stderr)
        return 1
    print(f"loaded {len(summaries)} runs")
    # Guard against a dataset assembled from runs with different phase timings,
    # which would make the arms incomparable without anything looking wrong.
    timings = {}
    for s in summaries:
        key = json.dumps(s.get("phases_s"), sort_keys=True)
        timings.setdefault(key, []).append(s["run_id"])
    per_scenario = defaultdict(set)
    for s in summaries:
        per_scenario[s["scenario"]].add(json.dumps(s.get("phases_s"), sort_keys=True))
    mixed = {sc: ts for sc, ts in per_scenario.items() if len(ts) > 1}
    if mixed:
        print("WARNING: runs within a scenario do not share phase timings; "
              f"scenarios affected: {sorted(mixed)}", file=sys.stderr)
        for key, ids in timings.items():
            print(f"  {key}: {len(ids)} run(s)", file=sys.stderr)

    # Tables are not generated any more: every table that used to live under
    # analysis/out/tables/ is now printed inside RESULTS.md or
    # RESULTS-APPENDIX.md, and keeping a second rendering of the same numbers
    # only invited them to drift apart. Per-run metrics stay machine-readable in
    # runs.csv, which is what the write-up points at.
    figs = [f for f in (fig_backlog(summaries), fig_outcomes(summaries),
                        fig_neighbour(summaries), fig_drain(summaries),
                        fig_kafka_offsets(summaries), fig_slack(summaries)) if f]

    unbalanced = [s["run_id"] for s in summaries if not s["conservation"]["balanced"]]
    if unbalanced:
        print(f"WARNING: {len(unbalanced)} run(s) failed the conservation check: "
              + ", ".join(unbalanced), file=sys.stderr)
    else:
        print("all runs passed the conservation check")
    print(f"wrote {len(figs)} figures to {FIG}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
