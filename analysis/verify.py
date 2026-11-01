#!/usr/bin/env python3
"""Check every published figure against the raw per-message records.

This script deliberately shares no logic with analyze.py. It reads
runs/<run_id>.messages.csv.gz, recounts everything from the per-message rows,
and compares the result with what the timeline file's readable header and its
SUMMARY_JSON claim. The point is that the human-readable summary cannot drift
away from the data it was derived from without this failing.

It also checks that the records are physically possible: a message cannot be
confirmed before it was created, removed before it existed, or finished before
it was received.

    python3 analysis/verify.py            # every run
    python3 analysis/verify.py <run_id>   # one run

Exit status is non-zero if any check fails.
"""

import csv
import os
import gzip
import json
import re
import statistics
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Which dataset to check. Defaults to runs/ and work/ so existing invocations
# behave as before; set QLAB_RUNS and QLAB_WORK to verify a second testbed
# (runs-aws/ and work-aws/) without copying anything around.
RUNS = ROOT / os.environ.get("QLAB_RUNS", "runs")
WORK = ROOT / os.environ.get("QLAB_WORK", "work")
NS = 1_000_000_000

# Timestamps are taken independently in the producer and the consumer process,
# both reading the VM wall clock, which is slewed while the run is in progress.
# That produces a handful of millisecond-scale inversions per run; the largest
# seen across the matrix is 65 ms, in the run whose recorded drift was 32 ms.
# Every threshold the analysis depends on is measured in seconds, so inversions
# below this bound are reported as clock noise rather than as broken records.
SLEW_TOLERANCE_NS = 100_000_000

# Labels as printed in the header, mapped to the terminal state they report.
HEADER_LABELS = {
    "processed in time": "processed_useful",
    "processed after its deadline (wasted work)": "processed_dead",
    "discarded by the consumer as already too old": "consumer_expired",
    "removed by the broker while still useful (USEFUL LOSS)": "lost_useful",
    "removed by the broker after its deadline": "lost_expired",
    "removed, exact time of removal unknown": "lost_unclassified",
    "refused by the broker at publish": "nacked",
    "refused by the producer because of consumer lag": "rejected_local_bp",
    "never published: the producer's local buffer was full": "shed",
    "publish failed or its confirmation never arrived": "error",
    "still waiting in the queue when the run ended": "remaining",
}


def read_header(path):
    """The commented header: the JSON lines and the state counts it prints."""
    meta, summary, events, printed = None, None, None, {}
    with path.open() as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            if line.startswith("# MANIFEST_JSON: "):
                meta = json.loads(line[len("# MANIFEST_JSON: "):])
            elif line.startswith("# SUMMARY_JSON: "):
                summary = json.loads(line[len("# SUMMARY_JSON: "):])
            elif line.startswith("# EVENTS_JSON: "):
                events = json.loads(line[len("# EVENTS_JSON: "):])
            else:
                body = line[1:].rstrip("\n")
                for label, key in HEADER_LABELS.items():
                    m = re.match(r"\s+" + re.escape(label) + r"\s+(\d+)\s+([\d.]+) %$", body)
                    if m:
                        printed[key] = (int(m.group(1)), float(m.group(2)))
    return meta, summary, events, printed


def read_messages(path):
    with gzip.open(path, "rt", newline="") as fh:
        for r in csv.DictReader(fh):
            yield r


def num(v):
    if v in (None, ""):
        return None
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return None


def verify(run_id):
    """Return (problems, facts) for one run."""
    tl = RUNS / f"{run_id}.timeline.csv"
    msg = RUNS / f"{run_id}.messages.csv.gz"
    problems, facts = [], {}
    if not tl.exists():
        return [f"{run_id}: no timeline file"], facts
    if not msg.exists():
        return [f"{run_id}: no messages file"], facts

    meta, summary, events, printed = read_header(tl)
    if meta is None or summary is None:
        return [f"{run_id}: header is missing MANIFEST_JSON or SUMMARY_JSON"], facts

    deadline_ms = meta["workload"]["deadline_ms"]
    rate = meta["workload"]["rate_msg_s"]
    publish_s = meta["phases_s"]["publish"]
    warmup = meta["phases_s"]["warmup"]

    states = Counter()
    nbr_states = Counter()
    main_rows = nbr_rows = 0
    impossible = Counter()
    slew = Counter()
    slew_max = 0.0
    misclassified = Counter()
    create_sec = Counter()
    delivered = 0
    dead_by_time = 0
    useful_loss_by_time = 0
    removed_total = 0
    nbr_failed = 0
    t_first = None

    for r in read_messages(msg):
        create = num(r["t_create_ns"])
        pub = num(r["t_publish_ns"])
        conf = num(r["t_confirm_ns"])
        recv = num(r["t_receive_ns"])
        done = num(r["t_done_ns"])
        dl = num(r["deadline_ns"])
        rem = num(r["t_removed_ns"])
        st = r["final_state"]

        if r["queue"] == "nbr":
            nbr_rows += 1
            nbr_states[r["publish_outcome"]] += 1
            if r["publish_outcome"] != "confirmed":
                nbr_failed += 1
            continue

        main_rows += 1
        states[st] += 1
        if t_first is None or (create is not None and create < t_first):
            t_first = create

        # physical possibility, with a tolerance for wall-clock slew
        def order(earlier, later, what):
            nonlocal slew_max
            if earlier is None or later is None or later >= earlier:
                return
            gap = earlier - later
            if gap <= SLEW_TOLERANCE_NS:
                slew[what] += 1
                slew_max = max(slew_max, gap / 1e6)
            else:
                impossible[what] += 1

        order(create, pub, "published before created")
        order(pub, conf, "confirmed before published")
        order(create, recv, "received before created")
        order(recv, done, "finished before received")
        order(create, rem, "removed before created")
        if dl is not None and create is not None and dl != create + deadline_ms * 1_000_000:
            impossible["deadline not create + D"] += 1

        # the classification must follow from the timestamps
        if st == "processed_useful" and done is not None and dl is not None and done > dl:
            misclassified["processed_useful but finished late"] += 1
        if st == "processed_dead" and done is not None and dl is not None and done <= dl:
            misclassified["processed_dead but finished in time"] += 1
        if st == "lost_useful" and rem is not None and dl is not None and rem > dl:
            misclassified["lost_useful but removed after deadline"] += 1
        if st == "lost_expired" and rem is not None and dl is not None and rem <= dl:
            misclassified["lost_expired but removed before deadline"] += 1

        if done is not None:
            delivered += 1
            # Dead work means processed after the deadline. A message the
            # consumer recognised as too old and discarded is the opposite of
            # dead work, even though its row also carries a late timestamp.
            if dl is not None and done > dl and r["consume_outcome"] == "done":
                dead_by_time += 1
        if st.startswith("lost"):
            removed_total += 1
            if rem is not None and dl is not None and rem <= dl:
                useful_loss_by_time += 1
        if create is not None:
            create_sec[int(create // NS)] += 1

    # ---- checks ----------------------------------------------------------
    expected = int(round(publish_s * rate))
    if main_rows != expected:
        problems.append(f"main rows {main_rows} != expected {expected}")
    if sum(states.values()) != main_rows:
        problems.append(f"states sum {sum(states.values())} != main rows {main_rows}")

    for k, v in impossible.items():
        problems.append(f"{v} rows: {k}")
    for k, v in misclassified.items():
        problems.append(f"{v} rows: {k}")

    # header's printed counts against the recount
    for key, (count, share) in printed.items():
        if states.get(key, 0) != count:
            problems.append(f"header prints {key}={count}, raw rows give {states.get(key, 0)}")
        exp_share = round(100 * count / max(main_rows, 1), 1)
        if abs(share - exp_share) > 0.15:
            problems.append(f"header prints {key} as {share}%, recomputed {exp_share}%")
    # every non-zero state must appear in the header
    for key, v in states.items():
        if v and key not in printed:
            problems.append(f"state {key}={v} is missing from the header")

    # SUMMARY_JSON against the recount
    for key, v in summary["states"].items():
        if states.get(key, 0) != v:
            problems.append(f"SUMMARY_JSON {key}={v}, raw rows give {states.get(key, 0)}")
    if summary["conservation"]["produced_rows"] != main_rows:
        problems.append(f"SUMMARY_JSON produced_rows={summary['conservation']['produced_rows']}, "
                        f"raw rows {main_rows}")
    if not summary["conservation"]["balanced"]:
        problems.append("SUMMARY_JSON says the accounting does not balance")

    # independent recomputation of the two headline metrics
    if dead_by_time != states.get("processed_dead", 0):
        problems.append(f"dead work by timestamp {dead_by_time} != state count "
                        f"{states.get('processed_dead', 0)}")
    if useful_loss_by_time != states.get("lost_useful", 0):
        problems.append(f"useful loss by timestamp {useful_loss_by_time} != state count "
                        f"{states.get('lost_useful', 0)}")

    # achieved rate as printed in the header
    if t_first is not None:
        warm = [v for s, v in create_sec.items()
                if 2 <= s - int(t_first // NS) < warmup]
        if warm:
            med = statistics.median(warm)
            facts["achieved_publish"] = med
            m = re.search(r"achieved\s+(\d+) msg/s published during the warmup",
                          tl.read_text()[:8000])
            if m and abs(int(m.group(1)) - med) > 1:
                problems.append(f"header says {m.group(1)} msg/s achieved, recomputed {med:.0f}")

    # neighbour figures
    nbr_expected = int(round(publish_s * meta["workload"]["neighbour_rate_msg_s"]))
    if nbr_rows != nbr_expected:
        problems.append(f"neighbour rows {nbr_rows} != expected {nbr_expected}")
    pf = summary["neighbour"]["publish_failures"]
    if sum(pf.values()) != nbr_failed:
        problems.append(f"neighbour publish failures: header {sum(pf.values())}, "
                        f"raw rows {nbr_failed}")

    # timeline body against the raw rows
    body = []
    with tl.open() as fh:
        body = [ln for ln in fh if not ln.startswith("#")]
    rows = list(csv.DictReader(body))
    tl_pub = sum(num(r["published"]) or 0 for r in rows)
    tl_deliv = sum(num(r["delivered"]) or 0 for r in rows)
    facts["timeline_rows"] = len(rows)
    if abs(tl_pub - main_rows) > 0.02 * main_rows:
        problems.append(f"timeline published column sums to {tl_pub}, raw rows {main_rows}")
    if delivered and abs(tl_deliv - delivered) > 0.02 * delivered:
        problems.append(f"timeline delivered column sums to {tl_deliv}, raw delivered {delivered}")

    # measurement quality: how many seconds of the run have no broker sample
    missing = sum(1 for r in rows if not r.get("queue_depth") and not r.get("kafka_lag_total"))
    facts["missing_samples"] = missing
    if missing > 20:
        problems.append(f"{missing} of {len(rows)} seconds have no broker sample; "
                        f"treat this run's depth-derived figures with care")

    # events in the header against the per-role files still in work/
    wd = WORK / run_id
    if wd.exists() and events is not None:
        hdr = Counter(e["event"] for e in events)
        for role in ("producer", "consumer", "nbr_producer", "nbr_consumer"):
            f = wd / f"{role}_events.csv"
            if not f.exists():
                continue
            with f.open() as fh:
                cnt = Counter(r["event"] for r in csv.DictReader(fh))
            for ev, v in cnt.items():
                if hdr.get(ev, 0) < v:
                    problems.append(f"work/{role}_events.csv has {v} {ev}, "
                                    f"header carries {hdr.get(ev, 0)}")

    facts.update(main_rows=main_rows, nbr_rows=nbr_rows, removed=removed_total,
                 delivered=delivered, states=dict(states),
                 slew_rows=sum(slew.values()), slew_max_ms=round(slew_max, 3))
    return problems, facts


def main():
    ids = [sys.argv[1]] if len(sys.argv) > 1 else \
        sorted(p.name[:-len(".timeline.csv")] for p in RUNS.glob("*.timeline.csv"))
    if not ids:
        print("no runs found", file=sys.stderr)
        return 1
    bad = 0
    print(f"{'run':<44} {'rows':>8} {'removed':>8} {'checks'}")
    for rid in ids:
        problems, facts = verify(rid)
        status = "ok" if not problems else f"{len(problems)} PROBLEM(S)"
        slewn = facts.get("slew_rows", 0)
        bits = []
        if slewn:
            bits.append(f"clock slew {slewn} rows, max {facts.get('slew_max_ms', 0)} ms")
        if facts.get("missing_samples"):
            bits.append(f"{facts['missing_samples']} s without a broker sample")
        note = ("  (" + "; ".join(bits) + ")") if bits else ""
        print(f"{rid:<44} {facts.get('main_rows', 0):>8} "
              f"{facts.get('removed', 0):>8} {status}{note}")
        for p in problems:
            print(f"    ! {p}")
        if problems:
            bad += 1
    print()
    print(f"{len(ids)} run(s) checked, {bad} with problems")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
