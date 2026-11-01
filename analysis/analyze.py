#!/usr/bin/env python3
"""Derive every number in the paper from the raw per-run files.

The central discipline here is conservation: each published sequence number is
assigned to exactly one terminal state, and the states must sum back to the
number of messages the producer attempted. A run whose accounting does not
balance is reported as such instead of being quietly averaged in.

Terminal states
---------------
    shed                 producer never published it (local load shedding)
    rejected_local_bp    producer refused it on consumer lag (Kafka class 3)
    nacked               broker refused the publish (reject-publish, quota)
    error                publish failed
    processed_useful     consumed and finished before its deadline
    processed_dead       consumed and finished after its deadline  -> dead work
    consumer_expired     consumer saw it was past its deadline and discarded it
    lost_useful          removed by the broker while still inside its deadline
    lost_expired         removed by the broker after its deadline had passed
    remaining            still in the queue when the run ended

Usage:
    python3 analysis/analyze.py                 # all runs
    python3 analysis/analyze.py --run <run_id>   # one run
"""

import argparse
import bisect
import csv
import gzip
import json
import math
import sys
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# QLAB_RUNS / QLAB_OUT select the dataset; defaults keep every existing
# invocation pointed at the laptop set, as before.
RUNS = ROOT / os.environ.get("QLAB_RUNS", "runs")
# The per-run working directories, each with its manifest.json, are what a
# full re-derivation reads; runs/ holds only the two published files.
WORK = ROOT / os.environ.get("QLAB_WORK", "work")
OUT = ROOT / os.environ.get("QLAB_OUT", "analysis/out")

NS = 1_000_000_000


def open_maybe_gz(run_dir, name):
    gz = run_dir / (name + ".gz")
    if gz.exists():
        return gzip.open(gz, "rt", newline="")
    p = run_dir / name
    if p.exists():
        return p.open(newline="")
    return None


def read_csv(run_dir, name):
    fh = open_maybe_gz(run_dir, name)
    if fh is None:
        return []
    with fh:
        return list(csv.DictReader(fh))


def _from_messages(run_dir, side):
    """Project the consolidated messages.csv back into a producer or consumer view.

    consolidate.py joins the two sides into one file and removes the originals,
    so re-analysing a consolidated run has to read the join. The projection keeps
    the original column names, which is what the rest of this script expects.
    """
    src = read_csv(run_dir, "messages.csv")
    out = []
    for r in src:
        if side == "producer":
            out.append({"seq": r.get("seq"), "t_create_ns": r.get("t_create_ns"),
                        "t_publish_ns": r.get("t_publish_ns"),
                        "t_confirm_ns": r.get("t_confirm_ns"),
                        "size": r.get("size_bytes"), "outcome": r.get("publish_outcome"),
                        "partition": r.get("partition"), "offset": r.get("offset")})
        elif r.get("consume_outcome"):
            out.append({"seq": r.get("seq"), "t_create_ns": r.get("t_create_ns"),
                        "deadline_ns": r.get("deadline_ns"),
                        "t_receive_ns": r.get("t_receive_ns"),
                        "t_done_ns": r.get("t_done_ns"),
                        "outcome": r.get("consume_outcome"),
                        "partition": r.get("partition"), "offset": r.get("offset")})
    return out


def _nbr(run_dir, side):
    """Neighbour rows from the consolidated neighbour.csv."""
    out = []
    for r in read_csv(run_dir, "neighbour.csv"):
        if side == "producer":
            out.append({"seq": r.get("seq"), "t_create_ns": r.get("t_create_ns"),
                        "t_confirm_ns": r.get("t_confirm_ns"),
                        "outcome": r.get("publish_outcome")})
        elif r.get("consume_outcome"):
            out.append({"seq": r.get("seq"), "t_create_ns": r.get("t_create_ns"),
                        "t_done_ns": r.get("t_done_ns"),
                        "outcome": r.get("consume_outcome")})
    return out


def read_side(run_dir, side):
    name = "producer.csv" if side == "producer" else "consumer.csv"
    if open_maybe_gz(run_dir, name):
        return read_csv(run_dir, name)
    return _from_messages(run_dir, side)


def kafka_lag_by_second(run_dir, t0):
    """Total main-topic lag per second of the run, from the probe's own samples.

    broker_metrics.csv cannot be used for this. The orchestrator fills its
    main_lag column by re-reading the last line of kafka_probe.csv at its own
    tick, so that column carries a reading up to one second old and stamps it
    with the current time. At the end of the slow phase the lag falls by some
    3000 messages a second, so the staleness was worth thousands of messages
    and made runs.csv disagree with the published timeline. The probe's rows
    are the measurement; this reads them directly.
    """
    out = {}
    if t0 is None:
        return out
    for r in read_csv(run_dir, "kafka_probe.csv"):
        if r.get("topic") != "main":
            continue
        t_ns, lag = num(r.get("t_ns")), num(r.get("lag"), 0) or 0
        if t_ns is None:
            continue
        s = int((t_ns - t0) // NS)
        if lag > 0:
            out[s] = out.get(s, 0) + lag
        else:
            out.setdefault(s, 0)
    return out


def read_timeline_metrics(run_dir):
    """Broker samples, from broker_metrics.csv or the consolidated timeline."""
    if open_maybe_gz(run_dir, "broker_metrics.csv"):
        return read_csv(run_dir, "broker_metrics.csv")
    out = []
    for r in read_csv(run_dir, "timeline.csv"):
        out.append({"elapsed_s": r.get("elapsed_s"), "t_ns": "",
                    "main_depth": r.get("queue_depth"),
                    "main_ready": r.get("queue_ready"),
                    "main_unacked": r.get("queue_unacked"),
                    "nbr_depth": r.get("nbr_depth"),
                    "node_mem_used": r.get("broker_mem_bytes"),
                    "node_mem_alarm": r.get("mem_alarm"),
                    "node_disk_free": r.get("disk_free_bytes"),
                    "node_disk_alarm": r.get("disk_alarm"),
                    "conn_total": r.get("connections_total"),
                    "conn_blocked": r.get("connections_blocked"),
                    "main_lag": r.get("kafka_lag_total")})
    return out


def header_json(timeline_path, key):
    """Read one of the JSON provenance lines out of a timeline file's header."""
    prefix = f"# {key}: "
    with timeline_path.open() as fh:
        for line in fh:
            if line.startswith(prefix):
                return json.loads(line[len(prefix):])
            if not line.startswith("#"):
                break
    return None


def load_summaries(include_pilot=False):
    """Summaries of all finished runs, taken from the timeline file headers."""
    out = []
    for p in sorted(RUNS.glob("*.timeline.csv")):
        if not include_pilot and ("pilot" in p.name or "smoke" in p.name):
            continue
        try:
            s = header_json(p, "SUMMARY_JSON")
        except Exception:
            s = None
        if s:
            out.append(s)
    return out


def read_timeline_csv(timeline_path):
    """The per-second rows of a timeline file, with the comment header skipped."""
    with timeline_path.open() as fh:
        rows = [ln for ln in fh if not ln.startswith("#")]
    return list(csv.DictReader(rows))


def read_events(run_dir):
    p = run_dir / "events.jsonl"
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def num(v, default=None):
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return default


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def latency_stats(values_ms):
    v = sorted(values_ms)
    if not v:
        return {}
    return {
        "n": len(v),
        "p50_ms": round(pct(v, 0.50), 2),
        "p95_ms": round(pct(v, 0.95), 2),
        "p99_ms": round(pct(v, 0.99), 2),
        "max_ms": round(v[-1], 2),
    }


# ---------------------------------------------------------------------------
# drop-time reconstruction
# ---------------------------------------------------------------------------
def rabbit_drop_times(run_dir, lost_seqs, confirmed_at, received_at, created_at,
                      manifest, events):
    """Exact removal times for RabbitMQ, by replaying the queue.

    An earlier version reconstructed these from the 1 Hz depth samples via
    confirmed(t) - delivered(t) - depth(t). That was wrong: the estimate is
    sensitive to the pipeline lag between a broker sample and the consumer's own
    timestamps, and once the monotonic envelope locked in a transient it
    attributed removals to instants before the messages existed.

    A classic queue is FIFO and every rule that removes a message is
    deterministic, so the queue can simply be replayed from the per-message
    timestamps, which are exact to the nanosecond:

      * a confirm appends the message,
      * a receive removes it (that is when it leaves the queue, not when the
        handler finishes),
      * x-max-length with drop-head removes the head at the instant the confirm
        that overflowed the queue arrives,
      * x-message-ttl removes the head once it is older than the TTL, recorded
        at the message's own expiry instant,
      * a purge removes everything present, at the purge timestamp.

    Nothing here depends on polling the broker, so the result carries no
    sampling resolution at all.
    """
    from collections import deque

    qa = manifest.get("queue_args") or {}
    max_len = num(qa.get("x-max-length"))
    ttl_ns = num(qa.get("x-message-ttl"))
    ttl_ns = ttl_ns * 1_000_000 if ttl_ns else None

    # Purge timestamps come from the orchestrator, which runs on the host clock,
    # while message timestamps are taken inside the VM. Shift them by the
    # measured offset, interpolated across the run.
    off = manifest.get("clock_offset_ns") or {}
    o0, o1 = off.get("start") or 0, off.get("end") or 0
    t_first = min(confirmed_at.values()) if confirmed_at else 0
    t_last = max(confirmed_at.values()) if confirmed_at else 1
    span = max(t_last - t_first, 1)

    def to_vm(host_ns):
        frac = min(max((host_ns + o0 - t_first) / span, 0.0), 1.0)
        return host_ns + o0 + int((o1 - o0) * frac)

    purges = sorted(to_vm(e["t_ns"]) for e in events
                    if e.get("event") == "operator.purge")

    stream = [(ts, 0, s) for s, ts in confirmed_at.items()]
    stream += [(ts, 1, s) for s, ts in received_at.items()]
    stream += [(ts, 2, -1) for ts in purges]
    stream.sort()

    q = deque()
    inq = set()
    drop_t = {}

    def expire(now):
        if ttl_ns is None:
            return
        while q:
            head = q[0]
            exp = created_at.get(head)
            if exp is None:
                q.popleft()
                inq.discard(head)
                continue
            exp += ttl_ns
            if exp > now:
                break
            q.popleft()
            inq.discard(head)
            drop_t[head] = exp

    for ts, kind, seq in stream:
        expire(ts)
        if kind == 0:
            q.append(seq)
            inq.add(seq)
            if max_len is not None:
                while len(q) > max_len:
                    head = q.popleft()
                    inq.discard(head)
                    drop_t[head] = ts
        elif kind == 1:
            if seq in inq:
                inq.discard(seq)
                try:
                    q.remove(seq)
                except ValueError:
                    pass
        else:
            while q:
                head = q.popleft()
                inq.discard(head)
                drop_t[head] = ts
    expire(t_last)

    # Report how well the replay explains the messages that really went missing.
    lost = set(lost_seqs)
    explained = sum(1 for s in lost if s in drop_t)
    note = None
    if lost and explained < len(lost):
        note = (f"queue replay explained {explained} of {len(lost)} removals; "
                f"{len(lost) - explained} have no removal time")
    return {s: drop_t[s] for s in lost if s in drop_t}, note


def kafka_drop_times(run_dir, lost_offsets):
    """Exact removal times from the probe's log_start_offset trace.

    lost_offsets maps seq -> (partition, offset). A record is gone once the
    partition's log start offset has moved past it, which the probe samples
    directly, so no reconstruction is needed.
    """
    probe = read_csv(run_dir, "kafka_probe.csv")
    starts = {}
    for r in probe:
        if r.get("topic") != "main":
            continue
        p = num(r.get("partition"))
        t = num(r.get("t_ns"))
        ls = num(r.get("log_start_offset"), -1)
        if p is None or t is None or ls is None or ls < 0:
            continue
        starts.setdefault(p, []).append((t, ls))
    for p in starts:
        starts[p].sort()

    drop_t = {}
    for seq, (part, off) in lost_offsets.items():
        trace = starts.get(part)
        if not trace:
            continue
        for t, ls in trace:
            if ls > off:
                drop_t[seq] = t
                break
    return drop_t


# ---------------------------------------------------------------------------
# per-run analysis
# ---------------------------------------------------------------------------
def analyse_run(run_dir, detail=False):
    manifest = json.loads((run_dir / "manifest.json").read_text())
    events = read_events(run_dir)
    broker = manifest["broker"]
    phases = manifest["phases_s"]
    warmup, slow = phases["warmup"], phases["slow"]
    t0 = next((e["t0_ns"] for e in events if e.get("event") == "run.t0"), None)

    prod = read_side(run_dir, "producer")
    cons = read_side(run_dir, "consumer")

    final_state = {}
    removed_at = {}
    states = {k: 0 for k in (
        "shed", "rejected_local_bp", "nacked", "error",
        "processed_useful", "processed_dead", "consumer_expired",
        "lost_useful", "lost_expired", "lost_unclassified", "remaining")}

    # The producer's rows define the universe of messages: every row gets
    # exactly one terminal state, which is what makes the conservation check
    # below meaningful rather than circular.
    prod_outcome, confirmed_at, created_at, offsets = {}, {}, {}, {}
    for r in prod:
        seq = num(r.get("seq"))
        o = r.get("outcome")
        prod_outcome[seq] = o
        ct = num(r.get("t_create_ns"))
        if ct is not None:
            created_at[seq] = ct
        if o == "confirmed":
            t = num(r.get("t_confirm_ns"))
            if t is not None:
                confirmed_at[seq] = t
            part, off = num(r.get("partition")), num(r.get("offset"))
            if part is not None and off is not None:
                offsets[seq] = (part, off)

    # Delivery is the stronger evidence. A message whose confirm was lost when
    # the connection dropped is recorded by the producer as an error, yet the
    # broker may well have stored and delivered it; counting the producer's view
    # and the consumer's view independently would count it twice.
    delivered = {}
    duplicate_deliveries = 0
    for r in cons:
        seq = num(r.get("seq"))
        if seq in delivered:
            duplicate_deliveries += 1
        delivered[seq] = r

    deadlines = {}
    delivered_at = []
    dead_work_ns = 0
    delivered_unconfirmed = 0
    orphan_deliveries = 0
    for seq, r in delivered.items():
        if seq not in prod_outcome:
            orphan_deliveries += 1
            continue
        if prod_outcome[seq] != "confirmed":
            delivered_unconfirmed += 1
        dl = num(r.get("deadline_ns"))
        deadlines[seq] = dl
        if r.get("outcome") == "skipped_expired":
            states["consumer_expired"] += 1
            final_state[seq] = "consumer_expired"
            delivered_at.append(num(r.get("t_receive_ns")))
            continue
        done = num(r.get("t_done_ns"))
        recv = num(r.get("t_receive_ns"))
        if done is not None:
            delivered_at.append(done)
        if dl is not None and done is not None and done > dl:
            states["processed_dead"] += 1
            final_state[seq] = "processed_dead"
            if recv is not None:
                dead_work_ns += done - recv
        else:
            states["processed_useful"] += 1
            final_state[seq] = "processed_useful"

    for seq, o in prod_outcome.items():
        if seq in delivered:
            continue
        if o == "confirmed":
            continue  # resolved below as lost or still queued
        if o == "skipped_backpressure":
            states["shed"] += 1
            final_state[seq] = "shed"
        elif o == "rejected_local_bp":
            states["rejected_local_bp"] += 1
            final_state[seq] = "rejected_local_bp"
        elif o == "nacked":
            states["nacked"] += 1
            final_state[seq] = "nacked"
        else:
            states["error"] += 1
            final_state[seq] = "error"

    # Messages the broker acknowledged but the consumer never saw are either
    # still queued at the end of the run or were removed by the broker.
    unseen = sorted(set(confirmed_at) - set(delivered))
    if broker == "rabbit":
        metrics = read_timeline_metrics(run_dir)
        tail = [num(r.get("main_depth"), -1) for r in metrics[-5:]]
        tail = [v for v in tail if v is not None and v >= 0]
        remaining_n = min(tail) if tail else 0
    else:
        probe = read_csv(run_dir, "kafka_probe.csv")
        last_t = max((num(r.get("t_ns")) for r in probe), default=None)
        remaining_n = 0
        if last_t is not None:
            for r in probe:
                if r.get("topic") == "main" and num(r.get("t_ns")) == last_t:
                    remaining_n += max(num(r.get("lag"), 0) or 0, 0)
    remaining_n = min(remaining_n, len(unseen))
    # FIFO: the newest unseen messages are the ones still waiting.
    remaining = set(unseen[len(unseen) - remaining_n:]) if remaining_n else set()
    lost = [s for s in unseen if s not in remaining]
    states["remaining"] = len(remaining)
    for s in remaining:
        final_state[s] = "remaining"

    note = None
    lost_useful_strict = 0
    if broker == "rabbit":
        received_at = {}
        for seq, r in delivered.items():
            rt = num(r.get("t_receive_ns"))
            if rt is not None:
                received_at[seq] = rt
        drop_t, note = rabbit_drop_times(
            run_dir, lost, confirmed_at, received_at, created_at, manifest, events)
    else:
        drop_t = kafka_drop_times(run_dir, {s: offsets[s] for s in lost if s in offsets})

    for s in lost:
        dl = deadlines.get(s)
        t = drop_t.get(s)
        if dl is None:
            # The consumer never saw it, so the deadline is reconstructed from
            # the creation time the producer recorded plus the useful lifetime D.
            # Creation time, not confirm time: the deadline is carried inside the
            # message and starts when the message is made.
            ct = created_at.get(s)
            dl = ct + manifest["workload"]["deadline_ms"] * 1_000_000 if ct else None
        if t is not None:
            removed_at[s] = t
        if t is None or dl is None:
            states["lost_unclassified"] += 1
            final_state[s] = "lost_unclassified"
        elif t <= dl:
            states["lost_useful"] += 1
            final_state[s] = "lost_useful"
            # Strict variant: exclude removals inside the last second of the
            # useful window, which is the resolution of the removal time. The
            # lenient count above is the one reported, because it is the count
            # that makes the bounded arms look worse, not better.
            if t <= dl - NS:
                lost_useful_strict += 1
        else:
            states["lost_expired"] += 1
            final_state[s] = "lost_expired"

    produced = len(prod_outcome)
    total = sum(states.values())
    conservation = {
        "produced_rows": produced,
        "producer_csv_rows": len(prod),
        "duplicate_producer_seqs": len(prod) - produced,
        "states_total": total,
        "balanced": produced == total and len(prod) == produced and orphan_deliveries == 0,
        "difference": produced - total,
        # Diagnostics that explain a difference rather than hiding it.
        "duplicate_deliveries": duplicate_deliveries,
        "delivered_without_confirm": delivered_unconfirmed,
        "deliveries_with_no_producer_row": orphan_deliveries,
    }

    # ---- neighbour impact -------------------------------------------------
    nbr_p = read_csv(run_dir, "nbr_producer.csv") or _nbr(run_dir, "producer")
    nbr_c = read_csv(run_dir, "nbr_consumer.csv") or _nbr(run_dir, "consumer")

    def window(t_ns):
        if t0 is None or t_ns is None:
            return None
        e = (t_ns - t0) / NS
        if e < warmup:
            return "baseline"
        if e < warmup + slow:
            return "slow"
        return "drain"

    nbr_confirm = {"baseline": [], "slow": [], "drain": []}
    nbr_blocked = {"baseline": 0, "slow": 0, "drain": 0}
    for r in nbr_p:
        ct, cf = num(r.get("t_create_ns")), num(r.get("t_confirm_ns"))
        w = window(ct)
        if w is None:
            continue
        if r.get("outcome") == "confirmed" and cf:
            nbr_confirm[w].append((cf - ct) / 1e6)
        elif r.get("outcome") in ("skipped_backpressure", "nacked", "error"):
            nbr_blocked[w] += 1
    nbr_e2e = {"baseline": [], "slow": [], "drain": []}
    for r in nbr_c:
        ct, done = num(r.get("t_create_ns")), num(r.get("t_done_ns"))
        w = window(ct)
        if w and done:
            nbr_e2e[w].append((done - ct) / 1e6)

    # ---- alarms, purges, drain -------------------------------------------
    def ev_first(kind, field="elapsed_s"):
        for e in events:
            if e.get("event") == kind:
                return e.get(field)
        return None

    alarm_on = ev_first("broker.alarm_on")
    drained_at = ev_first("phase.drained")
    purges = [e for e in events if e.get("event") == "operator.purge"]

    # The resource alarm does not latch: as the queue is consumed the node drops
    # back below the threshold and the alarm clears, only to fire again. Counting
    # episodes and total time in alarm describes the slow phase better than the
    # timestamp of the first alarm alone.
    alarm_episodes = 0
    alarm_samples = blocked_samples = 0
    alarm_samples_slow = 0
    prev_alarm = 0
    if broker == "rabbit":
        for r in read_timeline_metrics(run_dir):
            e = num(r.get("elapsed_s"))
            a = 1 if (num(r.get("node_disk_alarm"), 0) == 1
                      or num(r.get("node_mem_alarm"), 0) == 1) else 0
            if a and not prev_alarm:
                alarm_episodes += 1
            prev_alarm = a
            alarm_samples += a
            if e is not None and warmup <= e < warmup + slow:
                alarm_samples_slow += a
            if (num(r.get("conn_blocked"), 0) or 0) > 0:
                blocked_samples += 1

    # backlog at the moment the slowdown ended, and the model's drain estimate
    backlog_at_slow_end = None
    if broker == "rabbit":
        metrics = read_timeline_metrics(run_dir)
        for r in metrics:
            e = num(r.get("elapsed_s"))
            if e is not None and e >= warmup + slow:
                d = num(r.get("main_depth"), -1)
                if d is not None and d >= 0:
                    backlog_at_slow_end = d
                    break
    else:
        lag = kafka_lag_by_second(run_dir, t0)
        for s in sorted(k for k in lag if k >= warmup + slow):
            backlog_at_slow_end = lag[s]
            break

    wl = manifest["workload"]
    mu = wl["nominal_capacity_msg_s"]
    lam = wl["rate_msg_s"]
    model_drain_s = (backlog_at_slow_end / (mu - lam)
                     if backlog_at_slow_end and mu > lam else None)
    measured_drain_s = (drained_at - (warmup + slow)) if drained_at else None

    # A run where the queue never emptied needs saying so explicitly, together
    # with the rate it actually managed, otherwise an empty drain figure reads as
    # missing data rather than as a result.
    depth_at_end, elapsed_at_end = None, None
    for r in reversed(read_timeline_metrics(run_dir)):
        d = num(r.get("main_depth"), -1) if broker == "rabbit" else num(r.get("main_lag"), -1)
        e = num(r.get("elapsed_s"))
        if d is not None and d >= 0 and e is not None:
            depth_at_end, elapsed_at_end = d, e
            break
    drain_rate = None
    if (backlog_at_slow_end is not None and depth_at_end is not None
            and elapsed_at_end is not None and elapsed_at_end > warmup + slow):
        drain_rate = round((backlog_at_slow_end - depth_at_end)
                           / (elapsed_at_end - (warmup + slow)), 1)

    summary = {
        "run_id": manifest["run_id"],
        "config": manifest["config"],
        "config_label": manifest.get("config_label"),
        "broker": broker,
        "scenario": manifest["scenario"],
        "rep": manifest["rep"],
        "factor": manifest["slowdown_factor"],
        "slow_s": slow,
        "phases_s": phases,
        "status": manifest["status"],
        "states": states,
        "conservation": conservation,
        "ratios": {},
        "lost_useful_strict": lost_useful_strict,
        "dead_work_handler_seconds": round(dead_work_ns / NS, 1),
        "backlog_at_slow_end": backlog_at_slow_end,
        "model_drain_s": round(model_drain_s, 1) if model_drain_s else None,
        "measured_drain_s": round(measured_drain_s, 1) if measured_drain_s else None,
        "drain_completed": bool(drained_at),
        "depth_at_end": depth_at_end,
        "drain_rate_measured_msg_s": drain_rate,
        "drain_rate_model_msg_s": (mu - lam) if mu > lam else None,
        "time_to_alarm_s": round(alarm_on - warmup, 1) if alarm_on else None,
        "alarm_episodes": alarm_episodes,
        "alarm_seconds_total": alarm_samples,
        "alarm_fraction_of_slow": round(alarm_samples_slow / slow, 3) if slow else None,
        "publishers_blocked_seconds": blocked_samples,
        "purges": len(purges),
        "purged_depths": [p.get("depth_before") for p in purges],
        "neighbour": {
            "confirm_latency": {k: latency_stats(v) for k, v in nbr_confirm.items()},
            "end_to_end": {k: latency_stats(v) for k, v in nbr_e2e.items()},
            "publish_failures": nbr_blocked,
        },
        "notes": [n for n in [note] if n],
    }
    if produced:
        summary["ratios"] = {
            k: round(v / produced, 6) for k, v in states.items()
        }
        summary["ratios"]["useful_loss"] = round(states["lost_useful"] / produced, 6)
        summary["ratios"]["dead_work"] = round(states["processed_dead"] / produced, 6)
        delivered_total = states["processed_useful"] + states["processed_dead"]
        summary["ratios"]["dead_work_of_processed"] = (
            round(states["processed_dead"] / delivered_total, 6) if delivered_total else None)

    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    if detail:
        return summary, final_state, removed_at
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default=None)
    ap.add_argument("--include-pilot", action="store_true")
    args = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    if args.run:
        dirs = [d for d in (WORK / args.run, RUNS / args.run)
                if (d / "manifest.json").exists()]
        if not dirs:
            sys.exit(f"no manifest.json for {args.run} under {WORK} or {RUNS}")
    else:
        # Prefer WORK: a run still has its raw per-process inputs there, so the
        # whole pipeline runs. Anything only present as a published pair falls
        # through to load_summaries() below, which reads the timeline header.
        dirs = sorted(d for base in (WORK, RUNS) if base.is_dir()
                      for d in base.iterdir()
                      if d.is_dir() and (d / "manifest.json").exists())
        if not dirs:
            print(f"[note] no working directories under {WORK}; metrics will be "
                  f"read from the timeline headers rather than re-derived",
                  file=sys.stderr)
    summaries = []
    for d in dirs:
        if not args.include_pilot and ("pilot" in d.name or "smoke" in d.name):
            continue
        try:
            s = analyse_run(d)
        except Exception as e:
            print(f"[error] {d.name}: {e}", file=sys.stderr)
            continue
        summaries.append(s)
    # Runs that have already been reduced to their two files keep their summary
    # in the timeline header.
    have = {s["run_id"] for s in summaries}
    for s in load_summaries(args.include_pilot):
        if s["run_id"] not in have:
            summaries.append(s)
    summaries.sort(key=lambda s: s["run_id"])
    for s in summaries:
        bal = "ok" if s["conservation"]["balanced"] else f"OFF BY {s['conservation']['difference']}"
        print(f"{s['run_id']:<46} useful_loss={s['ratios'].get('useful_loss', 0):7.4f} "
              f"dead_work={s['ratios'].get('dead_work', 0):7.4f} "
              f"alarm={s['time_to_alarm_s']} purges={s['purges']} conservation={bal}")

    if not summaries:
        print("no runs analysed", file=sys.stderr)
        return 1
    cols = ["run_id", "config", "broker", "scenario", "rep", "factor", "slow_s", "status",
            "produced", "shed", "nacked", "rejected_local_bp", "error",
            "processed_useful", "processed_dead", "consumer_expired",
            "lost_useful", "lost_useful_strict", "lost_expired", "lost_unclassified",
            "remaining", "useful_loss_ratio", "dead_work_ratio", "dead_work_handler_seconds",
            "backlog_at_slow_end", "model_drain_s", "measured_drain_s",
            "drain_completed", "depth_at_end", "drain_rate_measured_msg_s",
            "time_to_alarm_s", "alarm_episodes", "alarm_seconds_total",
            "alarm_fraction_of_slow", "publishers_blocked_seconds",
            "purges", "conservation_balanced",
            "nbr_confirm_p99_baseline_ms", "nbr_confirm_p99_slow_ms",
            "nbr_e2e_p99_baseline_ms", "nbr_e2e_p99_slow_ms",
            "nbr_publish_failures_slow"]
    with (OUT / "runs.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for s in summaries:
            st, r = s["states"], s["ratios"]
            n = s["neighbour"]
            w.writerow({
                "run_id": s["run_id"], "config": s["config"], "broker": s["broker"],
                "scenario": s["scenario"], "rep": s["rep"], "factor": s["factor"],
                "slow_s": s["slow_s"], "status": s["status"],
                "produced": s["conservation"]["produced_rows"],
                **{k: st[k] for k in ("shed", "nacked", "rejected_local_bp", "error",
                                      "processed_useful", "processed_dead",
                                      "consumer_expired", "lost_useful", "lost_expired",
                                      "lost_unclassified", "remaining")},
                "lost_useful_strict": s["lost_useful_strict"],
                "useful_loss_ratio": r.get("useful_loss"),
                "dead_work_ratio": r.get("dead_work"),
                "dead_work_handler_seconds": s["dead_work_handler_seconds"],
                "backlog_at_slow_end": s["backlog_at_slow_end"],
                "model_drain_s": s["model_drain_s"],
                "measured_drain_s": s["measured_drain_s"],
                "drain_completed": s.get("drain_completed"),
                "depth_at_end": s.get("depth_at_end"),
                "drain_rate_measured_msg_s": s.get("drain_rate_measured_msg_s"),
                "time_to_alarm_s": s["time_to_alarm_s"],
                "alarm_episodes": s["alarm_episodes"],
                "alarm_seconds_total": s["alarm_seconds_total"],
                "alarm_fraction_of_slow": s["alarm_fraction_of_slow"],
                "publishers_blocked_seconds": s["publishers_blocked_seconds"],
                "purges": s["purges"],
                "conservation_balanced": s["conservation"]["balanced"],
                "nbr_confirm_p99_baseline_ms": n["confirm_latency"]["baseline"].get("p99_ms"),
                "nbr_confirm_p99_slow_ms": n["confirm_latency"]["slow"].get("p99_ms"),
                "nbr_e2e_p99_baseline_ms": n["end_to_end"]["baseline"].get("p99_ms"),
                "nbr_e2e_p99_slow_ms": n["end_to_end"]["slow"].get("p99_ms"),
                "nbr_publish_failures_slow": n["publish_failures"]["slow"],
            })
    print(f"\nwrote {OUT / 'runs.csv'} ({len(summaries)} runs)")
    bad = [s["run_id"] for s in summaries if not s["conservation"]["balanced"]]
    if bad:
        print(f"WARNING: {len(bad)} run(s) did not balance: {bad}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
