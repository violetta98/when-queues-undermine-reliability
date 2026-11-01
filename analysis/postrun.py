#!/usr/bin/env python3
"""Join one finished run's working files into two files a person can read.

    runs/<run_id>.timeline.csv       readable header + one row per second
    runs/<run_id>.messages.csv.gz    raw: one row per message

The timeline file opens with a commented block that states, in plain English,
which case the run covers, what that case is meant to test, the exact testbed,
what the broker did and when, and where every published message ended up. Below
the comments is an ordinary CSV with one row per second, so the same file can be
read by a person and plotted by a tool.

Three of the comment lines carry compact JSON (MANIFEST_JSON, SUMMARY_JSON,
EVENTS_JSON), so the whole run can be re-derived from the two files alone.

The working directory in work/<run_id>/ is left in place: these two files are
joins of what the individual processes wrote, and keeping the inputs means the
joins can be verified against them instead of taken on trust.

    python3 analysis/postrun.py <run_id>
"""

import csv
import gzip
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
WORK = ROOT / "work"
RUNLOG = ROOT / "RUNLOG.md"
NS = 1_000_000_000

# What each case is for. Without this a reader has to reverse-engineer the
# intent from the queue arguments.
CASE_PURPOSE = {
    "R-C1-unbounded": (
        "No bound at all: the configuration people reach for when they add a "
        "queue to avoid losing work. Tests whether it protects messages under "
        "sustained slowness or becomes the failing component itself.",
        "H2: a queue bounded only by broker resources degrades the whole node"),
    "R-C2-maxlen-drophead": (
        "Length bound with the default overflow policy, which silently discards "
        "the oldest messages. Tests whether a length bound alone is enough.",
        "H1/H2: bounding by length avoids the alarm but still loses useful work"),
    "R-C3-reject-publish": (
        "Length bound that refuses the publisher instead of discarding, so the "
        "producer learns about it and can decide. Tests the cost of refusing.",
        "H2/H3: refusal keeps the broker and the neighbour healthy"),
    "R-C4-ttl": (
        "Age bound: messages expire once they are too old to be useful, "
        "whatever the queue length. Tests the paper's main recommendation.",
        "H1: an age bound converts useful loss into expired loss"),
    "R-C5-combined": (
        "Age bound, publisher refusal and a consumer-side deadline check "
        "together. The configuration the design rules actually recommend.",
        "H1/H2/H3: the combination, measured against each part"),
    "R-C6-quorum-unbounded": (
        "The same unbounded configuration on a quorum queue, which keeps its "
        "index in memory rather than paging bodies to disk. Tests whether the "
        "failure depends on the queue type.",
        "H2: does the queue type change which resource runs out first"),
    "R-C7-maxlen-from-mu": (
        "A length bound sized from the normal service rate (mu * D = 120000) "
        "instead of the degraded one (mu_d * D = 24000). This is the sizing "
        "mistake the design rules warn about, measured rather than asserted.",
        "design rule: the bound must be computed from the degraded rate"),
    "R-C8-more-consumers": (
        "Three times the consumer concurrency against a downstream that starts "
        "rate-limiting during the incident. Degraded capacity is identical to "
        "R-C1 by construction, so this isolates the effect of adding consumers "
        "- the first remedy everyone proposes.",
        "baseline: scaling consumers does not help when the downstream is the limit"),
    "K-C1-long-retention": (
        "Retention far longer than the run, which is how a default Kafka topic "
        "behaves during an hour-long incident. Tests lag and page-cache "
        "pressure without deletion.",
        "H2: does a lagging consumer harm an unrelated topic on the same broker"),
    "K-C2-short-retention": (
        "Retention of 120 s, longer than the 60 s useful lifetime. Tests silent "
        "deletion: the broker removes records with no error anywhere.",
        "model threshold 2: deletion happens once the backlog outlives retention"),
    "K-C3-lag-backpressure": (
        "Producer-side refusal above a lag threshold. Kafka has no broker-side "
        "equivalent of reject-publish, so this is the class-3 arm for Kafka.",
        "H2/H3: producer-side refusal as a substitute for a broker bound"),
    "K-C4-consumer-deadline": (
        "The consumer discards records already past their deadline instead of "
        "processing them. Kafka has no per-message TTL, so this is the "
        "age-bound arm for Kafka.",
        "H1: an age check at the consumer removes the wasted work"),
    "K-C5-combined": (
        "Retention, producer-side lag refusal and a consumer deadline check "
        "together: the recommended Kafka configuration.",
        "H1/H2/H3: the combination, measured against each part"),
    "K-C6-retention-below-D": (
        "Retention of 30 s, shorter than the 60 s useful lifetime. The model "
        "predicts that this is exactly where silent deletion stops removing "
        "already-dead records and starts removing useful ones.",
        "model threshold 2: silent loss becomes useful loss when R < D"),
    "K-C7-retention-bytes": (
        "A byte bound instead of a time bound: retention.bytes of 4 MiB per "
        "partition, the Kafka counterpart of x-max-length. Compared against "
        "R-C2, which bounds the same volume by message count.",
        "F3: does the size of a bound, or the broker enforcing it, decide how "
        "predictable the moment of removal is"),
    "R-C9-wide-headroom": (
        "Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of "
        "60. Tests whether time to alarm is set by headroom divided by the "
        "rate the volume is consumed, rather than being a property of the arm.",
        "F6: more headroom postpones the alarm without preventing it"),
}

NARRATION = {
    "phase.slow_start": lambda e: (
        f"downstream slowed {e.get('factor')}x: capacity drops to "
        f"{2000 / e['factor']:.0f} msg/s, below the 1000 msg/s arrival rate, "
        f"so the backlog starts growing"),
    "phase.slow_end": lambda e: "downstream recovered, the queue starts draining",
    "phase.drained": lambda e: (
        f"queue drained, {e.get('drain_duration_s', 0):.0f} s after recovery"),
    "broker.alarm_on": lambda e: (
        "RESOURCE ALARM raised ("
        + ("disk" if e.get("disk_alarm") else "memory")
        + f"), {e.get('main_depth')} messages in the queue"),
    "broker.alarm_off": lambda e: f"alarm cleared after {e.get('duration_s', 0):.0f} s",
    "broker.connections_blocked_change": lambda e: (
        f"broker blocked {e['blocked']} of {e['total']} publishing connections"
        if e["blocked"] > e.get("was", 0)
        else f"broker released connections: {e['blocked']} of {e['total']} still blocked"),
    "operator.purge": lambda e: (
        f"OPERATOR PURGE: {e.get('depth_before')} messages deleted to clear the alarm"),
    "operator.purge_failed": lambda e: f"purge failed: {e.get('error')}",
    "connection.blocked": lambda e: f"a publisher was told to stop: {e.get('detail')}",
    "producer.reconnect": lambda e: f"producer reconnected ({e.get('detail')})",
    "backpressure.toggle": lambda e: f"producer-side backpressure changed: {e.get('detail')}",
    "offset.jump": lambda e: f"consumer skipped deleted records: {e.get('detail')}",
}

STATE_LABELS = [
    ("processed_useful", "processed in time"),
    ("processed_dead", "processed after its deadline (wasted work)"),
    ("consumer_expired", "discarded by the consumer as already too old"),
    ("lost_useful", "removed by the broker while still useful (USEFUL LOSS)"),
    ("lost_expired", "removed by the broker after its deadline"),
    ("lost_unclassified", "removed, exact time of removal unknown"),
    ("nacked", "refused by the broker at publish"),
    ("rejected_local_bp", "refused by the producer because of consumer lag"),
    ("shed", "never published: the producer's local buffer was full"),
    ("error", "publish failed or its confirmation never arrived"),
    ("remaining", "still waiting in the queue when the run ended"),
]

ROLES = ["producer", "consumer", "nbr_producer", "nbr_consumer"]


def merge_role_events(run_dir, events):
    """Fold the per-role event files into the run's event list.

    These carry the most direct evidence in the whole dataset: the reason the
    broker gave when it blocked a publisher ("low on disk"), producer
    reconnects, and the consumer's own record of skipping records that retention
    had already deleted. Leaving them in the working directory would make the
    published files weaker than the data behind them.
    """
    extra = []
    for role in ROLES:
        for r in analyze.read_csv(run_dir, f"{role}_events.csv"):
            ts = n(r.get("t_ns"))
            if ts is None:
                continue
            extra.append({"t_ns": ts, "event": r.get("event"), "source": role,
                          "detail": r.get("detail", "")})
    if not extra:
        return events
    for e in events:
        e.setdefault("source", "orchestrator")
    t0 = next((e["t0_ns"] for e in events if e.get("event") == "run.t0"), None)
    if t0 is not None:
        for e in extra:
            e["elapsed_s"] = (e["t_ns"] - t0) / NS
    return sorted(events + extra, key=lambda e: e.get("t_ns", 0))


MESSAGE_COLS = ["queue", "seq", "size_bytes", "t_create_ns", "t_publish_ns",
                "t_confirm_ns", "publish_outcome", "partition", "offset",
                "deadline_ns", "t_receive_ns", "t_done_ns", "consume_outcome",
                "age_at_receive_ms", "service_ms", "lateness_ms",
                "t_removed_ns", "final_state"]

TIMELINE_COLS = [
    "elapsed_s", "phase",
    "published", "confirmed", "refused", "shed", "delivered", "delivered_late",
    "queue_depth", "queue_ready", "queue_unacked", "oldest_message_age_s",
    "broker_mem_bytes", "broker_mem_limit_bytes", "mem_alarm",
    "disk_free_bytes", "disk_free_limit_bytes", "disk_alarm",
    "connections_total", "connections_blocked",
    "container_mem_bytes", "container_io_write_bytes", "container_io_read_bytes",
    "nbr_published", "nbr_publish_failed", "nbr_confirm_p99_ms", "nbr_depth",
    "kafka_lag_total", "kafka_log_start_p0", "kafka_end_p0", "kafka_committed_p0",
    "event",
]


def n(v, d=None):
    return analyze.num(v, d)


def _wrap(text, width):
    words, line, out = text.split(), "", []
    for w in words:
        if line and len(line) + 1 + len(w) > width:
            out.append(line)
            line = w
        else:
            line = (line + " " + w) if line else w
    if line:
        out.append(line)
    return out


def mmss(seconds):
    if seconds is None:
        return "  ?  "
    m, s = divmod(int(seconds), 60)
    return f"{m:>3}:{s:02d}"


def p99(vals):
    if not vals:
        return ""
    return f"{analyze.pct(sorted(vals), 0.99):.2f}"


def describe_bound(m):
    qa = m.get("queue_args") or {}
    tc = m.get("topic_config") or {}
    bits = []
    if m["broker"] == "rabbit":
        if "x-max-length" in qa:
            bits.append(f"length bound {qa['x-max-length']} messages, overflow policy "
                        f"{qa.get('x-overflow', 'drop-head (the default)')}")
        if "x-message-ttl" in qa:
            bits.append(f"age bound: messages expire after "
                        f"{int(qa['x-message-ttl']) / 1000:.0f} s")
        if not bits:
            bits.append("no bound of any kind")
        bits.append(f"{qa.get('x-queue-type', 'classic')} queue, durable, persistent messages")
    else:
        if tc.get("retention.bytes"):
            # The byte bound is the whole point of K-C7, and reporting only
            # retention.ms made that arm read as a default 7-day retention.
            bits.append(f"retention {int(tc['retention.bytes']) / 2 ** 20:.0f} MiB "
                        f"per partition")
        if tc.get("retention.ms"):
            bits.append(f"retention {int(tc['retention.ms']) / 1000:.0f} s")
        if tc.get("segment.ms"):
            bits.append(f"segment roll {int(tc['segment.ms']) / 1000:.0f} s")
        if tc.get("segment.bytes"):
            bits.append(f"segment size {int(tc['segment.bytes']) / 2 ** 20:.0f} MiB")
        if m.get("lag_backpressure"):
            bits.append(f"producer refuses to publish above "
                        f"{m['lag_backpressure']} messages of consumer lag")
    if m.get("deadline_check"):
        bits.append("consumer discards messages already past their deadline")
    return "; ".join(bits)


def broker_alarm_lines(run_dir, limit=8):
    """The broker's own words about the alarm, as independent corroboration."""
    p = run_dir / "broker.log"
    if not p.exists():
        return []
    out = []
    for line in p.read_text(errors="replace").splitlines():
        low = line.lower()
        if any(k in low for k in ("alarm", "free disk space", "memory resource limit",
                                  "blocking", "blocked")):
            out.append(line.strip()[:200])
    if len(out) > limit:
        head = out[:limit - 1]
        return head + [f"... {len(out) - len(head)} more matching lines in the broker log"]
    return out


# ---------------------------------------------------------------------------
def write_messages(run_dir, out_path, final_state, removed_at, deadline_ms):
    """One row per message, main queue and neighbour in one file.

    The deadline is a property of the message, carried inside its body, so it is
    filled in even for messages that were never delivered. Taking it only from
    the consumer's row would leave it blank on exactly the rows where it matters
    most: the ones the broker removed.
    """
    def side(name):
        d = {}
        for r in analyze.read_csv(run_dir, name):
            d[n(r.get("seq"))] = r
        return d

    rows_written = 0
    with out_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(MESSAGE_COLS)
        for label, pname, cname, states, removals in (
                ("main", "producer.csv", "consumer.csv", final_state, removed_at),
                ("nbr", "nbr_producer.csv", "nbr_consumer.csv", {}, {})):
            cons = side(cname)
            for r in analyze.read_csv(run_dir, pname):
                seq = n(r.get("seq"))
                c = cons.get(seq, {})
                create = n(r.get("t_create_ns"))
                recv = n(c.get("t_receive_ns"))
                done = n(c.get("t_done_ns"))
                dl = n(c.get("deadline_ns"))
                if dl is None and create is not None:
                    dl = create + deadline_ms * 1_000_000
                w.writerow([
                    label, seq, r.get("size"), create,
                    r.get("t_publish_ns"), r.get("t_confirm_ns"), r.get("outcome"),
                    r.get("partition") or c.get("partition") or "",
                    r.get("offset") or c.get("offset") or "",
                    dl or "", recv or "", done or "", c.get("outcome", ""),
                    f"{(recv - create) / 1e6:.3f}" if recv and create else "",
                    f"{(done - recv) / 1e6:.3f}" if done and recv else "",
                    f"{(done - dl) / 1e6:.3f}" if done and dl else "",
                    removals.get(seq, ""), states.get(seq, ""),
                ])
                rows_written += 1
    return rows_written


def build_timeline_rows(run_dir, manifest, events, final_state):
    t0 = next((e["t0_ns"] for e in events if e.get("event") == "run.t0"), None)
    ph = manifest["phases_s"]
    warmup, slow, last = ph["warmup"], ph["slow"], int(ph["consume"])

    def sec(t):
        return None if (t is None or t0 is None) else int((t - t0) // NS)

    size = last + 2
    published = [0] * size
    confirmed = [0] * size
    refused = [0] * size
    shed = [0] * size
    delivered = [0] * size
    late = [0] * size
    nbr_pub = [0] * size
    nbr_fail = [0] * size
    nbr_lat = [[] for _ in range(size)]

    create_by_seq = {}
    for r in analyze.read_csv(run_dir, "producer.csv"):
        ct = n(r.get("t_create_ns"))
        s = sec(ct)
        o = r.get("outcome")
        if s is not None and 0 <= s <= last:
            published[s] += 1
            if o == "confirmed":
                confirmed[s] += 1
            elif o in ("nacked", "rejected_local_bp"):
                refused[s] += 1
            elif o == "skipped_backpressure":
                shed[s] += 1
        if o == "confirmed" and ct is not None:
            create_by_seq[n(r.get("seq"))] = ct

    leaves = []
    for r in analyze.read_csv(run_dir, "consumer.csv"):
        t = n(r.get("t_done_ns")) or n(r.get("t_receive_ns"))
        s = sec(t)
        if s is not None and 0 <= s <= last:
            delivered[s] += 1
            dl = n(r.get("deadline_ns"))
            done = n(r.get("t_done_ns"))
            if dl and done and done > dl:
                late[s] += 1
        if t is not None:
            leaves.append((t, n(r.get("seq"))))
    leaves.sort()

    for r in analyze.read_csv(run_dir, "nbr_producer.csv"):
        ct = n(r.get("t_create_ns"))
        s = sec(ct)
        if s is None or not 0 <= s <= last:
            continue
        nbr_pub[s] += 1
        if r.get("outcome") == "confirmed":
            cf = n(r.get("t_confirm_ns"))
            if cf:
                nbr_lat[s].append((cf - ct) / 1e6)
        else:
            nbr_fail[s] += 1

    bm, hm = {}, {}
    for r in analyze.read_csv(run_dir, "broker_metrics.csv"):
        e = n(r.get("elapsed_s"))
        if e is not None and e >= 0:
            bm[int(e)] = r
    for r in analyze.read_csv(run_dir, "host_metrics.csv"):
        e = n(r.get("elapsed_s"))
        if e is not None and e >= 0:
            hm[int(e)] = r

    kp = {}
    for r in analyze.read_csv(run_dir, "kafka_probe.csv"):
        if r.get("topic") != "main":
            continue
        s = sec(n(r.get("t_ns")))
        if s is None or not 0 <= s <= last:
            continue
        d = kp.setdefault(s, {"lag": 0, "p0": {}})
        lag = n(r.get("lag"), 0) or 0
        if lag > 0:
            d["lag"] += lag
        if r.get("partition") == "0":
            d["p0"] = r

    ev_by_sec = {}
    for e in events:
        es = e.get("elapsed_s")
        if es is None and e.get("t_ns") is not None and t0 is not None:
            es = (e["t_ns"] - t0) / NS
        if es is None:
            continue
        txt = e["event"]
        for f in ("depth_before", "main_depth", "blocked", "duration_s", "factor", "detail"):
            if e.get(f) not in (None, ""):
                txt += f" {f}={e[f]}"
        ev_by_sec.setdefault(int(es), []).append(txt)

    gone = {s for s, st in final_state.items()
            if st in ("lost_useful", "lost_expired", "lost_unclassified")}
    pending = sorted(create_by_seq)
    li = pi = 0
    out = []
    for s in range(0, last + 1):
        while li < len(leaves) and (leaves[li][0] - (t0 or 0)) // NS <= s:
            gone.add(leaves[li][1])
            li += 1
        while pi < len(pending) and pending[pi] in gone:
            pi += 1
        oldest = ""
        if pi < len(pending) and t0 is not None:
            age = (t0 + s * NS - create_by_seq[pending[pi]]) / NS
            if age > 0:
                oldest = f"{age:.1f}"
        b, h = bm.get(s, {}), hm.get(s, {})
        k = kp.get(s, {})
        p0 = k.get("p0", {})
        phase = "warmup" if s < warmup else ("slow" if s < warmup + slow else "drain")
        out.append([
            s, phase, published[s], confirmed[s], refused[s], shed[s],
            delivered[s], late[s],
            b.get("main_depth", ""), b.get("main_ready", ""), b.get("main_unacked", ""),
            oldest,
            b.get("node_mem_used", ""), b.get("node_mem_limit", ""), b.get("node_mem_alarm", ""),
            b.get("node_disk_free", ""), b.get("node_disk_free_limit", ""),
            b.get("node_disk_alarm", ""),
            b.get("conn_total", ""), b.get("conn_blocked", ""),
            h.get("mem_current", ""), h.get("io_wbytes", ""), h.get("io_rbytes", ""),
            nbr_pub[s], nbr_fail[s], p99(nbr_lat[s]), b.get("nbr_depth", ""),
            k.get("lag", ""), p0.get("log_start_offset", ""),
            p0.get("end_offset", ""), p0.get("committed_offset", ""),
            "; ".join(ev_by_sec.get(s, [])),
        ])
    return out


def measured_rates(rows):
    """What the load generator actually achieved, as opposed to what was asked."""
    import statistics
    pub, deliv = [], []
    for r in rows:
        d = dict(zip(TIMELINE_COLS, r))
        if d["phase"] == "warmup" and int(d["elapsed_s"]) >= 2:
            pub.append(d["published"])
        if d["phase"] == "slow":
            deliv.append(d["delivered"])
    return (statistics.median(pub) if pub else None,
            statistics.median(deliv) if deliv else None)


def caveats(manifest, summary, rows, target_rate, achieved_pub):
    """What this run does and does not support, derived mechanically.

    Without this block a reader can draw a conclusion the run does not justify:
    an unbounded arm that never filled the broker looks safe, and a bounded arm
    whose bound never engaged looks equivalent to an unbounded one.
    """
    m, s = manifest, summary
    st = s["states"]
    total = max(s["conservation"]["produced_rows"], 1)
    out = []

    class1 = m["config"] in ("R-C1-unbounded", "R-C6-quorum-unbounded")
    if class1 and not s.get("alarm_episodes"):
        out.append("The backlog never reached the broker's resource limit in this run, so it "
                   "does NOT exercise the resource-exhaustion failure. A low loss figure here "
                   "says nothing about whether an unbounded queue is safe.")
    removed = st["lost_useful"] + st["lost_expired"] + st["lost_unclassified"]
    bounded = bool((m.get("queue_args") or {}).get("x-max-length")
                   or (m.get("queue_args") or {}).get("x-message-ttl")
                   or (m.get("topic_config") or {}).get("retention.ms", "604800000") != "604800000")
    if bounded and removed == 0 and st["nacked"] == 0 and st["rejected_local_bp"] == 0:
        out.append("The bound never engaged: nothing was removed and nothing was refused. "
                   "This run is therefore not a test of the bound, only of the workload.")
    if st["remaining"] > 0.01 * total:
        out.append(f"{st['remaining']} messages were still queued when the run ended, so the "
                   f"drain time is a lower bound and the loss figures are incomplete.")
    if st["shed"] > 0.2 * total:
        out.append(f"The producer could not hand over {st['shed']} messages "
                   f"({100 * st['shed'] / total:.0f}% of the total) because its local buffer was "
                   f"full while the broker held it back. Arrivals were therefore not stationary "
                   f"for part of this run; compare arms on outcomes per published message, not "
                   f"on absolute counts.")
    if achieved_pub is not None and achieved_pub < 0.95 * target_rate:
        out.append(f"Achieved publish rate during the warmup was {achieved_pub:.0f} msg/s against "
                   f"a target of {target_rate:.0f} msg/s, so the generator itself was a "
                   f"constraint in this run.")
    if st["lost_useful"] or st["lost_expired"]:
        if m["broker"] == "rabbit":
            out.append("Removal times for RabbitMQ come from replaying the queue over the "
                       "timestamps carried in the messages themselves, not from the depth "
                       "samples: confirm adds, delivery removes, drop-head removes the head when "
                       "an overflowing confirm arrives, TTL removes at expiry, purge removes "
                       "everything in the ready state. Drop-head and TTL therefore carry no "
                       "sampling resolution, but purge does -- its instant comes from the "
                       "orchestrator's own 1 Hz loop, recorded after the HTTP call returns. TTL "
                       "is modelled from the create timestamp while the broker measures it from "
                       "enqueue, so on arms whose publishers were blocked the age at removal is "
                       "a model output. The strict count above excludes removals inside the last "
                       "second before the deadline.")
        else:
            out.append("Removal times for Kafka are quantised to one second and biased late by "
                       "0-1 s plus the request round trip: the producer records the offset the "
                       "broker assigned and a probe samples the log start offset every second, "
                       "and a record counts as removed at the first sample where the log start "
                       "has passed it.")
    nb = s["neighbour"]["confirm_latency"].get("slow", {})
    if nb.get("n") is not None and nb["n"] < 2000:
        out.append(f"The neighbour's publish-confirm percentile for the slow phase rests on only "
                   f"{nb['n']} confirmations, because the broker was refusing its publishes. The "
                   f"count of failed publishes is the more meaningful figure here.")
    if s.get("depth_at_end") is not None and not s.get("drain_completed"):
        out.append("The queue had not emptied when the run ended, so the drain time is absent "
                   "rather than zero and the model comparison does not apply to this run. The "
                   "achieved drain rate above is the figure to use.")
    co = m.get("clock_offset_ns") or {}
    if co.get("start") is not None and co.get("end") is not None:
        drift = abs(co["end"] - co["start"]) / 1e6
        if drift > 500:
            out.append(f"The Docker VM clock drifted {drift:.0f} ms against the host over this "
                       f"run. Message timestamps and broker samples come from the two different "
                       f"clocks, so per-second alignment between them is good to about that "
                       f"much.")
    if st["lost_unclassified"]:
        out.append(f"{st['lost_unclassified']} removed messages could not be placed on either "
                   f"side of their deadline and are excluded from both loss figures.")
    if s.get("measured_drain_s") and s.get("model_drain_s"):
        err = 100 * (s["measured_drain_s"] - s["model_drain_s"]) / s["model_drain_s"]
        if abs(err) > 10:
            out.append(f"Measured draining differs from B/(mu-lambda) by {err:+.0f}%. The model "
                       f"treats the return of capacity as instantaneous, which it is not.")
    if not out:
        out.append("No caveats detected: the configuration engaged, arrivals were stationary, "
                   "the queue drained before the run ended and the accounting balances.")
    return out


def header_lines(run_id, manifest, summary, events, run_dir, msg_rows, rows):
    m, s = manifest, summary
    wl = m["workload"]
    bmeta = m.get("broker_meta", {})
    purpose, hyp = CASE_PURPOSE.get(m["config"], ("", ""))
    now = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    # The run date and the date this file was written are different things: every
    # run was re-derived from its preserved working files more than once, so a
    # single "finished" line stamped with the current clock made all 45 runs look
    # as though they had been executed on the same day.
    t0 = next((e["t0_ns"] for e in events if e.get("event") == "run.t0"), None)
    ran = (datetime.fromtimestamp(t0 / 1e9).astimezone()
           .strftime("%Y-%m-%d %H:%M:%S %Z") if t0 else "unknown")
    L = []

    def add(t=""):
        L.append(("# " + t).rstrip())

    add("=" * 86)
    add(f"CASE {m['config']}   replicate {m['rep']}   scenario {m['scenario']}")
    add("=" * 86)
    add(f"run id        {run_id}")
    add(f"run executed   {ran}      status: {m['status'].upper()}")
    add(f"file written    {now}   (re-derived from the preserved working files)")
    add()
    add("WHAT THIS CASE IS")
    for line in (purpose or m.get("config_label", "")).split(". "):
        if line.strip():
            add(f"  {line.strip().rstrip('.')}.")
    if hyp:
        add(f"  Supports -> {hyp}")
    add()
    add("CONFIGURATION UNDER TEST")
    add(f"  bound       {describe_bound(m)}")
    add(f"  length bound derived as mu_d * D = {m.get('length_bound')} messages")
    add()
    add("TESTBED")
    host = m.get("host", {})
    if host:
        add(f"  machine     {host.get('cpu', '?')}, "
            f"{host.get('cpu_physical', '?')} physical / {host.get('cpu_logical', '?')} "
            f"logical cores, {host.get('ram_bytes', 0) / 1024 ** 3:.0f} GiB RAM")
        add(f"  host os     {host.get('os', '?')}")
        d = host.get("docker", {})
        add(f"  docker      {d.get('server_version', '?')}, {d.get('storage_driver', '?')}, "
            f"cgroup v{d.get('cgroup_version', '?')}; VM {d.get('vm_kernel', '?')}, "
            f"{d.get('vm_cpus', '?')} vCPU, {d.get('vm_memory_bytes', 0) / 1024 ** 3:.1f} GiB")
    if m["broker"] == "rabbit":
        add(f"  broker      RabbitMQ {bmeta.get('rabbitmq_version', '?')} on "
            f"Erlang {bmeta.get('erlang_version', '?')}")
        add(f"  limits      {m['limits']['broker_cpus']} CPU, {m['limits']['broker_memory']} RAM, "
            f"memory watermark {bmeta.get('vm_memory_high_watermark')}")
        add(f"  disk alarm  fires when free space on the data volume drops below "
            f"{bmeta.get('disk_free_limit_bytes', 0) / 1024 ** 3:.2f} GiB; the volume "
            f"had {bmeta.get('volume_free_at_start_bytes', 0) / 1024 ** 3:.2f} GiB free at start")
        add("              (the threshold is scaled so the documented alarm path fires inside")
        add("               a ten-minute window; the mechanism itself is unmodified)")
    else:
        add(f"  broker      Kafka {bmeta.get('kafka_version', '?')}, KRaft, single node")
        add(f"  limits      {m['limits']['broker_cpus']} CPU, {m['limits']['broker_memory']} RAM, "
            f"heap {bmeta.get('heap_opts')}")
        add(f"  topic       {m.get('topic_config')}")
    for img, dig in (m.get("images") or {}).items():
        add(f"  image       {img}  {dig}")
    add(f"  harness     {m.get('harness_build', '').splitlines()[0] if m.get('harness_build') else ''}")
    for line in (m.get("harness_build") or "").splitlines():
        if line.startswith("binary_sha256") or line.startswith("go_version"):
            add(f"              {line}")
    co = m.get("clock_offset_ns", {})
    add(f"  clocks      message timestamps come from inside the Docker VM; the offset to "
        f"the host clock was {(co.get('start') or 0) / 1e6:.0f} ms at the start and "
        f"{(co.get('end') or 0) / 1e6:.0f} ms at the end")
    add()
    add("WORKLOAD")
    add(f"  arrivals    {wl['rate_msg_s']:.0f} msg/s, {wl['size_bytes']} B per message, "
        f"useful lifetime D = {wl['deadline_ms'] / 1000:.0f} s carried in each message")
    add(f"  consumer    {wl.get('workers')} handlers x {wl['service_ms']:.0f} ms service time "
        f"= {wl['nominal_capacity_msg_s']:.0f} msg/s nominal capacity")
    if wl.get("downstream_limit_msg_s"):
        add(f"  downstream  rate-limited to {wl['downstream_limit_msg_s']:.0f} msg/s "
            f"while degraded, so extra handlers cannot raise throughput")
    ach_pub, ach_deliv = measured_rates(rows)
    if ach_pub is not None:
        add(f"  achieved    {ach_pub:.0f} msg/s published during the warmup (target "
            f"{wl['rate_msg_s']:.0f}); {ach_deliv:.0f} msg/s processed during the slow phase "
            f"(expected {wl['nominal_capacity_msg_s'] / m['slowdown_factor']:.0f})")
    add(f"  slowdown    service time x{m['slowdown_factor']:g} for {m['phases_s']['slow']:.0f} s, "
        f"starting {m['phases_s']['warmup']:.0f} s in; degraded capacity "
        f"{wl['nominal_capacity_msg_s'] / m['slowdown_factor']:.0f} msg/s")
    add(f"  neighbour   a second, healthy queue on the same broker at "
        f"{wl['neighbour_rate_msg_s']:.0f} msg/s with its own producer and consumer")
    add(f"  phases      warmup 0-{m['phases_s']['warmup']:.0f}s, "
        f"slow {m['phases_s']['warmup']:.0f}-{m['phases_s']['warmup'] + m['phases_s']['slow']:.0f}s, "
        f"publishing stops at {m['phases_s']['publish']:.0f}s, "
        f"run ends at {m['phases_s']['consume']:.0f}s")
    add()
    add("WHAT HAPPENED")
    shown = 0
    for e in events:
        fn = NARRATION.get(e.get("event"))
        if not fn:
            continue
        try:
            txt = fn(e)
        except Exception:
            continue
        es = e.get("elapsed_s")
        add(f"  {mmss(es)}  {txt}")
        shown += 1
        if shown >= 45:
            add("          ... remaining events are in the EVENTS_JSON line and the event column")
            break
    if not shown:
        add("  nothing notable: no resource alarm, no purge, no publisher blocking")
    add()
    if s.get("alarm_episodes"):
        add(f"  the alarm was raised {s['alarm_episodes']} separate times and was active for "
            f"{s['alarm_seconds_total']} s in total, i.e. "
            f"{100 * (s.get('alarm_fraction_of_slow') or 0):.0f}% of the slow phase")
        add("  it does not latch: as the consumer drains the queue the node drops back below")
        add("  the threshold, the alarm clears, publishing resumes and the cycle repeats")
    else:
        add("  no resource alarm was raised at any point in this run")
    if s.get("publishers_blocked_seconds"):
        add(f"  publishers were blocked by the broker for {s['publishers_blocked_seconds']} s")
    if s.get("backlog_at_slow_end") is not None:
        add(f"  backlog when the downstream recovered: {s['backlog_at_slow_end']} messages")
    if s.get("measured_drain_s") and s.get("model_drain_s"):
        err = 100 * (s["measured_drain_s"] - s["model_drain_s"]) / s["model_drain_s"]
        add(f"  draining took {s['measured_drain_s']:.0f} s; the model B/(mu-lambda) predicted "
            f"{s['model_drain_s']:.0f} s ({err:+.0f}%)")
    elif s.get("depth_at_end") is not None and not s.get("drain_completed"):
        add(f"  the queue never emptied: {s['depth_at_end']} messages were still waiting when "
            f"the run ended, against {s.get('backlog_at_slow_end')} when the downstream "
            f"recovered")
        if s.get("drain_rate_measured_msg_s") is not None:
            add(f"  it drained at {s['drain_rate_measured_msg_s']:.0f} msg/s against the "
                f"{s.get('drain_rate_model_msg_s')} msg/s the model assumes, so recovery was "
                f"limited by something other than consumer capacity")
    said = broker_alarm_lines(run_dir)
    if said:
        add()
        add("WHAT THE BROKER ITSELF LOGGED")
        for line in said:
            add(f"  {line}")
    add()
    total = s["conservation"]["produced_rows"]
    add(f"WHERE THE {total} PUBLISHED MESSAGES ENDED UP")
    for key, label in STATE_LABELS:
        v = s["states"].get(key, 0)
        if v:
            add(f"  {label:<52} {v:>8}  {100 * v / max(total, 1):>5.1f} %")
    c = s["conservation"]
    add(f"  {'-' * 52} {'-' * 8}  {'-' * 7}")
    add(f"  {'every published message accounted for exactly once':<52} "
        f"{'yes' if c['balanced'] else 'NO':>8}")
    if not c["balanced"]:
        add(f"      states sum to {c['states_total']}, producer wrote "
            f"{c['producer_csv_rows']} rows, difference {c['difference']}")
    if s["states"].get("lost_useful"):
        add(f"      counted strictly, excluding removals in the last second before the "
            f"deadline: {s.get('lost_useful_strict')}")
    if s.get("dead_work_handler_seconds"):
        add(f"      handler time spent on messages that were already too late: "
            f"{s['dead_work_handler_seconds']:.0f} s")
    add()
    nb = s["neighbour"]
    add("THE UNRELATED QUEUE ON THE SAME BROKER")
    add(f"  publish-confirm p99   {nb['confirm_latency'].get('baseline', {}).get('p99_ms', '?')} ms "
        f"before  ->  {nb['confirm_latency'].get('slow', {}).get('p99_ms', '?')} ms during")
    add(f"  end-to-end p99        {nb['end_to_end'].get('baseline', {}).get('p99_ms', '?')} ms "
        f"before  ->  {nb['end_to_end'].get('slow', {}).get('p99_ms', '?')} ms during")
    pf = nb["publish_failures"]
    add(f"  publishes it could not complete: {pf.get('slow', 0)} during the slowdown, "
        f"{pf.get('baseline', 0)} before it")
    if pf.get("slow", 0):
        add("  this queue has nothing to do with the slow consumer. It was silenced because")
        add("  the broker blocks every publishing connection on the node, not just the one")
        add("  whose queue grew.")
    add()
    add("WHAT THIS RUN DOES AND DOES NOT SHOW")
    for c in caveats(m, s, rows, wl["rate_msg_s"], measured_rates(rows)[0]):
        first = True
        for chunk in _wrap(c, 82):
            add(("  - " if first else "    ") + chunk)
            first = False
    san = m.get("sanity") or {}
    if san.get("problems"):
        add()
        add("PROBLEMS DETECTED IN THIS RUN")
        for pr in san["problems"]:
            add(f"  ! {pr}")
    if m.get("error"):
        add()
        add(f"ERROR  {m['error']}")
    add()
    add("FILES FOR THIS RUN")
    add(f"  runs/{run_id}.timeline.csv     this file: the header above, then one row per second")
    add(f"  runs/{run_id}.messages.csv.gz  raw, {msg_rows} rows, one per message "
        f"(the 'queue' column separates the queue under test from the neighbour)")
    add(f"  work/{run_id}/                 the files the individual processes wrote,")
    add("                                 kept so these two joins can be checked against them")
    add()
    add("The three lines below carry the full provenance in compact JSON so that this run")
    add("can be re-derived from these two files alone.")
    L.append("# MANIFEST_JSON: " + json.dumps(manifest, separators=(",", ":")))
    L.append("# SUMMARY_JSON: " + json.dumps(summary, separators=(",", ":")))
    L.append("# EVENTS_JSON: " + json.dumps(events, separators=(",", ":")))
    add("=" * 86)
    return L


def runlog_entry(run_id, manifest, summary, n_runs):
    m, s = manifest, summary
    purpose, hyp = CASE_PURPOSE.get(m["config"], ("", ""))
    total = s["conservation"]["produced_rows"]
    st = s["states"]
    nb = s["neighbour"]
    L = ["", "-" * 86,
         f"RUN {n_runs}  {run_id}  [{m['status'].upper()}]",
         "-" * 86,
         f"case      {m['config']} - {(purpose.split('.')[0] + '.') if purpose else ''}",
         f"tests     {hyp}" if hyp else "",
         f"broker    " + (f"RabbitMQ {m.get('broker_meta', {}).get('rabbitmq_version', '?')}"
                          if m["broker"] == "rabbit"
                          else f"Kafka {m.get('broker_meta', {}).get('kafka_version', '?')}"),
         f"bound     {describe_bound(m)}",
         ""]
    L.append(f"outcome   {total} published: "
             f"{st['processed_useful']} in time, {st['processed_dead']} too late, "
             f"{st['lost_useful']} lost while useful, {st['lost_expired']} lost after deadline, "
             f"{st['nacked'] + st['rejected_local_bp']} refused, {st['shed']} never sent")
    L.append(f"alarm     " + (f"{s['alarm_episodes']} episodes, "
                              f"{100 * (s.get('alarm_fraction_of_slow') or 0):.0f}% of the slow phase, "
                              f"{s['purges']} purge(s)" if s.get("alarm_episodes")
                              else "none"))
    L.append(f"neighbour confirm p99 "
             f"{nb['confirm_latency'].get('baseline', {}).get('p99_ms', '?')} -> "
             f"{nb['confirm_latency'].get('slow', {}).get('p99_ms', '?')} ms, "
             f"{nb['publish_failures'].get('slow', 0)} publishes lost")
    if s.get("measured_drain_s") and s.get("model_drain_s"):
        L.append(f"drain     {s['measured_drain_s']:.0f} s measured vs "
                 f"{s['model_drain_s']:.0f} s predicted by B/(mu-lambda)")
    L.append(f"accounting {'balanced' if s['conservation']['balanced'] else 'DOES NOT BALANCE'}")
    L.append(f"files     runs/{run_id}.timeline.csv, runs/{run_id}.messages.csv.gz")
    return "\n".join(x for x in L if x != "") + "\n"


def main():
    if len(sys.argv) < 2:
        print("usage: postrun.py <run_id>", file=sys.stderr)
        return 2
    run_id = sys.argv[1]
    run_dir = WORK / run_id
    if not (run_dir / "manifest.json").exists():
        print(f"postrun: no working directory for {run_id}", file=sys.stderr)
        return 1

    manifest = json.loads((run_dir / "manifest.json").read_text())
    events = merge_role_events(run_dir, analyze.read_events(run_dir))
    summary, final_state, removed_at = analyze.analyse_run(run_dir, detail=True)

    msg_tmp = run_dir / "_messages.csv"
    msg_rows = write_messages(run_dir, msg_tmp, final_state, removed_at,
                              manifest["workload"]["deadline_ms"])
    rows = build_timeline_rows(run_dir, manifest, events, final_state)

    timeline = RUNS / f"{run_id}.timeline.csv"
    with timeline.open("w", newline="") as fh:
        fh.write("\n".join(header_lines(run_id, manifest, summary, events,
                                        run_dir, msg_rows, rows)) + "\n")
        w = csv.writer(fh)
        w.writerow(TIMELINE_COLS)
        w.writerows(rows)

    with msg_tmp.open("rb") as src, gzip.open(RUNS / f"{run_id}.messages.csv.gz",
                                              "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=4 << 20)
    msg_tmp.unlink()

    n_runs = 1 + (RUNLOG.read_text().count("\nRUN ") if RUNLOG.exists() else 0)
    if not RUNLOG.exists():
        RUNLOG.write_text(
            "# Run log\n\n"
            "One entry per experiment, appended as soon as the run finishes. Each entry\n"
            "names the case, what it tests, and the outcome. The full detail for a run is\n"
            "in the commented header of its timeline file.\n\n"
            "Two files per run, both in runs/:\n"
            "  <run_id>.timeline.csv      readable header, then one row per second\n"
            "  <run_id>.messages.csv.gz   raw, one row per message\n"
            "The files the individual processes wrote are kept in work/<run_id>/, so the\n"
            "two joins above can be checked against their inputs.\n\n"
            "Terms: 'useful loss' is a message the broker removed while it could still\n"
            "have been processed in time. 'too late' is a message that was processed after\n"
            "its deadline had already passed, i.e. wasted work. 'neighbour' is a second,\n"
            "healthy queue on the same broker, unrelated to the slow one.\n")
    log_text = RUNLOG.read_text() if RUNLOG.exists() else ""
    already = run_id in log_text
    if not already:
        with RUNLOG.open("a") as fh:
            fh.write(runlog_entry(run_id, manifest, summary, n_runs))

    # The working directory is kept on purpose. The two files in runs/ are joins
    # of what the individual processes wrote, and keeping the inputs means the
    # joins can be checked against them later rather than trusted.
    print(f"postrun: {run_id} -> runs/{run_id}.timeline.csv ({len(rows)} rows) + "
          f"runs/{run_id}.messages.csv.gz ({msg_rows} rows); "
          f"inputs kept in work/{run_id}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
