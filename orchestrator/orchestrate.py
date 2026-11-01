#!/usr/bin/env python3
"""Run one experiment and write its raw results to artifact/runs/<run_id>/.

Everything that ends up in the paper is derived from the files this script
writes; nothing is typed in by hand.

While a run is in progress each process writes its own stream into
work/<run_id>/, because they cannot share a file without blocking each other.
When the run ends, analysis/postrun.py joins everything and leaves exactly two
files in runs/:

    runs/<run_id>.timeline.csv      a commented header stating which case this
                                    is, what it tests, the exact testbed, what
                                    the broker did and when, and where every
                                    message ended up; then one row per second
    runs/<run_id>.messages.csv.gz   raw: one row per message, publish side and
                                    consume side joined, with the terminal state

The working directory is then removed. It is kept only if postrun fails, so the
raw data is never lost. A short entry per run is appended to artifact/RUNLOG.md.

Usage:
    python3 orchestrate.py --config R-C1-unbounded --factor 5 --rep 1
"""

import argparse
import base64
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import configs as C  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
# Each process writes its own stream while the run is in progress, so a run has
# many files until it finishes. They live under work/ rather than runs/, so that
# runs/ only ever contains finished results: two files per run.
WORK = ROOT / "work"
HARNESS = ROOT / "harness"

NET = "qlab-net"
RMQ = "qlab-rmq"
KAFKA = "qlab-kafka"
LOAD = "qlab-load"
# Pinned by digest, not by floating tag. rabbitmq:4-management was moved to
# 4.4.0 the day after these runs, and apache/kafka:latest moves continuously,
# so reproducing from the tags alone would no longer reproduce these results.
# The digests are the ones recorded in every run manifest.
RMQ_IMAGE = ("rabbitmq@sha256:"
             "54cb013d76cd1fd6c0a0708f34062ad61c452bb6f016b434c6c33f1a765d2b58")
KAFKA_IMAGE = ("apache/kafka@sha256:"
               "77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837")
LOAD_IMAGE = ("alpine@sha256:"
              "d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc")
# The human-readable equivalents: RabbitMQ 4.3.6 (tag rabbitmq:4.3.6-management),
# Kafka 4.3.1 (tag apache/kafka:4.3.1), Alpine 3.20.10.

RMQ_API = "http://127.0.0.1:15672/api"
# Queue depth is read from the Prometheus endpoint, not the management API. The
# management API serves aggregated samples and only refreshes every 5 s, which
# was measured directly: identical depths for five consecutive 1 Hz samples and
# then a jump of exactly five seconds' worth of messages. The Prometheus
# endpoint reads the metrics tables and, with collect_statistics_interval set to
# 1000 ms, tracks the queue second by second.
RMQ_PROM = "http://127.0.0.1:15692"
RMQ_AUTH = base64.b64encode(b"guest:guest").decode()

# An on-call engineer reacts to an alarm that keeps firing, not only to one that
# stays on. The disk alarm does not latch: as the consumer drains the queue the
# node drops back below the threshold and the alarm clears within ten seconds,
# so a rule requiring 60 s of continuous alarm never triggers. The trigger is
# therefore cumulative alarm time since the last intervention.
PURGE_AFTER_ALARM_SEC = 60
MAX_PURGES = 3
DRAINED_DEPTH = 500
DRAINED_SAMPLES = 10


# --------------------------------------------------------------------------
# shell / docker helpers
# --------------------------------------------------------------------------
def sh(cmd, check=True, timeout=120):
    p = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True,
                       text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"command failed ({p.returncode}): {cmd}\n{p.stderr[-2000:]}")
    return p.stdout.strip()


def quiet(cmd):
    try:
        return sh(cmd, check=False, timeout=120)
    except Exception:
        return ""


def image_digest(image):
    out = quiet(["docker", "image", "inspect", image, "--format", "{{index .RepoDigests 0}}"])
    return out or "<local>"


def ensure_network():
    if NET not in quiet(["docker", "network", "ls", "--format", "{{.Name}}"]).split():
        quiet(["docker", "network", "create", NET])


def teardown():
    for c in (LOAD, RMQ, KAFKA):
        quiet(["docker", "rm", "-f", c])
    for v in ("qlab-rmq-data", "qlab-kafka-data"):
        quiet(["docker", "volume", "rm", "-f", v])


# --------------------------------------------------------------------------
# RabbitMQ management API
# --------------------------------------------------------------------------
def api(path, method="GET", body=None, timeout=5):
    req = urllib.request.Request(RMQ_API + path, method=method)
    req.add_header("Authorization", "Basic " + RMQ_AUTH)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, data=data, timeout=timeout) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


def prom(path, timeout=4):
    """Scrape a Prometheus text endpoint into {metric_name: {labels: value}}."""
    with urllib.request.urlopen(RMQ_PROM + path, timeout=timeout) as r:
        body = r.read().decode("utf-8", "replace")
    out = {}
    for line in body.splitlines():
        if not line or line[0] == "#":
            continue
        name, _, value = line.rpartition(" ")
        name = name.strip()
        if not name:
            continue
        labels = ""
        if name.endswith("}"):
            name, _, labels = name.partition("{")
            labels = labels[:-1]
        try:
            v = float(value)
        except ValueError:
            continue
        out.setdefault(name, {})[labels] = v
    return out


def prom_queue(detailed, metric, queue):
    """One queue's value out of a detailed Prometheus family."""
    fam = detailed.get(metric, {})
    for labels, v in fam.items():
        if f'queue="{queue}"' in labels:
            return int(v)
    return -1


# --------------------------------------------------------------------------
# broker startup
# --------------------------------------------------------------------------
def vm_free_bytes():
    """Free bytes on the Docker VM filesystem that backs the broker volumes."""
    out = sh(["docker", "run", "--rm", "-v", "/var/lib/docker:/host:ro", LOAD_IMAGE,
              "df", "-B1", "/host"], check=False)
    try:
        return int(out.splitlines()[-1].split()[3])
    except Exception:
        out = sh(["docker", "run", "--rm", LOAD_IMAGE, "df", "-B1", "/"], check=False)
        return int(out.splitlines()[-1].split()[3])


def ensure_disk_space(min_free=8 * 1024 ** 3):
    """A Kafka run writes a few GB; refuse to start a run that cannot finish."""
    free = vm_free_bytes()
    if free < min_free:
        quiet(["docker", "system", "prune", "-f", "--volumes"])
        free = vm_free_bytes()
    if free < min_free:
        raise RuntimeError(
            f"only {free / 1024 ** 3:.1f} GiB free in the Docker VM; "
            f"need {min_free / 1024 ** 3:.1f} GiB. Reclaim space before running.")
    return free


def sanity_check(run_dir, cfg, args):
    """Verify a finished run actually contains data.

    A silently empty run is the worst outcome for an overnight matrix: the
    orchestrator reports success and the problem only surfaces at analysis time.
    """
    import csv as _csv

    def read_rows(name):
        p = run_dir / name
        gz = run_dir / (name + ".gz")
        if gz.exists():
            with gzip.open(gz, "rt") as fh:
                return list(_csv.DictReader(fh))
        if p.exists():
            with p.open() as fh:
                return list(_csv.DictReader(fh))
        return []

    prod = read_rows("producer.csv")
    cons = read_rows("consumer.csv")
    nbr_p = read_rows("nbr_producer.csv")
    out = {"producer_rows": len(prod), "consumer_rows": len(cons),
           "nbr_producer_rows": len(nbr_p), "problems": []}

    expected = args.publish * C.RATE
    out["expected_producer_rows"] = int(expected)
    if len(prod) < 0.9 * expected:
        out["problems"].append(
            f"producer wrote {len(prod)} rows, expected about {int(expected)}")

    outcomes = {}
    for r in prod:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
    out["producer_outcomes"] = outcomes
    shed = outcomes.get("skipped_backpressure", 0)
    if len(prod) and shed > 0.5 * len(prod):
        out["problems"].append(
            f"{shed}/{len(prod)} publishes were shed; check t0 and clock offset")
    if not cons:
        out["problems"].append("consumer wrote no rows at all")
    if not nbr_p:
        out["problems"].append("neighbour producer wrote no rows at all")
    for role in ("producer", "consumer", "nbr_producer", "nbr_consumer"):
        log = run_dir / f"{role}.log"
        if log.exists() and log.stat().st_size > 0:
            txt = log.read_text()[:2000]
            out["problems"].append(f"{role} wrote to stderr: {txt.strip()[:400]}")
    out["ok"] = not out["problems"]
    return out


def volume_free_bytes(volume):
    """Free bytes on the filesystem backing a named volume, as the broker sees it."""
    out = sh(["docker", "run", "--rm", "-v", f"{volume}:/d", LOAD_IMAGE,
              "df", "-B1", "/d"])
    return int(out.splitlines()[-1].split()[3])


def start_rabbit(run_dir, cfg, ev):
    quiet(["docker", "volume", "create", "qlab-rmq-data"])
    free = volume_free_bytes("qlab-rmq-data")
    # Per-arm override exists so the transferability claim can be measured at a
    # second headroom value instead of asserted from one.
    headroom = cfg.get("disk_headroom_bytes", C.RMQ_DISK_HEADROOM_BYTES)
    disk_limit = max(free - headroom, 64 * 1024 * 1024)
    conf = run_dir / "rabbitmq-20-qlab.conf"
    conf.write_text(
        "# generated by orchestrate.py; loaded after the image defaults\n"
        f"vm_memory_high_watermark.absolute = {C.RMQ_MEM_WATERMARK}\n"
        f"disk_free_limit.absolute = {disk_limit}\n"
        "log.console.level = info\n"
        # Keep the management plugin responsive while the node is in alarm.
        "management.rates_mode = basic\n"
        "collect_statistics_interval = 1000\n"
    )
    sh(["docker", "run", "-d", "--name", RMQ, "--network", NET,
        "--cpus", C.RMQ_CPUS, "--memory", C.RMQ_MEM, "--memory-swap", C.RMQ_MEM,
        # Bound to the loopback deliberately. The orchestrator is the only
        # client (RMQ_API above is 127.0.0.1) and the load generator reaches the
        # broker over the Docker network, not through a published port. Docker
        # publishes on 0.0.0.0 unless told otherwise, which on a cloud instance
        # would expose the management UI with its default guest:guest login.
        "-p", "127.0.0.1:15672:15672", "-p", "127.0.0.1:15692:15692",
        "-v", "qlab-rmq-data:/var/lib/rabbitmq",
        "-v", f"{conf}:/etc/rabbitmq/conf.d/20-qlab.conf:ro",
        RMQ_IMAGE])

    for _ in range(120):
        try:
            api("/overview", timeout=3)
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("RabbitMQ management API did not come up")

    for _ in range(60):
        try:
            prom("/metrics", timeout=3)
            break
        except Exception:
            time.sleep(1)
    ov = api("/overview")
    ev("broker.ready", broker="rabbit", version=ov.get("rabbitmq_version"),
       erlang=ov.get("erlang_version"), disk_free_limit=disk_limit,
       volume_free_at_start=free)

    args = dict(cfg.get("queue_args", {}))
    api("/queues/%2F/" + C.MAIN, "PUT", {"durable": True, "arguments": args})
    api("/queues/%2F/" + C.NBR, "PUT", {"durable": True, "arguments": {}})
    ev("queues.declared", main_arguments=args, neighbour_arguments={})
    return {"rabbitmq_version": ov.get("rabbitmq_version"),
            "erlang_version": ov.get("erlang_version"),
            "disk_free_limit_bytes": disk_limit,
            "volume_free_at_start_bytes": free,
            "disk_headroom_bytes": headroom,
            "vm_memory_high_watermark": C.RMQ_MEM_WATERMARK}


def start_kafka(run_dir, cfg, ev):
    quiet(["docker", "volume", "create", "qlab-kafka-data"])
    env = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{KAFKA}:9092",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
        "KAFKA_NUM_PARTITIONS": str(C.PARTITIONS),
        "KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS": "10000",
        "KAFKA_HEAP_OPTS": C.KAFKA_HEAP,
    }
    cmd = ["docker", "run", "-d", "--name", KAFKA, "--network", NET,
           "--cpus", C.KAFKA_CPUS, "--memory", C.KAFKA_MEM, "--memory-swap", C.KAFKA_MEM,
           "-v", "qlab-kafka-data:/var/lib/kafka/data"]
    for k, v in env.items():
        cmd += ["-e", f"{k}={v}"]
    cmd.append(KAFKA_IMAGE)
    sh(cmd)

    ready = False
    for _ in range(180):
        out = quiet(["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-broker-api-versions.sh",
                     "--bootstrap-server", "localhost:9092"])
        if "id" in out or "ApiVersion" in out:
            ready = True
            break
        time.sleep(1)
    if not ready:
        raise RuntimeError("Kafka did not come up")

    version = quiet(["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-topics.sh", "--version"]).split()[0]
    ev("broker.ready", broker="kafka", version=version)

    def create(topic, parts, conf):
        cmd = ["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-topics.sh",
               "--bootstrap-server", "localhost:9092", "--create", "--topic", topic,
               "--partitions", str(parts), "--replication-factor", "1"]
        for k, v in conf.items():
            cmd += ["--config", f"{k}={v}"]
        sh(cmd, timeout=120)

    main_conf = dict(cfg.get("topic_config", {}))
    create(C.MAIN, C.PARTITIONS, main_conf)
    create(C.NBR, C.NBR_PARTITIONS, {"retention.ms": "604800000"})
    ev("topics.created", main_config=main_conf, main_partitions=C.PARTITIONS,
       neighbour_partitions=C.NBR_PARTITIONS)
    return {"kafka_version": version, "heap_opts": C.KAFKA_HEAP,
            "main_topic_config": main_conf}


def start_load(run_dir):
    binary = HARNESS / "qlab-linux-amd64"
    if not binary.exists():
        raise RuntimeError(f"missing {binary}; run build.sh first")
    sh(["docker", "run", "-d", "--name", LOAD, "--network", NET,
        "--cpus", C.LOAD_CPUS,
        "-v", f"{run_dir}:/out",
        "-v", f"{binary}:/qlab:ro",
        LOAD_IMAGE, "sleep", "infinity"])


def spawn(role, argv):
    """Launch one harness role inside the load container, detached."""
    line = " ".join(argv) + f" >/out/{role}.log 2>&1"
    sh(["docker", "exec", "-d", LOAD, "sh", "-c", line])


# --------------------------------------------------------------------------
# sampling
# --------------------------------------------------------------------------
CGROUP_SCRIPT = (
    "cat /sys/fs/cgroup/memory.current; echo @@; "
    "cat /sys/fs/cgroup/cpu.stat; echo @@; "
    "cat /sys/fs/cgroup/io.stat; echo @@; "
    "head -1 /proc/stat; echo @@; "
    "grep -E '^(pgpgin|pgmajfault) ' /proc/vmstat"
)


def cgroup_sample(container):
    """cgroup v2 memory/cpu/io plus VM-wide iowait and page-in counters.

    Only cat/echo/head/grep are used, because the broker images do not all ship
    awk; the parsing happens here instead.
    """
    s = dict.fromkeys(HOST_COLS, -1)
    out = quiet(["docker", "exec", container, "sh", "-c", CGROUP_SCRIPT])
    parts = out.split("@@")
    if len(parts) < 5:
        return s
    try:
        s["mem_current"] = int(parts[0].strip())
    except ValueError:
        pass
    for line in parts[1].splitlines():
        if line.startswith("usage_usec"):
            s["cpu_usage_usec"] = int(line.split()[1])
    r = w = 0
    for line in parts[2].splitlines():
        for kv in line.split():
            if "=" not in kv:
                continue
            k, _, v = kv.partition("=")
            if k == "rbytes":
                r += int(v)
            elif k == "wbytes":
                w += int(v)
    s["io_rbytes"], s["io_wbytes"] = r, w
    # /proc/stat cpu line: user nice system idle iowait irq softirq ...
    f = parts[3].split()
    if len(f) >= 6 and f[0] == "cpu":
        s["vm_iowait_jiffies"] = int(f[5])
    for line in parts[4].splitlines():
        k, _, v = line.partition(" ")
        if k == "pgpgin":
            s["vm_pgpgin"] = int(v.strip())
        elif k == "pgmajfault":
            s["vm_pgmajfault"] = int(v.strip())
    return s


def kafka_lag_from_probe(run_dir, topic):
    """Total lag for a topic, read from the tail of the probe's own output.

    The probe already samples offsets at 1 Hz; re-reading its last sample is
    cheaper and more consistent than issuing a second set of admin calls.
    """
    p = run_dir / "kafka_probe.csv"
    if not p.exists():
        return -1
    try:
        with p.open() as fh:
            rows = fh.readlines()[-32:]
    except OSError:
        return -1
    best_t, total = None, 0
    for line in reversed(rows):
        f = line.strip().split(",")
        if len(f) != 7 or f[0] == "t_ns":
            continue
        if best_t is None:
            best_t = f[0]
        if f[0] != best_t:
            break
        if f[1] == topic:
            try:
                lag = int(f[6])
            except ValueError:
                continue
            if lag >= 0:
                total += lag
    return total if best_t is not None else -1


def rabbit_sample():
    """One 1 Hz sample of the broker, mostly from the Prometheus endpoint."""
    s = dict.fromkeys(RABBIT_COLS, -1)
    try:
        d = prom("/metrics/detailed?family=queue_coarse_metrics")
        s["main_depth"] = prom_queue(d, "rabbitmq_detailed_queue_messages", C.MAIN)
        s["main_ready"] = prom_queue(d, "rabbitmq_detailed_queue_messages_ready", C.MAIN)
        s["main_unacked"] = prom_queue(d, "rabbitmq_detailed_queue_messages_unacked", C.MAIN)
        s["nbr_depth"] = prom_queue(d, "rabbitmq_detailed_queue_messages", C.NBR)
    except Exception:
        pass
    try:
        m = prom("/metrics")

        def one(name, default=-1):
            v = m.get(name, {}).get("", None)
            return int(v) if v is not None else default

        s["node_mem_used"] = one("rabbitmq_process_resident_memory_bytes")
        s["node_mem_limit"] = one("rabbitmq_resident_memory_limit_bytes")
        s["node_mem_alarm"] = one("rabbitmq_alarms_memory_used_watermark", 0)
        s["node_disk_free"] = one("rabbitmq_disk_space_available_bytes")
        s["node_disk_free_limit"] = one("rabbitmq_disk_space_available_limit_bytes")
        s["node_disk_alarm"] = one("rabbitmq_alarms_free_disk_space_watermark", 0)
        s["conn_total"] = one("rabbitmq_connections", -1)
    except Exception:
        pass
    # Per-connection state has no Prometheus equivalent, so it still comes from
    # the management API and inherits its 5 s refresh. The exact moment a
    # publisher was blocked is recorded by the publisher itself, which receives
    # connection.blocked from the broker and logs it with the reason given.
    try:
        states = [c.get("state", "?") for c in (api("/connections", timeout=4) or [])]
        s["conn_total"] = len(states) if states else s["conn_total"]
        s["conn_flow"] = states.count("flow")
        s["conn_blocked"] = states.count("blocked") + states.count("blocking")
    except Exception:
        pass
    return s


KAFKA_COLS = ["main_lag", "nbr_lag"]
RABBIT_COLS = ["main_depth", "main_ready", "main_unacked", "nbr_depth",
               "node_mem_used", "node_mem_limit", "node_mem_alarm",
               "node_disk_free", "node_disk_free_limit", "node_disk_alarm",
               "conn_total", "conn_flow", "conn_blocked"]
HOST_COLS = ["mem_current", "cpu_usage_usec", "io_rbytes", "io_wbytes",
             "vm_iowait_jiffies", "vm_pgpgin", "vm_pgmajfault"]


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, choices=sorted(C.CONFIGS))
    ap.add_argument("--factor", type=float, default=5.0)
    ap.add_argument("--rep", type=int, default=1)
    ap.add_argument("--warmup", type=float, default=C.WARMUP_SEC)
    ap.add_argument("--slow", type=float, default=C.SLOW_SEC)
    ap.add_argument("--publish", type=float, default=C.PUBLISH_SEC)
    ap.add_argument("--consume", type=float, default=C.CONSUME_SEC)
    ap.add_argument("--downstream-limit", type=float, default=0.0)
    ap.add_argument("--scenario", default="A", help="A = sustained slowdown, B = short spike")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    cfg = C.resolve(args.config, args.factor)
    run_id = (f"{args.config}_f{args.factor:g}_s{int(args.slow)}"
              f"_{args.scenario}{('_' + args.tag) if args.tag else ''}_r{args.rep}")
    run_dir = WORK / run_id
    if (RUNS / f"{run_id}.timeline.csv").exists():
        print(f"[skip] {run_id} already complete")
        return 0
    if run_dir.exists():
        shutil.rmtree(run_dir)
    RUNS.mkdir(exist_ok=True)
    run_dir.mkdir(parents=True)
    run_dir.chmod(0o777)

    events = (run_dir / "events.jsonl").open("w")

    def ev(kind, **kw):
        rec = {"t_ns": time.time_ns(), "event": kind}
        rec.update(kw)
        events.write(json.dumps(rec) + "\n")
        events.flush()

    print(f"[run] {run_id}")
    teardown()
    ensure_network()
    broker_meta = {}
    off_start = off_end = 0
    # Hoisted out of the try block: a run that dies before the workload starts
    # still has to write a manifest saying why, and that manifest reads these.
    workers = int(cfg.get("workers") or C.WORKERS)
    downstream = args.downstream_limit or float(cfg.get("downstream_limit") or 0.0)
    try:
        ev("run.begin", run_id=run_id, config=args.config, factor=args.factor,
           scenario=args.scenario, rep=args.rep,
           vm_free_bytes=ensure_disk_space())
        if cfg["broker"] == "rabbit":
            broker_meta = start_rabbit(run_dir, cfg, ev)
            broker_ct = RMQ
            addr = f"amqp://guest:guest@{RMQ}:5672/"
        else:
            broker_meta = start_kafka(run_dir, cfg, ev)
            broker_ct = KAFKA
            addr = f"{KAFKA}:9092"
        start_load(run_dir)

        # Measure the offset between the orchestrator clock (macOS host) and the
        # clock inside the Docker VM, where every message timestamp is taken.
        def clock_offset():
            # Read the VM clock through the harness binary: BusyBox date has no
            # %N, and seconds-only resolution would put t0 off by up to a second.
            h = time.time_ns()
            g = int(sh(["docker", "exec", LOAD, "/qlab", "now"]))
            h2 = time.time_ns()
            return g - (h + h2) // 2

        off_start = clock_offset()

        slow_start = args.warmup
        slow_end = args.warmup + args.slow
        plan = f"{slow_start:g}:{slow_end:g}:{args.factor:g}"
        t0_ns = time.time_ns() + off_start + 3_000_000_000
        ev("run.t0", t0_ns=t0_ns, plan=plan, clock_offset_ns=off_start)

        base = ["/qlab"]
        common = ["--t0", str(t0_ns), "--addr", addr, "--broker", cfg["broker"]]
        spawn("producer", base + ["produce"] + common + [
            "--queue", C.MAIN, "--rate", str(C.RATE), "--size", str(C.SIZE),
            "--deadline-ms", str(C.DEADLINE_MS), "--duration", str(args.publish),
            "--partitions", str(C.PARTITIONS), "--group", C.MAIN_GROUP,
            "--lag-backpressure", str(int(cfg.get("lag_bp") or 0)),
            "--out", "/out/producer.csv", "--events", "/out/producer_events.csv"])
        # workers and downstream come from the arm, with the command line
        # winning, and are resolved above so the failure path can report them.
        spawn("consumer", base + ["consume"] + common + [
            "--queue", C.MAIN, "--group", C.MAIN_GROUP,
            "--workers", str(workers), "--prefetch", str(2 * workers),
            "--service-ms", str(C.SERVICE_MS), "--plan", plan,
            "--duration", str(args.consume),
            "--downstream-limit", str(downstream),
            *(["--deadline-check"] if cfg.get("deadline_check") else []),
            "--out", "/out/consumer.csv", "--events", "/out/consumer_events.csv"])
        spawn("nbr_producer", base + ["produce"] + common + [
            "--queue", C.NBR, "--rate", str(C.NBR_RATE), "--size", str(C.SIZE),
            "--deadline-ms", str(C.DEADLINE_MS), "--duration", str(args.publish),
            "--partitions", str(C.NBR_PARTITIONS), "--group", C.NBR_GROUP,
            "--lag-backpressure", "0",
            "--out", "/out/nbr_producer.csv", "--events", "/out/nbr_producer_events.csv"])
        spawn("nbr_consumer", base + ["consume"] + common + [
            "--queue", C.NBR, "--group", C.NBR_GROUP,
            "--workers", str(C.NBR_WORKERS), "--prefetch", str(2 * C.NBR_WORKERS),
            "--service-ms", str(C.NBR_SERVICE_MS), "--plan", "0:0:1",
            "--duration", str(args.consume),
            "--out", "/out/nbr_consumer.csv", "--events", "/out/nbr_consumer_events.csv"])
        if cfg["broker"] == "kafka":
            spawn("probe", base + ["probe-kafka", "--addr", addr,
                                   "--t0", str(t0_ns),
                                   "--topics", f"{C.MAIN},{C.NBR}",
                                   "--groups", f"{C.MAIN_GROUP},{C.NBR_GROUP}",
                                   "--interval", "1",
                                   "--duration", str(args.consume),
                                   "--out", "/out/kafka_probe.csv"])
        ev("load.spawned")

        metric_cols = RABBIT_COLS if cfg["broker"] == "rabbit" else KAFKA_COLS
        bm = (run_dir / "broker_metrics.csv").open("w")
        bm.write("t_ns,elapsed_s," + ",".join(metric_cols) + "\n")
        hm = (run_dir / "host_metrics.csv").open("w")
        hm.write("t_ns,elapsed_s," + ",".join(HOST_COLS) + "\n")

        t0_host = (t0_ns - off_start) / 1e9
        end_host = t0_host + args.consume + 15
        alarm_since = None
        alarm_accum = 0.0
        drain_streak_start = None
        prev_blocked = 0
        prev_conn_total = -1
        purges = 0
        drained_run = 0
        drained_at = None
        phase = "warmup"
        next_host_sample = t0_host
        last_tick = time.time()
        while time.time() < end_host:
            tick = time.time()
            elapsed = tick - t0_host

            if phase == "warmup" and elapsed >= slow_start:
                phase = "slow"
                ev("phase.slow_start", elapsed_s=elapsed, factor=args.factor)
            elif phase == "slow" and elapsed >= slow_end:
                phase = "drain"
                ev("phase.slow_end", elapsed_s=elapsed)

            if cfg["broker"] == "rabbit":
                s = rabbit_sample()
                bm.write(f"{time.time_ns()},{elapsed:.3f}," +
                         ",".join(str(s.get(k, -1)) for k in RABBIT_COLS) + "\n")
                bm.flush()

                alarm = s["node_disk_alarm"] == 1 or s["node_mem_alarm"] == 1
                if alarm:
                    alarm_accum += max(tick - last_tick, 0.0)
                if alarm and alarm_since is None:
                    alarm_since = tick
                    ev("broker.alarm_on", elapsed_s=elapsed,
                       disk_alarm=s["node_disk_alarm"], mem_alarm=s["node_mem_alarm"],
                       disk_free=s["node_disk_free"], mem_used=s["node_mem_used"],
                       main_depth=s["main_depth"])
                elif not alarm and alarm_since is not None:
                    ev("broker.alarm_off", elapsed_s=elapsed,
                       duration_s=tick - alarm_since, main_depth=s["main_depth"])
                    alarm_since = None
                # Log only transitions: the alarm rides the disk threshold and
                # flaps, so a sample-by-sample log would bury everything else.
                if s["conn_blocked"] != prev_blocked:
                    ev("broker.connections_blocked_change", elapsed_s=elapsed,
                       blocked=s["conn_blocked"], was=prev_blocked,
                       total=s["conn_total"])
                    prev_blocked = s["conn_blocked"]
                if s["conn_total"] != prev_conn_total and prev_conn_total >= 0:
                    ev("broker.connection_count_change", elapsed_s=elapsed,
                       total=s["conn_total"], was=prev_conn_total)
                prev_conn_total = s["conn_total"]

                # Operator response: an on-call engineer clears a sustained
                # resource alarm by purging the queue that caused it.
                if (cfg.get("operator_purge")
                        and alarm_accum >= PURGE_AFTER_ALARM_SEC
                        and purges < MAX_PURGES):
                    depth = s["main_depth"]
                    try:
                        api("/queues/%2F/" + C.MAIN + "/contents", "DELETE", timeout=30)
                        purges += 1
                        ev("operator.purge", elapsed_s=elapsed, depth_before=depth,
                           purge_index=purges,
                           alarm_seconds_before_purge=round(alarm_accum, 1))
                    except Exception as e:
                        ev("operator.purge_failed", elapsed_s=elapsed, error=str(e))
                    alarm_accum = 0.0

                depth_now = s["main_depth"]
            else:
                main_lag = kafka_lag_from_probe(run_dir, C.MAIN)
                nbr_lag = kafka_lag_from_probe(run_dir, C.NBR)
                bm.write(f"{time.time_ns()},{elapsed:.3f},{main_lag},{nbr_lag}\n")
                bm.flush()
                depth_now = main_lag

            if phase == "drain" and drained_at is None and 0 <= depth_now <= DRAINED_DEPTH:
                if drained_run == 0:
                    drain_streak_start = elapsed
                drained_run += 1
                if drained_run >= DRAINED_SAMPLES:
                    drained_at = drain_streak_start
                    ev("phase.drained", elapsed_s=drained_at,
                       drain_duration_s=drained_at - slow_end,
                       confirmed_after_samples=DRAINED_SAMPLES,
                       threshold_messages=DRAINED_DEPTH)
            elif depth_now > DRAINED_DEPTH:
                drained_run = 0
                drain_streak_start = None

            if tick >= next_host_sample:
                h = cgroup_sample(broker_ct)
                hm.write(f"{time.time_ns()},{elapsed:.3f}," +
                         ",".join(str(h.get(k, -1)) for k in HOST_COLS) + "\n")
                hm.flush()
                next_host_sample = tick + 2.0

            last_tick = tick
            time.sleep(max(0.0, 1.0 - (time.time() - tick)))

        bm.close()
        hm.close()
        off_end = clock_offset()
        ev("run.end", clock_offset_ns=off_end, purges=purges, drained_at_s=drained_at)
        status = "ok"
        error = None
    except Exception as e:
        status = "failed"
        error = str(e)
        off_end = 0
        ev("run.failed", error=error)
        print(f"[fail] {run_id}: {e}", file=sys.stderr)
    finally:
        (run_dir / "broker.log").write_text(
            quiet(["docker", "logs", RMQ if cfg["broker"] == "rabbit" else KAFKA]))
        try:
            sanity = sanity_check(run_dir, cfg, args)
        except Exception as e:
            sanity = {"ok": False, "problems": [f"sanity check crashed: {e}"]}
        if not sanity.get("ok"):
            status = "suspect" if status == "ok" else status
            print("  ! " + "; ".join(sanity.get("problems", []))[:500], file=sys.stderr)
        manifest = {
            "run_id": run_id,
            "status": status,
            "error": error,
            "config": args.config,
            "config_label": cfg.get("label"),
            "broker": cfg["broker"],
            "scenario": args.scenario,
            "rep": args.rep,
            "slowdown_factor": args.factor,
            "phases_s": {"warmup": args.warmup, "slow": args.slow,
                         "publish": args.publish, "consume": args.consume},
            "workload": {"rate_msg_s": C.RATE, "size_bytes": C.SIZE,
                         "deadline_ms": C.DEADLINE_MS,
                         "service_ms": C.SERVICE_MS,
                         "workers": workers,
                         "nominal_capacity_msg_s": workers / C.SERVICE_MS * 1000,
                         "neighbour_rate_msg_s": C.NBR_RATE,
                         "downstream_limit_msg_s": downstream},
            "length_bound": cfg.get("length_bound"),
            "queue_args": cfg.get("queue_args"),
            "topic_config": cfg.get("topic_config"),
            "deadline_check": bool(cfg.get("deadline_check")),
            "lag_backpressure": cfg.get("lag_bp"),
            "broker_meta": broker_meta,
            "images": {RMQ_IMAGE: image_digest(RMQ_IMAGE),
                       KAFKA_IMAGE: image_digest(KAFKA_IMAGE),
                       LOAD_IMAGE: image_digest(LOAD_IMAGE)},
            "limits": {"broker_cpus": C.RMQ_CPUS if cfg["broker"] == "rabbit" else C.KAFKA_CPUS,
                       "broker_memory": C.RMQ_MEM if cfg["broker"] == "rabbit" else C.KAFKA_MEM,
                       "load_cpus": C.LOAD_CPUS},
            "sanity": sanity,
            "clock_offset_ns": {"start": off_start, "end": off_end},
            "host": json.loads((ROOT / "env" / "host.json").read_text())
            if (ROOT / "env" / "host.json").exists() else {},
            "harness_build": (HARNESS / "build.txt").read_text().strip()
            if (HARNESS / "build.txt").exists() else "",
        }
        events.close()
        teardown()
        for f in sorted(run_dir.glob("*.csv")):
            if f.stat().st_size > 2 * 1024 * 1024:
                with f.open("rb") as src, gzip.open(str(f) + ".gz", "wb", compresslevel=6) as dst:
                    shutil.copyfileobj(src, dst)
                f.unlink()
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
        print(f"[{status}] {run_id}")

        # Analyse this run and append a plain-language block to RUNLOG.md while
        # the result is fresh, so the log is readable without waiting for the
        # whole matrix. Wrapped tightly: a reporting failure must never turn a
        # good run into a failed one, and the working directory is left in
        # place with the raw data rather than lost.
        try:
            pr = subprocess.run([sys.executable, str(ROOT / "analysis" / "postrun.py"), run_id],
                                cwd=str(ROOT), capture_output=True, text=True, timeout=900)
            print(("  " + (pr.stdout or "").strip())[:400])
            if pr.returncode != 0:
                print(f"  ! postrun exited {pr.returncode}; raw files kept in "
                      f"work/{run_id}/: {pr.stderr.strip()[:400]}", file=sys.stderr)
        except subprocess.TimeoutExpired:
            print(f"  ! postrun timed out after 900 s; raw files kept in work/{run_id}/",
                  file=sys.stderr)
        except Exception as e:
            print(f"  ! postrun failed: {e}; raw files kept in work/{run_id}/",
                  file=sys.stderr)
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
