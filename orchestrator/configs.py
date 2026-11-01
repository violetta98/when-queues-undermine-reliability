"""Experiment configuration table.

Configurations are named by the bounding mechanism, not by the product, because
the same broker lands in a different class depending on how it is configured.

Workload invariants shared by every run:

    arrival rate        lambda = 1000 msg/s
    message size        1024 B
    useful lifetime     D     = 60 s
    nominal capacity    mu    = 2000 msg/s  (100 handlers x 50 ms service time)
    degraded capacity   mu_d  = mu / factor
    neighbour workload  200 msg/s on a second queue/topic of the same broker

The length bound L used by the bounded arms is mu_d * D, i.e. the largest
backlog whose tail message can still be processed before its deadline at the
degraded service rate. For factor 5 that is 400 * 60 = 24000 messages.
"""

# Workload constants
RATE = 1000.0
SIZE = 1024
DEADLINE_MS = 60_000
# The age bound is set below the useful lifetime on purpose. A message removed
# exactly at its deadline is already useless, so TTL = D would be the wrong
# advice; it would also make the "was it still useful when removed" decision
# depend on timer jitter at the boundary.
AGE_BOUND_MS = 54_000
SERVICE_MS = 50.0
WORKERS = 100
NBR_RATE = 200.0
NBR_SERVICE_MS = 5.0
NBR_WORKERS = 20

# Default phase timing, in seconds from t0
WARMUP_SEC = 60
SLOW_SEC = 360
PUBLISH_SEC = 780
CONSUME_SEC = 900

# Container resource limits
RMQ_CPUS = "4"
RMQ_MEM = "1g"
# Left close to the image default (0.6 of the container's 1 GiB). A tighter
# watermark makes the node alarm at idle, because RabbitMQ 4.3 already uses
# 250-370 MiB with no backlog, and that would contaminate the baseline window.
# The experiment is driven through the disk alarm instead, which is the path
# classic queues actually take now that message bodies live on disk.
RMQ_MEM_WATERMARK = "700MiB"
# The alarm threshold is expressed as "leave this many bytes free". We set it
# relative to the free space observed on the data volume at run start, so the
# documented alarm path fires inside a ten-minute window instead of after
# hundreds of gigabytes. The mechanism is unmodified; only the threshold is
# scaled, and the measured value goes into the manifest.
# At 1 KiB and 600 msg/s the volume loses 0.71-0.72 MiB/s, more than the payload
# alone, so 60 MiB puts the alarm 76-81 s into the slow phase with 46k-48k in
# the queue -- early enough to watch the flapping and the purge.
RMQ_DISK_HEADROOM_BYTES = 60 * 1024 * 1024

KAFKA_CPUS = "4"
KAFKA_MEM = "2g"
KAFKA_HEAP = "-Xmx512m -Xms512m"

LOAD_CPUS = "4"

MAIN = "main"
NBR = "nbr"
MAIN_GROUP = "main-cg"
NBR_GROUP = "nbr-cg"
PARTITIONS = 6
NBR_PARTITIONS = 3


def length_bound(factor: float) -> int:
    """L = mu_d * D, the bound derived in the model section."""
    mu_d = (WORKERS / SERVICE_MS) * 1000.0 / factor
    return int(round(mu_d * DEADLINE_MS / 1000.0))


# Each entry describes one arm of the comparison.
#   queue_args      RabbitMQ x-arguments for the main queue
#   topic_config    Kafka per-topic config for the main topic
#   deadline_check  consumer discards messages already past their deadline
#   lag_bp          Kafka producer-side lag backpressure threshold (0 = off)
#   operator_purge  orchestrator purges the main queue after a sustained alarm,
#                   which is what an on-call engineer does to clear the alarm
CONFIGS = {
    # ---- RabbitMQ -------------------------------------------------------
    "R-C1-unbounded": dict(
        broker="rabbit",
        label="unbounded",
        queue_args={},
        deadline_check=False,
        operator_purge=True,
    ),
    "R-C2-maxlen-drophead": dict(
        broker="rabbit",
        label="length bound, drop-head",
        queue_args={"x-max-length": "L"},
        deadline_check=False,
        operator_purge=True,
    ),
    "R-C3-reject-publish": dict(
        broker="rabbit",
        label="length bound, reject-publish",
        queue_args={"x-max-length": "L", "x-overflow": "reject-publish"},
        deadline_check=False,
        operator_purge=True,
    ),
    "R-C4-ttl": dict(
        broker="rabbit",
        label="age bound (TTL = 0.9 D)",
        queue_args={"x-message-ttl": AGE_BOUND_MS},
        deadline_check=False,
        operator_purge=True,
    ),
    "R-C5-combined": dict(
        broker="rabbit",
        label="age bound + reject-publish + consumer deadline check",
        queue_args={
            "x-message-ttl": AGE_BOUND_MS,
            "x-max-length": "L",
            "x-overflow": "reject-publish",
        },
        deadline_check=True,
        operator_purge=True,
    ),
    # ---- Kafka ----------------------------------------------------------
    # Kafka cannot reach the resource-exhaustion class on this testbed: filling
    # the data volume would take hundreds of gigabytes. Its class-1 arm is
    # therefore "retention longer than the run", which is how a default Kafka
    # topic behaves over a one-hour incident.
    "K-C1-long-retention": dict(
        broker="kafka",
        label="retention 7 d (effectively unbounded)",
        topic_config={"retention.ms": "604800000", "segment.bytes": "67108864"},
        deadline_check=False,
        lag_bp=0,
    ),
    "K-C2-short-retention": dict(
        broker="kafka",
        label="retention 120 s (silent deletion)",
        topic_config={
            "retention.ms": "120000",
            "segment.ms": "30000",
            "segment.bytes": "33554432",
        },
        deadline_check=False,
        lag_bp=0,
    ),
    "K-C3-lag-backpressure": dict(
        broker="kafka",
        label="producer-side lag backpressure",
        topic_config={"retention.ms": "604800000", "segment.bytes": "67108864"},
        deadline_check=False,
        lag_bp="L",
    ),
    "K-C4-consumer-deadline": dict(
        broker="kafka",
        label="consumer deadline check",
        topic_config={"retention.ms": "604800000", "segment.bytes": "67108864"},
        deadline_check=True,
        lag_bp=0,
    ),
    "K-C5-combined": dict(
        broker="kafka",
        label="retention 120 s + lag backpressure + deadline check",
        topic_config={
            "retention.ms": "120000",
            "segment.ms": "30000",
            "segment.bytes": "33554432",
        },
        deadline_check=True,
        lag_bp="L",
    ),
    # ---- arms added after the coverage review ------------------------------
    # The transferability claim is "time to alarm = headroom / backlog growth
    # rate", and it was measured at one headroom value only. This arm is
    # R-C1-unbounded with the headroom doubled: if the claim holds, the alarm
    # arrives at roughly twice the delay and the depth at alarm roughly doubles.
    # The bounds the model actually implies, as opposed to the textbook mu_d * D
    # that the other arms use. Service time during the slowdown is 250 ms, so
    # D - s = 59.75 s: drop-head needs lambda * 59.75 = 59750 and reject-publish
    # mu_d * 59.75 = 23900, minus the 200 messages in flight that x-max-length
    # does not count. Until these run, the rule derived in the results is
    # arithmetic rather than a measurement.
    "R-C10-maxlen-correct": dict(
        broker="rabbit",
        label="length bound from lambda, drop-head (L = lambda * (D - s))",
        queue_args={"x-max-length": 59750},
        deadline_check=False,
        operator_purge=True,
    ),
    "R-C11-reject-correct": dict(
        broker="rabbit",
        label="length bound from mu_d minus in-flight, reject-publish",
        queue_args={"x-max-length": 23700, "x-overflow": "reject-publish"},
        deadline_check=False,
        operator_purge=True,
    ),
    # 59750 messages at the measured 1.19 KiB is about 70 MiB, more than the
    # 60 MiB of headroom the other arms get, so R-C10 is expected to raise the
    # alarm. This arm gives it room, which separates "the correct bound does not
    # fit" from "the correct bound does not work".
    "R-C12-maxlen-correct-wide": dict(
        broker="rabbit",
        label="L = lambda * (D - s), drop-head, 120 MiB headroom",
        queue_args={"x-max-length": 59750},
        deadline_check=False,
        operator_purge=True,
        disk_headroom_bytes=120 * 1024 * 1024,
    ),
    "R-C9-wide-headroom": dict(
        broker="rabbit",
        label="unbounded, 120 MiB disk headroom",
        queue_args={},
        deadline_check=False,
        operator_purge=True,
        disk_headroom_bytes=120 * 1024 * 1024,
    ),
    # Kafka's byte bound, the direct counterpart of x-max-length with drop-head
    # semantics: oldest segments are deleted to keep the partition under the
    # cap. retention.bytes is per partition, so the total is divided by the
    # partition count to match R-C2's L = 24000 messages of 1 KiB.
    # segment.bytes is set to the smallest value Kafka accepts (1 MiB; smaller
    # is rejected outright), so the cap is four segments per partition and the
    # effective bound oscillates between 3 and 4 MiB. That floor is itself a
    # result: a byte bound in Kafka cannot be made as tight as a message bound
    # in RabbitMQ. retention.ms is left long so only the byte cap binds.
    "K-C7-retention-bytes": dict(
        broker="kafka",
        label="byte bound (retention.bytes), drop-head by volume",
        topic_config={
            "retention.ms": "604800000",
            "retention.bytes": str(4 * 1024 * 1024),
            "segment.bytes": str(1024 * 1024),
        },
        deadline_check=False,
        lag_bp=0,
    ),
    # ---- arms added to close gaps a reviewer would find --------------------
    # Quorum queues keep their index in memory rather than paging bodies to
    # disk, so the same unbounded configuration is expected to fail through a
    # different resource. The draft promised both queue types.
    "R-C6-quorum-unbounded": dict(
        broker="rabbit",
        label="unbounded, quorum queue",
        queue_args={"x-queue-type": "quorum"},
        deadline_check=False,
        operator_purge=True,
    ),
    # The length bound sized from nominal capacity instead of degraded capacity:
    # L = mu * D = 120000 rather than mu_d * D = 24000. This is the sizing
    # mistake the design rules warn about, measured instead of asserted.
    "R-C7-maxlen-from-mu": dict(
        broker="rabbit",
        label="length bound sized from mu, not mu_d",
        queue_args={"x-max-length": 120000},
        deadline_check=False,
        operator_purge=True,
    ),
    # Three times the consumer concurrency against a downstream that rate-limits
    # during the incident. Degraded capacity is identical to R-C1 by
    # construction: min(300/0.25, 400) = min(100/0.25, 400) = 400 msg/s. The
    # only difference is how many handlers are waiting on it, which is the first
    # remedy a reviewer will propose.
    "R-C8-more-consumers": dict(
        broker="rabbit",
        label="3x consumers, rate-limited downstream",
        queue_args={},
        deadline_check=False,
        operator_purge=True,
        workers=300,
        downstream_limit=400.0,
    ),
    "K-C6-retention-below-D": dict(
        broker="kafka",
        label="retention 30 s, shorter than D",
        topic_config={
            "retention.ms": "30000",
            "segment.ms": "15000",
            "segment.bytes": "16777216",
        },
        deadline_check=False,
        lag_bp=0,
    ),
}


def resolve(name: str, factor: float) -> dict:
    """Substitute the derived length bound L into a configuration."""
    cfg = {k: (dict(v) if isinstance(v, dict) else v) for k, v in CONFIGS[name].items()}
    L = length_bound(factor)
    for k, v in cfg.get("queue_args", {}).items():
        if v == "L":
            cfg["queue_args"][k] = L
    if cfg.get("lag_bp") == "L":
        cfg["lag_bp"] = L
    cfg["length_bound"] = L
    cfg["name"] = name
    return cfg
