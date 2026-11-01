# Data dictionary

Three kinds of file: a per-run summary (`data/*-runs.csv`), a per-second
timeline (`<run_id>.timeline.csv`), and a per-message record
(`<run_id>.messages.csv.gz`). Times are nanoseconds since the Unix epoch unless
a column name says otherwise; `elapsed_s` is seconds since the run's `t0`
event. Shares in the paper are of publish *attempts*, not of confirmed
publishes.

## Run identifiers

```
R-C2-maxlen-drophead_f5_s360_A_r1
└─ configuration      │  │    │  └ replicate, 1..3
                      │  │    └─── scenario: A long slowdown, B short peak
                      │  └──────── slowdown held for this many seconds
                      └─────────── slowdown factor, f5 or f10
```

`README.md` maps configuration directories to the paper's R/K labels.

## The ten final states

Every publish attempt ends in exactly one state, and the ten sum to the number
of attempts. This is the conservation check `verify.py` re-derives
independently; it balances on all 99 published runs.

| State | Meaning |
|---|---|
| `processed_useful` | delivered and the handler finished at or before the deadline |
| `processed_dead` | delivered and the handler finished after the deadline — *dead work* |
| `consumer_expired` | delivered, but the consumer checked the deadline and discarded it without doing the work |
| `lost_useful` | removed by the broker while it could still have met its deadline — *useful loss* |
| `lost_expired` | removed by the broker after its deadline had already passed |
| `nacked` | refused by the broker; under `reject-publish` the producer gets a `basic.nack` |
| `rejected_local_bp` | refused by the producer's own lag-based backpressure before sending |
| `shed` | dropped from the producer's one-second output buffer while publishing was blocked |
| `error` | the publish or the handler returned an error |
| `remaining` | still in the queue when the run ended |

`lost_unclassified` is an eleventh column, not a state: it counts messages the
reconstruction could not place. It is zero on every published run, and a
non-zero value there means the reconstruction is wrong, not that a message
vanished.

The three refusal-like states are distinct and the paper keeps them apart.
*Refused* means the publisher was told — by the broker (`nacked`) or by its own
backpressure (`rejected_local_bp`). *Shed* means the producer dropped the
message from its own buffer with no per-message signal. Figure 2 merges all
three into one bar; Table 3 keeps them in separate columns.

## Derived metrics

**Useful loss** = `lost_useful` / `produced`. A message counts as useful loss if
the broker removed it at a moment when its deadline had not yet passed — it is
loss of work that was still worth doing, as distinct from loss of work that was
already too late.

**Strict useful loss** (`lost_useful_strict`) is the same count with removals in
the final second of a message's life excluded. R1 differs by 0.30 percentage points, R2 and R4
not at all; the one configuration where the choice matters is R12, whose limit removes messages with 0.25 s of life
left: its useful loss is 23.73% under the normal definition and zero under the
strict one. The paper reports the normal definition, which is the less
favourable one for bounded configurations.

**Dead work** = `processed_dead` / `produced`. It counts messages, not the value
of the work, and at a soft deadline it must be read together with the lateness
distribution: R3's 15.80% are late by a median of 0.63 s, K1's 64.28% by 78.89 s.

**Slack to the deadline** = `deadline_ns - t_done_ns`, in seconds, positive when
the handler finished before the deadline. Table 5 reports the *worst case*: the
minimum over all deliveries in a run, then the median of those minima over the
three runs, with the range across runs in parentheses. It is not pooled across
runs before the median is taken — the sizing argument is about the worst case a
single run produces.

**On-time share** = `processed_useful` / `produced`. The one column that folds
loss and dead work into a single number. Its ceiling on this workload, with a
perfect scheduler, is 76.4%.

## Per-run summary (`data/ec2-runs.csv`, `data/laptop-runs.csv`)

| Column | Unit | Meaning |
|---|---|---|
| `run_id`, `config`, `broker`, `scenario`, `rep`, `factor`, `slow_s` | — | identity, as decoded above |
| `status` | — | `ok`, or why the run was rejected |
| `produced` | messages | publish attempts; 660,000 in scenario A, 240,000 in B |
| `shed`, `nacked`, `rejected_local_bp`, `error` | messages | states, above |
| `processed_useful`, `processed_dead`, `consumer_expired` | messages | states |
| `lost_useful`, `lost_expired`, `lost_unclassified`, `remaining` | messages | states |
| `lost_useful_strict` | messages | useful loss excluding the final second |
| `useful_loss_ratio`, `dead_work_ratio` | fraction of attempts | multiply by 100 for the paper's percentages |
| `dead_work_handler_seconds` | s | handler time spent on messages already past their deadline |
| `backlog_at_slow_end` | messages | depth at the end of the slowdown, *not* the peak. RabbitMQ: queue depth. Kafka: consumer-group lag, not clamped to the log start, so it counts records no longer in the log |
| `model_drain_s`, `measured_drain_s` | s | `B / (μ − λ)` against the observed drain |
| `drain_completed` | bool | whether the queue emptied before the run ended |
| `depth_at_end`, `drain_rate_measured_msg_s` | messages, msg/s | |
| `time_to_alarm_s` | s | from the start of the slowdown to the first disk alarm; empty when no alarm fired |
| `alarm_episodes`, `alarm_seconds_total`, `alarm_fraction_of_slow` | count, s, fraction | from the orchestrator's one-second probe. The broker log reports more episodes, because it writes one per ten-second poll while the alarm is active; the paper's "117 episodes" is the broker-log count |
| `publishers_blocked_seconds` | s | summed across blocked connections |
| `purges` | count | operator purges, a harness policy (see below) |
| `conservation_balanced` | bool | whether the ten states sum to `produced` |
| `nbr_confirm_p99_baseline_ms`, `nbr_confirm_p99_slow_ms` | ms | publish-confirm p99 of the neighbour queue, before and during the slowdown |
| `nbr_e2e_p99_baseline_ms`, `nbr_e2e_p99_slow_ms` | ms | end-to-end p99 of the neighbour queue |
| `nbr_publish_failures_slow` | messages | neighbour publishes that failed during the slowdown |

The two brokers' baselines are not comparable: the Kafka producer runs with
`linger.ms` = 5, so its neighbour baseline is about 6 ms where RabbitMQ's is
0.8 ms. Read the change, not the level.

## Per-second timeline (`<run_id>.timeline.csv`)

The file opens with a commented provenance header — three JSON lines holding the
run manifest, the summary and the event list, including image digests — so a run
can be re-derived from its two files alone.

| Column | Unit | Meaning |
|---|---|---|
| `elapsed_s`, `phase` | s, — | seconds from `t0`; `warmup`, `slow`, `recover`, `drain` |
| `published`, `confirmed`, `refused`, `shed` | messages in this second | producer side |
| `delivered`, `delivered_late` | messages in this second | consumer side |
| `queue_depth`, `queue_ready`, `queue_unacked` | messages | RabbitMQ. `x-max-length` bounds `queue_ready` only, so depth sits at the limit plus the prefetch of 200 |
| `oldest_message_age_s` | s | **reconstructed**, not a broker reading. No claim in the paper rests on it |
| `broker_mem_bytes`, `broker_mem_limit_bytes`, `mem_alarm` | bytes, bool | |
| `disk_free_bytes`, `disk_free_limit_bytes`, `disk_alarm` | bytes, bool | |
| `connections_total`, `connections_blocked` | count | `connections_blocked` counts every connection on the node in `blocked` or `blocking`, including consumer connections that never publish; the publishing connections are two |
| `container_mem_bytes`, `container_io_write_bytes`, `container_io_read_bytes` | bytes | cgroup counters, cumulative for the two I/O columns |
| `nbr_published`, `nbr_publish_failed`, `nbr_confirm_p99_ms`, `nbr_depth` | | neighbour queue |
| `kafka_lag_total` | records | consumer-group lag, not clamped to the log start |
| `kafka_log_start_p0`, `kafka_end_p0`, `kafka_committed_p0` | offsets | partition 0; Figure 5 is these three |
| `event` | — | marker, e.g. `operator.purge` |

## Per-message record (`<run_id>.messages.csv.gz`)

One row per publish attempt, for **both** queues.

| Column | Unit | Meaning |
|---|---|---|
| `queue` | — | `main` or the neighbour. Every per-message statistic in the paper filters to `main` |
| `seq`, `size_bytes` | — , bytes | sequence number from the message header |
| `t_create_ns`, `t_publish_ns`, `t_confirm_ns` | ns | created, handed to the client, confirmed |
| `publish_outcome` | — | `confirmed`, `nacked`, `shed`, `error` |
| `partition`, `offset` | — | Kafka only |
| `deadline_ns` | ns | `t_create_ns` + 60 s, carried inside the message. Neither broker knows about it |
| `t_receive_ns`, `t_done_ns` | ns | delivered to the consumer, handler finished |
| `consume_outcome` | — | `processed`, `expired`, `error` |
| `age_at_receive_ms` | ms | `t_receive_ns − t_create_ns`; how long the message actually waited |
| `service_ms` | ms | handler time for this message |
| `lateness_ms` | ms | `t_done_ns − deadline_ns`, positive when late |
| `t_removed_ns` | ns | when the broker removed it, empty if it was not removed |
| `final_state` | — | one of the ten states |

Removal is measured differently per broker and this limits what can be claimed.
For Kafka a probe samples the log start offset once a second, so the instant is
quantised to a second. For RabbitMQ the broker exposes no removal counter, so
the queue is reconstructed from the nanosecond timestamps in the messages
themselves; the reconstruction assumes TTL removes exactly at expiry, which is
why R5's spread is partly zero by construction. The instant is cross-checked
independently: the reconstruction puts the first TTL removal at 150.1 s, and the
producer's refusals drop from 600/s to a few dozen in the same second. That
check resolves to one second, so Table 4 says "< 1" rather than zero.

## The operator purge

When the disk alarm has been active for 60 seconds of *accumulated* — not
continuous — time, the orchestrator purges the queue, at most three times per
run. This is a harness policy, not broker behaviour, and it produces all of R1's
and R9's useful loss and a third of R4's. A different threshold would move those
numbers; the direction is set by the model, the magnitude is not.
