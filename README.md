# When Queues Undermine Reliability — experiment artifact

Code, configurations and data behind *When Queues Undermine Reliability: An
Experimental Study of Message Backlogs in RabbitMQ and Kafka* (under review).

The experiment asks what a message queue is worth when the consumer does not
fail but slows down for minutes. A producer publishes 1000 messages/s of 1 KiB,
each carrying a 60 s deadline; the consumer's capacity is cut fivefold for six
minutes. Twelve bounding configurations were run three times each on an
EC2 instance, with a short-slowdown scenario and a tenfold slowdown
on top — 48 runs. The same configurations were run earlier on a laptop, 51 runs
in total. Every one of the 27.9 million publish attempts ends in exactly one of
ten accounted states.

Everything a reviewer needs to re-derive a number in the paper is either in this
repository or in the Zenodo dataset it points at.

## The experiments

A producer publishes 1000 messages per second of 1 KiB each. Every message
carries its creation time and a deadline 60 s later inside its own body, so
neither broker knows about the deadline — only the consumer does, and only
after delivery. The consumer runs 100 workers at 50 ms per message, so it can
drain 2000 messages/s, twice the arrival rate. The slowdown raises the handler
to 250 ms, cutting capacity to 400/s, and the backlog then grows by 600
messages every second. A second, healthy queue sits on the same broker, never
bounded and never slowed, to show what the backlog does to a neighbour.

| Scenario | Phases | Runs |
|---|---|---|
| Long slowdown | warm-up 60 s, slow 60–420 s, publishing stops at 660 s, run ends at 720 s | 12 configurations × 3 |
| Short peak | warm-up 60 s, slow 60–150 s, publishing stops at 240 s, ends at 300 s | R1, R2, R3 × 3 |
| Tenfold slowdown | same phases as the long slowdown, handler at 500 ms | R1 × 3 |

The short peak is 90 s, ten per cent below `T* = D / (1 − μd/λ)` = 100 s — the
duration at which waiting first reaches the deadline. Below it nothing can
expire, so anything a bound removes there was still useful.

### The twelve configurations

| ID | Broker | What is bounded | Directory |
|---|---|---|---|
| R1 | RabbitMQ | nothing | `R-C1-unbounded` |
| R2 | RabbitMQ | `x-max-length` = 24,000, drop-head | `R-C2-maxlen-drophead` |
| R3 | RabbitMQ | `x-max-length` = 24,000, reject-publish | `R-C3-reject-publish` |
| R4 | RabbitMQ | `x-message-ttl` = 54 s | `R-C4-ttl` |
| R5 | RabbitMQ | TTL 54 s + `x-max-length` 24,000 with reject-publish + a deadline check in the consumer | `R-C5-combined` |
| R9 | RabbitMQ | nothing, twice the disk headroom | `R-C9-wide-headroom` |
| R11 | RabbitMQ | `x-max-length` = 23,700, reject-publish | `R-C11-reject-correct` |
| R12 | RabbitMQ | `x-max-length` = 59,750, drop-head | `R-C12-maxlen-correct-wide` |
| K1 | Kafka | retention 7 days | `K-C1-long-retention` |
| K2 | Kafka | `retention.ms` = 120 s, longer than the deadline | `K-C2-short-retention` |
| K4 | Kafka | retention 7 days, consumer discards expired messages | `K-C4-consumer-deadline` |
| K6 | Kafka | `retention.ms` = 30 s, shorter than the deadline | `K-C6-retention-below-D` |

24,000 is the length limit the usual rule gives, μd·D. 23,700 and 59,750 are
what the model implies for the two overflow policies; R11 and R12 exist to
test whether those hold. The laptop set has two more, `K-C3-lag-backpressure`
and `K-C7-retention-bytes`, which were run but are not discussed in the paper.
R6, R7, R8, R10 and K5 were specified and never run, and no data for them
exists anywhere: `R-C6-quorum-unbounded` (a quorum queue), `R-C7-maxlen-from-mu`
(a limit sized from μ rather than μd), `R-C8-more-consumers` (the "add more
workers" remedy the motivating incident applied), `R-C10-maxlen-correct`
(superseded by R12, the same limit with twice the disk headroom) and
`K-C5-combined`. `orchestrator/configs.py` keeps them with a note on each,
because they mark what the experiment does not cover.

### What is measured

Every publish attempt ends in exactly one of ten states, and they sum to the
number of attempts — that identity is the verification the artifact rests on.
Two of those states carry the argument. **Useful loss** is a message the broker
removed while it could still have met its deadline; **dead work** is a message
processed after its deadline, so the effort was wasted. Alongside them: the
on-time share, the publish-confirm latency of the neighbour queue, the time to
the first disk alarm, the number of operator purges and the drain time.

`DATA_DICTIONARY.md` defines every state and every column, with the units.

## What is where

```
analysis/        scripts that turn runs into the paper's tables and figures
  analyze.py       per-run metrics -> runs.csv
  paper_tables.py  runs -> data/tables/*.csv (Tables 3-6)
  report.py        runs -> figures/*.png,svg (Figures 2-6)
  verify.py        independent re-derivation, writes the verification logs
harness/         Go load generator, consumer, neighbour queue and probes
orchestrator/    run matrix, broker configuration, container lifecycle
cloud/           EC2 bootstrap, run sync, two-testbed comparison
env/             environment snapshots for both testbeds
data/
  ec2-runs.csv     one row per run, 48 rows, all metrics
  laptop-runs.csv  one row per run, 51 rows
  tables/          Tables 3-6 as CSV, regenerated by paper_tables.py
verification/    verify.py output for both testbeds
figures/         the figures as they appear in the paper
```

The per-message records are not in git — they are 2.6 GB compressed. They live
in the Zenodo dataset (see *Getting the data*) and unpack into `runs-aws/` and
`runs/` next to the directories above, together with `work-aws/` and `work/`.

## Getting the data

Summary data is in `data/` and needs nothing. Tables 4 and 5 and all figures
need the per-message records, archived at Zenodo under
[10.5281/zenodo.23866087](https://doi.org/10.5281/zenodo.23866087) (CC BY 4.0).

The record holds four archives. `ec2-runs.zip` (1.2 GB) is enough for every
table and figure except the laptop rows of Table 5, which need
`laptop-runs.zip` (1.3 GB). The two `work` archives are a few megabytes each
and carry the broker logs, the one-second metric samples and the orchestrator
events — the only source for the alarm episode counts, the queue depth at each
operator purge and the Kafka offsets behind Figure 5.

```
# from the repository root
mkdir -p dataset && cd dataset
base=https://zenodo.org/records/23866088/files
for f in ec2-runs.zip ec2-work.zip laptop-runs.zip laptop-work.zip SHA256SUMS; do
  curl -L -O "$base/$f?download=1"
done
unzip -q '*.zip'
sha256sum -c SHA256SUMS            # 1713 files
cd ..
python3 tools/unpack_dataset.py dataset
```

`unpack_dataset.py` hardlinks the files into `runs-aws/`, `runs/`, `work-aws/`
and `work/`, the flat layout the analysis scripts read, so nothing is stored
twice. Pass `--copy` if the dataset sits on another filesystem.

Each run is two files. `<run_id>.timeline.csv` has a commented provenance
header and one row per second. `<run_id>.messages.csv.gz` has one row per
publish attempt, for both the queue under test and the neighbour queue — the
`queue` column separates them, and every per-message statistic in the paper is
computed over `main` only.

## Reproducing the paper

```
pip install -r requirements.txt
python3 analysis/paper_tables.py --print       # Tables 3-6
QLAB_RUNS=runs-aws QLAB_OUT=. python3 analysis/report.py   # -> figures/
QLAB_RUNS=runs-aws QLAB_WORK=work-aws python3 analysis/verify.py
```

`QLAB_RUNS`, `QLAB_WORK` and `QLAB_OUT` select the testbed; they default to the
laptop set (`runs/`, `work/`), so the EC2 numbers need them set as above.

### Where each table and figure comes from

| In the paper | Data | Command |
|---|---|---|
| Table 1 (classes of bounds) | — | conceptual, nothing to recompute |
| Table 2 (configurations) | `orchestrator/configs.py` | — |
| Table 3 (long slowdown) | `data/ec2-runs.csv` | `analysis/paper_tables.py` → `data/tables/table3_outcomes.csv` |
| Table 4 (moment of removal) | per-message records, EC2 | `analysis/paper_tables.py` → `table4_removal_moment.csv` |
| Table 5 (worst-case slack) | per-message records, both testbeds | `analysis/paper_tables.py` → `table5_slack.csv` |
| Table 6 (short slowdown) | `data/ec2-runs.csv`, scenario B | `analysis/paper_tables.py` → `table6_short_slowdown.csv` |
| Table 7 (rules) | — | derived from Sections 5.2–5.6 |
| Figure 1 (harness) | — | drawn by hand |
| Figure 2 (fate of every attempt) | EC2 runs | `report.py` → `figures/fig2_outcomes.png` |
| Figure 3 (backlog over time) | EC2 runs | `report.py` → `figures/fig1_backlog.png` |
| Figure 4 (neighbour p99) | EC2 runs | `report.py` → `figures/fig3_neighbour.png` |
| Figure 5 (Kafka offsets) | EC2 runs | `report.py` → `figures/fig5_kafka_offsets.png` |
| Figure 6 (slack CDF) | per-message records, EC2 | `report.py` → `figures/fig6_slack.png` |
| Figure A1 (drain, appendix) | EC2 runs | `report.py` → `figures/fig4_drain.png` |

Figure A1 is the check behind the statement that the fluid model predicts drain
time without a systematic bias: measured against predicted, all EC2 runs, median
ratio 1.04.

Figure 6 counts deliveries of the queue under test only. An earlier version of
`fig_slack` did not filter on the `queue` column, which pulled the neighbour
queue into the denominator and moved the plateaus from 22.4/22.3/20.9% down to
17.5/17.4/16.5%. The filter is in place and commented; if you regenerate the
figure and see the lower numbers, you are running the old code.

## The two testbeds

**EC2** (primary, `runs-aws/` + `work-aws/`). An m6i.4xlarge instance: Intel
Xeon Platinum 8375C at 2.90 GHz, 16 vCPU, 61.8 GiB, Ubuntu 24.04.5 LTS, kernel
7.0.0-1014-aws, Docker 29.1.3, overlayfs, cgroups v2. Broker data on a separate
100 GiB gp3 volume (ext4) mounted at `/var/lib/docker`; provisioned IOPS and
throughput were not recorded, and the gp3 defaults are 3000 IOPS and 125 MiB/s.
Writing 200 MiB into the harness output directory did not change the free space
the broker saw, which is the property the 60 MiB alarm headroom depends on.

**Laptop** (second testbed, `runs/` + `work/`). An Intel Core i9-9880H machine,
16 GiB RAM, NVMe SSD, macOS 14.1.1, with the brokers inside Docker Desktop's
Linux VM: Docker Engine 24.0.6, 12 vCPU and 7.8 GiB given to the VM, shared with
the host OS, no CPU pinning.

On both testbeds the brokers, the load generator and the neighbour queue share
one machine. Brokers: RabbitMQ 4.3.6 on Erlang/OTP 27.3.4.19 with 4 CPUs and
1 GiB; Kafka 4.3.1 in KRaft mode with 4 CPUs, 2 GiB, a 512 MiB heap, six
partitions, replication factor 1 and `acks=all`. Images pinned by digest in
every run manifest. The harness is Go 1.26.0 with amqp091-go v1.16.0 and
franz-go v1.22.2 (kadm v1.19.0), the Kafka producer running a 5 ms linger.

The laptop ran first; the EC2 set was collected afterwards and is the one the
paper reports. The broker's data **must** sit on a filesystem the harness does
not write to: the harness produces 24–34 MiB of compressed CSV per run, and the
RabbitMQ arms are given only 60 MiB of disk headroom. The first EC2 attempt put
both on the same volume, the alarm latched on at 107 s and never cleared, and
the producer shed 55% of its publishes. Those runs are not in the dataset.

Portability is checked by `cloud/compare_testbeds.py`, with the tolerance fixed
in the script before the runs: ±3 pp for useful loss, ±5 pp for dead work and
on-time share, ±1 for the number of operator purges. Twelve shared
configurations by four quantities is 48 comparisons, all in the long-slowdown
scenario; 47 are inside tolerance.

```
python3 cloud/compare_testbeds.py data/laptop-runs.csv data/ec2-runs.csv
```

## Running the experiment

One run, with Docker available:

```
python3 orchestrator/matrix.py --plan correct --dry-run   # see what would run
python3 orchestrator/matrix.py --plan correct             # R11 and R12, 3 each
```

The plans are in `orchestrator/matrix.py`: `paper` is the full 48-run EC2
matrix, `correct` the two length-limit arms, `core` the original laptop set.
A long-slowdown run takes about 13 minutes. `cloud/bootstrap.sh` prepares a
fresh Ubuntu instance, including the separate Docker volume.

## What is not here

Pilot runs and one replaced R3 attempt are excluded. In that attempt 3,415
messages (0.52%) were confirmed and never delivered, with no error, no alarm and
no purge; the broker log, the orchestrator events, the consumer telemetry and
the conservation check were all examined and none explained it. The attempt was
replaced and the replacement lost nothing. In the 48 EC2 and 51 laptop runs that
are published, `lost_unclassified` and `error` are zero everywhere.

R7 (a length limit sized from μ·D = 120,000) and R8 (three times the consumers)
were specified and never run; no data for them exists on either testbed. They
are kept in `orchestrator/configs.py` because they mark what the experiment does
not cover — R8 in particular is the "add more workers" remedy the motivating
incident applied.

## Known limitations

The required slack beyond the bound-implied wait is explained by the consumer's
prefetch-to-worker ratio, and that ratio is 200/100 in every run on both
testbeds — it was never varied, so the agreement between the testbeds supports
the mechanism rather than the generality of the number. No run was performed at
the limit the measurement points to, nor anywhere between 24,000 and 59,750.
The two brokers were not under equal pressure: RabbitMQ's disk threshold was
squeezed to 60 MiB of headroom while Kafka's volume was not bounded at all and
its backlog stayed inside the page cache, so the Kafka half of the comparison is
only tested up to that boundary. Broker, generator and neighbour share one
machine in every run. The slowdown was varied at two points, the duration at
two, the message size not at all. Retry storms, which raise the arrival rate
exactly when service capacity drops, are not modelled, and the operator purge is
a harness policy rather than broker behaviour — it produces all of R1's and R9's
useful loss. Section 6.3 of the paper states these in full.

## Before republishing

`scrub.py` strips hostnames, home-directory paths and hardware identifiers in
place. Run it **before** `tools/package_dataset.py`: it rewrites the gzip
members, so any checksum taken earlier stops matching.

```
python3 scrub.py --dry-run     # list what would change
python3 scrub.py
python3 tools/package_dataset.py --out ../zenodo-dataset
```

### What the dataset contains, and what it leaves out

Two files per run, 2.6 GB, plus 37 MB of per-run working files: the broker log,
the one-second metric samples, the orchestrator events and the Kafka probe.
Those 37 MB are small but not optional — several numbers in the paper come only
from them (the 117 alarm episodes and their 10.002 s median from the broker log,
the queue depth at each alarm and at each operator purge, and the offsets behind
Figure 5).

Left out by default: the four per-process CSVs each run writes (producer,
consumer and the two neighbour sides), 2.3 GB. They are the pre-join form of
`<run_id>.messages.csv.gz` — every column they carry appears in the published
file, renamed — so they would double the dataset and add nothing a reviewer
needs to check a number. `--with-prejoin` includes them, for anyone who wants to
redo the join rather than trust `postrun.py`.

## Licences and citation

Code is Apache-2.0 (`LICENSE`). Data, in this repository and in the Zenodo
dataset, is CC BY 4.0 (`LICENSE-DATA`). See `CITATION.cff`.

Violetta Pidvolotska, Independent Researcher, 00-842 Warsaw, Poland
— v.pidvolotska@gmail.com, ORCID 0009-0001-8490-9574.
Questions about a number in the paper are welcome; the fastest route is an
issue on this repository quoting the table or figure it comes from.
