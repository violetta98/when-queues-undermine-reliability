# Which dataset came from which testbed

Two testbeds, two directories, never mixed in one table. Every run also carries
its own provenance: the `TESTBED` block in the header of its
`<run>.timeline.csv` records the host, the OS, the Docker version and the
container limits, and the matching `host.json` sits next to the data.

| Directory | Testbed | Contents |
|---|---|---|
| `runs-aws/` + `work-aws/` | **aws** — EC2 m6i.4xlarge, Ubuntu 24.04.5 (kernel 7.0.0-1014-aws), Intel Xeon Platinum 8375C, 16 dedicated vCPU, broker data on a dedicated 100 GiB gp3 volume | 48 runs, complete. **The primary dataset**: `RESULTS-3.md`, `RESULTS-APPENDIX-3.md` and `ARTICLE-SKELETON-3.md` are written from it |
| `runs/` + `work/` | **laptop** — Apple MacBook Pro (Intel i9-9880H), macOS 14.1.1, brokers in the Docker Desktop linuxkit VM, 12 vCPU shared with the host OS | 51 runs; the dataset `RESULTS-2.md` and `RESULTS-APPENDIX-2.md` were written from, now the second testbed |
| `env/host.json` | laptop snapshot | hardware, OS, Docker, image digests |
| `runs-aws/host.json` | aws snapshot | the same fields, so the two testbeds can be compared field by field |

All twelve configurations were run on both testbeds — 45 of the 51 laptop runs.
`K-C3-lag-backpressure` and `K-C7-retention-bytes` are laptop-only; the x10 slowdown
arm is AWS-only. `R-C11-reject-correct` and `R-C12-maxlen-correct-wide` were added to
the laptop set last, by `python3 orchestrator/matrix.py --plan correct`, because the
paper's main result turns on sub-second margins that a quieter machine can flatter.

Each testbed keeps the published pairs and the per-process working files as
sibling directories, the same shape in both cases. The analysis scripts default
to `ROOT/"runs"` and `ROOT/"work"` and accept `QLAB_RUNS` / `QLAB_WORK` to point
at the other set without moving anything:

    QLAB_RUNS=runs-aws QLAB_WORK=work-aws python3 analysis/verify.py

`VERIFY.txt` is that script's output for the laptop set (51 of 51);
`VERIFY-AWS.txt` for the AWS set (48 of 48). Both are checked in.

## Why two

The laptop shares its CPU with a desktop OS, which is the one objection the data
cannot answer from the inside. The AWS testbed removes it. The comparison is run
by `cloud/compare_testbeds.py`, which splits the measured quantities into two
groups with a tolerance fixed in the script beforehand: shares and ratios that a
conclusion depends on must agree, absolute times and rates are expected to
differ because they are properties of the host. It groups runs by
`(config, factor)`, because folding the x10 arm into the same median as the x5
runs invents a disagreement between testbeds that is really a difference between
two experiments.

Result: 47 of 48 conclusion-bearing comparisons inside tolerance. The exception is dead
work for K2, 41.04% on the laptop against 35.51% on AWS; K2's useful loss, which is what
the arm is in the set for, is 0.00% on both. Note what the tolerance set does not cover:
worst-case slack to the deadline, which for R-C11 is +0.088 s on AWS and +0.017 s on the
laptop, negative in one of three replicates. Aggregate shares transfer; the margin that
decides the sizing rule does not.

## One difference that is not incidental

On the laptop the broker's data lived on the Docker VM's own filesystem while
the harness wrote its output through the macOS file-sharing layer, so the two
could not interfere. On a cloud instance both land on the same filesystem by
default, and the harness writes 25–31 MiB of compressed CSV per run — a
significant fraction of the 60 MiB of disk headroom the RabbitMQ arms are given.
The first AWS attempt showed exactly that: the alarm latched on at 107 s and
never cleared, and the producer shed 55% of its publishes against 12.5% in the
corrected runs. The AWS runs therefore put the broker's data on a dedicated
volume, which restores the property the laptop had by accident: the 60 MiB
headroom measures the queue, not the test harness. Verified directly — writing
200 MiB into the output directory moves the broker's reported free space by
zero bytes.

## Paths in the documents

The documents name the primary set explicitly as `artifact/runs-aws/<run>.timeline.csv`
and `artifact/work-aws/<run>/`, so no rename is required and none has been done.
`runs/` and `work/` remain the laptop set, which is what `RESULTS-2.md` refers
to.
