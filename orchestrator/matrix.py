#!/usr/bin/env python3
"""Run the experiment matrix sequentially, resumably.

Runs are ordered so that the dataset stays usable if the sequence is
interrupted: breadth first (one replicate of every arm), then the scenario-B
and slowdown-sweep blocks, then the remaining replicates. A run whose manifest
already exists is skipped, so re-invoking this script continues where it left
off.

    python3 orchestrator/matrix.py --plan core          # ~14 h, the full matrix
    python3 orchestrator/matrix.py --plan robustness    # ~3 h, 5 arms x 3 for a
                                                        # second testbed
    python3 orchestrator/matrix.py --plan core --dry-run
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
# Same switch as the analysis scripts, so a plan can be resumed against
# whichever testbed's runs are on disk.
RUNS = ROOT / os.environ.get("QLAB_RUNS", "runs")

# The arms kept for the paper, each because it carries a claim that would
# otherwise have no data behind it. Arms dropped after the review of coverage:
# K-C5 (the Kafka combination is already covered by K-C3 plus K-C4), R-C6
# (quorum queues; now a stated limitation), the 2x/10x slowdown sweep (the model
# predicts mu_d = mu / f and the 5x case already validates it through drain
# time), and Kafka in scenario B (H3 is settled by the RabbitMQ pair).
CORE = [
    "R-C1-unbounded",          # the thesis: alarm, blocked connections, purge
    "K-C1-long-retention",     # Kafka baseline: lag without deletion
    "R-C2-maxlen-drophead",    # H1: a length bound still loses useful messages
    "K-C2-short-retention",    # Kafka: deletion with no error anywhere
    "R-C3-reject-publish",     # H2/H3: class 3, and the basis for scenario B
    "K-C3-lag-backpressure",   # without it the taxonomy is incomplete for Kafka
    "R-C4-ttl",                # H1: the main recommendation
    "K-C4-consumer-deadline",  # H1 on Kafka, which has no per-message TTL
    "R-C5-combined",           # otherwise the recommendation is never measured whole
    "K-C6-retention-below-D",  # model threshold 2: useful loss appears when R < D
]

# Specified, never run. R-C7 sizes the limit from mu rather than mu_d; R-C8 is
# the "add more workers" remedy. No plan references them -- they are kept so
# that configs.py, the README and the paper agree about what was left out.
NEVER_RUN = ["R-C7-maxlen-from-mu", "R-C8-more-consumers",
             "R-C6-quorum-unbounded", "R-C10-maxlen-correct", "K-C5-combined"]

# The four arms added to the laptop set after CORE: the doubled disk headroom,
# the byte-based retention bound, and the two model-derived length limits.
LAPTOP_EXTRA = ["R-C9-wide-headroom", "K-C7-retention-bytes",
                "R-C11-reject-correct", "R-C12-maxlen-correct-wide"]
# The spike block as the paper reports it: drop-head is the arm that destroys
# live messages for nothing, so it has to be there alongside the two others.
SPIKE_B = ["R-C1-unbounded", "R-C2-maxlen-drophead", "R-C3-reject-publish"]

CORE_REPS = 3
SPIKE_REPS = 3

# Estimated wall-clock per run, used only for the ETA line.
EST_MIN = {"A": 12.8, "B": 5.6}


# The subset for a second testbed. Five arms carry every claim that a reviewer
# could call an artefact of the first one: the two baselines (R-C1, K-C1), whose
# numbers depend on fsync cost and therefore on the host; the length bound
# (R-C2), which fixes the L/lambda residence result; the recommended combination
# (R-C5), on which the rules rest; and Kafka retention below the deadline
# (K-C6), which carries the granularity comparison. Agreement in order of
# magnitude is the claim being tested, not agreement to the digit.
ROBUSTNESS = [
    "R-C1-unbounded",
    "K-C1-long-retention",
    "R-C2-maxlen-drophead",
    "R-C5-combined",
    "K-C6-retention-below-D",
]


def plan_robustness():
    A = dict(scenario="A", slow=360, publish=660, consume=720, factor=5)
    return [dict(A, config=c, rep=rep)
            for rep in range(1, CORE_REPS + 1) for c in ROBUSTNESS]


# Everything the paper's tables stand on, measured on one testbed so that no
# table mixes hosts, plus the two bounds the results section derives but never
# ran. Deliberately not here: extra replicates (the spread is a tenth of a point
# where the mechanism is deterministic), the arms that would close a stated
# limitation rather than support an existing claim, and the correct drop-head
# bound at the narrow headroom, whose outcome follows from arithmetic.
PAPER = [
    "R-C1-unbounded", "R-C2-maxlen-drophead", "R-C3-reject-publish",
    "R-C4-ttl", "R-C5-combined", "R-C9-wide-headroom",
    "K-C1-long-retention", "K-C2-short-retention",
    "K-C4-consumer-deadline", "K-C6-retention-below-D",
]
# The spike block as the paper reports it.
# Turns "by calculation" into "measured": refusal at the bound the rule implies,
# and the drop-head bound from lambda given room to hold it.
CORRECT = ["R-C11-reject-correct", "R-C12-maxlen-correct-wide"]
# One further operating point for the model. Factor 2 is degenerate here, since
# mu / lambda is 2 and mu_d would equal lambda, so nothing would accumulate.
SWEEP_FACTOR = 10.0
SWEEP = ["R-C1-unbounded"]


def plan_paper():
    A = dict(scenario="A", slow=360, publish=660, consume=720, factor=5)
    B = dict(scenario="B", slow=90, publish=240, consume=300, factor=5)
    jobs = []
    # Breadth first: one replicate of everything, then the rest, so an
    # interrupted matrix still covers every claim at n=1.
    for rep in (1, 2, 3):
        jobs += [dict(A, config=c, rep=rep) for c in PAPER]
        jobs += [dict(A, config=c, rep=rep) for c in CORRECT]
        jobs += [dict(B, config=c, rep=rep) for c in SPIKE_B]
        jobs += [dict(A, config=c, rep=rep, factor=SWEEP_FACTOR) for c in SWEEP]
    return jobs


def plan_correct():
    """The two model-derived length bounds, scenario A, three replicates.

    These two arms carry the paper's main result and were run only on the
    cloud testbed, so the portability check in the write-up does not cover
    them. This plan exists to close that gap on the second testbed; it is
    separate from plan_paper so the replication can be re-run on its own
    without re-running the other ten configurations.
    """
    A = dict(scenario="A", slow=360, publish=660, consume=720, factor=5)
    return [dict(A, config=c, rep=rep) for rep in (1, 2, 3) for c in CORRECT]


def plan_core():
    """The laptop set as it was actually run: 14 arms x 3 in the long
    slowdown, plus the three-arm spike block x 3. 51 runs."""
    A = dict(scenario="A", slow=360, publish=660, consume=720, factor=5)
    B = dict(scenario="B", slow=90, publish=240, consume=300, factor=5)
    jobs = []
    # Breadth first: one replicate of every arm, then the spike block, then the
    # remaining replicates. An interrupted matrix still covers every claim.
    arms = CORE + LAPTOP_EXTRA
    jobs += [dict(A, config=c, rep=1) for c in arms]
    for rep in range(1, SPIKE_REPS + 1):
        jobs += [dict(B, config=c, rep=rep) for c in SPIKE_B]
    jobs += [dict(A, config=c, rep=2) for c in arms]
    jobs += [dict(A, config=c, rep=3) for c in arms]
    return jobs


def run_id(job):
    return (f"{job['config']}_f{job['factor']:g}_s{int(job['slow'])}"
            f"_{job['scenario']}_r{job['rep']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default="core",
                    choices=["core", "robustness", "paper", "correct"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--settle", type=float, default=8.0,
                    help="seconds to let the host settle between runs")
    args = ap.parse_args()

    jobs = {"robustness": plan_robustness,
            "paper": plan_paper,
            "correct": plan_correct}.get(args.plan, plan_core)()
    todo = [j for j in jobs if not (RUNS / f"{run_id(j)}.timeline.csv").exists()]
    est = sum(EST_MIN[j["scenario"]] for j in todo)
    print(f"{len(jobs)} runs in plan, {len(jobs) - len(todo)} already done, "
          f"{len(todo)} to run, estimated {est / 60:.1f} h")
    if args.dry_run:
        for i, j in enumerate(todo, 1):
            print(f"  {i:3d}. {run_id(j)}")
        return 0

    started = time.time()
    failures = []
    for i, job in enumerate(todo, 1):
        done_min = (time.time() - started) / 60
        left = sum(EST_MIN[j["scenario"]] for j in todo[i - 1:])
        print(f"\n=== [{i}/{len(todo)}] {run_id(job)} "
              f"(elapsed {done_min / 60:.1f} h, remaining about {left / 60:.1f} h) ===",
              flush=True)
        cmd = [sys.executable, str(HERE / "orchestrate.py"),
               "--config", job["config"], "--factor", str(job["factor"]),
               "--rep", str(job["rep"]), "--scenario", job["scenario"],
               "--slow", str(job["slow"]), "--publish", str(job["publish"]),
               "--consume", str(job["consume"])]
        # Every loop inside a run is already bounded, but an unattended night
        # should not be lost to one unforeseen stall: kill a run that overruns
        # its own schedule by ten minutes and carry on with the next.
        budget = job["consume"] + 600
        try:
            p = subprocess.run(cmd, timeout=budget)
            rc = p.returncode
        except subprocess.TimeoutExpired:
            rc = -1
            print(f"  ! run exceeded {budget:.0f} s and was killed", file=sys.stderr, flush=True)
            subprocess.run(["docker", "rm", "-f", "qlab-load", "qlab-rmq", "qlab-kafka"],
                           capture_output=True)
        if rc != 0:
            failures.append(run_id(job))
            print(f"  ! run failed (exit {rc}), continuing", file=sys.stderr, flush=True)
        time.sleep(args.settle)

    print(f"\nmatrix finished in {(time.time() - started) / 3600:.2f} h")
    if failures:
        print(f"{len(failures)} failed run(s): {failures}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
