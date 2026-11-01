# Summary data

Six CSV files. Everything here is derived from the per-message records, which
are too large for git and live at
[10.5281/zenodo.23866087](https://doi.org/10.5281/zenodo.23866087).

| File | Rows | What it is |
|---|---|---|
| `ec2-runs.csv` | 48 | one row per run on the EC2 testbed, every metric |
| `laptop-runs.csv` | 51 | the same for the laptop testbed |
| `tables/table3_outcomes.csv` | 12 | Table 3 of the paper: outcomes of the long slowdown |
| `tables/table4_removal_moment.csv` | 8 | Table 4: when each bound removed a message |
| `tables/table5_slack.csv` | 8 | Table 5: worst-case slack to the deadline, both testbeds |
| `tables/table6_short_slowdown.csv` | 3 | Table 6: the 90 s peak |

The two `*-runs.csv` files are the raw material: one row per run, 42 columns,
written by `analysis/analyze.py`. The four in `tables/` are what the paper
prints, rebuilt by `analysis/paper_tables.py`. They are committed rather than
generated on demand so that a reviewer can compare a number against the paper
without downloading 2.6 GB first.

Tables 3 and 6 come from the per-run summaries alone, so they rebuild from this
directory. Tables 4 and 5 need the per-message records.

```
python3 analysis/paper_tables.py --print
```

## Reading the columns

`DATA_DICTIONARY.md` in the repository root defines every column and its units,
and the ten mutually exclusive states a publish attempt can end in. Three things
are worth knowing before opening a file:

**Shares are of publish attempts**, 660,000 per run in the long slowdown and
240,000 in the short peak — not of messages that were confirmed.

**`useful_loss_ratio` and `dead_work_ratio` are fractions**, not percentages.
Multiply by 100 to get the paper's figures.

**`backlog_at_slow_end` means different things per broker.** For RabbitMQ it is
the queue depth; for Kafka it is consumer-group lag, which is not clamped to the
log start and therefore counts records the broker has already deleted.

## Run identifiers

    R-C2-maxlen-drophead_f5_s360_A_r1
    └─ configuration      │  │    │  └ replicate, 1..3
                          │  │    └─── scenario: A long slowdown, B short peak
                          │  └──────── slowdown held this many seconds
                          └─────────── slowdown factor, f5 or f10

The repository README maps the configuration directories to the paper's R and K
labels and says what each one bounds.
