# Rerunning the matrix on AWS

The laptop testbed shares its CPU with macOS. That is the one objection the data
cannot answer from the inside, and it is what a second testbed exists to settle.
An EC2 instance with dedicated vCPU removes it; everything else about the setup
stays identical, so the two datasets are comparable arm by arm.

Prices below are from memory and must be checked in the console before you
launch anything.

## What to launch

| | Choice | Why |
|---|---|---|
| Instance | **m6i.4xlarge** — 16 vCPU, 64 GiB, dedicated | The laptop VM had 12 vCPU; the containers are capped at 4 CPU for RabbitMQ, 4 for Kafka and 4 for the load generator, so 16 leaves the caps binding rather than the host. Do **not** use `t`-family: burstable credits would reintroduce exactly the CPU contention we are trying to remove. |
| Storage | Root 30 GiB gp3, plus a **100 GiB gp3** data volume | The matrix writes about 2.5 GiB per run into Docker volumes and the orchestrator refuses to start with under 8 GiB free. gp3 at its 3000 IOPS / 125 MB/s default is also the honest choice: it is what production deployments actually use. |
| AMI | Ubuntu Server 24.04 LTS, x86-64 | The committed harness binary is `GOOS=linux GOARCH=amd64`, statically linked. An ARM instance (`m7g`) would need a rebuild — doable, but then the CPU architecture changes too, which muddies the comparison. |
| Region | whichever is nearest | Nothing here is latency-sensitive to the client. |

Rough cost: m6i.4xlarge on-demand is around $0.77/h in us-east-1. The matrix is
about 3.5 h, so with setup and transfer call it 5 h — **under $5**, plus pennies
for the volume. Spot would be roughly a third of that and the matrix is
resumable, so an interruption costs one run rather than the batch; on-demand is
simpler for a single afternoon.

## Runbook

```bash
# 1. attach and format the data volume (device name varies; check lsblk)
sudo mkfs.ext4 /dev/nvme1n1
sudo mkdir -p /data && sudo mount /dev/nvme1n1 /data
echo '/dev/nvme1n1 /data ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab

# 2. copy the artifact and bootstrap
scp -r artifact ubuntu@<host>:~/
ssh ubuntu@<host> 'bash ~/artifact/cloud/bootstrap.sh'
ssh ubuntu@<host>          # log in again so the docker group applies

# 3. run
cd ~/artifact
rm -rf runs work
python3 orchestrator/snapshot_env.py
python3 orchestrator/matrix.py --plan robustness --dry-run    # expect 15 runs
PLAN=robustness nohup ./run_all.sh > run_aws.log 2>&1 &
tail -f run_aws.log

# 4. bring back what the comparison needs (~500 MiB, not the whole work/ tree)
#    a run can be re-derived from its two files alone, so runs/ is enough
scp -r ubuntu@<host>:~/artifact/runs            aws-runs/
scp     ubuntu@<host>:~/artifact/env/host.json  aws-host.json
scp     ubuntu@<host>:~/artifact/analysis/out/runs.csv aws-runs.csv

# 5. compare
python3 cloud/compare_testbeds.py analysis/out/runs.csv aws-runs.csv
```

Then terminate the instance. The data volume can be deleted with it; everything
needed is in `runs/`.

## Decide the acceptance criterion before you look

Otherwise the temptation afterwards is to reinterpret whatever came out.
`compare_testbeds.py` splits the quantities into two groups and applies a
tolerance fixed in the script:

**Must transfer** — a claim in the paper breaks if one of these moves:
useful loss (±3 pp), dead work (±5 pp), share delivered on time (±5 pp), number
of purges (±1).

**Expected to differ** — no claim rests on them, and they are printed so the
difference is visible rather than buried: time to alarm, backlog at the end of
the slowdown, drain time, neighbour latency before and during the slowdown.

If the first group holds and the second moves, that is the result worth
reporting: the conclusions are properties of the mechanisms, and the constants
are properties of the host. If something in the first group moves, that is a
finding too, and a more interesting one — it would mean a conclusion in the
paper is testbed-dependent and must be narrowed.

## What this does and does not settle

Settles: dedicated CPU instead of time shared with a desktop OS; a real Linux
kernel and a mainstream cloud block device instead of Docker Desktop's VM; a
documented, reproducible host rather than one laptop.

Does not settle: broker and load generator still share one machine. Splitting
them needs the orchestrator to stop creating a local Docker network and to point
the harness at a remote `-addr` instead — about a day's work. Worth doing only
if a reviewer asks for it after seeing that the single-host numbers transfer.
