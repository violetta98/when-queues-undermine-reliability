#!/usr/bin/env bash
# Run from the laptop. Ships the artifact to a fresh instance, bootstraps it and
# starts the matrix detached, so the run survives closing the laptop.
#
#   bash cloud/launch.sh ubuntu@ec2-1-2-3-4.compute-1.amazonaws.com ~/.ssh/qlab.pem
#
# Ships code only: runs/, work/ and analysis/out/ are left behind, so the
# instance starts with an empty dataset and the plan does not think the runs are
# already done. About 29 MiB.
set -euo pipefail

HOST=${1:?usage: launch.sh user@host [identity_file]}
KEY=${2:-}
SSH=(ssh -o StrictHostKeyChecking=accept-new)
SCP=(scp -o StrictHostKeyChecking=accept-new)
[ -n "$KEY" ] && { SSH+=(-i "$KEY"); SCP+=(-i "$KEY"); }

cd "$(dirname "$0")/.."
ART=$(pwd)

echo "=== packing ==="
TAR=$(mktemp -t qlab).tar.gz
tar --exclude=runs --exclude=work --exclude=analysis/out \
    --exclude='*.log' --exclude=__pycache__ \
    -czf "$TAR" -C "$(dirname "$ART")" "$(basename "$ART")"
du -h "$TAR" | awk '{print "  " $1}'

echo "=== copying ==="
"${SCP[@]}" "$TAR" "$HOST:/tmp/qlab.tar.gz"
"${SSH[@]}" "$HOST" 'rm -rf ~/artifact && tar -xzf /tmp/qlab.tar.gz -C ~ && rm /tmp/qlab.tar.gz'
rm -f "$TAR"

echo "=== bootstrapping ==="
"${SSH[@]}" "$HOST" 'bash ~/artifact/cloud/bootstrap.sh' 2>&1 | tail -20

echo "=== starting the matrix, detached ==="
# sg docker: picks up the new group without a second login. setsid detaches the
# matrix from this SSH session, so it keeps running after the connection drops.
"${SSH[@]}" "$HOST" "sg docker -c 'cd ~/artifact && rm -rf runs work && \
    python3 orchestrator/snapshot_env.py >/dev/null 2>&1; \
    setsid nohup env PLAN=robustness ./run_all.sh > run_aws.log 2>&1 < /dev/null &' " || true
sleep 20
"${SSH[@]}" "$HOST" 'tail -5 ~/artifact/run_aws.log' || true

cat <<NEXT

=== started ===

It is safe to close the laptop now. Check on it with:

  ${SSH[*]} $HOST 'tail -20 ~/artifact/run_aws.log'
  ${SSH[*]} $HOST 'ls ~/artifact/runs/*.timeline.csv | wc -l'   # 15 when done

When all 15 are there, pull the data back and compare:

  ${SCP[*]} -r $HOST:~/artifact/runs ./aws-runs
  ${SCP[*]} $HOST:~/artifact/analysis/out/runs.csv ./aws-runs.csv
  ${SCP[*]} $HOST:~/artifact/env/host.json ./aws-host.json
  python3 cloud/compare_testbeds.py analysis/out/runs.csv aws-runs.csv

Then stop or terminate the instance in the console.
NEXT
