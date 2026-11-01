#!/usr/bin/env bash
# Prepare a fresh Ubuntu 24.04 instance to run the matrix, then stop.
#
#   scp -r artifact ubuntu@<host>:~/            # or git clone
#   ssh ubuntu@<host> 'bash ~/artifact/cloud/bootstrap.sh'
#   ssh ubuntu@<host>                           # log in again: docker group
#
# Deliberately does not start the runs. Starting them is a separate, explicit
# step, because the matrix takes hours and should not begin inside a bootstrap.
set -euo pipefail

echo "=== packages ==="
sudo apt-get update -qq
# python3-matplotlib is needed by analysis/report.py; without it the matrix
# completes and writes runs.csv but the figure step fails at the very end.
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  docker.io python3 python3-pip python3-matplotlib jq

# Not golang-go: Ubuntu 24.04 ships Go 1.22, and franz-go/pkg/kadm v1.19.0
# declares go >= 1.26.0, so the apt toolchain cannot build the harness at all.
echo "=== go toolchain ==="
GO_VERSION=1.26.0
if ! go version 2>/dev/null | grep -q "go${GO_VERSION}"; then
  curl -fsSL -o /tmp/go.tgz \
    "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf /tmp/go.tgz
  rm -f /tmp/go.tgz
fi
export PATH=/usr/local/go/bin:$PATH
grep -q '/usr/local/go/bin' ~/.profile 2>/dev/null \
  || echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.profile

echo "=== docker ==="
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

# The brokers write a few GB per Kafka run into named volumes under the Docker
# data root. If a separate data volume is mounted, point Docker at it: the root
# volume on a default instance is too small for the whole matrix.
if mountpoint -q /data; then
  echo "=== moving docker data-root to /data ==="
  sudo systemctl stop docker
  sudo mkdir -p /data/docker
  printf '{\n  "data-root": "/data/docker"\n}\n' | sudo tee /etc/docker/daemon.json >/dev/null
  sudo systemctl start docker
fi

echo "=== versions, for the record ==="
docker --version
python3 --version
go version || true
nproc
free -g | head -2
df -h / /data 2>/dev/null || df -h /

echo
echo "=== rebuild the harness natively ==="
cd "$(dirname "$0")/../harness"
if bash build.sh; then
  cat build.txt
else
  # Falling back silently is how a provenance mismatch gets into the record:
  # build.txt would keep describing a binary nobody on this host produced.
  echo
  echo "BUILD FAILED. The committed qlab-linux-amd64 will be used instead, and"
  echo "build.txt will not describe it. Fix the toolchain before a run you"
  echo "intend to publish:  go version  (expected go${GO_VERSION})"
  exit 1
fi

cat <<'NEXT'

=== bootstrap done ===

Log out and back in so the docker group takes effect, then:

  cd ~/artifact
  rm -rf runs work                                          # start clean
  python3 orchestrator/snapshot_env.py                      # record this testbed
  python3 orchestrator/matrix.py --plan robustness --dry-run # expect 15 runs
  PLAN=robustness nohup ./run_all.sh > run_aws.log 2>&1 &
  tail -f run_aws.log

About 3.5 hours. The matrix is resumable: if the instance is interrupted,
re-running the same command continues where it stopped.
NEXT
