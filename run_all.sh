#!/usr/bin/env bash
# Run the whole matrix and then derive the tables and figures, unattended.
#
# Launch it detached so it does not depend on the terminal (or on any editor
# session) staying open:
#
#   cd artifact
#   python3 -c "import subprocess; subprocess.Popen(['./run_all.sh'], \
#       stdout=open('run_all.log','a'), stderr=subprocess.STDOUT, start_new_session=True)"
#
# It is safe to run again after an interruption: runs that already have a
# manifest are skipped, so it continues where it stopped.
set -u
cd "$(dirname "$0")"

echo "=== started $(date '+%F %T %Z') ==="

# On macOS caffeinate keeps the machine awake for as long as the matrix runs (it
# only prevents sleep on AC power, so the machine has to stay plugged in). On a
# Linux host there is nothing to keep awake, so the matrix runs directly.
if command -v caffeinate >/dev/null 2>&1; then
  caffeinate -dims python3 -u orchestrator/matrix.py --plan "${PLAN:-core}"
else
  python3 -u orchestrator/matrix.py --plan "${PLAN:-core}"
fi
matrix_status=$?
echo "=== matrix exited with $matrix_status at $(date '+%F %T %Z') ==="

# Derive everything the paper needs, whether or not every run succeeded: a
# partial matrix still produces usable tables, with a smaller n.
python3 analysis/analyze.py
echo "=== analyze exited with $? ==="
python3 analysis/report.py
echo "=== report exited with $? ==="

echo "=== finished $(date '+%F %T %Z') ==="
echo "tables and figures: analysis/out/"
