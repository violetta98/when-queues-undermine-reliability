#!/usr/bin/env bash
# Pull finished runs off the instance into runs-aws/, so the dataset survives
# the instance and a bad run shows up while there is still time to react.
#
#   bash cloud/sync_runs.sh ubuntu@<host> ~/.ssh/key.pem          # once
#   bash cloud/sync_runs.sh ubuntu@<host> ~/.ssh/key.pem --watch  # every 5 min
#
# Safe to run while the matrix is working: rsync re-copies anything that changed
# since the last pass, so a file caught mid-write is corrected on the next one.
# The run currently being written is simply copied again next time.
set -uo pipefail

HOST=${1:?usage: sync_runs.sh user@host [key] [--watch]}
KEY=${2:-}
WATCH=${3:-}
[ "${2:-}" = "--watch" ] && { KEY=""; WATCH="--watch"; }

cd "$(dirname "$0")/.."
# Mirrors the laptop layout — runs/ and work/ as siblings — so that making AWS
# the primary dataset later is four renames and no edit to any path in the
# documents or to ROOT/"runs" and ROOT/"work" in the analysis scripts.
DEST=runs-aws
WORK=work-aws
mkdir -p "$DEST" "$WORK"
RSH="ssh -o StrictHostKeyChecking=accept-new"
[ -n "$KEY" ] && RSH="$RSH -i $KEY"

sync_once() {
  rsync -a --partial -e "$RSH" "$HOST:~/artifact/runs/" "$DEST/" 2>/dev/null
  # The working files too, but only for runs that already have a timeline: the
  # directory of the run in flight churns, and there is no point copying it
  # mid-write. They are needed because the broker's own log lives only here, and
  # because re-deriving the dataset after a change to the analysis (which has
  # happened once already) reads the per-process output, not the published pair.
  mkdir -p "$WORK"
  for f in "$DEST"/*.timeline.csv; do
    [ -e "$f" ] || continue
    r=$(basename "$f" .timeline.csv)
    rsync -a --partial -e "$RSH" "$HOST:~/artifact/work/$r" "$WORK/" 2>/dev/null
  done
  # Provenance travels with the data: without host.json a reader cannot tell
  # which testbed a directory came from.
  rsync -a -e "$RSH" "$HOST:~/artifact/env/host.json" "$DEST/host.json" 2>/dev/null
  rsync -a -e "$RSH" "$HOST:~/artifact/RUNLOG.md" "$DEST/RUNLOG.md" 2>/dev/null
  rsync -a -e "$RSH" "$HOST:~/artifact/run_aws.log" "$DEST/run_aws.log" 2>/dev/null

  local n ok susp
  n=$(ls "$DEST"/*.timeline.csv 2>/dev/null | wc -l | tr -d ' ')
  ok=0; susp=0
  for f in "$DEST"/*.timeline.csv; do
    [ -e "$f" ] || continue
    if grep -qm1 'status: OK' "$f"; then ok=$((ok+1)); else susp=$((susp+1)); fi
  done
  local w sz
  w=$(ls -d "$WORK"/*/ 2>/dev/null | wc -l | tr -d ' ')
  sz=$(du -shc "$DEST" "$WORK" 2>/dev/null | tail -1 | cut -f1)
  printf '%s  runs %s/48  (ok %s, suspect %s)  work %s  total %s\n' \
    "$(date '+%H:%M:%S')" "$n" "$ok" "$susp" "$w" "$sz"
  [ "$susp" -gt 0 ] && {
    echo "  WARNING, runs not marked ok:"
    for f in "$DEST"/*.timeline.csv; do
      grep -qm1 'status: OK' "$f" || echo "    $(basename "$f" .timeline.csv)"
    done
  }
  return 0
}

if [ "$WATCH" = "--watch" ]; then
  while :; do
    sync_once
    ls "$DEST"/*.timeline.csv 2>/dev/null | wc -l | grep -qx ' *48' && break
    sleep 300
  done
else
  sync_once
fi
