#!/usr/bin/env bash
# Build the load generator for the Docker VM (linux/amd64, static) and record
# exactly what went into the binary, so a run manifest can point at one build.
set -euo pipefail
cd "$(dirname "$0")"

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o qlab-linux-amd64 .
CGO_ENABLED=0 go build -trimpath -o qlab .

{
  echo "built_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "go_version=$(go version)"
  echo "binary_sha256=$(shasum -a 256 qlab-linux-amd64 | cut -d' ' -f1)"
  echo "binary_bytes=$(wc -c < qlab-linux-amd64 | tr -d ' ')"
  echo "modules:"
  go list -m all | grep -E 'amqp091|franz-go|klauspost|pierrec' | sed 's/^/  /'
} > build.txt

cat build.txt
