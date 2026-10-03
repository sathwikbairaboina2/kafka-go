#!/usr/bin/env sh
# Runs the Go toolchain inside Docker so the host needs no Go install.
set -e
cd "$(dirname "$0")/.."
root="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 exec docker run --rm --name "kafka-go-go-$$-$(date +%s)" -v "$root:/src" \
  -v kafka-go-gomod:/go/pkg/mod -v kafka-go-gobuild:/root/.cache/go-build -w /src golang:1.26 go "$@"
