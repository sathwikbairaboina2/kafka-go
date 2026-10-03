#!/usr/bin/env sh
# gofmt, vet and the stdlib-only rule for broker packages (ADR 0007).
set -e
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then echo "gofmt: needs formatting:"; echo "$unformatted"; exit 1; fi
echo "gofmt: ok"
go vet ./...
echo "vet: ok"
mod=github.com/sathwikbairaboina2/kafka-go
bad=$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./cmd/... ./internal/... | grep -v "^$mod" | grep -v '^$' || true)
if [ -n "$bad" ]; then echo "stdlib-only: broker imports non-stdlib packages:"; echo "$bad"; exit 1; fi
echo "stdlib-only: ok"
