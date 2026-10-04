#!/usr/bin/env bash
# Produce throughput of kgod (fsync never and always) against Apache Kafka 4.3.1 on the same machine.
# Writes bench/results/{kgod-never,kgod-always,kafka}.json and env.txt, then prints the report.
# Takes about 6 minutes: three 30 s runs (plus 3 s warm-up each) per target.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
root="$(pwd -W 2>/dev/null || pwd)"
trap 'docker compose --profile bench down -v >/dev/null 2>&1 || true' EXIT

mkdir -p bench/results
{
  echo "host: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes RAM, {{.OperatingSystem}}, kernel {{.KernelVersion}}')"
  echo "docker: $(docker version --format '{{.Server.Version}}')"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > bench/results/env.txt
cat bench/results/env.txt

kcat() {
  docker run --rm -i --name "kafka-go-kcat-$RANDOM" --network kafka-go_default edenhill/kcat:1.7.1 -b "$1" "${@:2}"
}
wait_for() {
  for _ in $(seq 1 60); do
    if kcat "$1" -L >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "bench: $1 did not become ready" >&2
  exit 1
}
load() { # label brokers
  docker run --rm --name "kafka-go-load-$RANDOM" --network kafka-go_default \
    -v "$root:/src" -v kafka-go-gomod:/go/pkg/mod -v kafka-go-gobuild:/root/.cache/go-build -w /src golang:1.26 \
    go run ./bench/load -brokers "$2" -label "$1" -out "bench/results/$1.json"
}

docker compose --profile bench down -v >/dev/null 2>&1 || true
KGOD_FSYNC=never docker compose --profile bench up -d --build
wait_for kafka-go-kgod:9092
wait_for kafka-go-kafka:9092
docker exec kafka-go-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic bench --partitions 8 --if-not-exists

load kgod-never kafka-go-kgod:9092
load kafka kafka-go-kafka:9092

KGOD_FSYNC=always docker compose up -d kgod
wait_for kafka-go-kgod:9092
load kgod-always kafka-go-kgod:9092

echo
docker run --rm --name "kafka-go-report-$RANDOM" -v "$root:/src" -v kafka-go-gomod:/go/pkg/mod \
  -v kafka-go-gobuild:/root/.cache/go-build -w /src golang:1.26 go run ./bench/report
