#!/usr/bin/env bash
# The 30-second demo: produce with kcat, kill -9 the broker, restart it, read everything back, verify the log.
# Record it with: bash scripts/demo.sh 2>&1 | tee docs/demo.txt
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
trap 'docker compose down -v >/dev/null 2>&1 || true' EXIT

NET=kafka-go_default
KCAT_IMAGE=edenhill/kcat:1.7.1

# kcat runs in a container on the compose network; the transcript shows the equivalent kcat command.
kcat() {
  docker run --rm -i --name "kafka-go-kcat-$RANDOM" --network "$NET" "$KCAT_IMAGE" -b kafka-go-kgod:9092 "$@"
}
show() { printf '\n$ %s\n' "$*"; }

wait_ready() {
  for _ in $(seq 1 30); do
    if kcat -L >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "demo: kgod did not become ready" >&2
  exit 1
}

echo "kafka-go demo: kgod is a Kafka broker written in Go; kcat 1.7.1 (librdkafka 1.8.2) is the client."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build kgod >/dev/null 2>&1
wait_ready

show "kcat -b kafka-go-kgod:9092 -L | grep topic"
kcat -L | grep 'topic "'

show "printf 'order-1:coffee\\norder-2:tea\\norder-3:soda\\norder-4:water\\norder-5:juice\\n' | kcat -b kafka-go-kgod:9092 -P -t demo -K:"
printf 'order-1:coffee\norder-2:tea\norder-3:soda\norder-4:water\norder-5:juice\n' | kcat -P -t demo -K: 2>&1

show "kcat -b kafka-go-kgod:9092 -C -t demo -o beginning -e -f '%k=%s\\n' | sort"
kcat -C -t demo -o beginning -e -f '%k=%s\n' 2>/dev/null | sort

show "docker kill -s KILL kafka-go-kgod        # kill -9 the broker, no clean shutdown"
docker kill -s KILL kafka-go-kgod

show "docker compose up -d kgod                # restart on the same data volume"
docker compose up -d kgod >/dev/null 2>&1
wait_ready

show "kcat -b kafka-go-kgod:9092 -C -t demo -o beginning -e -f '%k=%s\\n' | sort"
kcat -C -t demo -o beginning -e -f '%k=%s\n' 2>/dev/null | sort

show "kgoctl verify-log /data                   # checks every CRC, offset and index entry on disk"
docker run --rm --name "kafka-go-verify-$RANDOM" -v kafka-go_kgod-data:/data \
  --entrypoint /usr/local/bin/kgoctl kafka-go/kgod:dev verify-log /data
