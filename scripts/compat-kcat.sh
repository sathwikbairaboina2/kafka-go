#!/usr/bin/env bash
# Round trip and consumer group check with kcat 1.7.1 (librdkafka 1.8.2) against kgod in compose.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
trap 'docker compose down -v >/dev/null 2>&1 || true' EXIT

kcat() {
  docker run --rm -i --name "kafka-go-kcat-$RANDOM" --network kafka-go_default edenhill/kcat:1.7.1 \
    -b kafka-go-kgod:9092 "$@"
}

docker compose up -d --build kgod

ready=0
for _ in $(seq 1 30); do
  if kcat -L >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
if [ "$ready" != 1 ]; then echo "compat-kcat: FAIL kgod did not become ready in 30 s"; exit 1; fi

# 1. produce 100 keyed messages
for i in $(seq 1 100); do echo "k$i:v$i"; done | kcat -P -t compat-kcat -K:

# 2. consume everything back and compare with what was produced
expected=$(for i in $(seq 1 100); do echo "k$i:v$i"; done | sort)
actual=$(kcat -C -t compat-kcat -o beginning -e -f '%k:%s\n' | sort)
if [ "$expected" != "$actual" ]; then
  echo "compat-kcat: FAIL consumed messages differ from produced messages"
  diff <(echo "$expected") <(echo "$actual") || true
  exit 1
fi
echo "compat-kcat: round trip ok (100 messages)"

# 3. consumer group: first run reads all 100 and commits, second run reads none
# (options must come before -G: kcat treats everything after the group name as topics; -o beginning would
# override committed offsets in group mode, so the reset policy is set with auto.offset.reset instead)
first=$(kcat -X auto.offset.reset=earliest -e -f '%k:%s\n' -G kcatgrp compat-kcat | wc -l | tr -d ' ')
second=$(kcat -X auto.offset.reset=earliest -e -f '%k:%s\n' -G kcatgrp compat-kcat | wc -l | tr -d ' ')
echo "compat-kcat: group first run=$first second run=$second"
if [ "$first" != 100 ] || [ "$second" != 0 ]; then
  echo "compat-kcat: FAIL expected 100 then 0 messages from the consumer group"
  exit 1
fi

echo "compat-kcat: PASS"
