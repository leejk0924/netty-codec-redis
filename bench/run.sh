#!/usr/bin/env bash
# Latency baseline at a fixed request rate: A (Redis), B (Netty, 1 worker), C (Netty, worker = cores)
# for each server core count. Throughput is not compared here: on a single 8-CPU Docker Desktop host
# the client saturates before a multi-core Netty server does.
# Usage: bench/run.sh [cores ...]   (default: 1 2 4)
set -euo pipefail
cd "$(dirname "$0")/.."
# Git Bash rewrites container paths like /results/... into Windows paths.
export MSYS_NO_PATHCONV=1

CORES=("${@:-1 2 4}")
read -r -a CORES <<<"${CORES[*]}"
WARMUP=${WARMUP:-30}
DURATION=${DURATION:-60}
KEYS=${KEYS:-100000}
RATIO=${RATIO:-1:10}
THREADS=${THREADS:-4}
CLIENTS=${CLIENTS:-25}
DATA_SIZE=${DATA_SIZE:-32}
RATE=${RATE:-20000}
# Without an explicit GC the JVM picks Serial GC below 2 CPUs, which would add a second variable.
JVM_BASE="-Xms512m -Xmx512m -XX:+UseG1GC"

RUN_ID=$(date +%Y%m%d-%H%M%S)
OUT=bench/results/$RUN_ID
mkdir -p "$OUT"

memtier() {
  docker compose run --rm -T memtier -s "$1" -p 6379 --protocol=redis \
    --key-maximum="$KEYS" --data-size="$DATA_SIZE" "${@:2}"
}

load_args=(-t "$THREADS" -c "$CLIENTS" --ratio="$RATIO" --key-pattern=R:R
           --rate-limiting=$((RATE / (THREADS * CLIENTS)))
           --distinct-client-seed --hide-histogram --print-percentiles=50,99,99.9)

{
  echo "run=$RUN_ID warmup=${WARMUP}s duration=${DURATION}s keys=$KEYS ratio=$RATIO rate=${RATE}/s"
  echo "threads=$THREADS clients/thread=$CLIENTS data-size=$DATA_SIZE jvm=\"$JVM_BASE\""
  docker version --format 'docker server {{.Server.Version}}'
  docker info --format 'host cpus {{.NCPU}}, mem {{.MemTotal}}'
} >"$OUT/env.txt"

for n in "${CORES[@]}"; do
  cpuset="0-$((n - 1))"
  configs=("A redis" "B netty-redis 1")
  [ "$n" -gt 1 ] && configs+=("C netty-redis $n")
  for cfg in "${configs[@]}"; do
    read -r name host threads <<<"$cfg"
    java_opts="$JVM_BASE"
    [ -n "${threads:-}" ] && java_opts="$java_opts -Dio.netty.eventLoopThreads=$threads"
    label="$name-${n}c"
    echo "== $label (cpuset $cpuset)"

    SERVER_CPUSET=$cpuset NETTY_JAVA_OPTS=$java_opts \
      docker compose up -d --build --wait --force-recreate redis netty-redis >/dev/null 2>&1

    memtier "$host" -t 1 -c 10 --ratio=1:0 --key-pattern=P:P --requests=allkeys --hide-histogram >/dev/null 2>&1
    memtier "$host" "${load_args[@]}" --test-time="$WARMUP" >/dev/null 2>&1
    memtier "$host" "${load_args[@]}" --test-time="$DURATION" \
      --json-out-file="/results/$RUN_ID/$label.json" >"$OUT/$label.txt" 2>&1

    # Average latency is dropped: clock steps in the Docker Desktop VM record a few samples as ~2^32 us.
    awk -v l="$label" '/^(Sets|Gets|Totals) / { printf "%-6s %-6s %10s %8s %8s %8s\n", l, $1, $2, $6, $7, $8 }' \
      "$OUT/$label.txt" | tee -a "$OUT/summary.txt"
  done
done

echo "columns: config type ops/sec p50 p99 p99.9 (ms)"
echo "results: $OUT"
