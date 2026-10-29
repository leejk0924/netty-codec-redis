#!/usr/bin/env bash
# Mixed load: light GET/SET with a concurrent stream of heavy KEYS, A (Redis), B (Netty, 1 worker),
# C (Netty, worker = cores) for each server core count. Each config also runs the light load alone
# as a control, so the light-request latency added by KEYS is visible.
# Usage: bench/mixed.sh [cores ...]   (default: 1 2 4)
set -euo pipefail
cd "$(dirname "$0")/.."
# Git Bash rewrites container paths like /results/... into Windows paths.
export MSYS_NO_PATHCONV=1

CORES=("${@:-1 2 4}")
read -r -a CORES <<<"${CORES[*]}"
WARMUP=${WARMUP:-20}
DURATION=${DURATION:-60}
# KEYS over this many keys takes ~10ms inside Redis 7.4 (median of SLOWLOG on this host).
KEYS=${KEYS:-250000}
DATA_SIZE=${DATA_SIZE:-32}
LIGHT_RATE=${LIGHT_RATE:-950}
HEAVY_RATE=${HEAVY_RATE:-50}
# Matches no key: the cost is the server-side scan, not a multi-MB reply parsed by the client.
HEAVY_COMMAND=${HEAVY_COMMAND:-KEYS nomatch*}
THREADS=${THREADS:-4}
CLIENTS=${CLIENTS:-5}
JVM_BASE="-Xms512m -Xmx512m -XX:+UseG1GC"

RUN_ID=mixed-$(date +%Y%m%d-%H%M%S)
OUT=bench/results/$RUN_ID
mkdir -p "$OUT"

memtier() {
  docker compose run --rm --no-deps -T memtier -s "$1" -p 6379 --protocol=redis \
    --key-maximum="$KEYS" --data-size="$DATA_SIZE" --hide-histogram --print-percentiles=50,99,99.9 "${@:2}"
}

light_args=(-t "$THREADS" -c "$CLIENTS" --ratio=1:10 --key-pattern=R:R --distinct-client-seed
            --rate-limiting=$((LIGHT_RATE / (THREADS * CLIENTS))))
# One connection: several heavy connections fire on the same rate-limit tick and queue behind each other.
heavy_args=(-t 1 -c 1 --rate-limiting="$HEAVY_RATE" --command="$HEAVY_COMMAND")

{
  echo "run=$RUN_ID warmup=${WARMUP}s duration=${DURATION}s keys=$KEYS"
  echo "light=${LIGHT_RATE}/s (threads=$THREADS clients/thread=$CLIENTS ratio=1:10) heavy=${HEAVY_RATE}/s \"$HEAVY_COMMAND\" (1 connection)"
  echo "data-size=$DATA_SIZE jvm=\"$JVM_BASE\""
  docker version --format 'docker server {{.Server.Version}}'
  docker info --format 'host cpus {{.NCPU}}, mem {{.MemTotal}}'
} >"$OUT/env.txt"

summarize() {
  awk -v l="$1" '/^(Sets|Gets|Keyss|Totals) / { printf "%-14s %-6s %10s %8s %8s %8s\n", l, $1, $2, $6, $7, $8 }' "$2"
}

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

    memtier "$host" -t 1 -c 10 --ratio=1:0 --key-pattern=P:P --requests=allkeys >/dev/null 2>&1

    memtier "$host" "${light_args[@]}" --test-time="$WARMUP" >/dev/null 2>&1
    memtier "$host" "${light_args[@]}" --test-time="$DURATION" >"$OUT/$label-control.txt" 2>&1
    summarize "$label-control" "$OUT/$label-control.txt" | grep Totals | tee -a "$OUT/summary.txt"

    memtier "$host" "${heavy_args[@]}" --test-time="$WARMUP" >/dev/null 2>&1 &
    memtier "$host" "${light_args[@]}" --test-time="$WARMUP" >/dev/null 2>&1
    wait
    memtier "$host" "${heavy_args[@]}" --test-time="$DURATION" >"$OUT/$label-heavy.txt" 2>&1 &
    memtier "$host" "${light_args[@]}" --test-time="$DURATION" >"$OUT/$label-light.txt" 2>&1
    wait
    summarize "$label-light" "$OUT/$label-light.txt" | grep Totals | tee -a "$OUT/summary.txt"
    summarize "$label-heavy" "$OUT/$label-heavy.txt" | grep Keyss | tee -a "$OUT/summary.txt"
  done
done

echo "columns: config type ops/sec p50 p99 p99.9 (ms)"
echo "results: $OUT"
