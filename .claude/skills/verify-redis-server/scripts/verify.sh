#!/usr/bin/env bash
# Verification harness for the netty-codec-redis example server.
# Usage: verify.sh <launch|doctor|send|stop|probe|status> [args]
# Run from anywhere; paths resolve against the repo root.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
ARTIFACTS="$REPO_ROOT/.verify-artifacts"
CURRENT="$ARTIFACTS/current-run"
HOST=127.0.0.1

# ---- toolchain discovery (override with JAVA_HOME / MVN) ----
if [ -z "${JAVA_HOME:-}" ] && ! command -v java >/dev/null 2>&1; then
  JAVA_HOME="$(ls -d "$HOME"/.jdks/* 2>/dev/null | sort | tail -1)"
fi
[ -n "${JAVA_HOME:-}" ] && export JAVA_HOME && export PATH="$JAVA_HOME/bin:$PATH"
if [ -z "${MVN:-}" ]; then
  MVN="$(command -v mvn 2>/dev/null || ls -d /c/Program\ Files/JetBrains/IntelliJ\ IDEA*/plugins/maven-plugin/lib/maven3/bin/mvn 2>/dev/null | sort | tail -1)"
fi

# Windows java.exe needs Windows paths and ';' separators.
if command -v cygpath >/dev/null 2>&1; then
  winpath() { cygpath -w "$1"; }; SEP=';'
else
  winpath() { printf '%s' "$1"; }; SEP=':'
fi

run_dir() {
  [ -f "$CURRENT" ] || { echo "no current run; run 'verify.sh launch' first" >&2; exit 1; }
  echo "$ARTIFACTS/$(cat "$CURRENT")"
}
port_of() { cat "$(run_dir)/port"; }

send() {
  local dir; dir="$(run_dir)"
  local out rc
  out="$(java "$(winpath "$SCRIPT_DIR/RespClient.java")" "$HOST" "$(port_of)" "$@")"; rc=$?
  { printf '> %s\n' "$*"; printf '< %s\n' "$out"; printf '  exit=%s\n' "$rc"; } >> "$dir/transcript.txt"
  printf '%s\n' "$out"
  return $rc
}

pid_alive() {
  local pid; pid="$(cat "$1/server.winpid" 2>/dev/null)" || return 1
  tasklist //FI "PID eq $pid" 2>/dev/null | grep -q "$pid" 2>/dev/null \
    || kill -0 "$(cat "$1/server.pid" 2>/dev/null)" 2>/dev/null
}

case "${1:-}" in
  launch)
    port="${VERIFY_PORT:-16379}"
    id="${VERIFY_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
    dir="$ARTIFACTS/$id"
    mkdir -p "$dir"
    echo "$id" > "$CURRENT"
    echo "$port" > "$dir/port"
    echo "run $id -> $dir (port $port)"

    echo "building with $MVN ..."
    (cd "$REPO_ROOT" && "$MVN" -q -B compile dependency:build-classpath \
        -Dmdep.outputFile="$(winpath "$dir/classpath.txt")") > "$dir/build.log" 2>&1 \
      || { echo "build failed; see $dir/build.log" >&2; exit 1; }
    git -C "$REPO_ROOT" rev-parse --short HEAD > "$dir/revision" 2>/dev/null
    git -C "$REPO_ROOT" status --short >> "$dir/revision" 2>/dev/null

    cp="$(winpath "$REPO_ROOT/target/classes")$SEP$(cat "$dir/classpath.txt")"
    nohup java -Dport="$port" -cp "$cp" io.netty.example.server.RedisServer \
      > "$dir/server.log" 2>&1 < /dev/null &
    echo $! > "$dir/server.pid"
    cat "/proc/$!/winpid" > "$dir/server.winpid" 2>/dev/null || echo $! > "$dir/server.winpid"
    disown 2>/dev/null

    for _ in $(seq 1 60); do
      if grep -q "now listening at" "$dir/server.log" 2>/dev/null; then
        echo "ready: $(grep 'now listening at' "$dir/server.log")"
        exit 0
      fi
      pid_alive "$dir" || { echo "server exited; see $dir/server.log" >&2; cat "$dir/server.log" >&2; exit 1; }
      sleep 0.5
    done
    echo "timed out waiting for ready line; see $dir/server.log" >&2
    exit 1
    ;;

  doctor)
    dir="$(run_dir)"
    ok=0
    echo "run:      $(cat "$CURRENT")"
    echo "port:     $(port_of)"
    echo "revision: $(head -1 "$dir/revision" 2>/dev/null)"
    if pid_alive "$dir"; then echo "process:  alive (winpid $(cat "$dir/server.winpid"))"; else echo "process:  NOT running"; ok=1; fi
    grep -q "now listening at .*:$(port_of)\.\.\." "$dir/server.log" \
      && echo "listen:   $(grep -o 'now listening at [^ ]*' "$dir/server.log")" \
      || { echo "listen:   ready line for port $(port_of) missing"; ok=1; }
    reply="$(java "$(winpath "$SCRIPT_DIR/RespClient.java")" "$HOST" "$(port_of)" COMMAND)"
    if [ "$reply" = '*0\r\n' ]; then echo "probe:    COMMAND -> *0 (ok)"; else echo "probe:    COMMAND -> $reply (unexpected)"; ok=1; fi
    [ $ok -eq 0 ] && echo "doctor: OK" || echo "doctor: FAIL"
    exit $ok
    ;;

  send)
    shift
    send "$@"
    ;;

  stop)
    dir="$(run_dir)"
    if pid_alive "$dir"; then
      send SHUTDOWN >/dev/null
      for _ in $(seq 1 20); do pid_alive "$dir" || break; sleep 0.5; done
    fi
    if pid_alive "$dir"; then
      echo "SHUTDOWN did not stop the server; killing winpid $(cat "$dir/server.winpid")"
      taskkill //PID "$(cat "$dir/server.winpid")" //F >/dev/null 2>&1 || kill "$(cat "$dir/server.pid")" 2>/dev/null
    fi
    pid_alive "$dir" && { echo "server still running" >&2; exit 1; }
    rm -f "$CURRENT" "$dir/classpath.txt"
    echo "stopped. evidence kept in $dir"
    ls "$dir"
    ;;

  probe)
    # Send COMMAND to a port without a current run and without recording; for post-shutdown checks.
    java "$(winpath "$SCRIPT_DIR/RespClient.java")" "$HOST" "${2:-16379}" COMMAND
    ;;

  status)
    [ -f "$CURRENT" ] && echo "current run: $(cat "$CURRENT")" || echo "no current run"
    ls "$ARTIFACTS" 2>/dev/null
    ;;

  *)
    echo "usage: verify.sh <launch|doctor|send|stop|probe|status>" >&2
    exit 2
    ;;
esac
