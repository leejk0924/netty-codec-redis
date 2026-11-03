---
name: verify-redis-server
description: Launch the netty-codec-redis example Redis server on a private port and drive it over real TCP with RESP requests (GET/SET/DEL/COMMAND/SHUTDOWN and malformed requests) to prove server behavior end to end. Use after changing RedisServer or RedisServerHandler, or whenever a claim about how the server answers a client needs proof beyond unit tests.
---

# Verify the Redis server

The app is a Netty-based Redis-compatible TCP server (`io.netty.example.server.RedisServer`). Its only user surface is the Redis protocol (RESP) on a TCP port; a user drives it with `redis-cli` or any Redis client. Data lives in an in-memory map that starts empty on every launch.

`redis-cli` and `nc` are not installed on this machine, so the harness ships its own one-shot RESP client. Every command below runs from the repo root in Git Bash:

```bash
V=.claude/skills/verify-redis-server/scripts/verify.sh
```

## Launch

```bash
VERIFY_PORT=16379 $V launch
```

- Builds with Maven (`compile` + `dependency:build-classpath`), then starts the server in the background with `-Dport=$VERIFY_PORT` (default `16379`, never the real Redis port `6379`).
- Ready means the line `now listening at /[...]:<port>...` appears in `server.log`; `launch` waits for it (30s) and exits 0 only then. First run downloads Maven plugins and can take ~20s.
- Toolchain: uses `mvn`/`java` from `PATH`; otherwise falls back to IntelliJ's bundled Maven and the newest JDK under `~/.jdks/`. Override with `MVN=...` and `JAVA_HOME=...`. Java 21+ is required.
- Each launch creates a run directory `.verify-artifacts/<run-id>/` (set `VERIFY_RUN_ID` to name it) and records it as the current run. Only one current run is tracked at a time; for a parallel instance use a different `VERIFY_PORT` from a separate checkout.

## Doctor

```bash
$V doctor
```

Read-only. Checks: the process this run started is alive, its ready line names our port, and a `COMMAND` probe answers `*0\r\n`. Prints `doctor: OK` / `doctor: FAIL` and the git revision the server was built from. Run it first whenever a reply looks wrong. If it fails, `stop` and `launch` again rather than driving a half-up instance. Never drive a server this run did not start.

## Drive

```bash
$V send <arg>...           # sends a RESP array of bulk strings, e.g. $V send SET A hello
$V send --raw '<resp>'     # sends raw RESP bytes; write \r\n literally, e.g. $V send --raw '*0\r\n'
```

- Each `send` opens one fresh connection, sends one request, prints **every byte** the server replied with (CR/LF shown as `\r\n`), then waits 300ms of silence so an unexpected second reply is captured too.
- Exit code `0`: reply received, connection still open. Exit `3`: the server closed the connection (printed as `[connection closed by server]`). Exit `1`: could not connect.
- Use `--raw` for things a normal client cannot send: nil bulk strings (`$-1\r\n`), empty arrays (`*0\r\n`), non-array requests (`+PING\r\n`).
- State persists across `send` calls within one launch (shared map), so set up preconditions with earlier sends.

The per-feature recipes live in [`features/`](features/README.md). Start there.

## Evidence

- `send` appends every request, reply, and exit code to `.verify-artifacts/<run-id>/transcript.txt`. That transcript is the proof: it shows the action, the exact bytes returned, and whether the connection survived.
- The run directory also keeps `server.log` (startup line and stack traces; requests are not logged), `build.log`, and `revision` (commit + dirty files the server was built from).
- Proof standards: drive the real TCP path, not handler unit tests or the map directly. For a mutation (SET/DEL), prove it with a second read (`GET`) on a new connection. A single correct-looking reply is not proof of "exactly one reply" — check the transcript line has nothing after the expected `\r\n` and exit is `0`.
- Cite the transcript lines and `server.log` excerpts in your report.

## Cleanup

```bash
$V stop
```

Sends `SHUTDOWN` (the app's own stop path), waits for the process this run started to exit, and falls back to killing that recorded PID only. Never kill `java` by name — the user may run other JVMs. `stop` removes the current-run marker and the classpath file but keeps the evidence directory; `$V status` lists past runs. `.verify-artifacts/` is git-ignored; delete old runs by hand when no longer needed.

## Helpers

- `scripts/verify.sh` — `launch | doctor | send | stop | status`, as above, plus `probe [port]`: sends `COMMAND` to a port without a current run and without recording (use it to confirm a stopped server no longer answers).
- `scripts/RespClient.java` — single-file RESP client (`java RespClient.java <host> <port> (<arg>... | --raw <resp>)`). `verify.sh` wraps it with toolchain discovery; prefer `send` / `probe` over calling it directly, since `java` may not be on `PATH`.
