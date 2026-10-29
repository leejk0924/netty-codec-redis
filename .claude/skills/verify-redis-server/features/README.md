# Redis server verification map

This directory is the maintained source for verifying what a Redis client sees from the netty-codec-redis server. Read this index, then use the matching feature file as the recipe.

## Baseline preconditions

- Launch with `$V launch` (`V=.claude/skills/verify-redis-server/scripts/verify.sh`) on a disposable port, default `16379`.
- `$V doctor` reports `doctor: OK` for this run.
- The store is empty at launch. Every recipe seeds its own keys.
- Never drive a server this run did not start.

## Driving conventions

- Run every request through `$V send` so it lands in the transcript.
- One `send` = one connection = one request. Use a new `send` for each follow-up read.
- Commands and expected replies are literal. Replies are shown with CR/LF escaped as `\r\n`.
- Use `$V send --raw '<resp>'` only when the request cannot be expressed as plain string arguments.

## Proof and skip reporting

- Proof is the transcript entry: request, exact reply bytes, exit code.
- A mutation is proven only by a second read on a new connection.
- "Exactly one reply" means the reply ends at the first `\r\n` of the expected message and the exit code is `0`.
- Exit `3` (`[connection closed by server]`) on a request the server should reject is a failure, even if an error line was printed first.
- Report an unreachable path with the attempted command and the unmet precondition. Do not report a path as verified through a different command.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the client-visible behavior, then exactly four H2 sections in order: `Sub-features`, `How to get to it (user POV)`, `Driving it with verify.sh`, `Gotchas`.

## Features

- [String get/set](./get-set.md) covers `SET`, `GET`, and `SET ... GET` replies, nil replies, and argument errors.
- [Delete keys](./del.md) covers single and multi-key `DEL`, the deleted-count reply, and the missing-argument error.
- [List keys by pattern](./keys.md) covers `KEYS` with `*`, `?`, `[...]`, and escaped patterns, the empty-array reply, and argument errors.
- [Ping](./ping.md) covers `PING`, `PING <message>`, and the too-many-arguments error.
- [Connection handshake](./command.md) covers the `COMMAND` request `redis-cli` sends on connect.
- [Shutdown](./shutdown.md) covers `SHUTDOWN` stopping the server process.
- [Malformed requests](./malformed-requests.md) covers non-array, empty-array, nil-argument, unknown, and lowercase requests.
