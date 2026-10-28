# Shutdown

A client stops the server by sending `SHUTDOWN`. The server replies `+OK`, logs that it is shutting down, and the process exits.

## Sub-features

- `shutdown-ok` replies `+OK` and the server process exits.

## How to get to it (user POV)

- `redis-cli` → `SHUTDOWN`.

## Driving it with verify.sh

Preconditions:

- `$V doctor` reports `doctor: OK`.

- **Shut down.** Run `$V stop`. It sends `SHUTDOWN` and waits for the process to exit; it prints `stopped. evidence kept in ...` and does not print `SHUTDOWN did not stop the server`.
- **Confirm the reply.** The last transcript entry is `> SHUTDOWN` with reply `+OK\r\n`.
- **Confirm the exit.** `server.log` ends with `Received a SHUTDOWN command; shutting down..`, and `$V probe 16379` prints `[connect failed: ...]` with exit `1`.
- **Proof.** Transcript entry, the `server.log` tail, and the failed probe output.

## Gotchas

- This ends the run. Drive it last, or `launch` again afterwards.
- The `SHUTDOWN` transcript entry ends with `[connection closed by server]` and exit `3`. That is expected here, since the process exits; the reply line `+OK\r\n` before it is what matters.
- If `stop` had to fall back to a forced kill, the shutdown feature failed even though cleanup succeeded.
