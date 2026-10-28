# Connection handshake

`redis-cli` sends `COMMAND` (or `COMMAND DOCS`) when it connects to fetch command metadata. The server answers with an empty array so the client connects without errors.

## Sub-features

- `command-empty` replies to `COMMAND` with an empty array.

## How to get to it (user POV)

- Start `redis-cli -p <port>`; it sends the request automatically on connect.

## Driving it with verify.sh

Preconditions:

- The server was launched by this run. (`$V doctor` itself uses this request as its probe.)

- **Handshake.** Run `$V send COMMAND`. Reply `*0\r\n`, exit `0`.
- **Proof.** The transcript entry, and a `RCVD: [COMMAND]` line in `server.log`.

## Gotchas

- Newer `redis-cli` versions send `COMMAND DOCS`; the server ignores extra arguments and still replies `*0\r\n`.
