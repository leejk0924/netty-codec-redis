# Malformed requests

A client that sends a request the server cannot serve gets exactly one `-ERR` reply and keeps its connection. Commands are case-insensitive, as in Redis.

## Sub-features

- `non-array` rejects a request that is not an array of bulk strings.
- `empty-array` rejects an empty array request.
- `nil-key` rejects a nil key in `GET` / `SET`.
- `nil-value` rejects a nil value in `SET`.
- `unknown-command` rejects an unsupported command.
- `lowercase-command` accepts commands and the `SET ... GET` option in any case.

## How to get to it (user POV)

- `redis-cli` → `PING` (unsupported), `set a b`, `get a`, `SET a b get`.
- Raw protocol from a buggy or hand-written client (nil arguments, empty arrays, inline strings).

## Driving it with verify.sh

Preconditions:

- `$V doctor` reports `doctor: OK`.

- **Non-array.** Run `$V send --raw '+PING\r\n'`. Exactly one `-ERR ...\r\n`, exit `0`.
- **Empty array.** Run `$V send --raw '*0\r\n'`. Exactly one `-ERR ...\r\n`, exit `0`.
- **Nil key in GET.** Run `$V send --raw '*2\r\n$3\r\nGET\r\n$-1\r\n'`. Exactly one `-ERR ...\r\n`, exit `0`.
- **Nil key in SET.** Run `$V send --raw '*3\r\n$3\r\nSET\r\n$-1\r\n$1\r\nB\r\n'`. Exactly one `-ERR ...\r\n`, exit `0`.
- **Nil value in SET.** Run `$V send --raw '*3\r\n$3\r\nSET\r\n$1\r\nA\r\n$-1\r\n'`. Exactly one `-ERR ...\r\n`, exit `0`; `$V send GET A` replies `$-1\r\n`.
- **Unknown command.** Run `$V send PING`. Reply `-ERR Unsupported command\r\n`, exit `0`.
- **Lowercase command.** Run `$V send set a b` then `$V send get a`. Replies `+OK\r\n` and `$1\r\nb\r\n`.
- **Lowercase option.** Run `$V send SET a c get`. Reply `$1\r\nb\r\n`; then `$V send GET a` replies `$1\r\nc\r\n`.
- **Proof.** Transcript entries; for any exit `3`, include the stack trace from `server.log`.

## Gotchas

- Before issue #1 was fixed, the empty array and nil-key `GET` closed the connection (exit `3`, stack trace in `server.log`), and lowercase commands replied `-ERR Unsupported command`. Any of these coming back is a regression; do not soften the expectations.
- A nil command name (`$V send --raw '*1\r\n$-1\r\n'`) gets the same single `-ERR` as an empty array.
- Single-quote `--raw` arguments so Bash keeps `$3` and `\r\n` literal.
- Real Redis silently ignores an empty array; this server's contract (issue #1 tests) is a single `-ERR` reply instead.
