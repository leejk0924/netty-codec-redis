# List keys by pattern

A client lists every key whose name matches a glob-style pattern with `KEYS pattern` and receives the matching keys as an array of bulk strings, in no particular order.

## Sub-features

- `keys-match` returns the keys matching `*`, `?`, and `[...]` patterns.
- `keys-all` returns every key for the pattern `*`.
- `keys-none` replies with an empty array when nothing matches.
- `keys-escape` matches a literal `*`, `?`, or `[` escaped with `\`.
- `keys-args-error` rejects `KEYS` with no pattern or more than one argument with a single error reply.

## How to get to it (user POV)

- `redis-cli` → `KEYS user:*`, `KEYS *`, `KEYS user:?`.
- Any Redis client library's `keys` call.

## Driving it with verify.sh

Preconditions:

- `$V doctor` reports `doctor: OK`.
- Seed with `$V send SET user:1 a`, `$V send SET user:2 b`, `$V send SET order:1 c` (each replies `+OK\r\n`).

- **Prefix match.** Run `$V send KEYS 'user:*'`. Reply is `*2\r\n` followed by `$6\r\nuser:1\r\n` and `$6\r\nuser:2\r\n` in either order, exit `0`.
- **All keys.** Run `$V send KEYS '*'`. Reply is `*3\r\n` followed by the three seeded keys in any order.
- **Single character.** Run `$V send KEYS 'order:?'`. Reply `*1\r\n$7\r\norder:1\r\n`.
- **Character class.** Run `$V send KEYS 'user:[^1]'`. Reply `*1\r\n$6\r\nuser:2\r\n`.
- **No match.** Run `$V send KEYS 'nope*'`. Reply `*0\r\n`.
- **Escaped star.** Run `$V send SET 'a*b' x`, then `$V send KEYS 'a\*b'`. Reply `*1\r\n$3\r\na*b\r\n`.
- **No pattern.** Run `$V send KEYS`. Exactly one `-ERR ...\r\n` reply, exit `0`.
- **Too many arguments.** Run `$V send KEYS a b`. Exactly one `-ERR ...\r\n` reply, exit `0`.
- **Proof.** The transcript shows each request with its reply and exit code.

## Gotchas

- Quote patterns in the shell. An unquoted `*` or `?` is expanded by the shell before `verify.sh` sees it.
- Key order is unspecified, as in Redis. Compare the set of keys, not the byte sequence, when more than one key matches.
- Matching works on Java `char`s, not bytes. `?` matches one UTF-16 unit of a non-ASCII key, where Redis matches one byte.
- The scan is weakly consistent: a key written by another client during `KEYS` may or may not appear.
