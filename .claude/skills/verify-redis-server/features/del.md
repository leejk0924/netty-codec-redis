# Delete keys

A client removes one or more keys with `DEL` and receives the number of keys that actually existed and were removed.

## Sub-features

- `del-single` removes one existing key and replies `:1`.
- `del-multi` removes several keys, counting only those that existed.
- `del-absent` replies `:0` when none of the keys exist.
- `del-args-error` rejects `DEL` with no key with a single error reply.

## How to get to it (user POV)

- `redis-cli` → `DEL A`, `DEL A B C`.
- Any Redis client library's `del` call.

## Driving it with verify.sh

Preconditions:

- `$V doctor` reports `doctor: OK`.
- Seed with `$V send SET A 1` and `$V send SET B 2` (each replies `+OK\r\n`).

- **Delete several.** Run `$V send DEL A B C`. Reply `:2\r\n`, exit `0`.
- **Confirm removal.** Run `$V send GET A` and `$V send GET B`. Each replies `$-1\r\n`.
- **Delete absent.** Run `$V send DEL A`. Reply `:0\r\n`.
- **Delete single.** Run `$V send SET C 3`, then `$V send DEL C`. Reply `:1\r\n`; `$V send GET C` replies `$-1\r\n`.
- **No key.** Run `$V send DEL`. Exactly one `-ERR ...\r\n` reply, nothing after it, exit `0`.
- **Proof.** Transcript entries for the deletes and the follow-up `GET`s.

## Gotchas

- `DEL` with no key used to reply `-ERR ...\r\n:0\r\n` — two replies to one request (issue #1). A real client would then read the stray `:0` as the answer to its next command. Treat any output after the first `\r\n` as a failure.
- The count covers only keys that existed; a key listed twice is counted once.
