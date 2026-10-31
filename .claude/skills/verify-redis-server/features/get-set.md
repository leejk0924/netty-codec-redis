# String get/set

A client stores a string value under a key with `SET`, reads it back with `GET`, and can swap a value while receiving the previous one with `SET key value GET`.

## Sub-features

- `set-ok` stores a value and replies `+OK`.
- `get-hit` returns the stored value as a bulk string.
- `get-miss` returns nil for an absent key.
- `set-get-option` replaces the value and returns the old one, or nil when there was none.
- `set-args-error` rejects `SET` with fewer than two arguments.
- `get-args-error` rejects `GET` with no key.

## How to get to it (user POV)

- `redis-cli` → `SET A hello`, `GET A`, `SET A new GET`.
- Any Redis client library's `set` / `get` calls.

## Driving it with verify.sh

Preconditions:

- `$V doctor` reports `doctor: OK`.
- Key `A` and key `ZZ` are absent (fresh launch).

- **Set.** Run `$V send SET A hello`. Reply `+OK\r\n`, exit `0`.
- **Get hit.** Run `$V send GET A`. Reply `$5\r\nhello\r\n`, exit `0`.
- **Get miss.** Run `$V send GET ZZ`. Reply `$-1\r\n`, exit `0`.
- **Swap with GET option.** Run `$V send SET A new GET`. Reply `$5\r\nhello\r\n`. Then `$V send GET A` replies `$3\r\nnew\r\n`.
- **GET option on absent key.** Run `$V send SET B v GET`. Reply `$-1\r\n`. Then `$V send GET B` replies `$1\r\nv\r\n`.
- **SET missing value.** Run `$V send SET A`. Exactly one `-ERR ...\r\n` reply, exit `0`. Then `$V send GET A` still replies `$3\r\nnew\r\n`.
- **GET missing key.** Run `$V send GET`. Exactly one `-ERR ...\r\n` reply, exit `0`.
- **Proof.** The transcript shows each request with its reply and exit code.

## Gotchas

- The `GET` option of `SET` is case-insensitive; the lowercase/mixed-case forms are covered in [Malformed requests](./malformed-requests.md).
- The `-ERR` text for argument errors is not part of the contract; assert the `-ERR ` prefix and that it is the only reply.
- `GET` with no key used to close the connection after the error (issue #1). Exit `3` here is a regression, not "error returned".
