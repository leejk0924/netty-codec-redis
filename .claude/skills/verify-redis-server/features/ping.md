# Ping

A client checks that the server is alive with `PING` and gets `+PONG`, or its own message back when it passes one. The compose healthcheck and benchmark tools rely on this.

## Sub-features

- `ping-pong` replies `+PONG` to `PING` with no argument.
- `ping-echo` replies with the argument as a bulk string to `PING <message>`.
- `ping-args-error` rejects `PING` with two or more arguments with a single error reply.

## How to get to it (user POV)

- `redis-cli -p <port> PING`, or `redis-cli PING hello`.

## Driving it with verify.sh

Preconditions:

- The server was launched by this run.

- **Pong.** Run `$V send PING`. Reply `+PONG\r\n`, exit `0`.
- **Echo.** Run `$V send PING hello`. Reply `$5\r\nhello\r\n`, exit `0`.
- **Too many arguments.** Run `$V send PING a b`. Exactly one `-ERR ...\r\n` reply, exit `0`.
- **Lowercase.** Run `$V send ping`. Reply `+PONG\r\n`, exit `0`.
- **Proof.** The transcript entries.

## Gotchas

- Redis replies `+PONG` as a simple string but echoes a message as a bulk string; the two reply types differ on purpose.
