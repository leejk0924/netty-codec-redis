package io.netty.example.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.*;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RedisServerHandler extends ChannelInboundHandlerAdapter {
    private final ConcurrentMap<String, String> map;
    private final CountDownLatch shutdownLatch;
    private final Executor heavyExecutor;
    // Requests decoded from the same read while a heavy command runs; held unreleased
    // so replies keep request order.
    private final ArrayDeque<Object> pending = new ArrayDeque<>();
    private boolean heavyInFlight;

    public RedisServerHandler(ConcurrentMap<String, String> map, CountDownLatch shutdownLatch,
                              Executor heavyExecutor) {
        this.map = map;
        this.shutdownLatch = shutdownLatch;
        this.heavyExecutor = heavyExecutor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (heavyInFlight) {
            pending.add(msg);
            return;
        }
        process(ctx, msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releasePending();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        releasePending();
    }

    private void releasePending() {
        Object msg;
        while ((msg = pending.poll()) != null) {
            ReferenceCountUtil.release(msg);
        }
    }

    private void process(ChannelHandlerContext ctx, Object msg) {
        try {
            if (!(msg instanceof ArrayRedisMessage)) {
                rejectMalformedRequest(ctx);
                return;
            }
            final ArrayRedisMessage req = ((ArrayRedisMessage) msg);
            List<RedisMessage> args = req.children();
            for (RedisMessage a : args) {
                if (!(a instanceof FullBulkStringRedisMessage)) {
                    rejectMalformedRequest(ctx);
                    return;
                }
            }
            // convert all arguments into Strings.
            // 예시 이므로 실제 사용시 핸들러로 변경하거나 byte[] 으로 변경 필요
            final List<String> strArgs =
                    args.stream()
                            .map(a -> {
                                final FullBulkStringRedisMessage bulkStr = (FullBulkStringRedisMessage) a;
                                if (bulkStr.isNull()) {
                                    return null;
                                } else {
                                    return bulkStr.content().toString(StandardCharsets.UTF_8);
                                }
                            })
                            .collect(Collectors.toList());

            if (strArgs.isEmpty() || strArgs.get(0) == null) {
                reject(ctx, "ERR A command name is required.");
                return;
            }
            final String command = strArgs.get(0).toUpperCase(Locale.ROOT);

            switch (command) {
                case "COMMAND":
                    ctx.writeAndFlush(ArrayRedisMessage.EMPTY_INSTANCE);
                    break;
                case "PING":
                    handlePing(ctx, strArgs);
                    break;
                case "GET": {
                    handleGet(ctx, strArgs);
                    break;
                }
                case "SET": {
                    handleSet(ctx, strArgs);
                    break;
                }
                case "DEL": {
                    handleDel(ctx, strArgs);
                    break;
                }
                case "KEYS":
                    handleKeys(ctx, strArgs);
                    break;
                case "SHUTDOWN":
                    ctx.writeAndFlush(new SimpleStringRedisMessage("OK"))
                            .addListener((ChannelFutureListener) f -> {
                                shutdownLatch.countDown();
                            });
                    break;
                default:
                    reject(ctx, "ERR Unsupported command");
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void handlePing(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() > 2) {
            reject(ctx, "ERR wrong number of arguments for 'ping' command");
            return;
        }
        if (strArgs.size() == 1) {
            ctx.writeAndFlush(new SimpleStringRedisMessage("PONG"));
            return;
        }
        final String message = strArgs.get(1);
        if (message == null) {
            ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        ctx.writeAndFlush(new FullBulkStringRedisMessage(
                Unpooled.copiedBuffer(message, StandardCharsets.UTF_8)));
    }

    private void handleKeys(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() != 2) {
            reject(ctx, "ERR wrong number of arguments for 'keys' command");
            return;
        }
        final String pattern = strArgs.get(1);
        if (pattern == null) {
            rejectNilKey(ctx);
            return;
        }
        runHeavy(ctx, () -> scanKeys(pattern));
    }

    private RedisMessage scanKeys(String pattern) {
        // Redis keysCommand short-circuits a lone '*', which also returns the empty key
        // that stringmatchlen("*", "") would reject.
        final boolean allKeys = "*".equals(pattern);
        final List<RedisMessage> keys = new ArrayList<>();
        for (String key : map.keySet()) {
            if (allKeys || GlobPattern.matches(pattern, key)) {
                keys.add(new FullBulkStringRedisMessage(
                        Unpooled.copiedBuffer(key, StandardCharsets.UTF_8)));
            }
        }
        return new ArrayRedisMessage(keys);
    }

    private void runHeavy(ChannelHandlerContext ctx, Supplier<RedisMessage> command) {
        try {
            heavyExecutor.execute(() -> {
                RedisMessage reply;
                try {
                    reply = command.get();
                } catch (RuntimeException e) {
                    reply = new ErrorRedisMessage("ERR " + e);
                }
                final RedisMessage result = reply;
                try {
                    // Handler state and writes stay confined to the channel's EventLoop.
                    ctx.executor().execute(() -> completeHeavy(ctx, result));
                } catch (RejectedExecutionException e) {
                    ReferenceCountUtil.release(result);
                }
            });
        } catch (RejectedExecutionException e) {
            reject(ctx, "ERR heavy command rejected");
            return;
        }
        // Safe after submit: completion is queued on this EventLoop, so it runs after this read.
        heavyInFlight = true;
        // Stop reading so a client pipelining behind a slow command cannot grow pending without bound.
        ctx.channel().config().setAutoRead(false);
    }

    private void completeHeavy(ChannelHandlerContext ctx, RedisMessage reply) {
        ctx.writeAndFlush(reply);
        heavyInFlight = false;
        Object next;
        while (!heavyInFlight && (next = pending.poll()) != null) {
            process(ctx, next);
        }
        if (!heavyInFlight) {
            ctx.channel().config().setAutoRead(true);
        }
    }

    private void handleDel(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 2) {
            reject(ctx, "ERR A DEL command requires at least one key argument.");
            return;
        }
        int removedEntries = 0;
        for (int i = 1; i < strArgs.size(); i++) {
            final String key = strArgs.get(i);
            if (key == null) {
                continue;
            }
            if (map.remove(key) != null) {
                removedEntries++;
            }
        }
        ctx.writeAndFlush(new IntegerRedisMessage(removedEntries));
        return;
    }

    private void handleSet(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 3) {
            reject(ctx, "ERR A SET command requires a key and a value argument.");
            return;
        }
        final String key = strArgs.get(1);
        if (key == null) {
            rejectNilKey(ctx);
            return;
        }

        final String value = strArgs.get(2);
        if (value == null) {
            reject(ctx, "ERR A nil value is not allowed");
            return;
        }

        final boolean shouldReplyOldValue =
                strArgs.size() > 3 && "GET".equalsIgnoreCase(strArgs.get(3));

        final String oldValue = map.put(key, value);
        final RedisMessage reply;
        if (shouldReplyOldValue) {
            if (oldValue != null) {
                reply = new FullBulkStringRedisMessage(
                        Unpooled.copiedBuffer(oldValue, StandardCharsets.UTF_8)
                );
            } else {
                reply = FullBulkStringRedisMessage.NULL_INSTANCE;
            }
        } else {
            reply = new SimpleStringRedisMessage("OK");
        }

        ctx.writeAndFlush(reply);
    }

    private void handleGet(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 2) {
            reject(ctx, "ERR A GET command requires a key argument.");
            return;
        }
        final String key = strArgs.get(1);
        if (key == null) {
            rejectNilKey(ctx);
            return;
        }
        final String value = map.get(key);
        final FullBulkStringRedisMessage reply;
        if (value != null) {
            reply = new FullBulkStringRedisMessage(
                    Unpooled.copiedBuffer(value, StandardCharsets.UTF_8)
            );
        } else {
            reply = FullBulkStringRedisMessage.NULL_INSTANCE;
        }
        ctx.writeAndFlush(reply);
    }

    private static void rejectNilKey(ChannelHandlerContext ctx) {
        reject(ctx, "ERR A nil key is not allowed.");
    }

    private void rejectMalformedRequest(ChannelHandlerContext ctx) {
        reject(ctx, "ERR Client request must be an array of bulk strings.");
    }

    private static void reject(ChannelHandlerContext ctx, String error) {
        ctx.writeAndFlush(new ErrorRedisMessage(error));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("Unexpected exception handling " + ctx.channel());
        cause.printStackTrace(System.err);
        ctx.close();
    }
}
