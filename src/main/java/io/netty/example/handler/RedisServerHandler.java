package io.netty.example.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.*;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class RedisServerHandler extends ChannelInboundHandlerAdapter {
    private final ConcurrentMap<String, String> map;
    private final CountDownLatch shutdownLatch;

    public RedisServerHandler(ConcurrentMap<String, String> map, CountDownLatch shutdownLatch) {
        this.map = map;
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
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
            final String command = strArgs.get(0);

            System.err.println(ctx.channel() + " RCVD: " + strArgs);

            switch (command) {
                case "COMMAND":
                    ctx.writeAndFlush(ArrayRedisMessage.EMPTY_INSTANCE);
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

    private void handleDel(ChannelHandlerContext ctx, List<String> strArgs) {
        if (strArgs.size() < 2) {
            reject(ctx, "ERR A DELETE command requires at one key argument.");
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
            reject(ctx, "ERR A GET command requires a key argument.");
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
                strArgs.size() > 3 && "GET".equals(strArgs.get(3));

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
        }
        final String key = strArgs.get(1);
        if (key == null) {
            rejectNilKey(ctx);
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
        reject(ctx, "ERR Client request bust be an array of bulk strings.");
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
