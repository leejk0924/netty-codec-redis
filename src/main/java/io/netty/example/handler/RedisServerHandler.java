package io.netty.example.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.CountDownLatch;

public class RedisServerHandler extends ChannelInboundHandlerAdapter {
    private final CountDownLatch shutdownLatch;
    public RedisServerHandler(CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.err.println(msg);
    }

}
