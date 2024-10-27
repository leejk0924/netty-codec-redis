package io.netty.example.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.example.handler.RedisServerHandler;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;

import java.util.concurrent.CountDownLatch;

public class RedisServer {
    private static final int PORT = Integer.parseInt(System.getProperty("port", "6379"));

    public static void main(String[] args) throws Exception{
        final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        // `RedisServerHandler`가 `SHUTDOWN` 커맨드를 받으면 Latch 가 0로 변경
        final CountDownLatch shutdownLatch = new CountDownLatch(1);

        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.channel(NioServerSocketChannel.class);
            b.group(bossGroup, workerGroup);
            b.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    final ChannelPipeline p = ch.pipeline();
                    p.addLast(new RedisDecoder());
                    p.addLast(new RedisEncoder());
                    p.addLast(new RedisServerHandler(shutdownLatch));
                }
            });
            final Channel ch = b.bind(PORT).sync().channel();
            System.err.println("An example Redis server, This server now listening at " + ch.localAddress() + "...");

            // latch 가 0이 될 떄까지 대기
            shutdownLatch.await();
        }finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
