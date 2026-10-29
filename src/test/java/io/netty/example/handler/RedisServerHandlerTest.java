package io.netty.example.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisServerHandlerTest {
    private static final String OK = "+OK\r\n";
    private static final String NIL = "$-1\r\n";

    private ConcurrentMap<String, String> map;
    private CountDownLatch shutdownLatch;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        map = new ConcurrentHashMap<>();
        shutdownLatch = new CountDownLatch(1);
        channel = new EmbeddedChannel(
                new RedisDecoder(),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator(),
                new RedisEncoder(),
                new RedisServerHandler(map, shutdownLatch));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void getExistingKey() {
        map.put("A", "hello");
        assertEquals(bulk("hello"), send("GET", "A"));
    }

    @Test
    void getMissingKeyReturnsNil() {
        assertEquals(NIL, send("GET", "A"));
    }

    @Test
    void setStoresValue() {
        assertEquals(OK, send("SET", "A", "B"));
        assertEquals("B", map.get("A"));
    }

    @Test
    void setThenGet() {
        send("SET", "A", "B");
        assertEquals(bulk("B"), send("GET", "A"));
    }

    @Test
    void setWithGetOptionReturnsOldValue() {
        map.put("A", "old");
        assertEquals(bulk("old"), send("SET", "A", "new", "GET"));
        assertEquals("new", map.get("A"));
    }

    @Test
    void setWithGetOptionReturnsNilWhenNoOldValue() {
        assertEquals(NIL, send("SET", "A", "new", "GET"));
        assertEquals("new", map.get("A"));
    }

    @Test
    void delSingleKey() {
        map.put("A", "1");
        assertEquals(":1\r\n", send("DEL", "A"));
        assertFalse(map.containsKey("A"));
    }

    @Test
    void delMultipleKeysReturnsRemovedCount() {
        map.put("A", "1");
        map.put("B", "2");
        assertEquals(":2\r\n", send("DEL", "A", "B", "C"));
        assertTrue(map.isEmpty());
    }

    @Test
    void commandReturnsEmptyArray() {
        assertEquals("*0\r\n", send("COMMAND"));
    }

    @Test
    void shutdownRepliesOkAndCountsDownLatch() {
        assertEquals(OK, send("SHUTDOWN"));
        assertEquals(0, shutdownLatch.getCount());
    }

    @Test
    void pingRepliesPong() {
        assertEquals("+PONG\r\n", send("PING"));
    }

    @Test
    void pingWithMessageEchoesMessage() {
        assertEquals(bulk("hello"), send("PING", "hello"));
    }

    @Test
    void lowercasePingIsSupported() {
        assertEquals("+PONG\r\n", send("ping"));
    }

    @Test
    void pingWithTooManyArgumentsReturnsSingleError() {
        assertSingleError(send("PING", "a", "b"));
    }

    @Test
    void unsupportedCommandReturnsError() {
        assertSingleError(send("FOO"));
    }

    @Test
    void nonArrayRequestReturnsError() {
        assertSingleError(sendRaw("+PING\r\n"));
    }

    @Test
    void emptyArrayRequestReturnsError() {
        assertSingleError(sendRaw("*0\r\n"));
    }

    @Test
    void nilCommandNameReturnsSingleError() {
        assertSingleError(sendRaw("*1\r\n$-1\r\n"));
    }

    @Test
    void getWithoutKeyReturnsSingleError() {
        assertSingleError(send("GET"));
    }

    @Test
    void getWithNilKeyReturnsSingleError() {
        assertSingleError(sendRaw("*2\r\n$3\r\nGET\r\n$-1\r\n"));
    }

    @Test
    void setWithoutValueReturnsSingleError() {
        assertSingleError(send("SET", "A"));
        assertTrue(map.isEmpty());
    }

    @Test
    void setWithNilKeyReturnsSingleError() {
        assertSingleError(sendRaw("*3\r\n$3\r\nSET\r\n$-1\r\n$1\r\nB\r\n"));
        assertTrue(map.isEmpty());
    }

    @Test
    void setWithNilValueReturnsSingleError() {
        assertSingleError(sendRaw("*3\r\n$3\r\nSET\r\n$1\r\nA\r\n$-1\r\n"));
        assertTrue(map.isEmpty());
    }

    @Test
    void delWithoutKeyReturnsSingleError() {
        assertSingleError(send("DEL"));
    }

    @Test
    void lowercaseCommandIsSupported() {
        assertEquals(OK, send("set", "A", "B"));
        assertEquals(bulk("B"), send("get", "A"));
        assertEquals(":1\r\n", send("del", "A"));
        assertEquals("*0\r\n", send("command"));
    }

    @Test
    void mixedCaseCommandIsSupported() {
        assertEquals(OK, send("SeT", "A", "B"));
        assertEquals(bulk("B"), send("GeT", "A"));
        assertEquals(":1\r\n", send("dEl", "A"));
        assertEquals("*0\r\n", send("CoMmAnD"));
    }

    @Test
    void lowercaseShutdownIsSupported() {
        assertEquals(OK, send("shutdown"));
        assertEquals(0, shutdownLatch.getCount());
    }

    @Test
    void lowercaseSetGetOptionIsSupported() {
        map.put("A", "old");
        assertEquals(bulk("old"), send("SET", "A", "new", "get"));
        assertEquals("new", map.get("A"));
    }

    @Test
    void mixedCaseSetGetOptionIsSupported() {
        map.put("A", "old");
        assertEquals(bulk("old"), send("set", "A", "new", "GeT"));
        assertEquals("new", map.get("A"));
    }

    @Test
    void keysReturnsMatchingKeys() {
        map.put("user:1", "a");
        map.put("user:2", "b");
        map.put("order:1", "c");
        assertEquals(Set.of("user:1", "user:2"), bulkArray(send("KEYS", "user:*")));
    }

    @Test
    void keysStarReturnsAllKeysIncludingEmptyKey() {
        map.put("", "a");
        map.put("A", "b");
        assertEquals(Set.of("", "A"), bulkArray(send("KEYS", "*")));
    }

    @Test
    void keysWithNoMatchReturnsEmptyArray() {
        map.put("A", "1");
        assertEquals("*0\r\n", send("KEYS", "B*"));
    }

    @Test
    void lowercaseKeysIsSupported() {
        map.put("A", "1");
        assertEquals("*1\r\n" + bulk("A"), send("keys", "?"));
    }

    @Test
    void keysWithoutPatternReturnsSingleError() {
        assertSingleError(send("KEYS"));
    }

    @Test
    void keysWithTooManyArgumentsReturnsSingleError() {
        assertSingleError(send("KEYS", "*", "extra"));
    }

    @Test
    void keysWithNilPatternReturnsSingleError() {
        assertSingleError(sendRaw("*2\r\n$4\r\nKEYS\r\n$-1\r\n"));
    }

    private String send(String... args) {
        final StringBuilder sb = new StringBuilder();
        sb.append('*').append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append(bulk(arg));
        }
        return sendRaw(sb.toString());
    }

    private String sendRaw(String resp) {
        channel.writeInbound(Unpooled.copiedBuffer(resp, StandardCharsets.UTF_8));
        final StringBuilder sb = new StringBuilder();
        ByteBuf buf;
        while ((buf = channel.readOutbound()) != null) {
            sb.append(buf.toString(StandardCharsets.UTF_8));
            buf.release();
        }
        return sb.toString();
    }

    private static String bulk(String value) {
        return "$" + value.getBytes(StandardCharsets.UTF_8).length + "\r\n" + value + "\r\n";
    }

    private static Set<String> bulkArray(String response) {
        final String[] lines = response.split("\r\n", -1);
        assertTrue(lines[0].startsWith("*"), "array reply expected but was: " + response);
        final int count = Integer.parseInt(lines[0].substring(1));
        final Set<String> values = new HashSet<>();
        for (int i = 0; i < count; i++) {
            values.add(lines[2 + 2 * i]);
        }
        assertEquals(2 + 2 * count, lines.length,"single reply expected but was: " + response);
        return values;
    }

    private void assertSingleError(String response) {
        assertTrue(response.startsWith("-ERR "), "error reply expected but was: " + response);
        assertEquals(response.length() - 2, response.indexOf("\r\n"),
                "single reply expected but was: " + response);
        assertTrue(channel.isOpen(), "channel should remain open");
        assertNull(channel.readInbound(), "no pending inbound message");
    }
}
