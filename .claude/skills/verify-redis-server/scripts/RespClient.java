import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

/**
 * Minimal RESP client for verification. Opens one connection, sends one request,
 * prints every byte the server replies with (CR/LF escaped), and reports whether
 * the server closed the connection.
 *
 * Usage:
 *   java RespClient.java <host> <port> <arg>...          send args as a RESP array of bulk strings
 *   java RespClient.java <host> <port> --raw '<resp>'    send raw RESP; write \r\n literally
 *
 * Exit codes: 0 reply received and connection still open, 3 server closed the
 * connection, 1 connect failure.
 */
public class RespClient {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: RespClient <host> <port> (--raw <resp> | <arg>...)");
            System.exit(2);
        }
        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final byte[] request;
        if ("--raw".equals(args[2])) {
            request = args[3].replace("\\r", "\r").replace("\\n", "\n").getBytes(StandardCharsets.UTF_8);
        } else {
            final StringBuilder sb = new StringBuilder();
            sb.append('*').append(args.length - 2).append("\r\n");
            for (int i = 2; i < args.length; i++) {
                final byte[] b = args[i].getBytes(StandardCharsets.UTF_8);
                sb.append('$').append(b.length).append("\r\n").append(args[i]).append("\r\n");
            }
            request = sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        try (Socket socket = new Socket()) {
            try {
                socket.connect(new InetSocketAddress(host, port), 2000);
            } catch (Exception e) {
                System.out.println("[connect failed: " + e.getMessage() + "]");
                System.exit(1);
            }
            final OutputStream out = socket.getOutputStream();
            out.write(request);
            out.flush();

            // Wait up to 1s for the first byte, then keep reading until 300ms of silence
            // so a second (unexpected) reply is captured too.
            socket.setSoTimeout(Integer.getInteger("wait", 1000));
            final InputStream in = socket.getInputStream();
            final ByteArrayOutputStream reply = new ByteArrayOutputStream();
            final byte[] buf = new byte[4096];
            boolean closed = false;
            try {
                int n;
                while ((n = in.read(buf)) != -1) {
                    reply.write(buf, 0, n);
                    socket.setSoTimeout(300);
                }
                closed = true;
            } catch (SocketTimeoutException ignored) {
                // silence: the server is done replying and the connection is still open
            } catch (java.net.SocketException e) {
                closed = true;
            }

            System.out.println(reply.toString(StandardCharsets.UTF_8)
                    .replace("\r", "\\r").replace("\n", "\\n"));
            if (closed) {
                System.out.println("[connection closed by server]");
                System.exit(3);
            }
        }
    }
}
