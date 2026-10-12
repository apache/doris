// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.net.Socket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLException
import java.sql.Statement

// What a MySQL client receives for a request that carries several statements.
//
// With CLIENT_MULTI_STATEMENTS negotiated, every statement's response is delivered, the
// intermediate ones flagged SERVER_MORE_RESULTS_EXISTS. Without it Doris still runs every
// statement but delivers only the response of the last one: what an earlier statement wrote is
// dropped when the next statement starts (MysqlProtocolAdapter.beforeStatement), whatever kind of
// statement follows. A request that ends in a SET, an INSERT or a BEGIN therefore answers with that
// statement's OK alone; it used to answer with the packets of the preceding query followed by the
// OK, a stream no client can parse.
//
// Connector/J exercises the second case only: it asks for CLIENT_MULTI_STATEMENTS only when the
// server advertises it, which Doris does not, and it always asks for CLIENT_DEPRECATE_EOF. The
// other combinations are driven through a bare protocol client below, which also sees the response
// packet by packet.
suite("test_multi_statement_response") {
    def tableName = "multi_statement_response"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (k INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // The rows of the current result set of a statement.
    def readRows = { Statement statement ->
        def rows = []
        statement.getResultSet().withCloseable { resultSet ->
            while (resultSet.next()) {
                rows.add(resultSet.getObject(1))
            }
        }
        return rows
    }

    // 1. Connector/J, no CLIENT_MULTI_STATEMENTS: only the last statement's response reaches the
    //    client. The socket timeout turns a response the client cannot parse into a failure
    //    instead of a hang.
    def url = context.config.jdbcUrl + (context.config.jdbcUrl.contains("?") ? "&" : "?") + "socketTimeout=30000"
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        context.getConnection().createStatement().withCloseable { statement ->
            statement.execute("USE ${context.dbName}")

            // The last statement is a query: its result set, and nothing of the query before it.
            assertTrue(statement.execute("SELECT 1; SELECT 2"))
            assertEquals([2], readRows(statement))
            assertFalse(statement.getMoreResults())
            assertEquals(-1, statement.getUpdateCount())

            // The last statement is a SET: its OK alone, then the connection is still in step.
            assertFalse(statement.execute("SELECT 1; SET @multi_stmt_var = 7"))
            assertEquals(0, statement.getUpdateCount())
            assertTrue(statement.execute("SELECT @multi_stmt_var"))
            assertEquals([7], readRows(statement))

            // Two queries before the SET: both dropped.
            assertFalse(statement.execute("SELECT 1; SELECT 2; SET @multi_stmt_var = 8"))
            assertEquals(0, statement.getUpdateCount())
            assertTrue(statement.execute("SELECT @multi_stmt_var"))
            assertEquals([8], readRows(statement))

            // The last statement is an INSERT: its OK carries the affected rows.
            assertFalse(statement.execute("SELECT 1; INSERT INTO ${tableName} VALUES (1)"))
            assertEquals(1, statement.getUpdateCount())
            assertTrue(statement.execute("SELECT k FROM ${tableName}"))
            assertEquals([1], readRows(statement))

            // The last statement opens a transaction: its OK alone, and the transaction is open.
            assertFalse(statement.execute("SELECT 1; BEGIN"))
            assertEquals(0, statement.getUpdateCount())
            statement.execute("ROLLBACK")

            // A SET before the query: the query's result set.
            assertTrue(statement.execute("SET @multi_stmt_var = 9; SELECT @multi_stmt_var"))
            assertEquals([9], readRows(statement))

            // A failing statement in the middle ends the request with its error, and the
            // connection is still usable afterwards.
            try {
                statement.execute("SELECT 1; SELECT * FROM no_such_table_multi_stmt; SELECT 2")
                throw new AssertionError("a request with a failing statement did not fail")
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("does not exist"), e.getMessage())
            }
            assertTrue(statement.execute("SELECT 3"))
            assertEquals([3], readRows(statement))
        }
    }

    // 2. The bare client, every combination of CLIENT_MULTI_STATEMENTS and CLIENT_DEPRECATE_EOF.
    String hostPort = context.config.jdbcUrl.substring(context.config.jdbcUrl.indexOf("://") + 3)
    hostPort = hostPort.substring(0, hostPort.indexOf("/") >= 0 ? hostPort.indexOf("/") : hostPort.length())
    String host = hostPort.substring(0, hostPort.indexOf(":"))
    int port = hostPort.substring(hostPort.indexOf(":") + 1).toInteger()
    def rawClient = { boolean multiStatements, boolean deprecateEof ->
        return new RawMysqlClient(host, port, context.config.jdbcUser, context.config.jdbcPassword,
                context.dbName, multiStatements, deprecateEof)
    }

    [true, false].each { deprecateEof ->
        // With CLIENT_MULTI_STATEMENTS: every statement's response, in order, each but the last
        // flagged SERVER_MORE_RESULTS_EXISTS.
        rawClient(true, deprecateEof).withCloseable { client ->
            def results = client.query("SELECT 1; SET @multi_stmt_var = 10; SELECT @multi_stmt_var; "
                    + "INSERT INTO ${tableName} VALUES (2); SELECT k FROM ${tableName} ORDER BY k")
            assertEquals(5, results.size(), "deprecate_eof=${deprecateEof}: ${results}")
            assertEquals([kind: "rows", rows: [["1"]], more: true], results[0])
            assertEquals([kind: "ok", affectedRows: 0L, more: true], results[1])
            assertEquals([kind: "rows", rows: [["10"]], more: true], results[2])
            assertEquals([kind: "ok", affectedRows: 1L, more: true], results[3])
            assertEquals([kind: "rows", rows: [["1"], ["2"]], more: false], results[4])
            sql "DELETE FROM ${tableName} WHERE k = 2"

            // A failing statement ends the request where it fails: the responses before it were
            // delivered, then its error, and the connection is still usable afterwards.
            try {
                client.query("SELECT 1; SELECT * FROM no_such_table_multi_stmt; SELECT 2")
                throw new AssertionError("a request with a failing statement did not fail")
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("does not exist"), e.getMessage())
                assertEquals([[kind: "rows", rows: [["1"]], more: true]], client.getPartialResults())
            }
            assertEquals([[kind: "rows", rows: [["3"]], more: false]], client.query("SELECT 3"))
        }

        // Without CLIENT_MULTI_STATEMENTS: only the last statement's response.
        rawClient(false, deprecateEof).withCloseable { client ->
            assertEquals([[kind: "rows", rows: [["2"]], more: false]], client.query("SELECT 1; SELECT 2"))
            assertEquals([[kind: "ok", affectedRows: 0L, more: false]],
                    client.query("SELECT 1; SET @multi_stmt_var = 11"))
            assertEquals([[kind: "rows", rows: [["11"]], more: false]], client.query("SELECT @multi_stmt_var"))
            assertEquals([[kind: "ok", affectedRows: 1L, more: false]],
                    client.query("SELECT 1; INSERT INTO ${tableName} VALUES (3)"))
            assertEquals([[kind: "rows", rows: [["1"], ["3"]], more: false]],
                    client.query("SELECT k FROM ${tableName} ORDER BY k"))
            sql "DELETE FROM ${tableName} WHERE k = 3"
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}

/**
 * A bare MySQL protocol client: the handshake with the capability flags a test asks for, COM_QUERY,
 * and the response read packet by packet into a list of results, one per statement delivered:
 * [kind: "rows", rows: [[column text or null, ...], ...], more: boolean] for a result set and
 * [kind: "ok", affectedRows: long, more: boolean] for an OK, where "more" is the
 * SERVER_MORE_RESULTS_EXISTS flag of the packet that ended it. An ERR packet is thrown as an
 * SQLException; the results delivered before it stay in getPartialResults().
 */
class RawMysqlClient implements Closeable {
    static final int CLIENT_LONG_PASSWORD = 0x00000001
    static final int CLIENT_LONG_FLAG = 0x00000004
    static final int CLIENT_CONNECT_WITH_DB = 0x00000008
    static final int CLIENT_PROTOCOL_41 = 0x00000200
    static final int CLIENT_SECURE_CONNECTION = 0x00008000
    static final int CLIENT_MULTI_STATEMENTS = 0x00010000
    static final int CLIENT_MULTI_RESULTS = 0x00020000
    static final int CLIENT_PLUGIN_AUTH = 0x00080000
    static final int CLIENT_DEPRECATE_EOF = 0x01000000
    static final int SERVER_MORE_RESULTS_EXISTS = 0x0008

    private final Socket socket
    private final DataInputStream input
    private final OutputStream output
    private final boolean deprecateEof
    private int sequenceId = 0
    private List<Map> partialResults = []

    RawMysqlClient(String host, int port, String user, String password, String db,
            boolean multiStatements, boolean deprecateEof) {
        this.deprecateEof = deprecateEof
        socket = new Socket(host, port)
        socket.setSoTimeout(30000)
        input = new DataInputStream(new BufferedInputStream(socket.getInputStream()))
        output = socket.getOutputStream()

        // The greeting: protocol version, server version, connection id, the two parts of the
        // 20-byte scramble around the capability flags, charset, status and the plugin name.
        ByteBuffer greeting = ByteBuffer.wrap(readPacket()).order(ByteOrder.LITTLE_ENDIAN)
        greeting.get()
        while (greeting.get() != 0) {
        }
        greeting.getInt()
        byte[] scramble = new byte[20]
        greeting.get(scramble, 0, 8)
        greeting.get()
        greeting.getShort()
        greeting.get()
        greeting.getShort()
        greeting.getShort()
        greeting.get()
        greeting.position(greeting.position() + 10)
        greeting.get(scramble, 8, 12)

        int flags = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION
                | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB
        if (multiStatements) {
            flags |= CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS
        }
        if (deprecateEof) {
            flags |= CLIENT_DEPRECATE_EOF
        }
        ByteArrayOutputStream response = new ByteArrayOutputStream()
        writeInt4(response, flags)
        writeInt4(response, 16777216)
        response.write(33)
        response.write(new byte[23])
        response.write(user.getBytes(StandardCharsets.UTF_8))
        response.write(0)
        byte[] auth = password.isEmpty() ? new byte[0] : nativePasswordScramble(password, scramble)
        response.write(auth.length)
        response.write(auth)
        response.write(db.getBytes(StandardCharsets.UTF_8))
        response.write(0)
        response.write("mysql_native_password".getBytes(StandardCharsets.UTF_8))
        response.write(0)
        sendPacket(response.toByteArray())

        byte[] reply = readPacket()
        if ((reply[0] & 0xFF) == 0xFE) {
            // An auth switch to the same plugin: answer the new scramble.
            ByteBuffer request = ByteBuffer.wrap(reply)
            request.get()
            while (request.get() != 0) {
            }
            byte[] newScramble = new byte[20]
            request.get(newScramble)
            sendPacket(password.isEmpty() ? new byte[0] : nativePasswordScramble(password, newScramble))
            reply = readPacket()
        }
        if ((reply[0] & 0xFF) != 0x00) {
            throw new IllegalStateException("handshake failed: " + describeError(reply))
        }
    }

    // SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
    private static byte[] nativePasswordScramble(String password, byte[] scramble) {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1")
        byte[] stage1 = sha1.digest(password.getBytes(StandardCharsets.UTF_8))
        sha1.reset()
        byte[] stage2 = sha1.digest(stage1)
        sha1.reset()
        sha1.update(scramble)
        sha1.update(stage2)
        byte[] mixed = sha1.digest()
        byte[] result = new byte[stage1.length]
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (stage1[i] ^ mixed[i])
        }
        return result
    }

    List<Map> getPartialResults() {
        return partialResults
    }

    List<Map> query(String sql) {
        sequenceId = 0
        ByteArrayOutputStream command = new ByteArrayOutputStream()
        command.write(0x03)
        command.write(sql.getBytes(StandardCharsets.UTF_8))
        sendPacket(command.toByteArray())

        List<Map> results = []
        partialResults = results
        while (true) {
            byte[] first = readPacket()
            int header = first[0] & 0xFF
            if (header == 0xFF) {
                throw new SQLException(describeError(first))
            }
            if (header == 0x00) {
                Map ok = parseOk(first)
                results.add(ok)
                if (!ok.more) {
                    return results
                }
                continue
            }
            int columnCount = (int) readLenenc(ByteBuffer.wrap(first).order(ByteOrder.LITTLE_ENDIAN))
            for (int i = 0; i < columnCount; i++) {
                readPacket()
            }
            if (!deprecateEof) {
                byte[] eof = readPacket()
                if ((eof[0] & 0xFF) != 0xFE || eof.length != 5) {
                    throw new IllegalStateException("expected an EOF after the column definitions, got "
                            + describePacket(eof))
                }
            }
            List<List<String>> rows = []
            while (true) {
                byte[] packet = readPacket()
                int rowHeader = packet[0] & 0xFF
                if (rowHeader == 0xFF) {
                    throw new SQLException(describeError(packet))
                }
                if (rowHeader == 0xFE && (deprecateEof ? packet.length >= 7 : packet.length < 9)) {
                    boolean more = (terminatorStatus(packet) & SERVER_MORE_RESULTS_EXISTS) != 0
                    results.add([kind: "rows", rows: rows, more: more])
                    if (!more) {
                        return results
                    }
                    break
                }
                rows.add(parseTextRow(packet, columnCount))
            }
        }
    }

    private Map parseOk(byte[] packet) {
        ByteBuffer buffer = ByteBuffer.wrap(packet).order(ByteOrder.LITTLE_ENDIAN)
        buffer.get()
        long affectedRows = readLenenc(buffer)
        readLenenc(buffer)
        int status = buffer.getShort() & 0xFFFF
        return [kind: "ok", affectedRows: affectedRows, more: (status & SERVER_MORE_RESULTS_EXISTS) != 0]
    }

    // The status flags of the packet that ends a result set: an EOF packet (warnings, status) or,
    // when the client deprecated EOF, an OK packet with the 0xFE header.
    private int terminatorStatus(byte[] packet) {
        ByteBuffer buffer = ByteBuffer.wrap(packet).order(ByteOrder.LITTLE_ENDIAN)
        buffer.get()
        if (!deprecateEof) {
            buffer.getShort()
            return buffer.getShort() & 0xFFFF
        }
        readLenenc(buffer)
        readLenenc(buffer)
        return buffer.getShort() & 0xFFFF
    }

    private static List<String> parseTextRow(byte[] packet, int columnCount) {
        ByteBuffer buffer = ByteBuffer.wrap(packet).order(ByteOrder.LITTLE_ENDIAN)
        List<String> row = []
        for (int i = 0; i < columnCount; i++) {
            if ((buffer.get(buffer.position()) & 0xFF) == 0xFB) {
                buffer.get()
                row.add(null)
            } else {
                int length = (int) readLenenc(buffer)
                byte[] value = new byte[length]
                buffer.get(value)
                row.add(new String(value, StandardCharsets.UTF_8))
            }
        }
        return row
    }

    private static long readLenenc(ByteBuffer buffer) {
        int first = buffer.get() & 0xFF
        if (first < 0xFB) {
            return first
        }
        if (first == 0xFC) {
            return buffer.getShort() & 0xFFFFL
        }
        if (first == 0xFD) {
            return (buffer.get() & 0xFFL) | ((buffer.get() & 0xFFL) << 8) | ((buffer.get() & 0xFFL) << 16)
        }
        return buffer.getLong()
    }

    private static String describeError(byte[] packet) {
        ByteBuffer buffer = ByteBuffer.wrap(packet).order(ByteOrder.LITTLE_ENDIAN)
        buffer.get()
        int code = buffer.getShort() & 0xFFFF
        byte[] rest = new byte[buffer.remaining()]
        buffer.get(rest)
        return "error " + code + ": " + new String(rest, StandardCharsets.UTF_8)
    }

    private static String describePacket(byte[] packet) {
        return packet.length + " bytes starting with 0x" + Integer.toHexString(packet[0] & 0xFF)
    }

    private byte[] readPacket() {
        byte[] header = new byte[4]
        input.readFully(header)
        int length = (header[0] & 0xFF) | ((header[1] & 0xFF) << 8) | ((header[2] & 0xFF) << 16)
        sequenceId = ((header[3] & 0xFF) + 1) & 0xFF
        byte[] payload = new byte[length]
        input.readFully(payload)
        return payload
    }

    private void sendPacket(byte[] payload) {
        byte[] header = [(byte) payload.length, (byte) (payload.length >> 8), (byte) (payload.length >> 16),
                         (byte) sequenceId]
        output.write(header)
        output.write(payload)
        output.flush()
        sequenceId = (sequenceId + 1) & 0xFF
    }

    private static void writeInt4(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF)
        out.write((value >> 8) & 0xFF)
        out.write((value >> 16) & 0xFF)
        out.write((value >> 24) & 0xFF)
    }

    @Override
    void close() {
        try {
            sequenceId = 0
            sendPacket([0x01] as byte[])
        } catch (IOException ignored) {
            // the server may have closed first
        }
        socket.close()
    }
}
