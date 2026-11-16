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

import java.security.MessageDigest

// The MySQL compressed protocol (CLIENT_COMPRESS, zlib) on the FE channel.
//
// The FE advertises the flag only while mysql_compression_algorithms lists zlib
// (default: empty, nothing advertised), with or without TLS on its MySQL port.
// A Connector/J connection opened with useCompression=true then exchanges
// zlib-compressed frames with the FE from the first packet after
// authentication; the result it reads must be byte-for-byte the result a
// plain connection reads, including a result wider than one 16 MiB frame and a
// statement deflated by the driver on the way in. Every other connection is
// unaffected, and with the config empty a client asking for compression
// silently stays on the plain protocol (the behaviour before this feature).
//
// The FE counts negotiated compressed connections in
// doris_fe_mysql_compressed_connection_total, which is how the suite tells a
// compressed connection from a plain one, and the bytes of each direction in
// doris_fe_mysql_compressed_{send_raw_bytes,send_bytes,recv_bytes}, which is
// how it tells frames that were deflated from frames that travelled raw.
//
// Connector/J takes TLS whenever the FE offers it (sslMode=PREFERRED), so on a
// TLS-enabled FE the first compressed connection runs compress-then-encrypt;
// the second one disables TLS on the client side and runs the plain socket
// shape, which the FE accepts whether or not it offers TLS.
suite("test_mysql_compressed_protocol") {
    def feHttp = context.config.feHttpAddress
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def baseUrl = context.config.jdbcUrl
    def sep = baseUrl.contains("?") ? "&" : "?"
    def compressedUrl = baseUrl + sep + "useCompression=true"
    def compressedPlainUrl = compressedUrl + "&sslMode=DISABLED"

    def frontendConfig = { String key ->
        def rows = sql "ADMIN SHOW FRONTEND CONFIG LIKE '${key}'"
        return rows.isEmpty() ? null : String.valueOf(rows[0][1]).trim()
    }

    // One FE counter from /metrics, 0 when it has not been exported yet.
    def feMetric = { String name ->
        long value = -1
        httpTest {
            endpoint feHttp
            uri "/metrics"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                def line = body.readLines().find { it.startsWith("doris_fe_${name} ") }
                value = line == null ? 0L : Long.parseLong(line.split(" ")[1].trim())
            }
        }
        return value
    }
    def compressedConnections = { feMetric("mysql_compressed_connection_total") }
    def sendRawBytes = { feMetric("mysql_compressed_send_raw_bytes") }
    def sendBytes = { feMetric("mysql_compressed_send_bytes") }
    def recvBytes = { feMetric("mysql_compressed_recv_bytes") }

    def md5Of = { List<List<Object>> rows ->
        def digest = MessageDigest.getInstance("MD5")
        rows.each { row -> row.each { cell -> digest.update(String.valueOf(cell).getBytes("UTF-8")); digest.update((byte) 0x1f) } }
        return digest.digest().encodeHex().toString()
    }

    // 40 rows of 900 000 chars: ~36 MB, well past one 16 MiB frame, several packets per frame.
    def wideQuery = "SELECT repeat('x', 900000) AS s FROM numbers(\"number\" = \"40\")"
    // 100 000 short rows: many packets, each frame holds hundreds of them.
    def manyRowsQuery = "SELECT number, md5(cast(number AS string)) AS h FROM numbers(\"number\" = \"100000\") ORDER BY number"
    // A 3 MB statement on the way IN, deflated by the driver into one compressed frame (the FE folds
    // length() of a literal, so the BE never sees the string). Connector/J refuses a statement wider than
    // the server's max_allowed_packet, and on a shared p0 FE that is the 4 MiB which
    // datatype_p0/nested_types/query/sql/nested_with_join.sql sets globally, not the 16 MiB default; so the
    // literal stays under 4 MiB, and a multi-frame inbound statement cannot be sent from here (that path
    // is pinned by MysqlChannelCompressionTest).
    def literal = 'y' * 3000000
    assertTrue(literal.length() + 64 < 4194304, "the inbound statement must fit a 4 MiB max_allowed_packet")
    def inboundQuery = "SELECT length('${literal}')"

    // Runs the three queries on one connection and checks them against the plain reference.
    def readAll = { String url, String plainWideMd5, String plainManyMd5 ->
        connect(user, password, url) {
            def wide = sql wideQuery
            assertEquals(40, wide.size())
            wide.each { row -> assertEquals(900000, String.valueOf(row[0]).length()) }
            assertEquals(plainWideMd5, md5Of(wide))

            def many = sql manyRowsQuery
            assertEquals(100000, many.size())
            assertEquals(plainManyMd5, md5Of(many))

            def inbound = sql inboundQuery
            assertEquals(3000000L, (inbound[0][0] as long))
        }
    }

    // A compressed connection on the given URL: counted once, and its frames deflated both ways.
    def compressedRound = { String label, String url, String plainWideMd5, String plainManyMd5 ->
        long connectionsBefore = compressedConnections()
        long rawBefore = sendRawBytes()
        long sentBefore = sendBytes()
        long recvBefore = recvBytes()
        readAll(url, plainWideMd5, plainManyMd5)
        assertEquals(connectionsBefore + 1, compressedConnections(),
                "${label}: the useCompression=true connection should have negotiated the compressed protocol")
        long rawDelta = sendRawBytes() - rawBefore
        long sentDelta = sendBytes() - sentBefore
        long recvDelta = recvBytes() - recvBefore
        // ~36 MB of 'x' plus ~4 MB of hex go out; a raw frame costs 7 bytes more than its payload, so a
        // sender that never deflated would show sentDelta > rawDelta
        assertTrue(rawDelta > 36000000L, "${label}: the results went through the compressed sender (raw ${rawDelta})")
        assertTrue(sentDelta * 4 < rawDelta, "${label}: the frames were deflated (${sentDelta} of ${rawDelta} raw bytes on the wire)")
        // the 3 MB literal came in as a frame too, deflated by the client
        assertTrue(recvDelta * 4 < literal.length(), "${label}: the inbound statement arrived deflated (${recvDelta} bytes received)")
    }

    def savedAlgorithms = frontendConfig("mysql_compression_algorithms") ?: ""
    def savedLevel = frontendConfig("mysql_zlib_compression_level") ?: "1"
    sql "ADMIN SET FRONTEND CONFIG ('mysql_compression_algorithms' = 'zlib')"
    try {
        // Plain reference results, on the suite's own (uncompressed) connection.
        def plainWide = sql wideQuery
        def plainMany = sql manyRowsQuery
        assertEquals(40, plainWide.size())
        assertEquals(100000, plainMany.size())
        def plainWideMd5 = md5Of(plainWide)
        def plainManyMd5 = md5Of(plainMany)

        // Compressed over whatever transport the driver picks (TLS when the FE offers it), then
        // compressed over a plain socket.
        compressedRound("driver default", compressedUrl, plainWideMd5, plainManyMd5)
        compressedRound("sslMode=DISABLED", compressedPlainUrl, plainWideMd5, plainManyMd5)

        // A higher zlib level is picked up by the next connection.
        sql "ADMIN SET FRONTEND CONFIG ('mysql_zlib_compression_level' = '6')"
        compressedRound("level 6", compressedUrl, plainWideMd5, plainManyMd5)

        // A plain connection does not negotiate it, even while it is offered.
        long counted = compressedConnections()
        connect(user, password, baseUrl) {
            def rows = sql manyRowsQuery
            assertEquals(plainManyMd5, md5Of(rows))
        }
        assertEquals(counted, compressedConnections(), "a plain connection must not be counted as compressed")

        // With the config empty the flag is not advertised: a client asking for compression
        // stays on the plain protocol and still reads the same result.
        sql "ADMIN SET FRONTEND CONFIG ('mysql_compression_algorithms' = '')"
        connect(user, password, compressedUrl) {
            def rows = sql manyRowsQuery
            assertEquals(plainManyMd5, md5Of(rows))
        }
        assertEquals(counted, compressedConnections(), "nothing is negotiated while mysql_compression_algorithms is empty")
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('mysql_zlib_compression_level' = '${savedLevel}')"
        sql "ADMIN SET FRONTEND CONFIG ('mysql_compression_algorithms' = '${savedAlgorithms}')"
    }
}
