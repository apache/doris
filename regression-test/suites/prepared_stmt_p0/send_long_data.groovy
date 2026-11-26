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

import java.io.ByteArrayInputStream
import java.io.StringReader
import java.nio.charset.StandardCharsets

suite("send_long_data") {
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0") +
            "&emulateUnsupportedPstmts=false&blobSendChunkSize=16384&socketTimeout=10000"
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        def prepared = prepareStatement "SELECT MD5(?), ?, CONCAT('hex:', HEX(?))"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
        try {
            // Reuse the statement with both fresh and cached parameter type metadata.
            ["first", "second", ""].each { text ->
                prepared.setBlob(1, new ByteArrayInputStream(
                        (("a\u0000中文-" + text) * 2048).getBytes(StandardCharsets.UTF_8)))
                prepared.setInt(2, 42)
                prepared.setBinaryStream(3, new ByteArrayInputStream(new byte[] {0, -1, -128, 65}))
                qe_blob prepared
                qt_after_blob "SELECT 1"
            }
            prepared.clearParameters()
            prepared.setCharacterStream(1, new StringReader("中文-long-data"))
            prepared.setInt(2, 43)
            prepared.setBlob(3, new ByteArrayInputStream(new byte[0]))
            qe_character_stream prepared
            qt_after_character_stream "SELECT 2"

            prepared.setString(1, "inline")
            prepared.setInt(2, 44)
            prepared.setString(3, "value")
            qe_inline_after_stream prepared
        } finally {
            prepared.close()
        }
        qt_after_close "SELECT 3"
    }

    def follower = sql_return_maparray("SHOW FRONTENDS").find {
        it.IsMaster == "false" && it.Alive == "true"
    }
    String forwardingUrl = url
    if (follower != null) {
        forwardingUrl = getServerPrepareJdbcUrl(
                "jdbc:mysql://${follower.Host}:${follower.QueryPort}/", "regression_test_prepared_stmt_p0", false) +
                "&emulateUnsupportedPstmts=false&socketTimeout=10000"
    }
    connect(context.config.jdbcUser, context.config.jdbcPassword, forwardingUrl) {
        def connection = context.getConnection()
        connection.createStatement().withCloseable { control ->
            control.execute("SET force_forward_all_queries=false")
            connection.prepareStatement("SELECT HEX(?), ?").withCloseable { prepared ->
                assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
                control.execute("SET force_forward_all_queries=true")
                try {
                    2.times {
                        prepared.setBlob(1, new ByteArrayInputStream(new byte[] {0, -1, -128, 65}))
                        prepared.setInt(2, 45)
                        qe_forwarded_blob prepared
                    }
                } finally {
                    control.execute("SET force_forward_all_queries=false")
                }
            }
        }
        qt_after_forwarding "SELECT 4"
    }
}
