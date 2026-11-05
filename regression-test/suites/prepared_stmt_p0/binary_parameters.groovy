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

import java.nio.charset.StandardCharsets

suite("binary_parameters") {
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0") +
            "&emulateUnsupportedPstmts=false&socketTimeout=10000"
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        def connection = context.getConnection()
        // setBytes uses MYSQL_TYPE_VAR_STRING, while setBlob/setBinaryStream use MYSQL_TYPE_BLOB.
        connection.prepareStatement("SELECT HEX(?), MD5(?)").withCloseable { prepared ->
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
            byte[] allBytes = new byte[256]
            for (int i = 0; i < allBytes.length; i++) {
                allBytes[i] = (byte) i
            }
            [new byte[] {0, -128, -1, 65, 0, -61, 40}, allBytes,
                    "中文🙂".getBytes(StandardCharsets.UTF_8), new byte[0], null,
                    new byte[] {-61}, new byte[] {65, 0, -1}].each { bytes ->
                prepared.setBytes(1, bytes)
                prepared.setBytes(2, bytes)
                qe_binary_parameter prepared
            }
        }

        sql "DROP TABLE IF EXISTS prepared_binary_parameters"
        sql """CREATE TABLE prepared_binary_parameters (id INT, value STRING)
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ('replication_num' = '1')"""
        connection.prepareStatement("INSERT INTO prepared_binary_parameters VALUES (?, ?)").withCloseable { prepared ->
            prepared.setInt(1, 1)
            prepared.setBytes(2, new byte[] {0, -128, -1, 65, 0, -61, 40})
            prepared.executeUpdate()
            prepared.setInt(1, 2)
            prepared.setString(2, "中文🙂")
            prepared.executeUpdate()
        }
        order_qt_stored_bytes "SELECT id, HEX(value), MD5(value) FROM prepared_binary_parameters"
        connection.prepareStatement("SELECT id FROM prepared_binary_parameters WHERE CONCAT(value, '') = ?")
                .withCloseable { prepared ->
                    prepared.setBytes(1, new byte[] {0, -128, -1, 65, 0, -61, 40})
                    qe_binary_predicate prepared
                }
        connection.prepareStatement("SELECT ?, CONCAT(?, '!'), LENGTH(?)").withCloseable { prepared ->
            prepared.setString(1, "中文🙂")
            prepared.setString(2, "text")
            prepared.setString(3, "a\u0000b")
            qe_text_parameters prepared
        }
    }
}
