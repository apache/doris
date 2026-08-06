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

import com.mysql.cj.jdbc.ServerPreparedStatement

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

suite("test_timestamptz_jdbc_binary_protocol") {
    String tableName = "test_timestamptz_jdbc_binary_protocol"
    String dbName = "regression_test_datatype_p0_timestamptz"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword

    sql "SET time_zone = '+00:00'"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6),
            note VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO ${tableName} VALUES
            (1, NULL, 'null'),
            (2, '2024-01-01 08:00:00.123456 +08:00', 'equiv_utc'),
            (3, '2024-01-01 00:00:01.654321 +00:00', 'micro')
    """

    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName) +
            "&emulateUnsupportedPstmts=true&useLocalSessionState=true"
    logger.info("jdbc prepare statement url: ${url}")

    connect(user, password, url) {
        sql "SET time_zone = '+00:00'"

        PreparedStatement stmt = prepareStatement("""
            SELECT id, ts, CAST(ts AS STRING) AS ts_text, note
            FROM ${tableName}
            ORDER BY id
        """)
        assertEquals(ServerPreparedStatement, stmt.class)

        ResultSet rs = stmt.executeQuery()
        int rowCount = 0
        try {
            while (rs.next()) {
                rowCount++
                String direct
                try {
                    direct = rs.getString(2)
                } catch (SQLException e) {
                    logger.info("failed to read TIMESTAMPTZ directly with ResultSet.getString", e)
                    throw e
                }

                String castText = rs.getString(3)
                assertEquals(castText, direct)
            }
            assertEquals(3, rowCount)
        } finally {
            rs.close()
            stmt.close()
        }
    }
}
