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

suite("cursor_fetch_empty_result") {
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0") +
            "&useCursorFetch=true&defaultFetchSize=10000&socketTimeout=10000&emulateUnsupportedPstmts=false"

    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        // With a positive defaultFetchSize Connector/J also converts a plain Statement into a
        // server-prepared cursor execution, which is how BI tools commonly enter this path.
        context.getConnection().createStatement().withCloseable { statement ->
            ["SELECT 1 AS c WHERE 1 = 2", "SELECT 1 AS c WHERE 1 = 1"].each { query ->
                statement.executeQuery(query).withCloseable { result ->
                    while (result.next()) {
                        result.getInt(1)
                    }
                }
            }
        }

        def emptyResult = prepareStatement "SELECT 1 AS c WHERE 1 = 2"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, emptyResult.class)
        qe_empty_result emptyResult
        emptyResult.close()

        def nonEmptyResult = prepareStatement "SELECT 1 AS c WHERE 1 = 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, nonEmptyResult.class)
        qe_non_empty_result nonEmptyResult
        nonEmptyResult.close()
    }

    String unidentifiedClientUrl = getServerPrepareJdbcUrl(
            context.config.jdbcUrl, "regression_test_prepared_stmt_p0") +
            "&useCursorFetch=true&defaultFetchSize=10000&connectionAttributes=none&socketTimeout=10000" +
            "&emulateUnsupportedPstmts=false"
    connect(context.config.jdbcUser, context.config.jdbcPassword, unidentifiedClientUrl) {
        qt_anonymous_empty "SELECT 1 AS c WHERE 1 = 2"
        qt_anonymous_non_empty "SELECT 1 AS c WHERE 1 = 1"
    }

    def followers = sql_return_maparray("SHOW FRONTENDS").findAll {
        it.IsMaster == "false" && it.Alive == "true"
    }
    if (followers.isEmpty()) {
        logger.info("Skip prepared forwarding coverage: no live non-master FE")
    } else {
        sql "DROP TABLE IF EXISTS cursor_fetch_forwarding"
        sql """CREATE TABLE cursor_fetch_forwarding (k INT)
               DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES ("replication_num" = "1")"""
        sql "INSERT INTO cursor_fetch_forwarding VALUES (1), (2), (3)"
        followers.each { fe ->
            String followerUrl = getServerPrepareJdbcUrl(
                    "jdbc:mysql://${fe.Host}:${fe.QueryPort}/", "regression_test_prepared_stmt_p0", false) +
                    "&useCursorFetch=true&emulateUnsupportedPstmts=false&socketTimeout=10000"
            connect(context.config.jdbcUser, context.config.jdbcPassword, followerUrl) {
                def connection = context.getConnection()
                connection.createStatement().withCloseable { control ->
                    // SET must use COM_QUERY: enabling forwarding before PREPARE rejects server prepare.
                    control.execute("SET force_forward_all_queries=false")
                    control.execute("SYNC")
                    [0, 1, 10000].each { fetchSize ->
                        ["SELECT k FROM cursor_fetch_forwarding ORDER BY k",
                         "SELECT k FROM cursor_fetch_forwarding WHERE k < 0 ORDER BY k"].each { query ->
                            connection.prepareStatement(query).withCloseable { prepared ->
                                assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
                                prepared.setFetchSize(fetchSize)
                                def readRows = {
                                    def rows = []
                                    prepared.executeQuery().withCloseable { result ->
                                        while (result.next()) {
                                            rows.add(result.getInt(1))
                                        }
                                    }
                                    return rows
                                }
                                def directRows = readRows()
                                control.execute("SET force_forward_all_queries=true")
                                try {
                                    // Compare execution modes using the same server-prepared statement.
                                    3.times { assertEquals(directRows, readRows()) }
                                } finally {
                                    control.execute("SET force_forward_all_queries=false")
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
