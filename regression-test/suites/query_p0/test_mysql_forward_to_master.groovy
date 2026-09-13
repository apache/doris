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

import org.apache.doris.regression.suite.ClusterOptions

// A statement issued over MySQL to a non-master FE is forwarded to the master, and what the client
// receives is the response the master produced for it: the follower tells the master what its
// client negotiated (MysqlProtocolAdapter.fillForwardRequest / restoreFromForwardRequest: the
// capability flags, CLIENT_DEPRECATE_EOF, and for a COM_STMT_EXECUTE the execute packet and whether
// a cursor was asked for), and relays the master's packets as they are
// (MysqlProtocolAdapter.finishCommand -> StmtExecutor.sendProxyQueryResult), reshaped for the
// client's cursor expectations by FEOpExecutor.prepareQueryResultForClient. This needs more than
// one FE, so it can only be exercised in a docker cluster: on a single-FE deployment
// StmtExecutor.shouldForwardToMaster() returns false immediately.
//
// The Arrow Flight SQL side of the same path is test_arrow_flight_forward_to_master.
suite("test_mysql_forward_to_master", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.connectToFollower = true

    docker(options) {
        def follower = cluster.getOneFollowerFe()
        assertNotNull(follower, "a follower FE is required to exercise the forward-to-master path")

        def tableName = "mysql_forward_to_master"
        // `sql` runs on the follower. A DDL is a Redirect command and is forwarded to the master.
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (k INT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        // The same DDL again fails on the master; the master's error is what the client gets.
        test {
            sql """
                CREATE TABLE ${tableName} (k INT)
                DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "already exists"
        }
        sql "INSERT INTO ${tableName} VALUES (1), (2), (3)"

        // 1. A forwarded query answers with the master's result set packets, relayed as they are.
        sql "SET force_forward_all_queries = true"
        try {
            assertEquals([[1], [2], [3]], sql("SELECT k FROM ${tableName} ORDER BY k"))
            assertEquals([[1]], sql("SELECT 1"))
            assertEquals([[3L]], sql("SELECT count(*) FROM ${tableName}"))
            // A query without rows: header, column definitions and the end of the result set only.
            assertEquals([], sql("SELECT k FROM ${tableName} WHERE k < 0"))

            // A forwarded SHOW carries a result set of its own (a ShowResultSet, not packets).
            def frontends = sql_return_maparray "SHOW FRONTENDS"
            assertTrue(frontends.size() >= 2, "expected both FEs, got " + frontends.size())
            assertEquals(1, frontends.count { it.IsMaster == "true" })

            // A forwarded query that fails on the master: the master's error, and the connection
            // is still usable afterwards.
            test {
                sql "SELECT * FROM no_such_table_forward"
                exception "does not exist"
            }
            assertEquals([[2]], sql("SELECT k FROM ${tableName} WHERE k = 2"))
        } finally {
            sql "SET force_forward_all_queries = false"
        }

        // 2. A forwarded COM_STMT_EXECUTE: the execute packet travels with the request, and the
        //    result comes back shaped for the cursor the client asked for. A server-side prepared
        //    statement has to be prepared while forwarding is off (a forwarded COM_STMT_PREPARE
        //    is refused), so forwarding is switched on between PREPARE and EXECUTE.
        String followerUrl = "jdbc:mysql://${follower.host}:${follower.queryPort}/${context.dbName}"
                + "?useServerPrepStmts=true&useCursorFetch=true&emulateUnsupportedPstmts=false&socketTimeout=30000"
        connect(context.config.jdbcUser, context.config.jdbcPassword, followerUrl) {
            def connection = context.getConnection()
            connection.createStatement().withCloseable { control ->
                control.execute("SET force_forward_all_queries = false")
                [0, 1, 10000].each { fetchSize ->
                    ["SELECT k FROM ${tableName} ORDER BY k".toString(),
                     "SELECT k FROM ${tableName} WHERE k < 0 ORDER BY k".toString()].each { query ->
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
                            control.execute("SET force_forward_all_queries = true")
                            try {
                                // The same server-prepared statement, executed on the master.
                                3.times { assertEquals(directRows, readRows()) }
                            } finally {
                                control.execute("SET force_forward_all_queries = false")
                            }
                        }
                    }
                }
            }
        }
    }
}
