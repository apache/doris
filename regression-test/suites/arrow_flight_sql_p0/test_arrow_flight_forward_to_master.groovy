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

// A statement issued over Arrow Flight SQL to a non-master FE has to be forwarded to the master.
// This needs more than one FE, so it can only be exercised in a docker cluster: on a single-FE
// deployment StmtExecutor.shouldForwardToMaster() returns false immediately.
//
// The group is 'docker' and not 'arrow_flight_sql' on purpose. SuiteContext.useArrowFlightSql()
// keys off the suite group, so keeping it out means `sql` resolves to the thread-local connection
// this suite opens itself (the follower's Flight port) instead of the globally configured one.
suite("test_arrow_flight_forward_to_master", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.connectToFollower = true

    docker(options) {
        def follower = cluster.getOneFollowerFe()
        assertNotNull(follower, "a follower FE is required to exercise the forward-to-master path")

        // Every docker FE serves Arrow Flight SQL on the same fixed port, see
        // FE_ARROW_FLIGHT_SQL_PORT in docker/runtime/doris-compose/cluster.py.
        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
        def flightUrl = "jdbc:arrow-flight-sql://${follower.host}:8070/catalog=${context.dbName}" +
                "?useServerPrepStmts=false&useSSL=false&useEncryption=false"
        logger.info("connect to follower over arrow flight sql: ${flightUrl}".toString())

        sql "DROP TABLE IF EXISTS test_arrow_flight_forward_to_master"

        connect('root', '', flightUrl) {
            // 1. A DDL is a Redirect command, so on a follower it is forwarded to the master.
            //    Before the CLIENT_DEPRECATE_EOF capability was read only for MySQL connections
            //    this failed with "getMysqlChannel not in mysql connection".
            sql """
                CREATE TABLE test_arrow_flight_forward_to_master (k1 int)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            // 2. The same DDL again fails on the master. The follower replays a forwarded
            //    statement's outcome only in ConnectProcessor.finalizeCommand(), which is
            //    MySQL-only, so without carryForwardedOutcomeToFlightSession() the master's error
            //    was dropped and the client was told StatusResult=0 -- success for a statement
            //    that failed.
            test {
                sql """
                    CREATE TABLE test_arrow_flight_forward_to_master (k1 int)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """
                exception "already exists"
            }

            // 3. SHOW FRONTENDS is forwarded too (forward_to_master defaults to true) and it
            //    carries a result set. Without the fix the master's rows were dropped and the
            //    client got the synthesized single column named StatusResult instead.
            def frontends = sql_return_maparray "SHOW FRONTENDS"
            assertFalse(frontends.isEmpty(), "SHOW FRONTENDS returned nothing")
            assertFalse(frontends[0].containsKey("StatusResult"),
                    "got the synthesized status row instead of the master's result set: " + frontends[0])
            assertTrue(frontends.size() >= 2, "expected both FEs, got " + frontends.size())
            assertEquals(1, frontends.count { it.IsMaster == "true" })
        }

        // The forwarded DDL really took effect on the master. CreateTableCommand forwards with
        // sync, so by now the follower has replayed it too.
        def tables = sql_return_maparray "SHOW TABLES LIKE 'test_arrow_flight_forward_to_master'"
        assertEquals(1, tables.size())

        // 4. A forwarded *query* returns its result as MySQL wire packets, which cannot be turned
        //    into Arrow batches. That must be refused explicitly rather than answered with the
        //    synthesized empty success. Use a fresh Flight session so force_forward_all_queries
        //    cannot leak into the assertions above.
        connect('root', '', flightUrl) {
            sql "SET force_forward_all_queries = true"
            test {
                sql "SELECT k1 FROM test_arrow_flight_forward_to_master"
                exception "not supported on an Arrow Flight SQL connection"
            }
        }
    }
}
