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

import java.sql.DriverManager
import java.sql.SQLException

suite("prepared_short_circuit_security_refresh", "nonConcurrent") {
    def dbName = context.config.getDbNameByFile(context.file)
    def policyName = "prepared_short_circuit_security_refresh_policy"
    def testUser = "prepared_short_circuit_security_refresh_user"
    def testPassword = "PreparedSecurity@123"
    def adminUser = context.config.jdbcUser
    def adminPassword = context.config.jdbcPassword
    String serverPrepareUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName)

    sql "DROP TABLE IF EXISTS prepared_short_circuit_security_refresh_tbl"
    sql "DROP USER IF EXISTS ${testUser}"
    sql "CREATE USER ${testUser} IDENTIFIED BY '${testPassword}'"
    sql """
        CREATE TABLE prepared_short_circuit_security_refresh_tbl (
            k INT NOT NULL,
            tenant_id INT NOT NULL,
            payload VARCHAR(32) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "store_row_column" = "true"
        )
    """
    sql """DROP ROW POLICY IF EXISTS ${policyName}
            ON ${dbName}.prepared_short_circuit_security_refresh_tbl FOR ${testUser}"""
    sql """INSERT INTO prepared_short_circuit_security_refresh_tbl
            VALUES (1, 10, 'allowed'), (2, 20, 'restricted')"""
    sql "GRANT SELECT_PRIV ON ${dbName}.prepared_short_circuit_security_refresh_tbl TO ${testUser}"
    sql "SET GLOBAL enable_server_side_prepared_statement = true"
    sql "SYNC"

    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        sql "GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${testUser}"
    }

    def adminConnection = DriverManager.getConnection(context.config.jdbcUrl, adminUser, adminPassword)
    def adminExecute = { String statement ->
        adminConnection.createStatement().withCloseable { adminStatement ->
            adminStatement.execute(statement)
        }
    }

    try {
        explain {
            sql """
                SELECT /*+ SET_VAR(enable_nereids_planner=true,
                                  enable_fallback_to_original_planner=false,
                                  enable_short_circuit_query=true) */
                       k, tenant_id, payload
                FROM prepared_short_circuit_security_refresh_tbl
                WHERE k = 2
            """
            contains "SHORT-CIRCUIT"
        }
        connect(testUser, testPassword, serverPrepareUrl) {
            sql "SET enable_fallback_to_original_planner = false"
            def prepared = prepareStatement(
                    """SELECT /*+ SET_VAR(enable_nereids_planner=true,
                                         enable_fallback_to_original_planner=false,
                                         enable_short_circuit_query=true) */
                              k, tenant_id, payload
                       FROM prepared_short_circuit_security_refresh_tbl
                       WHERE k = ?""")
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
            prepared.setInt(1, 2)

            // The second execution must use the cached point-query plan.
            qe_before_policy prepared
            qe_cached_before_policy prepared

            adminExecute("""
                CREATE ROW POLICY ${policyName}
                ON ${dbName}.prepared_short_circuit_security_refresh_tbl
                AS RESTRICTIVE TO ${testUser} USING (tenant_id = 10)
            """)
            qe_after_policy_added prepared

            adminExecute("""DROP ROW POLICY ${policyName}
                    ON ${dbName}.prepared_short_circuit_security_refresh_tbl FOR ${testUser}""")
            qe_after_policy_dropped prepared

            adminExecute("""REVOKE SELECT_PRIV
                    ON ${dbName}.prepared_short_circuit_security_refresh_tbl FROM ${testUser}""")
            boolean denied = false
            try {
                prepared.executeQuery().close()
            } catch (SQLException e) {
                denied = true
                logger.info("prepared execution was denied after SELECT revoke: ${e.message}")
            }
            assertTrue(denied, "the cached point-query plan must not survive SELECT revocation")

            adminExecute("""GRANT SELECT_PRIV
                    ON ${dbName}.prepared_short_circuit_security_refresh_tbl TO ${testUser}""")
            qe_after_select_granted prepared
            prepared.close()
        }
    } finally {
        adminExecute("""DROP ROW POLICY IF EXISTS ${policyName}
                ON ${dbName}.prepared_short_circuit_security_refresh_tbl FOR ${testUser}""")
        adminExecute("""GRANT SELECT_PRIV
                ON ${dbName}.prepared_short_circuit_security_refresh_tbl TO ${testUser}""")
        adminConnection.close()
        sql "DROP USER IF EXISTS ${testUser}"
    }
}
