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

suite("prepared_point_query_row_policy", "p0") {
    def dbName = context.config.getDbNameByFile(context.file)
    def policyName = "prepared_point_query_key_guard"
    def user = "prepared_point_query_policy_user"
    def password = "Prepared_policy_123!"

    sql "DROP ROW POLICY IF EXISTS ${policyName} ON ${dbName}.prepared_point_query_row_policy FOR ${user}"
    sql "DROP USER IF EXISTS ${user}"
    sql "DROP TABLE IF EXISTS prepared_point_query_row_policy"
    sql """
        CREATE TABLE prepared_point_query_row_policy (
            tenant_id INT NOT NULL,
            item_id INT NOT NULL,
            value VARCHAR(32)
        ) ENGINE=OLAP
        UNIQUE KEY(tenant_id, item_id)
        DISTRIBUTED BY HASH(tenant_id, item_id) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "store_row_column" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        INSERT INTO prepared_point_query_row_policy VALUES
            (1, 10, 'allowed'), (1, 20, 'other'), (2, 10, 'hidden'), (10, 10, 'cast-match')
    """
    sql "CREATE USER ${user} IDENTIFIED BY '${password}'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.prepared_point_query_row_policy TO ${user}"
    sql """
        CREATE ROW POLICY ${policyName} ON ${dbName}.prepared_point_query_row_policy
        AS RESTRICTIVE TO ${user} USING (tenant_id = 1)
    """

    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        sql "GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${user}"
    }
    sql "SET GLOBAL enable_server_side_prepared_statement = true"
    sql "SYNC"

    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName)
    String explainUrl = url.replace("useServerPrepStmts=true", "useServerPrepStmts=false")
    connect(user, password, explainUrl) {
        def explainRows = sql """
            EXPLAIN SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ tenant_id, item_id, value
            FROM prepared_point_query_row_policy WHERE tenant_id = 1 AND item_id = 10
        """
        assertTrue(explainRows.toString().contains("SHORT-CIRCUIT"))
    }

    connect(user, password, url) {
        def prepared = prepareStatement """
            SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ tenant_id, item_id, value
            FROM prepared_point_query_row_policy
            WHERE tenant_id = ? AND item_id = ?
        """
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)

        def readRows = { Integer tenant, int item ->
            if (tenant == null) {
                prepared.setNull(1, java.sql.Types.INTEGER)
            } else {
                prepared.setInt(1, tenant)
            }
            prepared.setInt(2, item)
            def rows = []
            prepared.executeQuery().withCloseable { result ->
                assertEquals(3, result.getMetaData().getColumnCount())
                assertEquals("tenant_id", result.getMetaData().getColumnLabel(1))
                assertEquals("item_id", result.getMetaData().getColumnLabel(2))
                assertEquals("value", result.getMetaData().getColumnLabel(3))
                while (result.next()) {
                    rows.add([result.getInt(1), result.getInt(2), result.getString(3)])
                }
            }
            return rows
        }

        assertEquals([[1, 10, "allowed"]], readRows(1, 10))
        assertEquals([], readRows(2, 10))
        assertEquals([[1, 10, "allowed"]], readRows(1, 10))
        assertEquals([], readRows(null, 10))

        // A non-integral parameter cannot be represented by the physical INT lookup key. It is
        // evaluated by the normal planner, exercising direct-reuse FALLBACK without an error.
        prepared.setBigDecimal(1, new BigDecimal("1.2"))
        prepared.setInt(2, 10)
        def fallbackRows = []
        prepared.executeQuery().withCloseable { result ->
            while (result.next()) {
                fallbackRows.add([result.getInt(1), result.getInt(2), result.getString(3)])
            }
        }
        assertEquals([], fallbackRows)
        prepared.close()
    }

    // A fixed predicate on a cast key is not an exact physical-key constraint. Keep it on
    // the normal path unless a future proof can establish that the cast is lossless/injective.
    sql "DROP ROW POLICY IF EXISTS ${policyName} ON ${dbName}.prepared_point_query_row_policy FOR ${user}"
    sql """
        CREATE ROW POLICY ${policyName} ON ${dbName}.prepared_point_query_row_policy
        AS RESTRICTIVE TO ${user} USING (CAST(tenant_id AS CHAR(1)) = '1')
    """
    sql "SYNC"
    connect(user, password, explainUrl) {
        def explainRows = sql """
            EXPLAIN SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ tenant_id, item_id, value
            FROM prepared_point_query_row_policy WHERE tenant_id = 1 AND item_id = 10
        """
        assertFalse(explainRows.toString().contains("SHORT-CIRCUIT"))
    }

    connect(user, password, url) {
        def prepared = prepareStatement """
            SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ tenant_id, item_id, value
            FROM prepared_point_query_row_policy
            WHERE tenant_id = ? AND item_id = ?
        """
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, prepared.class)
        prepared.setInt(1, 10)
        prepared.setInt(2, 10)
        def rows = []
        prepared.executeQuery().withCloseable { result ->
            while (result.next()) {
                rows.add([result.getInt(1), result.getInt(2), result.getString(3)])
            }
        }
        prepared.close()
        assertEquals([[10, 10, "cast-match"]], rows)
    }

    sql "DROP ROW POLICY IF EXISTS ${policyName} ON ${dbName}.prepared_point_query_row_policy FOR ${user}"
    sql "DROP USER IF EXISTS ${user}"
}
