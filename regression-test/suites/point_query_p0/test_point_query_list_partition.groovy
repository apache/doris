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

// Regression test for issue #66030: a short-circuit point query on a table
// partitioned by LIST used to throw IllegalStateException because the tablet
// list was not pruned to a single tablet when the matched partition contained
// more than one tablet. The legacy distribution-prune path taken for point
// queries could not prune (no column filters for the nereids-planned conjuncts),
// so PointQueryExecutor.setScanRangeLocations hit its single-tablet checkState
// with an un-pruned list. With multiple buckets per partition the bug surfaces.
//
// This suite also covers the prepared-statement point query on the same table
// to guard against regressing that path: a prepared point query (parameter
// values unknown at planning time) must keep taking the legacy runtime-prune
// path and must not throw.

suite("test_point_query_list_partition") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_serving_p0"
    def tableName = realDb + ".tbl_point_query_list_partition"
    sql "CREATE DATABASE IF NOT EXISTS ${realDb}"

    // Parse the JDBC url so we can build a server-side-prepared-statement url.
    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    def prepare_url = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + realDb + "?&useServerPrepStmts=true"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
              CREATE TABLE IF NOT EXISTS ${tableName} (
                `pk` varchar(64) NOT NULL,
                `_id` bigint NOT NULL
              ) ENGINE=OLAP
              UNIQUE KEY(`pk`, `_id`)
              PARTITION BY LIST(`pk`) (
                  PARTITION `p_abcd` VALUES IN ("abcd"),
                  PARTITION `p_efgh` VALUES IN ("efgh")
              )
              DISTRIBUTED BY HASH(`pk`, `_id`) BUCKETS 3
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "store_row_column" = "true",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2")
              """

    sql """INSERT INTO ${tableName} VALUES ('abcd', 1)"""
    sql """INSERT INTO ${tableName} VALUES ('abcd', 2)"""
    sql """INSERT INTO ${tableName} VALUES ('efgh', 1)"""

    // --- Direct point query (the path fixed by #66030) ---
    // The point query covers the full unique key, so it must be planned as a
    // short-circuit point query. Confirm the short-circuit path is taken so
    // this test actually exercises the buggy code path.
    explain {
        sql("""SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 1""")
        contains "SHORT-CIRCUIT"
    }

    // Before the fix this threw: (1105, 'IllegalStateException, msg: null').
    // The point query must succeed and return exactly the matched row.
    def result1 = sql """SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 1"""
    assertEquals(1, result1.size())

    // Point query hitting a different LIST partition must also succeed.
    def result2 = sql """SELECT * FROM ${tableName} WHERE pk = 'efgh' AND _id = 1"""
    assertEquals(1, result2.size())

    // A point query that matches no row should return zero rows, not throw.
    def result3 = sql """SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 999"""
    assertEquals(0, result3.size())

    // --- Prepared-statement point query (the path that must NOT regress) ---
    // A prepared point query cannot be pruned to a single tablet at planning
    // time (parameter value unknown), so it keeps taking the legacy runtime
    // distribution-prune path. It must still resolve to the right tablet at
    // execution time and succeed.
    connect(user, password, prepare_url) {
        def stmt = prepareStatement "select * from ${tableName} where pk = ? and _id = ?"
        stmt.setString(1, "abcd")
        stmt.setLong(2, 1)
        def rs = stmt.executeQuery()
        int rowCount = 0
        while (rs.next()) {
            assertEquals("abcd", rs.getString(1))
            assertEquals(1L, rs.getLong(2))
            rowCount++
        }
        assertEquals(1, rowCount)
    }

    sql """DROP TABLE IF EXISTS ${tableName}"""
}
