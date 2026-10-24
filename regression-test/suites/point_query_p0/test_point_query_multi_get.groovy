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

suite("test_point_query_multi_get", "nonConcurrent") {
    def multiGetConfig = sql """
        ADMIN SHOW FRONTEND CONFIG LIKE 'enable_point_query_multi_get'
    """
    String oldMultiGetValue = multiGetConfig[0][1]
    try {
        sql """ADMIN SET FRONTEND CONFIG ("enable_point_query_multi_get" = "true")"""
        sql "DROP TABLE IF EXISTS tbl_point_query_multi_get"
        sql """
            CREATE TABLE tbl_point_query_multi_get (
                k1 INT NOT NULL,
                k2 VARCHAR(20) NOT NULL,
                v VARCHAR(20) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1, k2) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true"
            )
        """
        sql """
            INSERT INTO tbl_point_query_multi_get VALUES
                (1, 'x', 'v1'), (2, 'x', 'v2'), (3, 'x', 'v3'), (4, 'y', 'v4')
        """

        test {
            sql "SET max_point_query_in_values = 0"
            exception "max_point_query_in_values must be greater than 0"
        }

        def preparedUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName, false)
        connect(context.config.jdbcUser, context.config.jdbcPassword, preparedUrl) {
            sql "USE ${context.dbName}"
            def stmt = prepareStatement(
                    "SELECT k1, v FROM tbl_point_query_multi_get "
                            + "WHERE k1 IN (?, ?, ?) AND k2 = ?")

            stmt.setInt(1, 2)
            stmt.setInt(2, 2)
            stmt.setInt(3, 2)
            stmt.setString(4, "x")
            qe_multi_get_prepared_first stmt

            stmt.setInt(1, 3)
            stmt.setInt(2, 9)
            stmt.setInt(3, 3)
            stmt.setString(4, "x")
            qe_multi_get_prepared_second stmt

            // Reuse the cached multi-get plan with a different, duplicate binding.
            stmt.setInt(1, 1)
            stmt.setInt(2, 1)
            stmt.setInt(3, 1)
            stmt.setString(4, "x")
            qe_multi_get_prepared_cached stmt

            sql "DELETE FROM tbl_point_query_multi_get WHERE k1 = 2 AND k2 = 'x'"
            stmt.setInt(1, 1)
            stmt.setInt(2, 2)
            stmt.setInt(3, 3)
            stmt.setString(4, "x")
            // Sort at the test harness: SQL ORDER BY would bypass the point-query path.
            quickRunTest("multi_get_after_delete", stmt, true)

            sql "SET max_point_query_in_values = 2"
            // The new limit applies when planning, not when reusing an existing point-query cache.
            stmt.close()
            stmt = prepareStatement(
                    "SELECT k1, v FROM tbl_point_query_multi_get "
                            + "WHERE k1 IN (?, ?, ?) AND k2 = ?")
            stmt.setInt(1, 1)
            stmt.setInt(2, 8)
            stmt.setInt(3, 9)
            stmt.setString(4, "x")
            qe_multi_get_prepared_fallback stmt
            stmt.close()
        }
    } finally {
        sql """ADMIN SET FRONTEND CONFIG ("enable_point_query_multi_get" = "${oldMultiGetValue}")"""
    }
}
