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

suite("test_point_query_multi_get_delete", "nonConcurrent") {
    def multiGetConfig = sql """
        ADMIN SHOW FRONTEND CONFIG LIKE 'enable_point_query_multi_get'
    """
    String oldMultiGetValue = multiGetConfig[0][1]
    try {
        sql """ADMIN SET FRONTEND CONFIG ("enable_point_query_multi_get" = "true")"""
        sql "DROP TABLE IF EXISTS tbl_point_query_multi_get_delete"
        // One bucket keeps live rows and tombstones in the same BE lookup request.
        // Retain delete-sign rows so compaction/light delete cannot bypass the filter.
        sql """
            CREATE TABLE tbl_point_query_multi_get_delete (
                k1 INT NOT NULL,
                k2 VARCHAR(20) NOT NULL,
                v VARCHAR(20) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1, k2) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true",
                "enable_mow_light_delete" = "false",
                "disable_auto_compaction" = "true"
            )
        """
        sql """
            INSERT INTO tbl_point_query_multi_get_delete VALUES
                (1, 'x', 'v1'), (2, 'x', 'v2'), (3, 'x', NULL)
        """

        def preparedUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName, false)
        connect(context.config.jdbcUser, context.config.jdbcPassword, preparedUrl) {
            sql "USE ${context.dbName}"
            def stmt = prepareStatement(
                    "SELECT k1, k2, v FROM tbl_point_query_multi_get_delete "
                            + "WHERE k1 IN (?, ?, ?) AND k2 = ?")
            try {
                stmt.setString(4, "x")
                stmt.setInt(1, 1)
                stmt.setInt(2, 2)
                stmt.setInt(3, 3)
                // Sort in the harness: SQL ORDER BY would bypass multi-get.
                quickRunTest("multi_get_delete_all_live", stmt, true)

                sql "DELETE FROM tbl_point_query_multi_get_delete WHERE k1 = 2 AND k2 = 'x'"
                stmt.setInt(1, 2)
                stmt.setInt(2, 1)
                stmt.setInt(3, 3)
                // A deleted first row must not discard the live rows or misalign nullable values.
                quickRunTest("multi_get_delete_mixed", stmt, true)

                stmt.setInt(1, 3)
                stmt.setInt(2, 2)
                stmt.setInt(3, 1)
                quickRunTest("multi_get_delete_mixed_cached_plan", stmt, true)

                stmt.setInt(1, 2)
                stmt.setInt(2, 2)
                stmt.setInt(3, 1)
                quickRunTest("multi_get_delete_duplicate_key", stmt, true)

                stmt.setInt(1, 2)
                stmt.setInt(2, 9)
                stmt.setInt(3, 3)
                quickRunTest("multi_get_delete_missing_key", stmt, true)

                sql "DELETE FROM tbl_point_query_multi_get_delete WHERE k1 IN (1, 3) AND k2 = 'x'"
                stmt.setInt(1, 1)
                stmt.setInt(2, 2)
                stmt.setInt(3, 3)
                quickRunTest("multi_get_delete_all_deleted", stmt, true)

                stmt.setInt(1, 8)
                stmt.setInt(2, 9)
                stmt.setInt(3, 10)
                quickRunTest("multi_get_delete_all_missing", stmt, true)
            } finally {
                stmt.close()
            }
        }
    } finally {
        sql """ADMIN SET FRONTEND CONFIG ("enable_point_query_multi_get" = "${oldMultiGetValue}")"""
    }
}
