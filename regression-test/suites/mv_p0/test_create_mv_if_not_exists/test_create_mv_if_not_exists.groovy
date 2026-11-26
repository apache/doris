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

suite("test_create_mv_if_not_exists") {
    sql """ DROP TABLE IF EXISTS t_create_mv_if_not_exists """
    sql """
        CREATE TABLE t_create_mv_if_not_exists (
            k1 INT,
            k2 INT,
            v1 BIGINT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv_create_if_not_exists_k1 ON t_create_mv_if_not_exists """
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv_create_if_not_exists_k2 ON t_create_mv_if_not_exists """
    sql """ INSERT INTO t_create_mv_if_not_exists VALUES (1, 1, 10), (2, 2, 20) """

    def syncMvIndexId = { String tableName, String mvName ->
        def dbId = sql("SHOW PROC '/dbs'").find { it[1].toString() == context.dbName }[0]
        def tableId = sql("SHOW PROC '/dbs/${dbId}'").find { it[1].toString() == tableName }[0]
        def rows = sql("SHOW PROC '/dbs/${dbId}/${tableId}/index_schema'")
        return rows.find { it[1].toString() == mvName }[0].toString()
    }

    create_sync_mv(context.dbName, "t_create_mv_if_not_exists", "mv_create_if_not_exists_k1", """
        SELECT k1 AS mv_k1, SUM(v1) AS mv_sum_v1
        FROM t_create_mv_if_not_exists
        GROUP BY k1""")
    def k1IndexId = syncMvIndexId("t_create_mv_if_not_exists", "mv_create_if_not_exists_k1")

    // Both variants of IF NOT EXISTS must succeed and leave the existing MV untouched.
    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_create_if_not_exists_k1 AS
        SELECT k1 AS mv_k1, SUM(v1) AS mv_sum_v1
        FROM t_create_mv_if_not_exists
        GROUP BY k1
    """
    assertEquals(k1IndexId, syncMvIndexId("t_create_mv_if_not_exists", "mv_create_if_not_exists_k1"))
    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_create_if_not_exists_k1 AS
        SELECT k2 AS mv_k2, SUM(v1) AS mv_sum_v1_2
        FROM t_create_mv_if_not_exists
        GROUP BY k2
    """
    assertEquals(k1IndexId, syncMvIndexId("t_create_mv_if_not_exists", "mv_create_if_not_exists_k1"))

    // IF NOT EXISTS must still create the sync MV when the name is free.
    sql """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_create_if_not_exists_k2 AS
        SELECT k2 AS mv_k2, SUM(v1) AS mv_sum_v1_2
        FROM t_create_mv_if_not_exists
        GROUP BY k2
    """
    waitingMVTaskFinishedByMvName(context.dbName, "t_create_mv_if_not_exists", "mv_create_if_not_exists_k2")

    order_qt_select_k1 "SELECT * FROM t_create_mv_if_not_exists INDEX mv_create_if_not_exists_k1 ORDER BY 1"
    order_qt_select_k2 "SELECT * FROM t_create_mv_if_not_exists INDEX mv_create_if_not_exists_k2 ORDER BY 1"
}
