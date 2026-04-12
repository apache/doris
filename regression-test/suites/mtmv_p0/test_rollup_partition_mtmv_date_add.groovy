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

suite("test_rollup_partition_mtmv_date_add") {
    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add"""
    sql """drop table if exists t_test_rollup_partition_mtmv_date_add"""

    sql """
        CREATE TABLE t_test_rollup_partition_mtmv_date_add (
          id BIGINT NOT NULL,
          k2 DATETIME NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_20250724 VALUES [("2025-07-24 21:00:00"),("2025-07-25 21:00:00")),
          PARTITION p_20250725 VALUES [("2025-07-25 21:00:00"),("2025-07-26 21:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    sql """
        INSERT INTO t_test_rollup_partition_mtmv_date_add VALUES
            (1, "2025-07-24 21:01:23"),
            (2, "2025-07-25 20:59:00"),
            (3, "2025-07-25 21:10:00");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(day_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day') AS day_alias, count(*) AS cnt
            FROM t_test_rollup_partition_mtmv_date_add
            GROUP BY day_alias;
    """
    waitingMTMVTaskFinishedByMvName("mv_test_rollup_partition_mtmv_date_add")
    def showPartitionsResult = sql """show partitions from mv_test_rollup_partition_mtmv_date_add"""
    assertEquals(2, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("2025-07-25 00:00:00"))
    assertTrue(showPartitionsResult.toString().contains("2025-07-26 00:00:00"))

    def mvRows = sql """
        SELECT day_alias, cnt
        FROM mv_test_rollup_partition_mtmv_date_add
        ORDER BY day_alias
    """
    assertEquals(2, mvRows.size())
    assertEquals("2025-07-25 00:00:00", mvRows[0][0].toString())
    assertEquals("2", mvRows[0][1].toString())
    assertEquals("2025-07-26 00:00:00", mvRows[1][0].toString())
    assertEquals("1", mvRows[1][1].toString())

    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add"""

    test {
        sql """
            CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL
                partition by (date_trunc(day_alias, 'day'))
                DISTRIBUTED BY RANDOM BUCKETS 1
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT date_trunc(date_add(k2, INTERVAL 30 MINUTE), 'day') AS day_alias, count(*) AS cnt
                FROM t_test_rollup_partition_mtmv_date_add
                GROUP BY day_alias;
        """
        exception "unsupported argument"
    }
}
