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

suite("test_union_compensation_mtmv_date_add_hour_offset", "mtmv") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_materialized_view_nest_rewrite=true"
    sql "SET enable_materialized_view_union_rewrite=true"
    sql "SET enable_nereids_timeout=false"

    sql """drop materialized view if exists mv_test_union_compensation_mtmv_date_add_hour_offset"""
    sql """drop table if exists t_test_union_compensation_mtmv_date_add_hour_offset"""

    sql """
        CREATE TABLE t_test_union_compensation_mtmv_date_add_hour_offset (
          id BIGINT NOT NULL,
          k2 DATETIME NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_20250724 VALUES [("2025-07-23 21:00:00"),("2025-07-24 21:00:00")),
          PARTITION p_20250725 VALUES [("2025-07-24 21:00:00"),("2025-07-25 21:00:00")),
          PARTITION p_20250726 VALUES [("2025-07-25 21:00:00"),("2025-07-26 21:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    // Two mv partitions: 2025-07-25 and 2025-07-26 (with +3 hour shift).
    sql """
        INSERT INTO t_test_union_compensation_mtmv_date_add_hour_offset VALUES
            (1, "2025-07-24 22:00:00"),
            (2, "2025-07-25 20:00:00"),
            (3, "2025-07-25 22:00:00"),
            (4, "2025-07-26 10:00:00");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_union_compensation_mtmv_date_add_hour_offset
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(day_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT id, date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day') AS day_alias
            FROM t_test_union_compensation_mtmv_date_add_hour_offset;
    """

    def showPartitionsResult = sql """show partitions from mv_test_union_compensation_mtmv_date_add_hour_offset"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())

    String partitionToRefresh = null
    for (def row : showPartitionsResult) {
        boolean containsLower = row.any { it != null && it.toString().contains("2025-07-26 00:00:00") }
        boolean containsUpper = row.any { it != null && it.toString().contains("2025-07-27 00:00:00") }
        if (!(containsLower && containsUpper)) {
            continue
        }
        for (def cell : row) {
            if (cell != null && cell.toString().startsWith("p_")) {
                partitionToRefresh = cell.toString()
                break
            }
        }
        if (partitionToRefresh != null) {
            break
        }
    }
    assertTrue(partitionToRefresh != null)

    sql """
        REFRESH MATERIALIZED VIEW mv_test_union_compensation_mtmv_date_add_hour_offset partitions(${partitionToRefresh});
    """
    waitingMTMVTaskFinishedByMvName("mv_test_union_compensation_mtmv_date_add_hour_offset")

    def mvRows = sql """
        SELECT date_format(day_alias, '%Y-%m-%d %H:%i:%s') AS k, count(*) AS cnt
        FROM mv_test_union_compensation_mtmv_date_add_hour_offset
        GROUP BY k
        ORDER BY k;
    """
    assertEquals(1, mvRows.size())
    assertEquals("2025-07-26 00:00:00", mvRows[0][0].toString())
    assertEquals("2", mvRows[0][1].toString())

    def querySql = """
        SELECT id, date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day') AS day_alias
        FROM t_test_union_compensation_mtmv_date_add_hour_offset
        ORDER BY id
    """
    mv_rewrite_success_without_check_chosen(querySql, "mv_test_union_compensation_mtmv_date_add_hour_offset")

    def explainResult = sql """ explain ${querySql} """
    logger.info("explainResult: " + explainResult.toString())
    assertTrue(explainResult.toString().contains("VUNION"))

    def queryRows = sql """ ${querySql} """
    assertEquals(4, queryRows.size())
    assertEquals("1", queryRows[0][0].toString())
    assertEquals("2025-07-25 00:00:00", queryRows[0][1].toString())
    assertEquals("2", queryRows[1][0].toString())
    assertEquals("2025-07-25 00:00:00", queryRows[1][1].toString())
    assertEquals("3", queryRows[2][0].toString())
    assertEquals("2025-07-26 00:00:00", queryRows[2][1].toString())
    assertEquals("4", queryRows[3][0].toString())
    assertEquals("2025-07-26 00:00:00", queryRows[3][1].toString())

    // Base partitions are UTC-midnight ([00:00, 00:00)), but MV rolls up by local-day via +3h shift.
    // This requires 1->N related-partition mapping when refreshing MV partitions.
    sql """drop materialized view if exists mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight"""
    sql """drop table if exists t_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight"""

    sql """
        CREATE TABLE t_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight (
          id BIGINT NOT NULL,
          k2 DATETIME NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_20250724 VALUES [("2025-07-24 00:00:00"),("2025-07-25 00:00:00")),
          PARTITION p_20250725 VALUES [("2025-07-25 00:00:00"),("2025-07-26 00:00:00")),
          PARTITION p_20250726 VALUES [("2025-07-26 00:00:00"),("2025-07-27 00:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    // Two mv partitions: 2025-07-25 and 2025-07-26 (with +3 hour shift).
    sql """
        INSERT INTO t_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight VALUES
            (1, "2025-07-24 22:00:00"),
            (2, "2025-07-25 20:00:00"),
            (3, "2025-07-25 22:00:00"),
            (4, "2025-07-26 10:00:00");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(day_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT id, date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day') AS day_alias
            FROM t_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight;
    """

    def midnightShowPartitionsResult = sql """show partitions from mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight"""
    logger.info("midnightShowPartitionsResult: " + midnightShowPartitionsResult.toString())

    String midnightPartitionToRefresh = null
    for (def row : midnightShowPartitionsResult) {
        boolean containsLower = row.any { it != null && it.toString().contains("2025-07-26 00:00:00") }
        boolean containsUpper = row.any { it != null && it.toString().contains("2025-07-27 00:00:00") }
        if (!(containsLower && containsUpper)) {
            continue
        }
        for (def cell : row) {
            if (cell != null && cell.toString().startsWith("p_")) {
                midnightPartitionToRefresh = cell.toString()
                break
            }
        }
        if (midnightPartitionToRefresh != null) {
            break
        }
    }
    assertTrue(midnightPartitionToRefresh != null)

    sql """
        REFRESH MATERIALIZED VIEW mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight partitions(${midnightPartitionToRefresh});
    """
    waitingMTMVTaskFinishedByMvName("mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight")

    def midnightMvRows = sql """
        SELECT date_format(day_alias, '%Y-%m-%d %H:%i:%s') AS k, count(*) AS cnt
        FROM mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight
        GROUP BY k
        ORDER BY k;
    """
    assertEquals(1, midnightMvRows.size())
    assertEquals("2025-07-26 00:00:00", midnightMvRows[0][0].toString())
    assertEquals("2", midnightMvRows[0][1].toString())

    def midnightQuerySql = """
        SELECT id, date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day') AS day_alias
        FROM t_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight
        ORDER BY id
    """
    mv_rewrite_success_without_check_chosen(midnightQuerySql, "mv_test_union_compensation_mtmv_date_add_hour_offset_utc_midnight")

    def midnightExplainResult = sql """ explain ${midnightQuerySql} """
    logger.info("midnightExplainResult: " + midnightExplainResult.toString())
    assertTrue(midnightExplainResult.toString().contains("VUNION"))

    def midnightQueryRows = sql """ ${midnightQuerySql} """
    assertEquals(4, midnightQueryRows.size())
    assertEquals("1", midnightQueryRows[0][0].toString())
    assertEquals("2025-07-25 00:00:00", midnightQueryRows[0][1].toString())
    assertEquals("2", midnightQueryRows[1][0].toString())
    assertEquals("2025-07-25 00:00:00", midnightQueryRows[1][1].toString())
    assertEquals("3", midnightQueryRows[2][0].toString())
    assertEquals("2025-07-26 00:00:00", midnightQueryRows[2][1].toString())
    assertEquals("4", midnightQueryRows[3][0].toString())
    assertEquals("2025-07-26 00:00:00", midnightQueryRows[3][1].toString())
}
