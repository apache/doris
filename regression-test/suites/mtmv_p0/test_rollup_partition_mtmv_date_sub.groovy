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

suite("test_rollup_partition_mtmv_date_sub", "mtmv") {
    // Test date_sub (negative hour offset) partition roll-up
    // This covers the HoursSub predicate generation path in UpdateMvByPartitionCommand

    def baseTableName = "t_rollup_mtmv_date_sub"
    def mvName = "mv_rollup_mtmv_date_sub"

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${baseTableName}"""

    // Create base table with datetime partition column
    // Start from a specific date to avoid datetime underflow when subtracting hours
    sql """
        CREATE TABLE ${baseTableName} (
            id INT,
            k2 DATETIME NOT NULL
        )
        PARTITION BY RANGE(k2) (
            PARTITION p20250724 VALUES [('2025-07-24 00:00:00'), ('2025-07-25 00:00:00')),
            PARTITION p20250725 VALUES [('2025-07-25 00:00:00'), ('2025-07-26 00:00:00')),
            PARTITION p20250726 VALUES [('2025-07-26 00:00:00'), ('2025-07-27 00:00:00')),
            PARTITION p20250727 VALUES [('2025-07-27 00:00:00'), ('2025-07-28 00:00:00'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ('replication_num' = '1');
    """

    // Insert test data spanning multiple days
    // With date_sub(k2, INTERVAL 8 HOUR), we shift timestamps backward:
    // - 2025-07-24 10:00:00 - 8h = 2025-07-24 02:00:00 -> day trunc = 2025-07-24
    // - 2025-07-25 05:00:00 - 8h = 2025-07-24 21:00:00 -> day trunc = 2025-07-24
    // - 2025-07-25 12:00:00 - 8h = 2025-07-25 04:00:00 -> day trunc = 2025-07-25
    // - 2025-07-26 07:00:00 - 8h = 2025-07-25 23:00:00 -> day trunc = 2025-07-25
    // - 2025-07-26 20:00:00 - 8h = 2025-07-26 12:00:00 -> day trunc = 2025-07-26
    // - 2025-07-27 03:00:00 - 8h = 2025-07-26 19:00:00 -> day trunc = 2025-07-26
    sql """
        INSERT INTO ${baseTableName} VALUES
        (1, '2025-07-24 10:00:00'),
        (2, '2025-07-25 05:00:00'),
        (3, '2025-07-25 12:00:00'),
        (4, '2025-07-26 07:00:00'),
        (5, '2025-07-26 20:00:00'),
        (6, '2025-07-27 03:00:00');
    """

    // Create MV with date_sub hour offset partition expression
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by (day_alias)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
            SELECT date_trunc(date_sub(k2, INTERVAL 8 HOUR), 'day') AS day_alias, count(*) AS cnt
            FROM ${baseTableName}
            GROUP BY day_alias;
    """

    waitingMTMVTaskFinishedByMvName(mvName)

    // Verify MV partitions were created correctly
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: ${showPartitionsResult}")

    // Expected: 3 MV partitions for days 2025-07-24, 2025-07-25, 2025-07-26
    assertTrue(showPartitionsResult.size() >= 3, "Expected at least 3 MV partitions")

    // Verify MV data
    def mvRows = sql """
        SELECT date_format(day_alias, '%Y-%m-%d') AS day, cnt
        FROM ${mvName}
        ORDER BY day
    """
    logger.info("mvRows: ${mvRows}")

    // Expected counts per day after date_sub:
    // 2025-07-24: 2 rows (id=1,2)
    // 2025-07-25: 2 rows (id=3,4)
    // 2025-07-26: 2 rows (id=5,6)
    assertEquals(3, mvRows.size())
    assertEquals("2025-07-24", mvRows[0][0])
    assertEquals(2, mvRows[0][1])
    assertEquals("2025-07-25", mvRows[1][0])
    assertEquals(2, mvRows[1][1])
    assertEquals("2025-07-26", mvRows[2][0])
    assertEquals(2, mvRows[2][1])

    // Test incremental refresh - insert new data and refresh specific partition
    sql """
        INSERT INTO ${baseTableName} VALUES
        (7, '2025-07-25 10:00:00'),
        (8, '2025-07-26 09:00:00');
    """
    // id=7: 2025-07-25 10:00 - 8h = 2025-07-25 02:00 -> day 2025-07-25
    // id=8: 2025-07-26 09:00 - 8h = 2025-07-26 01:00 -> day 2025-07-26

    // Refresh the partition containing 2025-07-25
    sql """REFRESH MATERIALIZED VIEW ${mvName} partitions(p_20250725000000_20250726000000)"""
    waitingMTMVTaskFinishedByMvName(mvName)

    def mvRowsAfterRefresh = sql """
        SELECT date_format(day_alias, '%Y-%m-%d') AS day, cnt
        FROM ${mvName}
        WHERE day_alias = '2025-07-25 00:00:00'
    """
    logger.info("mvRowsAfterRefresh for 2025-07-25: ${mvRowsAfterRefresh}")
    // After refresh, 2025-07-25 should have 3 rows (id=3,4,7)
    assertEquals(1, mvRowsAfterRefresh.size())
    assertEquals("2025-07-25", mvRowsAfterRefresh[0][0])
    assertEquals(3, mvRowsAfterRefresh[0][1])

    // Test query rewrite with date_sub MV
    def querySql = """
        SELECT date_trunc(date_sub(k2, INTERVAL 8 HOUR), 'day') AS day_alias, count(*) AS cnt
        FROM ${baseTableName}
        GROUP BY day_alias
        ORDER BY day_alias
    """

    // Check that MV rewrite is possible
    explain {
        sql("${querySql}")
        contains("${mvName}")
    }

    // Cleanup
    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${baseTableName}"""
}
