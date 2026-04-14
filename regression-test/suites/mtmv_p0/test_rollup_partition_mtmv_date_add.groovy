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

suite("test_rollup_partition_mtmv_date_add", "mtmv") {
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
        SELECT date_format(day_alias, '%Y-%m-%d %H:%i:%s') AS day_alias, cnt
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
        exception "invalid expression is minutes_add"
    }

    // DATETIMEV2(6) + 'T' separator + fractional seconds
    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add_datetimev2_formats"""
    sql """drop table if exists t_test_rollup_partition_mtmv_date_add_datetimev2_formats"""

    sql """
        CREATE TABLE t_test_rollup_partition_mtmv_date_add_datetimev2_formats (
          id BIGINT NOT NULL,
          k2 DATETIMEV2(6) NOT NULL
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
        INSERT INTO t_test_rollup_partition_mtmv_date_add_datetimev2_formats VALUES
            (1, "2025-07-24T21:01:23.123456"),
            (2, "2025-07-25 20:59:00.999999"),
            (3, "2025-07-25T21:10:00.000001");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add_datetimev2_formats
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (date_trunc(day_alias, 'day'))
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day') AS day_alias, count(*) AS cnt
            FROM t_test_rollup_partition_mtmv_date_add_datetimev2_formats
            GROUP BY day_alias;
    """

    waitingMTMVTaskFinishedByMvName("mv_test_rollup_partition_mtmv_date_add_datetimev2_formats")

    def v2ShowPartitionsResult = sql """show partitions from mv_test_rollup_partition_mtmv_date_add_datetimev2_formats"""
    assertEquals(2, v2ShowPartitionsResult.size())
    assertTrue(v2ShowPartitionsResult.toString().contains("2025-07-25 00:00:00"))
    assertTrue(v2ShowPartitionsResult.toString().contains("2025-07-26 00:00:00"))

    def v2MvRows = sql """
        SELECT date_format(day_alias, '%Y-%m-%d %H:%i:%s') AS day_alias, cnt
        FROM mv_test_rollup_partition_mtmv_date_add_datetimev2_formats
        ORDER BY day_alias
    """
    assertEquals(2, v2MvRows.size())
    assertEquals("2025-07-25 00:00:00", v2MvRows[0][0].toString())
    assertEquals("2", v2MvRows[0][1].toString())
    assertEquals("2025-07-26 00:00:00", v2MvRows[1][0].toString())
    assertEquals("1", v2MvRows[1][1].toString())

    // WEEK (week starts at Monday 00:00:00, so base partition boundary should be Sunday 21:00:00 for +3h shift)
    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add_week"""
    sql """drop table if exists t_test_rollup_partition_mtmv_date_add_week"""

    sql """
        CREATE TABLE t_test_rollup_partition_mtmv_date_add_week (
          id BIGINT NOT NULL,
          k2 DATETIMEV2(6) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_20250707 VALUES [("2025-07-06 21:00:00"),("2025-07-13 21:00:00")),
          PARTITION p_20250714 VALUES [("2025-07-13 21:00:00"),("2025-07-20 21:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    sql """
        INSERT INTO t_test_rollup_partition_mtmv_date_add_week VALUES
            (1, "2025-07-06 21:01:23.000000"),
            (2, "2025-07-13 20:59:00.000000"),
            (3, "2025-07-13 21:10:00.000000");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add_week
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (week_alias)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT date_trunc(date_add(k2, INTERVAL 3 HOUR), 'week') AS week_alias, count(*) AS cnt
            FROM t_test_rollup_partition_mtmv_date_add_week
            GROUP BY week_alias;
    """

    waitingMTMVTaskFinishedByMvName("mv_test_rollup_partition_mtmv_date_add_week")

    def weekPartitions = sql """show partitions from mv_test_rollup_partition_mtmv_date_add_week"""
    assertEquals(2, weekPartitions.size())
    assertTrue(weekPartitions.toString().contains("2025-07-07 00:00:00"))
    assertTrue(weekPartitions.toString().contains("2025-07-14 00:00:00"))

    def weekRows = sql """
        SELECT date_format(week_alias, '%Y-%m-%d %H:%i:%s') AS k, cnt
        FROM mv_test_rollup_partition_mtmv_date_add_week
        ORDER BY k
    """
    assertEquals(2, weekRows.size())
    assertEquals("2025-07-07 00:00:00", weekRows[0][0].toString())
    assertEquals("2", weekRows[0][1].toString())
    assertEquals("2025-07-14 00:00:00", weekRows[1][0].toString())
    assertEquals("1", weekRows[1][1].toString())

    // MONTH (base boundary should be (month_start - 3h) for +3h shift)
    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add_month"""
    sql """drop table if exists t_test_rollup_partition_mtmv_date_add_month"""

    sql """
        CREATE TABLE t_test_rollup_partition_mtmv_date_add_month (
          id BIGINT NOT NULL,
          k2 DATETIMEV2(6) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_202508 VALUES [("2025-07-31 21:00:00"),("2025-08-31 21:00:00")),
          PARTITION p_202509 VALUES [("2025-08-31 21:00:00"),("2025-09-30 21:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    sql """
        INSERT INTO t_test_rollup_partition_mtmv_date_add_month VALUES
            (1, "2025-07-31 21:01:23.000000"),
            (2, "2025-08-31 20:59:00.000000"),
            (3, "2025-08-31 21:10:00.000000");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add_month
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (month_alias)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT date_trunc(date_add(k2, INTERVAL 3 HOUR), 'month') AS month_alias, count(*) AS cnt
            FROM t_test_rollup_partition_mtmv_date_add_month
            GROUP BY month_alias;
    """

    waitingMTMVTaskFinishedByMvName("mv_test_rollup_partition_mtmv_date_add_month")

    def monthPartitions = sql """show partitions from mv_test_rollup_partition_mtmv_date_add_month"""
    assertEquals(2, monthPartitions.size())
    assertTrue(monthPartitions.toString().contains("2025-08-01 00:00:00"))
    assertTrue(monthPartitions.toString().contains("2025-09-01 00:00:00"))

    def monthRows = sql """
        SELECT date_format(month_alias, '%Y-%m-%d %H:%i:%s') AS k, cnt
        FROM mv_test_rollup_partition_mtmv_date_add_month
        ORDER BY k
    """
    assertEquals(2, monthRows.size())
    assertEquals("2025-08-01 00:00:00", monthRows[0][0].toString())
    assertEquals("2", monthRows[0][1].toString())
    assertEquals("2025-09-01 00:00:00", monthRows[1][0].toString())
    assertEquals("1", monthRows[1][1].toString())

    // QUARTER (base boundary should be (quarter_start - 3h) for +3h shift)
    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add_quarter"""
    sql """drop table if exists t_test_rollup_partition_mtmv_date_add_quarter"""

    sql """
        CREATE TABLE t_test_rollup_partition_mtmv_date_add_quarter (
          id BIGINT NOT NULL,
          k2 DATETIMEV2(6) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_2025q3 VALUES [("2025-06-30 21:00:00"),("2025-09-30 21:00:00")),
          PARTITION p_2025q4 VALUES [("2025-09-30 21:00:00"),("2025-12-31 21:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    sql """
        INSERT INTO t_test_rollup_partition_mtmv_date_add_quarter VALUES
            (1, "2025-06-30 21:01:23.000000"),
            (2, "2025-09-30 20:59:00.000000"),
            (3, "2025-09-30 21:10:00.000000");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add_quarter
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (quarter_alias)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT date_trunc(date_add(k2, INTERVAL 3 HOUR), 'quarter') AS quarter_alias, count(*) AS cnt
            FROM t_test_rollup_partition_mtmv_date_add_quarter
            GROUP BY quarter_alias;
    """

    waitingMTMVTaskFinishedByMvName("mv_test_rollup_partition_mtmv_date_add_quarter")

    def quarterPartitions = sql """show partitions from mv_test_rollup_partition_mtmv_date_add_quarter"""
    assertEquals(2, quarterPartitions.size())
    assertTrue(quarterPartitions.toString().contains("2025-07-01 00:00:00"))
    assertTrue(quarterPartitions.toString().contains("2025-10-01 00:00:00"))

    def quarterRows = sql """
        SELECT date_format(quarter_alias, '%Y-%m-%d %H:%i:%s') AS k, cnt
        FROM mv_test_rollup_partition_mtmv_date_add_quarter
        ORDER BY k
    """
    assertEquals(2, quarterRows.size())
    assertEquals("2025-07-01 00:00:00", quarterRows[0][0].toString())
    assertEquals("2", quarterRows[0][1].toString())
    assertEquals("2025-10-01 00:00:00", quarterRows[1][0].toString())
    assertEquals("1", quarterRows[1][1].toString())

    // YEAR (base boundary should be (year_start - 3h) for +3h shift)
    sql """drop materialized view if exists mv_test_rollup_partition_mtmv_date_add_year"""
    sql """drop table if exists t_test_rollup_partition_mtmv_date_add_year"""

    sql """
        CREATE TABLE t_test_rollup_partition_mtmv_date_add_year (
          id BIGINT NOT NULL,
          k2 DATETIMEV2(6) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        PARTITION BY range(k2)
        (
          PARTITION p_2025 VALUES [("2024-12-31 21:00:00"),("2025-12-31 21:00:00")),
          PARTITION p_2026 VALUES [("2025-12-31 21:00:00"),("2026-12-31 21:00:00"))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
    """

    sql """
        INSERT INTO t_test_rollup_partition_mtmv_date_add_year VALUES
            (1, "2024-12-31 21:01:23.000000"),
            (2, "2025-12-31 20:59:00.000000"),
            (3, "2025-12-31 21:10:00.000000");
    """

    sql """
        CREATE MATERIALIZED VIEW mv_test_rollup_partition_mtmv_date_add_year
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by (year_alias)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT date_trunc(date_add(k2, INTERVAL 3 HOUR), 'year') AS year_alias, count(*) AS cnt
            FROM t_test_rollup_partition_mtmv_date_add_year
            GROUP BY year_alias;
    """

    waitingMTMVTaskFinishedByMvName("mv_test_rollup_partition_mtmv_date_add_year")

    def yearPartitions = sql """show partitions from mv_test_rollup_partition_mtmv_date_add_year"""
    assertEquals(2, yearPartitions.size())
    assertTrue(yearPartitions.toString().contains("2025-01-01 00:00:00"))
    assertTrue(yearPartitions.toString().contains("2026-01-01 00:00:00"))

    def yearRows = sql """
        SELECT date_format(year_alias, '%Y-%m-%d %H:%i:%s') AS k, cnt
        FROM mv_test_rollup_partition_mtmv_date_add_year
        ORDER BY k
    """
    assertEquals(2, yearRows.size())
    assertEquals("2025-01-01 00:00:00", yearRows[0][0].toString())
    assertEquals("2", yearRows[0][1].toString())
    assertEquals("2026-01-01 00:00:00", yearRows[1][0].toString())
    assertEquals("1", yearRows[1][1].toString())
}
