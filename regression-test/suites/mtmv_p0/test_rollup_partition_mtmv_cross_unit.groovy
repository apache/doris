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

suite("test_rollup_partition_mtmv_cross_unit", "mtmv") {
    // Comprehensive test for ALL valid PARTITION BY / SELECT unit combinations
    // Valid: PARTITION unit <= SELECT unit (in granularity)
    // hour < day < week < month < quarter < year
    //
    // Total: 21 combinations × 2 directions (add/sub) = 42 test cases

    def createBaseTable = { tableName, partitions ->
        sql """drop table if exists ${tableName}"""
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                k2 DATETIME NOT NULL
            )
            PARTITION BY RANGE(k2) (
                ${partitions}
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ('replication_num' = '1');
        """
    }

    def createAndTestMV = { mvName, tableName, partitionExpr, selectUnit, offset, isAdd ->
        def funcName = isAdd ? "date_add" : "date_sub"
        def aliasName = "${selectUnit}_alias"
        
        sql """drop materialized view if exists ${mvName}"""
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL
                partition by (${aliasName})
                DISTRIBUTED BY RANDOM BUCKETS 1
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT date_trunc(${funcName}(k2, INTERVAL ${offset} HOUR), '${selectUnit}') AS ${aliasName}, count(*) AS cnt
                FROM ${tableName}
                GROUP BY ${aliasName};
        """
        waitingMTMVTaskFinishedByMvName(mvName)
        def partitions = sql """show partitions from ${mvName}"""
        assertTrue(partitions.size() >= 1, "${mvName} should create at least 1 partition, got ${partitions.size()}")
        return partitions.size()
    }

    // =========================================================================
    // HOUR PARTITION BASE TABLE (for hour → hour/day/week/month/quarter/year)
    // =========================================================================
    
    createBaseTable("t_hour_base", """
        PARTITION p1 VALUES [('2025-07-25 18:00:00'), ('2025-07-25 21:00:00')),
        PARTITION p2 VALUES [('2025-07-25 21:00:00'), ('2025-07-26 00:00:00'))
    """)
    sql """INSERT INTO t_hour_base VALUES (1, '2025-07-25 19:00:00'), (2, '2025-07-25 22:00:00');"""

    // hour → hour (date_add)
    createAndTestMV("mv_hour_to_hour_add", "t_hour_base", "hour", "hour", 3, true)
    // hour → day (date_add)
    createAndTestMV("mv_hour_to_day_add", "t_hour_base", "hour", "day", 3, true)
    // hour → week (date_add)
    createAndTestMV("mv_hour_to_week_add", "t_hour_base", "hour", "week", 3, true)
    // hour → month (date_add)
    createAndTestMV("mv_hour_to_month_add", "t_hour_base", "hour", "month", 3, true)
    // hour → quarter (date_add)
    createAndTestMV("mv_hour_to_quarter_add", "t_hour_base", "hour", "quarter", 3, true)
    // hour → year (date_add)
    createAndTestMV("mv_hour_to_year_add", "t_hour_base", "hour", "year", 3, true)

    // hour → hour (date_sub)
    createAndTestMV("mv_hour_to_hour_sub", "t_hour_base", "hour", "hour", 3, false)
    // hour → day (date_sub)
    createAndTestMV("mv_hour_to_day_sub", "t_hour_base", "hour", "day", 3, false)
    // hour → week (date_sub)
    createAndTestMV("mv_hour_to_week_sub", "t_hour_base", "hour", "week", 3, false)
    // hour → month (date_sub)
    createAndTestMV("mv_hour_to_month_sub", "t_hour_base", "hour", "month", 3, false)
    // hour → quarter (date_sub)
    createAndTestMV("mv_hour_to_quarter_sub", "t_hour_base", "hour", "quarter", 3, false)
    // hour → year (date_sub)
    createAndTestMV("mv_hour_to_year_sub", "t_hour_base", "hour", "year", 3, false)

    // =========================================================================
    // DAY PARTITION BASE TABLE (for day → day/week/month/quarter/year)
    // =========================================================================

    createBaseTable("t_day_base", """
        PARTITION p1 VALUES [('2025-07-24 21:00:00'), ('2025-07-25 21:00:00')),
        PARTITION p2 VALUES [('2025-07-25 21:00:00'), ('2025-07-26 21:00:00'))
    """)
    sql """INSERT INTO t_day_base VALUES (1, '2025-07-24 22:00:00'), (2, '2025-07-25 22:00:00');"""

    // day → day (date_add)
    createAndTestMV("mv_day_to_day_add", "t_day_base", "day", "day", 3, true)
    // day → week (date_add)
    createAndTestMV("mv_day_to_week_add", "t_day_base", "day", "week", 3, true)
    // day → month (date_add)
    createAndTestMV("mv_day_to_month_add", "t_day_base", "day", "month", 3, true)
    // day → quarter (date_add)
    createAndTestMV("mv_day_to_quarter_add", "t_day_base", "day", "quarter", 3, true)
    // day → year (date_add)
    createAndTestMV("mv_day_to_year_add", "t_day_base", "day", "year", 3, true)

    // day → day (date_sub)
    createAndTestMV("mv_day_to_day_sub", "t_day_base", "day", "day", 3, false)
    // day → week (date_sub)
    createAndTestMV("mv_day_to_week_sub", "t_day_base", "day", "week", 3, false)
    // day → month (date_sub)
    createAndTestMV("mv_day_to_month_sub", "t_day_base", "day", "month", 3, false)
    // day → quarter (date_sub)
    createAndTestMV("mv_day_to_quarter_sub", "t_day_base", "day", "quarter", 3, false)
    // day → year (date_sub)
    createAndTestMV("mv_day_to_year_sub", "t_day_base", "day", "year", 3, false)

    // =========================================================================
    // WEEK PARTITION BASE TABLE (for week → week/month/quarter/year)
    // =========================================================================

    createBaseTable("t_week_base", """
        PARTITION p1 VALUES [('2025-07-06 21:00:00'), ('2025-07-13 21:00:00')),
        PARTITION p2 VALUES [('2025-07-13 21:00:00'), ('2025-07-20 21:00:00'))
    """)
    sql """INSERT INTO t_week_base VALUES (1, '2025-07-07 22:00:00'), (2, '2025-07-14 22:00:00');"""

    // week → week (date_add)
    createAndTestMV("mv_week_to_week_add", "t_week_base", "week", "week", 3, true)
    // week → month (date_add)
    createAndTestMV("mv_week_to_month_add", "t_week_base", "week", "month", 3, true)
    // week → quarter (date_add)
    createAndTestMV("mv_week_to_quarter_add", "t_week_base", "week", "quarter", 3, true)
    // week → year (date_add)
    createAndTestMV("mv_week_to_year_add", "t_week_base", "week", "year", 3, true)

    // week → week (date_sub)
    createAndTestMV("mv_week_to_week_sub", "t_week_base", "week", "week", 3, false)
    // week → month (date_sub)
    createAndTestMV("mv_week_to_month_sub", "t_week_base", "week", "month", 3, false)
    // week → quarter (date_sub)
    createAndTestMV("mv_week_to_quarter_sub", "t_week_base", "week", "quarter", 3, false)
    // week → year (date_sub)
    createAndTestMV("mv_week_to_year_sub", "t_week_base", "week", "year", 3, false)

    // =========================================================================
    // MONTH PARTITION BASE TABLE (for month → month/quarter/year)
    // =========================================================================

    createBaseTable("t_month_base", """
        PARTITION p1 VALUES [('2025-06-30 21:00:00'), ('2025-07-31 21:00:00')),
        PARTITION p2 VALUES [('2025-07-31 21:00:00'), ('2025-08-31 21:00:00'))
    """)
    sql """INSERT INTO t_month_base VALUES (1, '2025-07-15 22:00:00'), (2, '2025-08-15 22:00:00');"""

    // month → month (date_add)
    createAndTestMV("mv_month_to_month_add", "t_month_base", "month", "month", 3, true)
    // month → quarter (date_add)
    createAndTestMV("mv_month_to_quarter_add", "t_month_base", "month", "quarter", 3, true)
    // month → year (date_add)
    createAndTestMV("mv_month_to_year_add", "t_month_base", "month", "year", 3, true)

    // month → month (date_sub)
    createAndTestMV("mv_month_to_month_sub", "t_month_base", "month", "month", 3, false)
    // month → quarter (date_sub)
    createAndTestMV("mv_month_to_quarter_sub", "t_month_base", "month", "quarter", 3, false)
    // month → year (date_sub)
    createAndTestMV("mv_month_to_year_sub", "t_month_base", "month", "year", 3, false)

    // =========================================================================
    // QUARTER PARTITION BASE TABLE (for quarter → quarter/year)
    // =========================================================================

    createBaseTable("t_quarter_base", """
        PARTITION p1 VALUES [('2025-03-31 21:00:00'), ('2025-06-30 21:00:00')),
        PARTITION p2 VALUES [('2025-06-30 21:00:00'), ('2025-09-30 21:00:00'))
    """)
    sql """INSERT INTO t_quarter_base VALUES (1, '2025-05-15 22:00:00'), (2, '2025-08-15 22:00:00');"""

    // quarter → quarter (date_add)
    createAndTestMV("mv_quarter_to_quarter_add", "t_quarter_base", "quarter", "quarter", 3, true)
    // quarter → year (date_add)
    createAndTestMV("mv_quarter_to_year_add", "t_quarter_base", "quarter", "year", 3, true)

    // quarter → quarter (date_sub)
    createAndTestMV("mv_quarter_to_quarter_sub", "t_quarter_base", "quarter", "quarter", 3, false)
    // quarter → year (date_sub)
    createAndTestMV("mv_quarter_to_year_sub", "t_quarter_base", "quarter", "year", 3, false)

    // =========================================================================
    // YEAR PARTITION BASE TABLE (for year → year)
    // =========================================================================

    createBaseTable("t_year_base", """
        PARTITION p1 VALUES [('2024-12-31 21:00:00'), ('2025-12-31 21:00:00')),
        PARTITION p2 VALUES [('2025-12-31 21:00:00'), ('2026-12-31 21:00:00'))
    """)
    sql """INSERT INTO t_year_base VALUES (1, '2025-06-15 22:00:00'), (2, '2026-06-15 22:00:00');"""

    // year → year (date_add)
    createAndTestMV("mv_year_to_year_add", "t_year_base", "year", "year", 3, true)

    // year → year (date_sub)
    createAndTestMV("mv_year_to_year_sub", "t_year_base", "year", "year", 3, false)

    // =========================================================================
    // Summary: 42 MV combinations tested
    // hour:    6 units × 2 directions = 12
    // day:     5 units × 2 directions = 10
    // week:    4 units × 2 directions = 8
    // month:   3 units × 2 directions = 6
    // quarter: 2 units × 2 directions = 4
    // year:    1 unit  × 2 directions = 2
    // Total: 12 + 10 + 8 + 6 + 4 + 2 = 42
    // =========================================================================
}
