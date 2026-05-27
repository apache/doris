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

suite("test_timestamptz_utc_functions") {
    sql "SET enable_nereids_planner = true;"
    sql "SET enable_fallback_to_original_planner = false;"

    // Fall-back DST fold: 
    // lhs UTC=2024-11-03 06:05:00, NY local=2024-11-03 01:05:00;
    // rhs UTC=2024-11-03 05:55:00, NY local=2024-11-03 01:55:00.
    // Expected: TIMESTAMPTZ functions use UTC instants, so lhs - rhs = +10 minutes, not local -50 minutes.
    def lhs = "CAST('2024-11-03 01:05:00 -05:00' AS TIMESTAMPTZ(6))"
    def rhs = "CAST('2024-11-03 01:55:00 -04:00' AS TIMESTAMPTZ(6))"

    // Spring-forward DST gap: 
    // lhs UTC=2024-03-10 07:05:00, NY local=2024-03-10 03:05:00;
    // rhs UTC=2024-03-10 06:55:00, NY local=2024-03-10 01:55:00.
    // Expected: TIMESTAMPTZ functions use UTC instants, so lhs - rhs = +10 minutes, not local +70 minutes.
    def springLhs = "CAST('2024-03-10 03:05:00 -04:00' AS TIMESTAMPTZ(6))"
    def springRhs = "CAST('2024-03-10 01:55:00 -05:00' AS TIMESTAMPTZ(6))"

    // Mixed scale: lhs UTC=2024-11-03 06:05:00.123, rhs UTC=2024-11-03 05:55:00.
    // Expected: lhs - rhs = +600123 ms, and non-default TIMESTAMPTZ scales bind directly.
    def scale3Lhs = "CAST('2024-11-03 01:05:00.123 -05:00' AS TIMESTAMPTZ(3))"
    def scale0Rhs = "CAST('2024-11-03 01:55:00 -04:00' AS TIMESTAMPTZ(0))"

    // Scalar diff results should be identical in UTC and America/New_York sessions.
    sql "SET time_zone = '+00:00';"
    qt_timestamptz_diff_utc """
        SELECT milliseconds_diff(${lhs}, ${rhs}) AS ms_diff,
               microseconds_diff(${lhs}, ${rhs}) AS us_diff,
               seconds_diff(${lhs}, ${rhs}) AS sec_diff,
               minutes_diff(${lhs}, ${rhs}) AS min_diff,
               hours_diff(${lhs}, ${rhs}) AS hour_diff,
               days_diff(${lhs}, ${rhs}) AS day_diff,
               weeks_diff(${lhs}, ${rhs}) AS week_diff,
               months_diff(${lhs}, ${rhs}) AS month_diff,
               quarters_diff(${lhs}, ${rhs}) AS quarter_diff,
               years_diff(${lhs}, ${rhs}) AS year_diff,
               datediff(${lhs}, ${rhs}) AS date_diff,
               timediff(${lhs}, ${rhs}) AS time_diff;
    """

    sql "SET time_zone = 'America/New_York';"
    qt_timestamptz_diff_ny """
        SELECT milliseconds_diff(${lhs}, ${rhs}) AS ms_diff,
               microseconds_diff(${lhs}, ${rhs}) AS us_diff,
               seconds_diff(${lhs}, ${rhs}) AS sec_diff,
               minutes_diff(${lhs}, ${rhs}) AS min_diff,
               hours_diff(${lhs}, ${rhs}) AS hour_diff,
               days_diff(${lhs}, ${rhs}) AS day_diff,
               weeks_diff(${lhs}, ${rhs}) AS week_diff,
               months_diff(${lhs}, ${rhs}) AS month_diff,
               quarters_diff(${lhs}, ${rhs}) AS quarter_diff,
               years_diff(${lhs}, ${rhs}) AS year_diff,
               datediff(${lhs}, ${rhs}) AS date_diff,
               timediff(${lhs}, ${rhs}) AS time_diff;
    """

    qt_timestamptz_diff_spring_forward_ny """
        SELECT milliseconds_diff(${springLhs}, ${springRhs}) AS ms_diff,
               seconds_diff(${springLhs}, ${springRhs}) AS sec_diff,
               minutes_diff(${springLhs}, ${springRhs}) AS min_diff,
               hours_diff(${springLhs}, ${springRhs}) AS hour_diff,
               timediff(${springLhs}, ${springRhs}) AS time_diff;
    """

    qt_timestamptz_diff_mixed_scale_ny """
        SELECT milliseconds_diff(${scale3Lhs}, ${scale0Rhs}) AS ms_diff,
               microseconds_diff(${scale3Lhs}, ${scale0Rhs}) AS us_diff,
               seconds_diff(${scale3Lhs}, ${scale0Rhs}) AS sec_diff,
               timediff(${scale3Lhs}, ${scale0Rhs}) AS time_diff;
    """

    testFoldConst("""
        SELECT milliseconds_diff(${lhs}, ${rhs}),
               seconds_diff(${lhs}, ${rhs}),
               timediff(${lhs}, ${rhs});
    """)

    sql "DROP TABLE IF EXISTS tz_utc_function_events;"
    sql """
        CREATE TABLE tz_utc_function_events (
            id INT,
            grp INT,
            weight BIGINT,
            ts TIMESTAMPTZ(6),
            e1 BOOLEAN,
            e2 BOOLEAN
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO tz_utc_function_events VALUES
        (1, 1, 10, CAST('2024-11-03 01:55:00 -04:00' AS TIMESTAMPTZ(6)), true, false),
        (2, 1, 20, CAST('2024-11-03 01:05:00 -05:00' AS TIMESTAMPTZ(6)), false, true),
        (3, 1, 30, CAST('2024-11-03 01:30:00 -05:00' AS TIMESTAMPTZ(6)), false, false),
        (4, 2, 40, NULL, true, false),
        (5, 3, 50, CAST('2024-03-10 01:55:00 -05:00' AS TIMESTAMPTZ(6)), true, false),
        (6, 3, 60, CAST('2024-03-10 03:05:00 -04:00' AS TIMESTAMPTZ(6)), false, true),
        (7, 3, 70, CAST('2024-03-10 03:30:00 -04:00' AS TIMESTAMPTZ(6)), false, false);
    """

    // Aggregates should also compare TIMESTAMPTZ values by UTC instant.
    sql "SET time_zone = '+00:00';"
    qt_timestamptz_agg_utc """
        SELECT sequence_match('(?1)(?t<=600)(?2)', ts, e1, e2) AS seq_match,
               sequence_count('(?1)(?t<=600)(?2)', ts, e1, e2) AS seq_count,
               window_funnel(600, 'default', ts, e1, e2) AS funnel_v1
        FROM tz_utc_function_events
        WHERE grp = 1;
    """

    sql "SET time_zone = 'America/New_York';"
    qt_timestamptz_agg_ny """
        SELECT sequence_match('(?1)(?t<=600)(?2)', ts, e1, e2) AS seq_match,
               sequence_count('(?1)(?t<=600)(?2)', ts, e1, e2) AS seq_count,
               window_funnel(600, 'default', ts, e1, e2) AS funnel_v1
        FROM tz_utc_function_events
        WHERE grp = 1;
    """

    // Grouped aggregation verifies both DST shapes.
    order_qt_timestamptz_agg_by_group_ny """
        SELECT grp,
               sequence_match('(?1)(?t<=600)(?2)', ts, e1, e2) AS seq_match,
               sequence_count('(?1)(?t<=600)(?2)', ts, e1, e2) AS seq_count,
               window_funnel(600, 'default', ts, e1, e2) AS funnel_v1
        FROM tz_utc_function_events
        WHERE grp IN (1, 3)
        GROUP BY grp
        ORDER BY grp;
    """

    // Column inputs should also use UTC instant semantics.
    qt_timestamptz_column_diff_ny """
        SELECT milliseconds_diff(t2.ts, t1.ts) AS ms_diff,
               seconds_diff(t2.ts, t1.ts) AS sec_diff,
               timediff(t2.ts, t1.ts) AS time_diff
        FROM tz_utc_function_events t1, tz_utc_function_events t2
        WHERE t1.id = 1 AND t2.id = 2;
    """

    // topn_weighted should preserve TIMESTAMPTZ as the result array item type.
    sql "SET time_zone = '+00:00';"
    order_qt_timestamptz_topn_weighted """
        SELECT grp,
               CAST(topn_weighted(ts, weight, 2)[1] AS VARCHAR(64)) AS top_ts,
               CAST(topn_weighted(ts, weight, 2, 100)[1] AS VARCHAR(64)) AS top_ts_with_default
        FROM tz_utc_function_events
        WHERE grp IN (1, 3)
        GROUP BY grp
        ORDER BY grp;
    """

    // NULL input remains NULL, and sequence_count ignores the NULL timestamp row.
    qt_timestamptz_null """
        SELECT milliseconds_diff(MAX(ts), NULL), sequence_count('(?1)(?2)', ts, e1, e2)
        FROM tz_utc_function_events
        WHERE grp = 2;
    """

    sql "SET time_zone = default;"
}
