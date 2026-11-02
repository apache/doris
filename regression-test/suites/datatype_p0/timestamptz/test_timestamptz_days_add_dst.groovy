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

suite("test_timestamptz_days_add_dst") {
    sql "SET time_zone = 'America/New_York'"
    sql "DROP TABLE IF EXISTS timestamptz_days_add_dst"
    sql """
        CREATE TABLE timestamptz_days_add_dst (id INT, ts TIMESTAMPTZ(6), delta INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql """
        INSERT INTO timestamptz_days_add_dst VALUES
        (1, '2024-03-09 12:00:00.123456-05:00', 1),
        (2, '2024-11-02 12:00:00.123456-04:00', 1),
        (3, '2024-03-10 12:00:00.123456-04:00', -1),
        (4, '2024-11-03 12:00:00.123456-05:00', -1),
        (5, '2024-02-01 12:00:00.123456-05:00', 1),
        (6, '2024-11-03 01:30:00.123456-05:00', 0),
        (7, '2024-11-02 01:30:00.123456-04:00', 1),
        (8, '2024-11-04 01:30:00.123456-05:00', -1),
        (9, '2024-03-09 02:30:00.123456-05:00', 1),
        (10, NULL, 1),
        (11, '2024-03-09 12:00:00.123456-05:00', NULL),
        (12, '2024-11-03 01:50:00.123456-04:00', 1),
        (13, '2024-11-03 01:10:00.123456-05:00', 1)
    """
    sql "DROP TABLE IF EXISTS timestamptz_days_zero_events"
    sql "DROP TABLE IF EXISTS timestamptz_days_zero_ids"
    sql """
        CREATE TABLE timestamptz_days_zero_events (id INT, ts TIMESTAMPTZ(6))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql """
        CREATE TABLE timestamptz_days_zero_ids (id INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql """
        INSERT INTO timestamptz_days_zero_events VALUES
        (1, '2024-01-01 00:00:00+00:00'),
        (2, '0000-01-01 00:00:00+00:00'),
        (3, '0000-02-28 20:00:00.123456+00:00'),
        (4, '0000-03-01 01:00:00.123456+00:00')
    """
    sql "INSERT INTO timestamptz_days_zero_ids VALUES (1)"
    try {
        for (def skipFold : [false, true]) {
            sql "SET debug_skip_fold_constant = ${skipFold}"
            def tag = skipFold ? "be" : "fe"
            // Literal expressions exercise FE constant folding and BE constant evaluation separately.
            for (def input : ["2024-03-09 12:00:00.123456-05:00",
                               "2024-11-02 12:00:00.123456-04:00",
                               "2024-02-01 12:00:00.123456-05:00"]) {
                "order_qt_${tag}_literal"("""
                    SELECT CAST(ts AS STRING), CAST(days_add(ts, 1) AS STRING),
                           CAST(hours_add(ts, 24) AS STRING), seconds_diff(days_add(ts, 1), ts),
                           CAST(days_sub(days_add(ts, 1), 1) AS STRING),
                           CAST(date_add(ts, INTERVAL 1 DAY) AS STRING),
                           CAST(date_sub(ts, INTERVAL -1 DAY) AS STRING)
                    FROM (SELECT CAST('${input}' AS TIMESTAMPTZ(6)) AS ts) t
                """)
            }
            "order_qt_${tag}_overlap_gap"("""
                SELECT CAST(days_add(CAST('2024-11-03 01:30:00.123456-05:00' AS TIMESTAMPTZ(6)), 0)
                            AS STRING),
                       CAST(days_add(CAST('2024-11-02 01:30:00.123456-04:00' AS TIMESTAMPTZ(6)), 1)
                            AS STRING),
                       CAST(days_sub(CAST('2024-11-04 01:30:00.123456-05:00' AS TIMESTAMPTZ(6)), 1)
                            AS STRING),
                       CAST(days_add(CAST('2024-03-09 02:30:00.123456-05:00' AS TIMESTAMPTZ(6)), 1)
                            AS STRING)
            """)
            // Cover vector/vector, vector/constant, constant/vector, and NULL propagation.
            "order_qt_${tag}_columns"("""
                SELECT id, CAST(days_add(ts, delta) AS STRING), CAST(days_sub(ts, -delta) AS STRING),
                       seconds_diff(days_add(ts, delta), ts), CAST(days_add(ts, 1) AS STRING),
                       CAST(days_add(CAST('2024-03-09 12:00:00.123456-05:00' AS TIMESTAMPTZ(6)), delta)
                            AS STRING)
                FROM timestamptz_days_add_dst ORDER BY id
            """)
            "order_qt_${tag}_non_monotonic"("""
                SELECT id, CAST(days_add(ts, 1) AS STRING)
                FROM timestamptz_days_add_dst
                WHERE id IN (12, 13)
                  AND days_add(ts, 1) < CAST('2024-11-04 01:30:00-05:00' AS TIMESTAMPTZ(6))
                ORDER BY id
            """)
            // Zero days must remain safe to push before the join, even for a boundary row
            // whose local display year is -1. Return only ids to avoid formatting that row.
            sql "SET runtime_filter_mode = 'OFF'"
            "order_qt_${tag}_zero_join"("""
                SELECT id FROM (
                    SELECT e.id, days_add(e.ts, 0) AS shifted
                    FROM timestamptz_days_zero_events e JOIN timestamptz_days_zero_ids i ON e.id = i.id
                ) s WHERE shifted >= CAST('2000-01-01 00:00:00+00:00' AS TIMESTAMPTZ(6)) ORDER BY id
            """)
            sql "SET runtime_filter_mode = default"
            sql "SET time_zone = '+08:00'"
            "order_qt_${tag}_year_zero_east"("""
                SELECT CAST(days_add(ts, -1) AS STRING),
                       CAST(days_sub(CAST('0000-02-28 20:00:00.123456+00:00' AS TIMESTAMPTZ(6)), 1)
                            AS STRING),
                       days_add(CAST('9999-12-31 23:00:00+00:00' AS TIMESTAMPTZ(6)), 0)
                            = CAST('9999-12-31 23:00:00+00:00' AS TIMESTAMPTZ(6))
                FROM timestamptz_days_zero_events WHERE id = 3
            """)
            sql "SET time_zone = '-08:00'"
            "order_qt_${tag}_year_zero_west"("""
                SELECT CAST(days_add(ts, 1) AS STRING),
                       CAST(days_add(CAST('0000-03-01 01:00:00.123456+00:00' AS TIMESTAMPTZ(6)), 1)
                            AS STRING)
                FROM timestamptz_days_zero_events WHERE id = 4
            """)
            sql "SET time_zone = 'UTC'"
            "order_qt_${tag}_utc"("""
                SELECT CAST(days_add(ts, 1) AS STRING), CAST(hours_add(ts, 24) AS STRING),
                       seconds_diff(days_add(ts, 1), ts)
                FROM (SELECT CAST('2024-03-09 12:00:00.123456-05:00' AS TIMESTAMPTZ(6)) AS ts) t
            """)
            sql "SET time_zone = 'America/New_York'"
        }
    } finally {
        sql "SET debug_skip_fold_constant = false"
        sql "SET runtime_filter_mode = default"
        sql "SET time_zone = default"
    }
}
