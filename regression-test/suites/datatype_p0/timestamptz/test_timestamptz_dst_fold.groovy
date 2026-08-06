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

suite("test_timestamptz_dst_fold") {
    sql "SET enable_nereids_planner = true;"
    sql "SET enable_fallback_to_original_planner = false;"

    sql "DROP TABLE IF EXISTS tz_dst_fold_events;"
    sql "DROP TABLE IF EXISTS tz_dst_fold_trunc_out;"
    sql """
        CREATE TABLE tz_dst_fold_events (
            id INT,
            label VARCHAR(64),
            ts TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    sql "SET time_zone = 'America/New_York';"
    sql """
        INSERT INTO tz_dst_fold_events VALUES
        (1, 'pre_fold_utc',  CAST('2024-11-03 05:05:00 +00:00' AS TIMESTAMPTZ(6))),
        (2, 'post_fold_utc', CAST('2024-11-03 06:05:00 +00:00' AS TIMESTAMPTZ(6))),
        (3, 'pre_explicit',  CAST('2024-11-03 01:05:00 -04:00' AS TIMESTAMPTZ(6))),
        (4, 'post_explicit', CAST('2024-11-03 01:05:00 -05:00' AS TIMESTAMPTZ(6)));
    """

    sql "SET debug_skip_fold_constant = true;"
    qt_sql """
        SELECT id, label, CAST(ts AS VARCHAR(64)) AS rendered
        FROM tz_dst_fold_events
        ORDER BY id;
    """

    sql "SET debug_skip_fold_constant = false;"
    qt_sql """
        SELECT id, label
        FROM tz_dst_fold_events
        WHERE ts = CAST('2024-11-03 01:05:00 -05:00' AS TIMESTAMPTZ(6))
        ORDER BY id;
    """

    sql "SET debug_skip_fold_constant = true;"
    qt_sql """
        SELECT id,
               CAST(minute_floor(ts, 10) AS VARCHAR(64)) AS minute_floor_rendered,
               CAST(minute_ceil(ts, 10) AS VARCHAR(64)) AS minute_ceil_rendered,
               CAST(hour_floor(ts, 1) AS VARCHAR(64)) AS hour_floor_rendered,
               CAST(date_trunc(ts, 'second') AS VARCHAR(64)) AS second_trunc_rendered,
               CAST(date_trunc(ts, 'minute') AS VARCHAR(64)) AS minute_trunc_rendered,
               CAST(date_trunc(ts, 'hour') AS VARCHAR(64)) AS hour_trunc_rendered
        FROM tz_dst_fold_events
        WHERE id = 4
        ORDER BY id;
    """

    sql """
        CREATE TABLE tz_dst_fold_trunc_out (
            second_trunc TIMESTAMPTZ(6),
            minute_trunc TIMESTAMPTZ(6),
            hour_trunc TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(second_trunc)
        DISTRIBUTED BY HASH(second_trunc) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO tz_dst_fold_trunc_out
        SELECT date_trunc(ts, 'second'),
               date_trunc(ts, 'minute'),
               date_trunc(ts, 'hour')
        FROM tz_dst_fold_events
        WHERE id = 4;
    """

    sql "SET time_zone = '+00:00';"
    qt_sql """
        SELECT CAST(second_trunc AS VARCHAR(64)) AS stored_second_utc,
               CAST(minute_trunc AS VARCHAR(64)) AS stored_minute_utc,
               CAST(hour_trunc AS VARCHAR(64)) AS stored_hour_utc
        FROM tz_dst_fold_trunc_out
        ORDER BY 1, 2, 3;
    """

    sql "SET time_zone = default;"
    sql "SET debug_skip_fold_constant = false;"
}
