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

suite("test_timestamptz_year_partition_pruning") {
    sql "SET time_zone = '+08:00'"
    sql "DROP TABLE IF EXISTS test_timestamptz_year_partition_pruning"
    sql """
        CREATE TABLE test_timestamptz_year_partition_pruning (
            ts TIMESTAMPTZ(6) NOT NULL
        ) DUPLICATE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION leap_day VALUES [('2024-02-28 12:00:00 +00:00'), ('2024-03-01 00:00:00 +00:00')),
            PARTITION dst_overlap VALUES [('2024-11-03 05:45:00 +00:00'), ('2024-11-03 06:45:00 +00:00')),
            PARTITION dst_gap VALUES [('2023-03-10 07:10:00.900000 +00:00'),
                                      ('2023-03-10 08:10:00.900000 +00:00'))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_timestamptz_year_partition_pruning VALUES
            ('2024-02-28 16:30:00 +00:00'),
            ('2024-11-03 06:15:00 +00:00'),
            ('2023-03-10 07:30:00.100000 +00:00')
    """

    // The leap-day partition contains this match below both folded UTC endpoints.
    order_qt_leap_add """
        SELECT ts FROM test_timestamptz_year_partition_pruning
        WHERE years_add(ts, 1) = CAST('2025-02-27 16:30:00 +00:00' AS TIMESTAMPTZ(6))
    """
    order_qt_leap_sub """
        SELECT ts FROM test_timestamptz_year_partition_pruning
        WHERE years_sub(ts, 1) = CAST('2023-02-27 16:30:00 +00:00' AS TIMESTAMPTZ(6))
    """

    sql "SET time_zone = 'America/New_York'"
    // Both endpoints are 01:45 locally, but the repeated hour contains 01:15.
    order_qt_overlap_add """
        SELECT ts FROM test_timestamptz_year_partition_pruning
        WHERE years_add(ts, 1) = CAST('2025-11-03 06:15:00 +00:00' AS TIMESTAMPTZ(6))
    """
    order_qt_overlap_sub """
        SELECT ts FROM test_timestamptz_year_partition_pruning
        WHERE years_sub(ts, -1) = CAST('2025-11-03 06:15:00 +00:00' AS TIMESTAMPTZ(6))
    """
    // The gap shifts local 02:10 forward; local 03:10 is already outside the gap.
    // Both endpoints become 07:10:00.900000 UTC, below the interior match at 07:30:00.100000.
    order_qt_gap_add """
        SELECT ts FROM test_timestamptz_year_partition_pruning
        WHERE years_add(ts, 1) = CAST('2024-03-10 07:30:00.100000 +00:00' AS TIMESTAMPTZ(6))
    """
    order_qt_gap_sub """
        SELECT ts FROM test_timestamptz_year_partition_pruning
        WHERE years_sub(ts, -1) = CAST('2024-03-10 07:30:00.100000 +00:00' AS TIMESTAMPTZ(6))
    """
}
