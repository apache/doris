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

suite("test_dynamic_partition_timestamptz_timezone") {
    sql "drop table if exists test_dp_timestamptz_shanghai"
    sql "drop table if exists test_dp_timestamptz_utc"

    // Table A: TIMESTAMPTZ dynamic partition with Asia/Shanghai timezone
    // A DAY boundary at Asia/Shanghai midnight should map to 16:00 UTC
    // (Asia/Shanghai is UTC+8, no DST).
    sql """
        CREATE TABLE test_dp_timestamptz_shanghai (
            k1 TIMESTAMPTZ NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.time_zone" = "Asia/Shanghai",
            "dynamic_partition.start" = "-1",
            "dynamic_partition.end" = "1",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.create_history_partition" = "true"
        )
    """

    // Table B: TIMESTAMPTZ dynamic partition with UTC timezone (reference)
    // A DAY boundary at UTC midnight should map to 00:00 UTC.
    sql """
        CREATE TABLE test_dp_timestamptz_utc (
            k1 TIMESTAMPTZ NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.time_zone" = "UTC",
            "dynamic_partition.start" = "-1",
            "dynamic_partition.end" = "1",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.create_history_partition" = "true"
        )
    """

    def createShanghai = sql "SHOW CREATE TABLE test_dp_timestamptz_shanghai"
    def createUtc = sql "SHOW CREATE TABLE test_dp_timestamptz_utc"
    logger.info("Shanghai create: ${createShanghai}")
    logger.info("UTC create: ${createUtc}")

    def shStr = createShanghai[0][1]
    def utcStr = createUtc[0][1]

    // Verify that both tables have their dynamic_partition.time_zone set correctly
    assertTrue(shStr.contains('"dynamic_partition.time_zone" = "Asia/Shanghai"'),
        "Shanghai table should have time_zone=Asia/Shanghai")
    assertTrue(utcStr.contains('"dynamic_partition.time_zone" = "UTC"'),
        "UTC table should have time_zone=UTC")

    // Verify both tables have the expected number of partitions (yesterday, today, tomorrow)
    def partsShanghai = sql "SHOW PARTITIONS FROM test_dp_timestamptz_shanghai"
    def partsUtc = sql "SHOW PARTITIONS FROM test_dp_timestamptz_utc"
    logger.info("Shanghai partitions: ${partsShanghai}")
    logger.info("UTC partitions: ${partsUtc}")
    assertEquals(3, partsShanghai.size(),
        "Shanghai table should have 3 partitions (yesterday, today, tomorrow)")
    assertEquals(3, partsUtc.size(),
        "UTC table should have 3 partitions (yesterday, today, tomorrow)")

    // Insert data: midnight and afternoon in their respective timezones
    sql """
        INSERT INTO test_dp_timestamptz_shanghai VALUES
        (CAST('2026-06-17 16:00:00 UTC' AS TIMESTAMPTZ), 1),
        (CAST('2026-06-18 08:00:00 UTC' AS TIMESTAMPTZ), 2)
    """
    sql """
        INSERT INTO test_dp_timestamptz_utc VALUES
        (CAST('2026-06-18 00:00:00 UTC' AS TIMESTAMPTZ), 1),
        (CAST('2026-06-18 16:00:00 UTC' AS TIMESTAMPTZ), 2)
    """

    // Verify data can be read back
    def shanghaiRows = sql "SELECT COUNT(*) FROM test_dp_timestamptz_shanghai"
    assertEquals(2, shanghaiRows[0][0].toInteger(),
        "Shanghai table should have 2 rows")
    def utcRows = sql "SELECT COUNT(*) FROM test_dp_timestamptz_utc"
    assertEquals(2, utcRows[0][0].toInteger(),
        "UTC table should have 2 rows")

    // Query with UTC session timezone for deterministic output
    sql "SET time_zone = '+00:00'"
    order_qt_shanghai_data """
        SELECT CAST(k1 AS STRING), v1
        FROM test_dp_timestamptz_shanghai
        ORDER BY v1
    """
    order_qt_utc_data """
        SELECT CAST(k1 AS STRING), v1
        FROM test_dp_timestamptz_utc
        ORDER BY v1
    """
}
