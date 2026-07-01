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

import java.time.ZonedDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

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

    // -- Verify configuration --
    def createShanghai = sql "SHOW CREATE TABLE test_dp_timestamptz_shanghai"
    def createUtc = sql "SHOW CREATE TABLE test_dp_timestamptz_utc"
    def shStr = createShanghai[0][1]
    def utcStr = createUtc[0][1]
    assertTrue(shStr.contains('"dynamic_partition.time_zone" = "Asia/Shanghai"'),
        "Shanghai table should have time_zone=Asia/Shanghai")
    assertTrue(utcStr.contains('"dynamic_partition.time_zone" = "UTC"'),
        "UTC table should have time_zone=UTC")

    // -- Compute expected dates in each timezone --
    def shanghaiZone = ZoneId.of("Asia/Shanghai")
    def utcZone = ZoneId.of("UTC")
    def now = ZonedDateTime.now()
    def todayShanghai = now.withZoneSameInstant(shanghaiZone).toLocalDate()
    def todayUtc = now.withZoneSameInstant(utcZone).toLocalDate()
    def dateFmt = DateTimeFormatter.ofPattern("yyyyMMdd")
    def tsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def expectedShanghaiNames = [
        "p" + todayShanghai.minusDays(1).format(dateFmt),
        "p" + todayShanghai.format(dateFmt),
        "p" + todayShanghai.plusDays(1).format(dateFmt)
    ]
    def expectedUtcNames = [
        "p" + todayUtc.minusDays(1).format(dateFmt),
        "p" + todayUtc.format(dateFmt),
        "p" + todayUtc.plusDays(1).format(dateFmt)
    ]

    // -- Verify partition count --
    def partsShanghai = sql "SHOW PARTITIONS FROM test_dp_timestamptz_shanghai"
    def partsUtc = sql "SHOW PARTITIONS FROM test_dp_timestamptz_utc"
    logger.info("Shanghai partitions: ${partsShanghai}")
    logger.info("UTC partitions: ${partsUtc}")
    assertEquals(3, partsShanghai.size(),
        "Shanghai table should have 3 partitions (yesterday, today, tomorrow)")
    assertEquals(3, partsUtc.size(),
        "UTC table should have 3 partitions (yesterday, today, tomorrow)")

    // -- Verify partition names use LOCAL time (regression test for timezone naming fix) --
    // SHOW PARTITIONS column 0=PartitionId, column 1=PartitionName
    def shanghaiPartNames = partsShanghai.collect { it[1] }.sort()
    def utcPartNames = partsUtc.collect { it[1] }.sort()
    logger.info("Shanghai part names: ${shanghaiPartNames}, expected: ${expectedShanghaiNames.sort()}")
    logger.info("UTC part names: ${utcPartNames}, expected: ${expectedUtcNames.sort()}")
    assertEquals(expectedShanghaiNames.sort(), shanghaiPartNames,
        "Shanghai partition names must use Shanghai local dates, not UTC dates")
    assertEquals(expectedUtcNames.sort(), utcPartNames,
        "UTC partition names must use UTC dates")

    // -- Insert data: timestamps that fall within today's partition --
    // For Shanghai: today's partition covers [today 00:00 CST, tomorrow 00:00 CST)
    //               = [today-1 16:00 UTC, today 16:00 UTC)
    // We insert a value at today midday CST (= today 04:00 UTC)
    def todayShanghaiMidnight = ZonedDateTime.of(todayShanghai.atStartOfDay(), shanghaiZone)
    def shanghaiInsert1 = todayShanghaiMidnight.withHour(8)  // 08:00 CST = 00:00 UTC
    def shanghaiInsert2 = todayShanghaiMidnight.withHour(12) // 12:00 CST = 04:00 UTC
    sql """INSERT INTO test_dp_timestamptz_shanghai VALUES
           ('${shanghaiInsert1.withZoneSameInstant(utcZone).format(tsFmt)}+00:00', 1),
           ('${shanghaiInsert2.withZoneSameInstant(utcZone).format(tsFmt)}+00:00', 2)"""

    // For UTC: today's partition covers [today 00:00 UTC, tomorrow 00:00 UTC)
    def todayUtcMidnight = ZonedDateTime.of(todayUtc.atStartOfDay(), utcZone)
    def utcInsert1 = todayUtcMidnight.withHour(0)   // 00:00 UTC
    def utcInsert2 = todayUtcMidnight.withHour(12)  // 12:00 UTC
    sql """INSERT INTO test_dp_timestamptz_utc VALUES
           ('${utcInsert1.format(tsFmt)}+00:00', 1),
           ('${utcInsert2.format(tsFmt)}+00:00', 2)"""

    // -- Verify data can be read back --
    def shanghaiRows = sql "SELECT COUNT(*) FROM test_dp_timestamptz_shanghai"
    assertEquals(2, shanghaiRows[0][0].toInteger(),
        "Shanghai table should have 2 rows")
    def utcRows = sql "SELECT COUNT(*) FROM test_dp_timestamptz_utc"
    assertEquals(2, utcRows[0][0].toInteger(),
        "UTC table should have 2 rows")
}
