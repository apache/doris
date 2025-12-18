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
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
suite("test_timestamptz_storage_agg_key") {

    def timezone_str = "+08:00"
    sql "set time_zone = '${timezone_str}'; "

    // default value
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_default_value_no_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_default_value_no_scale` (
          `ts_tz` TIMESTAMPTZ default current_timestamp,
          `ts_tz_replace` TIMESTAMPTZ replace default current_timestamp,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL default current_timestamp,
          `ts_tz_min` TIMESTAMPTZ min default current_timestamp,
          `ts_tz_max` TIMESTAMPTZ max default current_timestamp,
          `int_val` INT sum
        ) AGGREGATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
    insert into timestamptz_storage_agg_key_default_value_no_scale(int_val) VALUES (1), (2), (3);
    """
    qt_default_value_no_scale """
        SELECT int_val FROM timestamptz_storage_agg_key_default_value_no_scale order by int_val;
    """
    def zoned_now = ZonedDateTime.now(ZoneId.of(timezone_str))
    def formatter_no_scale = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX")

    for (col_name in ["ts_tz ", "ts_tz_replace", "ts_tz_replace_if_not_null", "ts_tz_min", "ts_tz_max"]) {
        def query_result = sql """ 
            SELECT cast(${col_name} as string) FROM timestamptz_storage_agg_key_default_value_no_scale;
        """
        assertEquals(1, query_result.size())
        for (row in query_result) {
            def query_result_value = row[0].toString()
            println("row: " + row + ", column: " + query_result_value)
            def query_result_value_zdt  = ZonedDateTime.parse(query_result_value, formatter_no_scale)
            def diff_in_seconds = ChronoUnit.SECONDS.between(query_result_value_zdt, zoned_now)
            assertTrue(diff_in_seconds >=0 && diff_in_seconds < 60)
        }
    }

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_default_value_with_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_default_value_with_scale` (
          `ts_tz` TIMESTAMPTZ(6) default current_timestamp(6),
          `ts_tz_replace` TIMESTAMPTZ(6) replace default current_timestamp(6),
          `ts_tz_replace_if_not_null` TIMESTAMPTZ(6) REPLACE_IF_NOT_NULL default current_timestamp(6),
          `ts_tz_min` TIMESTAMPTZ(6) min default current_timestamp(6),
          `ts_tz_max` TIMESTAMPTZ(6) max default current_timestamp(6),
          `int_val` INT sum
        ) AGGREGATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
    insert into timestamptz_storage_agg_key_default_value_with_scale(int_val) VALUES (1), (2), (3);
    """
    qt_default_value_with_scale """
        SELECT int_val FROM timestamptz_storage_agg_key_default_value_with_scale order by int_val;
    """
    zoned_now = ZonedDateTime.now(ZoneId.of(timezone_str))
    def formatter_with_scale = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX")

    for (col_name in ["ts_tz ", "ts_tz_replace", "ts_tz_replace_if_not_null", "ts_tz_min", "ts_tz_max"]) {
        def query_result = sql """ 
            SELECT cast(${col_name} as string) FROM timestamptz_storage_agg_key_default_value_with_scale;
        """
        assertEquals(1, query_result.size())
        for (row in query_result) {
            def query_result_value = row[0].toString()
            println("row: " + row + ", column: " + query_result_value)
            def query_result_value_zdt  = ZonedDateTime.parse(query_result_value, formatter_with_scale)
            def diff_in_seconds = ChronoUnit.SECONDS.between(query_result_value_zdt, zoned_now)
            assertTrue(diff_in_seconds >=0 && diff_in_seconds < 60)
        }
    }

    def partition_value0 = '0000-01-01 08:00:01+08:00'
    def partition_value1 = '2023-01-02 00:00:00+08:00'
    def partition_value2 = '2023-08-08 20:20:21+08:00'
    def partition_value3 = '9999-12-31 23:59:59+08:00'
    def expected_partitions = [
        [start: "0000-01-01 00:00:00+00:00", end: partition_value0],
        [start: partition_value0, end: partition_value1],
        [start: partition_value1, end: partition_value2],
        [start: partition_value2, end: partition_value3]
    ]

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_no_scale_no_max_partition`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_no_scale_no_max_partition` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_replace` TIMESTAMPTZ replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ min,
          `ts_tz_max` TIMESTAMPTZ max,
        ) AGGREGATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p0 VALUES LESS THAN ('${partition_value0}'),
            PARTITION p1 VALUES LESS THAN ('${partition_value1}'),
            PARTITION p2 VALUES LESS THAN ('${partition_value2}'),
            PARTITION p3 VALUES LESS THAN ('${partition_value3}')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_agg_key_no_scale_no_max_partition VALUES ('9999-12-31 23:59:59 +08:00', null, null, null, null);
        """
        exception "no partition for this tuple"
    }


    def show_result = sql """
    show create table timestamptz_storage_agg_key_no_scale_no_max_partition;
    """
    println "show create table result: ${show_result}"

    def partitionPattern = /\[\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'\),\s*\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'\)\)/
    def partitionRanges = []
    show_result[0][1].findAll(partitionPattern) { match, startTime, endTime ->
        partitionRanges << [start: startTime, end: endTime]
    }
    println "found partitions: ${partitionRanges}"
    assertEquals(4, partitionRanges.size())
    partitionRanges.eachWithIndex { range, index ->
        println "  partition ${index}: [${range.start}, ${range.end})"
        def replaceYear0000 = { str ->
            return str.startsWith("0000-") ? str.replaceFirst("^0000-", "0001-") : str
        }
        def expected_start_str = replaceYear0000(expected_partitions[index].start)
        def expected_end_str = replaceYear0000(expected_partitions[index].end)
        def actual_start_str = replaceYear0000(range.start)
        def actual_end_str = replaceYear0000(range.end)

        def expected_start = ZonedDateTime.parse(expected_start_str, formatter_no_scale)
        def expected_end = ZonedDateTime.parse(expected_end_str, formatter_no_scale)
        def actual_start = ZonedDateTime.parse(actual_start_str, formatter_no_scale)
        def actual_end = ZonedDateTime.parse(actual_end_str, formatter_no_scale)

        assertTrue(actual_start.isEqual(expected_start))
        assertTrue(actual_end.isEqual(expected_end))
    }

    expected_partitions = [
        [start: "0000-01-01 00:00:00+00:00", end: partition_value0],
        [start: partition_value0, end: partition_value1],
        [start: partition_value1, end: partition_value2],
        [start: partition_value2, end: partition_value3],
        [start: partition_value3, end: "MAXVALUE"],
    ]
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_no_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_no_scale` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_replace` TIMESTAMPTZ replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ min,
          `ts_tz_max` TIMESTAMPTZ max,
        ) AGGREGATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p0 VALUES LESS THAN ('${partition_value0}'),
            PARTITION p1 VALUES LESS THAN ('${partition_value1}'),
            PARTITION p2 VALUES LESS THAN ('${partition_value2}'),
            PARTITION p3 VALUES LESS THAN ('${partition_value3}'),
            PARTITION p_max VALUES LESS THAN (MAXVALUE)
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_no_scale VALUES
    (null, '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 08:00:00 +08:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('2025-12-12 12:12:12 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('9999-12-30 23:59:59 +08:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00'),
    ('9999-12-31 23:59:58 +08:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00'),
    ('9999-12-31 23:59:59 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00');
    """
    qt_agg_key0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """INSERT INTO timestamptz_storage_agg_key_no_scale VALUES
    (null, '2025-12-12 12:12:12 +08:00', '2025-12-12 12:12:12 +08:00', '2000-01-01 00:00:00 +00:00', '2025-12-31 12:12:12 +08:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-02 00:00:00 +00:00', null, '0000-01-01 00:00:00 +00:00', '2000-01-02 00:00:00 +00:00'),
    ('9999-12-30 23:59:59 +08:00', '3023-10-10 10:10:10 -03:00', '3023-10-10 10:10:10 -03:00', '1023-10-10 10:10:10 -03:00', '3023-10-10 10:10:10 -03:00'),
    ('9999-12-31 23:59:58 +08:00', '9999-12-31 23:59:59 +08:00', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00');
    """
    qt_agg_key1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale ORDER BY 1, 2;
    """

    sql """INSERT INTO timestamptz_storage_agg_key_no_scale VALUES
    (null, null, null, null, null),
    ('0000-01-01 00:00:00 +00:00', null, null, null, null),
    ('9999-12-30 23:59:59 +08:00', null, null, null, null),
    ('9999-12-31 23:59:58 +08:00', null, null, null, null),
    ('9999-12-31 23:59:59 +08:00', null, null, null, null);
    """
    qt_agg_key2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale ORDER BY 1, 2;
    """

    // test =
    def ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz = '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p0)"));
    assertTrue(ret.toString().contains("tablets=1/16"));
    qt_eq0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz = '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz = '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p3)"));
    assertTrue(ret.toString().contains("tablets=1/16"));
    qt_eq1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz = '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz = '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p_max)"));
    assertTrue(ret.toString().contains("tablets=1/16"));
    qt_eq2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz = '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """

    // test !=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz != '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_neq0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz != '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz != '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_neq1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz != '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz != '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_neq2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz != '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """

    // test >
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz > '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_gt0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz > '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz > '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_gt1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz > '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz > '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p_max)"));
    assertTrue(ret.toString().contains("tablets=16/16"));
    qt_gt2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz > '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """

    // test >=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz >= '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_ge0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz >= '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz >= '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_ge1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz >= '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz >= '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p_max)"));
    assertTrue(ret.toString().contains("tablets=16/16"));
    qt_ge2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz >= '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """

    // test <
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz < '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_lt0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz < '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz < '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_lt1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz < '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz < '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=2/5 (p0,p3)"));
    assertTrue(ret.toString().contains("tablets=32/32"));
    qt_lt2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz < '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """

    // test <=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz <= '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p0)"));
    assertTrue(ret.toString().contains("tablets=16/16"));
    qt_le0 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz <= '0000-01-01 08:00:00+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz <= '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_le1 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz <= '2025-12-12 12:12:12+08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz <= '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_le2 """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz <= '9999-12-31 23:59:59+08:00' ORDER BY 1, 2;
    """

    // test IN
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz in('0000-01-01 08:00:00+08:00', '2025-12-12 12:12:12+08:00', '9999-12-31 23:59:59+08:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=12/48"));
    qt_agg_key_in """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz in('0000-01-01 08:00:00+08:00', '2025-12-12 12:12:12+08:00', '9999-12-31 23:59:59+08:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """

    // test not IN
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz not in('0000-01-01 08:00:00+08:00', '2025-12-12 12:12:12+08:00', '9999-12-31 23:59:59+08:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_agg_key_not_in """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz not in('0000-01-01 08:00:00+08:00', '2025-12-12 12:12:12+08:00', '9999-12-31 23:59:59+08:00', '2023-10-10 10:10:10 -03:00') ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz IS NULL ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("""IS NULL"""))
    qt_agg_key_is_null """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz IS NULL ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz IS NOT NULL ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("""IS NOT NULL"""))
    qt_agg_key_is_not_null """
        SELECT * FROM timestamptz_storage_agg_key_no_scale where ts_tz IS NOT NULL ORDER BY 1, 2;
    """

    // multi key columns
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_multi_key_cols`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_multi_key_cols` (
          `ts_tz` TIMESTAMPTZ,
          `name` VARCHAR(50),
          `ts_tz_replace` TIMESTAMPTZ replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ min,
          `ts_tz_max` TIMESTAMPTZ max,
        ) AGGREGATE KEY(`ts_tz`, `name`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_multi_key_cols VALUES
    (null, null, '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    (null, 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    (null, 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', null,   '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', null,   '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', null,   '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'jack', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'rose', '0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','0000-01-01 00:00:00 +00:00')
    """
    qt_multi_key_cols_all0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols ORDER BY 1, 2;
    """
    sql """
    INSERT INTO timestamptz_storage_agg_key_multi_key_cols VALUES
    (null, null, null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    (null, 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    (null, 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', null,   null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', null,   null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', null,   null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'jack', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'rose', null, null, '8999-12-31 23:59:59 +08:00','9000-01-01 00:00:00 +00:00');
    """

    qt_multi_key_cols_all1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols ORDER BY 1, 2;
    """

    sql """
    INSERT INTO timestamptz_storage_agg_key_multi_key_cols VALUES
    (null, null, '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    (null, 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    (null, 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', null,   '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('0000-01-01 03:00:00 +03:00', 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', null,   '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('2023-01-01 15:00:00 -05:00', 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', null,   '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'jack', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00'),
    ('9999-12-31 23:59:59 +08:00', 'rose', '2023-01-01 15:00:00 -05:00', '9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00','1000-01-01 00:00:00 +00:00');
    """
    qt_multi_key_cols_all2 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols ORDER BY 1, 2;
    """

    // test =
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz = '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_eq0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz = '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz = '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_eq1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz = '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test !=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz != '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_neq0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz != '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz != '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_neq1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz != '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test >
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz > '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_gt0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz > '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz > '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_gt1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz > '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test >=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz >= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_ge0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz >= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz >= '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_ge1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz >= '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test <
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz < '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_lt0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz < '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz < '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_lt1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz < '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test <=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz <= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_le0 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz <= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz <= '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_le1 """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz <= '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test in
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz in ('0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00') ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_in """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz in ('0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00') ORDER BY 1, 2, 3;
    """
    // test not in
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz not in ('0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00') ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_in """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz not in ('0000-01-01 00:00:00 +00:00', '9999-12-31 23:59:59 +08:00') ORDER BY 1, 2, 3;
    """
    // test is null
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz is null ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_in """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz is null ORDER BY 1, 2, 3;
    """
    // test is not null
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz is not null ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_in """
        SELECT * FROM timestamptz_storage_agg_key_multi_key_cols where ts_tz is not null ORDER BY 1, 2, 3;
    """

    // test agg key with scale
    def partition_value_with_scale0 = '0000-01-01 08:00:01.000001+08:00'
    def partition_value_with_scale1 = '2023-01-02 00:00:00.123456+08:00'
    def partition_value_with_scale2 = '2023-08-08 20:20:21.900000+08:00'
    def partition_value_with_scale3 = '9999-12-31 23:59:59.999999+08:00'
    def expected_partitions_with_scale = [
        [start: "0000-01-01 00:00:00.000000+00:00", end: partition_value_with_scale0],
        [start: partition_value_with_scale0, end: partition_value_with_scale1],
        [start: partition_value_with_scale1, end: partition_value_with_scale2],
        [start: partition_value_with_scale2, end: partition_value_with_scale3]
    ]
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale_no_max_partition`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale_no_max_partition` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_replace` TIMESTAMPTZ(6) replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ(6) REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ(6) min,
          `ts_tz_max` TIMESTAMPTZ(6) max
        ) AGGREGATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p0 VALUES LESS THAN ('${partition_value_with_scale0}'),
            PARTITION p1 VALUES LESS THAN ('${partition_value_with_scale1}'),
            PARTITION p2 VALUES LESS THAN ('${partition_value_with_scale2}'),
            PARTITION p3 VALUES LESS THAN ('${partition_value_with_scale3}')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    show_result = sql """
    show create table timestamptz_storage_agg_key_scale_no_max_partition;
    """
    println "show create table result: ${show_result}"

    def partitionPatternWithScale = /\[\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})'\),\s*\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}[+-]\d{2}:\d{2})'\)\)/
    partitionRanges = []
    show_result[0][1].findAll(partitionPatternWithScale) { match, startTime, endTime ->
        partitionRanges << [start: startTime, end: endTime]
    }
    println "found partitions: ${partitionRanges}"
    assertEquals(4, partitionRanges.size())
    partitionRanges.eachWithIndex { range, index ->
        println "  partition ${index}: [${range.start}, ${range.end})"
        def replaceYear0000 = { str ->
            return str.startsWith("0000-") ? str.replaceFirst("^0000-", "0001-") : str
        }
        def expected_start_str = replaceYear0000(expected_partitions_with_scale[index].start)
        def expected_end_str = replaceYear0000(expected_partitions_with_scale[index].end)
        def actual_start_str = replaceYear0000(range.start)
        def actual_end_str = replaceYear0000(range.end)

        def expected_start = ZonedDateTime.parse(expected_start_str, formatter_with_scale)
        def expected_end = ZonedDateTime.parse(expected_end_str, formatter_with_scale)
        def actual_start = ZonedDateTime.parse(actual_start_str, formatter_with_scale)
        def actual_end = ZonedDateTime.parse(actual_end_str, formatter_with_scale)

        assertTrue(actual_start.isEqual(expected_start))
        assertTrue(actual_end.isEqual(expected_end))
    }

    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_agg_key_scale_no_max_partition VALUES ('9999-12-31 23:59:59.999999 +08:00', null, null, null, null);
        """
        exception "no partition for this tuple"
    }

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_replace` TIMESTAMPTZ(6) replace,
          `ts_tz_replace_if_not_null` TIMESTAMPTZ(6) REPLACE_IF_NOT_NULL,
          `ts_tz_min` TIMESTAMPTZ(6) min,
          `ts_tz_max` TIMESTAMPTZ(6) max
        ) AGGREGATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p0 VALUES LESS THAN ('${partition_value_with_scale0}'),
            PARTITION p1 VALUES LESS THAN ('${partition_value_with_scale1}'),
            PARTITION p2 VALUES LESS THAN ('${partition_value_with_scale2}'),
            PARTITION p3 VALUES LESS THAN ('${partition_value_with_scale3}'),
            PARTITION p_max VALUES LESS THAN (MAXVALUE)
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """INSERT INTO timestamptz_storage_agg_key_scale VALUES
    (null, '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00','0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 08:00:00 +08:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('2025-12-12 12:12:12 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.000001 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.123456 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.999999 +08:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('9999-12-31 23:59:59 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('9999-12-31 23:59:59.000001 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('9999-12-31 23:59:59.123456 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00'),
    ('9999-12-31 23:59:59.999999 +08:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00');
    """
    qt_scale0 """
        SELECT * FROM timestamptz_storage_agg_key_scale ORDER BY 1, 2;
    """

    sql """INSERT INTO timestamptz_storage_agg_key_scale VALUES
    (null, '2025-12-12 12:12:12 +08:00', '2025-12-12 12:12:12 +08:00', '2000-01-01 00:00:00 +00:00', '2025-12-12 12:12:12 +08:00'),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 08:00:00 +08:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00'),
    ('0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00'),
    ('2025-12-12 12:12:12 +08:00', '3023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.000001 +08:00', '3023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00', '1023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.123456 +08:00', '3023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00', '1023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00'),
    ('2025-12-12 12:12:12.999999 +08:00', '3023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00', '1023-09-09 09:09:09 +01:00', '3023-09-09 09:09:09 +01:00'),
    ('9999-12-31 23:59:59 +08:00', '0000-01-01 00:00:00.000000+00:00', '0000-01-01 00:00:00.000000+00:00', '0000-01-01 00:00:00.000000+00:00', '0000-01-01 00:00:00.000000+00:00'),
    ('9999-12-31 23:59:59.000001 +08:00', '0000-01-01 00:00:00.000000+00:00', '9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000+00:00', '9999-12-31 23:59:59.999999 +08:00'),
    ('9999-12-31 23:59:59.123456 +08:00', '0000-01-01 00:00:00.000000+00:00', '9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000+00:00', '9999-12-31 23:59:59.999999 +08:00'),
    ('9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000+00:00', '9999-12-31 23:59:59.999999 +08:00', '0000-01-01 00:00:00.000000+00:00', '9999-12-31 23:59:59.999999 +08:00');
    """
    qt_scale1 """
        SELECT * FROM timestamptz_storage_agg_key_scale ORDER BY 1, 2;
    """

    // test =
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p0)"));
    assertTrue(ret.toString().contains("tablets=1/16"));
    qt_scale_eq0 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_eq1 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p_max)"));
    assertTrue(ret.toString().contains("tablets=1/16"));
    qt_scale_eq2 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz = '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """

    // test !=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_scale_neq0 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_neq1 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_scale_neq2 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz != '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """

    // test >
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_scale_gt0 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_gt1 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p_max)"));
    assertTrue(ret.toString().contains("tablets=16/16"));
    qt_scale_gt2 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz > '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """

    // test >=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_scale_ge0 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_ge1 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p_max)"));
    assertTrue(ret.toString().contains("tablets=16/16"));
    qt_scale_ge2 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz >= '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """

    // test <
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_lt0 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_lt1 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=2/5 (p0,p3)"));
    assertTrue(ret.toString().contains("tablets=32/32"));
    qt_scale_lt2 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz < '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """

    // test <=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=1/5 (p0)"));
    assertTrue(ret.toString().contains("tablets=16/16"));
    qt_scale_le0 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '0000-01-01 00:00:00.000000 +00:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_scale_le1 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '2025-12-12 12:12:12.123456 +08:00' ORDER BY 1, 2;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("partitions=3/5 (p0,p3,p_max)"));
    assertTrue(ret.toString().contains("tablets=48/48"));
    qt_scale_le2 """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz <= '9999-12-31 23:59:59.999999 +08:00' ORDER BY 1, 2;
    """

    qt_scale_in """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz in('0000-01-01 00:00:00.000000 +00:00', '0000-01-01 00:00:00.000001 +00:00', '2025-12-12 12:12:12.123456 +08:00', '9999-12-31 23:59:59.999999 +00:00') ORDER BY 1, 2;
    """
    qt_scale_not_in """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz not in('0000-01-01 00:00:00.000000 +00:00', '0000-01-01 00:00:00.000001 +00:00', '2025-12-12 12:12:12.123456 +08:00', '9999-12-31 23:59:59.999999 +00:00') ORDER BY 1, 2;
    """
    qt_scale_is_null """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz IS NULL ORDER BY 1, 2;
    """
    qt_scale_is_not_null """
        SELECT * FROM timestamptz_storage_agg_key_scale where ts_tz IS NOT NULL ORDER BY 1, 2;
    """

    // list partition
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale_list_partition`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale_list_partition` (
          `ts_tz` TIMESTAMPTZ(6),
          `VALUE` INT sum
        ) AGGREGATE KEY(`ts_tz`)
        partition by LIST(`ts_tz`) (
            PARTITION pnull VALUES IN (null),
            PARTITION p0 VALUES IN (
                '0000-01-01 00:00:00.000000+00:00',
                '0000-01-01 00:00:00.000001+00:00',
                '0000-01-01 00:00:00.123456+00:00',
                '0000-01-01 00:00:00.999999+00:00'),
            PARTITION p1 VALUES IN ('2023-01-02 00:00:00.123456+08:00'),
            PARTITION p2 VALUES IN ('2023-08-08 20:20:21.900000+08:00'),
            PARTITION p3 VALUES IN (
                '9999-12-31 23:59:59.000000+08:00',
                '9999-12-31 23:59:59.000001+08:00',
                '9999-12-31 23:59:59.123456+08:00',
                '9999-12-31 23:59:59.999999+08:00')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_scale_list_partition VALUES
    (null, -1),
    ('0000-01-01 00:00:00.000000+00:00', 0),
    ('0000-01-01 00:00:00.000001+00:00', 1),
    ('0000-01-01 00:00:00.123456+00:00', 2),
    ('0000-01-01 00:00:00.999999+00:00', 3),
    ('2023-01-02 00:00:00.123456+08:00', 4),
    ('2023-08-08 20:20:21.900000+08:00', 5),
    ('9999-12-31 23:59:59.000000+08:00', 6),
    ('9999-12-31 23:59:59.000001+08:00', 7),
    ('9999-12-31 23:59:59.123456+08:00', 8),
    ('9999-12-31 23:59:59.999999+08:00', 9);
    """
    qt_agg_key_scale_list_partition0 """
        SELECT * FROM timestamptz_storage_agg_key_scale_list_partition ORDER BY 1, 2;
    """

    // list partition, multi columns
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_scale_list_partition_multi_columns`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_agg_key_scale_list_partition_multi_columns` (
          `ts_tz` TIMESTAMPTZ(6),
          `name` VARCHAR(64),
          `VALUE` INT sum
        ) AGGREGATE KEY(`ts_tz`, `name`)
        partition by LIST(`ts_tz`, `name`) (
            PARTITION pnull VALUES IN ((null, null)),
            PARTITION p0 VALUES IN (
                ('0000-01-01 00:00:00.000000+00:00', 'jack'),
                ('0000-01-01 00:00:00.000001+00:00', 'jack'),
                ('0000-01-01 00:00:00.123456+00:00', 'jack'),
                ('0000-01-01 00:00:00.999999+00:00', 'jack')),
            PARTITION p1 VALUES IN (('2023-01-02 00:00:00.123456+08:00', 'rose')),
            PARTITION p2 VALUES IN (('2023-08-08 20:20:21.900000+08:00', 'lily')),
            PARTITION p3 VALUES IN (('9999-12-31 23:59:59.999999+08:00', 'tulip'))
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_agg_key_scale_list_partition_multi_columns VALUES
    (null, null, -1),
    ('0000-01-01 00:00:00.000000 +00:00', 'jack', 1),
    ('0000-01-01 00:00:00.000001 +00:00', 'jack', 2),
    ('0000-01-01 00:00:00.123456 +00:00', 'jack', 3),
    ('0000-01-01 00:00:00.999999 +00:00', 'jack', 4),
    ('2023-01-02 00:00:00.123456+08:00', 'rose', 5),
    ('2023-08-08 20:20:21.900000+08:00', 'lily', 6),
    ('9999-12-31 23:59:59.999999+08:00', 'tulip', 7);
    """
    qt_agg_key_scale_list_partition1 """
        SELECT * FROM timestamptz_storage_agg_key_scale_list_partition_multi_columns ORDER BY 1, 2;
    """
}
