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
suite("test_timestamptz_storage_dup_key") {
    def timezone_str = "+08:00"
    sql "set time_zone = '${timezone_str}'; "

    // default value
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_default_value_no_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_default_value_no_scale` (
          `ts_tz` TIMESTAMPTZ default current_timestamp,
          `ts_tz_value` TIMESTAMPTZ default current_timestamp,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into timestamptz_storage_dup_key_default_value_no_scale(value) VALUES(1), (2), (3);
    """
    qt_default_value_no_scale """
        SELECT value FROM timestamptz_storage_dup_key_default_value_no_scale order by value;
    """

    def zoned_now = ZonedDateTime.now(ZoneId.of(timezone_str))
    def formatter_no_scale = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX")

    for (col_name in ["ts_tz ", "ts_tz_value"]) {
        def query_result = sql """ 
            SELECT cast(${col_name} as string) FROM timestamptz_storage_dup_key_default_value_no_scale;
        """
        assertEquals(3, query_result.size())
        // query_result: [[2025-12-01 15:22:50+08:00], [2025-12-01 15:22:50+08:00], [2025-12-01 15:22:50+08:00]]
        for (row in query_result) {
            def query_result_value = row[0].toString()
            // println("row: " + row + ", column: " + query_result_value)
            def query_result_value_zdt  = ZonedDateTime.parse(query_result_value, formatter_no_scale)
            def diff_in_seconds = ChronoUnit.SECONDS.between(query_result_value_zdt, zoned_now)
            assertTrue(diff_in_seconds >=0 && diff_in_seconds < 60)
        }
    }

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_default_value_with_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_default_value_with_scale` (
          `ts_tz` TIMESTAMPTZ(6) default current_timestamp(6),
          `ts_tz_value` TIMESTAMPTZ(6) default current_timestamp(6),
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into timestamptz_storage_dup_key_default_value_with_scale(value) VALUES (1), (2), (3);
    """
    qt_default_value_with_scale """
        SELECT value FROM timestamptz_storage_dup_key_default_value_with_scale order by value;
    """
    def formatter_with_scale = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX")
    zoned_now = ZonedDateTime.now(ZoneId.of(timezone_str))

    for (col_name in ["ts_tz ", "ts_tz_value"]) {
        def query_result = sql """ 
            SELECT cast(${col_name} as string) FROM timestamptz_storage_dup_key_default_value_with_scale;
        """
        assertEquals(3, query_result.size())
        for (row in query_result) {
            def query_result_value = row[0].toString()
            // println("row: " + row + ", column: " + query_result_value)
            def query_result_value_zdt  = ZonedDateTime.parse(query_result_value, formatter_with_scale)
            def diff_in_seconds = ChronoUnit.SECONDS.between(query_result_value_zdt, zoned_now)
            assertTrue(diff_in_seconds >=0 && diff_in_seconds < 60)
        }
    }

/*
        CREATE TABLE `dt_storage_dup_key_no_scale` (
          `ts_tz` datetimev2,
          `ts_tz_value` datetimev2,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
        partition by RANGE(`ts_tz`) (
            PARTITION p2023_01 VALUES LESS THAN ('2023-02-01 00:00:00 +00:00'),
            PARTITION p2023_02 VALUES LESS THAN ('2023-03-01 00:00:00 +00:00'),
            PARTITION p2023_03 VALUES LESS THAN ('2023-04-01 00:00:00 +00:00'),
            PARTITION p2023_04 VALUES LESS THAN ('2023-05-01 00:00:00 +00:00'),
            PARTITION p2023_05 VALUES LESS THAN ('2023-06-01 00:00:00 +00:00'),
            PARTITION p2023_06 VALUES LESS THAN ('2023-07-01 00:00:00 +00:00'),
            PARTITION p2023_07 VALUES LESS THAN ('2023-08-01 00:00:00 +00:00'),
            PARTITION p2023_08 VALUES LESS THAN ('2023-09-01 00:00:00 +00:00'),
            PARTITION p2023_09 VALUES LESS THAN ('2023-10-01 00:00:00 +00:00'),
            PARTITION p2023_10 VALUES LESS THAN ('2023-11-01 00:00:00 +00:00'),
            PARTITION p2023_11 VALUES LESS THAN ('2023-12-01 00:00:00 +00:00'),
            PARTITION p2023_12 VALUES LESS THAN ('2024-01-01 00:00:00 +00:00')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );

    INSERT INTO dt_storage_dup_key_no_scale VALUES
    (null, null, -1),
    (null, '0000-01-01 00:00:00 +00:00', -1),
    (null, '1000-01-01 00:00:00 +00:00', -1),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
    ('0000-01-01 00:00:00 +00:00', '9000-01-01 00:00:00 +00:00', 0),
    ('2023-01-01 12:00:00 +03:00', '2023-01-01 12:00:00 +03:00', 1),
    ('2023-02-02 12:00:00 +03:00', '2023-02-02 12:00:00 +03:00', 2),
    ('2023-02-02 12:00:00 +03:00', '0000-01-01 00:00:00 +00:00', 2),
    ('2023-02-02 12:00:00 +03:00', '9999-12-31 23:59:59 +08:00', 2),
    ('2023-03-03 12:00:00 -05:00', '2023-03-03 12:00:00 -05:00', 3),
    ('2023-09-09 09:09:09 +01:00', '2023-09-09 09:09:09 +01:00', 9),
    ('2023-10-10 10:10:10 -03:00', '2023-10-10 10:10:10 -03:00', 10),
    ('2023-11-11 11:11:11 +00:00', '2023-11-11 11:11:11 +00:00', 11),
    ('2023-04-04 23:59:59 +00:00', '2023-04-04 23:59:59 +00:00', 4),
    ('2023-05-05 00:00:00 +08:00', '2023-05-05 00:00:00 +08:00', 5),
    ('2023-06-06 15:30:30 -02:00', '2023-06-06 15:30:30 -02:00', 6),
    ('2023-07-07 07:07:07 +05:30', '2023-07-07 07:07:07 +05:30', 7),
    ('2023-08-08 20:20:20 -04:00', '2023-08-08 20:20:20 -04:00', 8),
    ('2023-12-12 12:12:12 +09:00', '2023-12-12 12:12:12 +09:00', 12);
    */

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
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_no_scale_no_max_partition`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_no_scale_no_max_partition` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_value` TIMESTAMPTZ,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
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

    def show_result = sql """
    show create table timestamptz_storage_dup_key_no_scale_no_max_partition;
    """
    println "show create table result: ${show_result}"

    def partitionPattern = /\[\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'\),\s*\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'\)\)/
    // def partitionPattern    = /\[\('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'\),\s*\(('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})'|(MAXVALUE))\)\)/
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


    sql """
    INSERT INTO timestamptz_storage_dup_key_no_scale_no_max_partition VALUES
        (null, null, 0),
        (null, '0000-01-01 00:00:00 +00:00', 1),
        (null, '2023-08-08 20:20:20 +08:00', 2),
        (null, '9999-12-31 23:59:59 +08:00', -1),
        ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
        ('0000-01-01 08:00:00 +08:00', '0000-01-01 08:00:00 +08:00', 1),
        ('2023-01-01 12:00:00 +08:00', '2023-01-01 12:00:00 +08:00', 0),
        ('2023-08-08 20:20:20 +08:00', '2023-08-08 20:20:20 +08:00', 1),
        ('2023-12-12 12:12:12 +08:00', '2023-12-12 12:12:12 +08:00', 2),
        ('9999-12-30 23:59:59 +08:00', '9999-12-30 23:59:59 +08:00', 2),
        ('9999-12-31 23:59:58 +08:00', '9999-12-31 23:59:59 +08:00', 2);
    """

    qt_all0 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale_no_max_partition ORDER BY 1, 2, 3;
    """

    // no partition
    test {
        sql """
        INSERT INTO timestamptz_storage_dup_key_no_scale_no_max_partition VALUES ('9999-12-31 23:59:59 +08:00', null, 9999);
        """
        exception "no partition for this tuple"
    }

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_no_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_no_scale` (
          `ts_tz` TIMESTAMPTZ,
          `ts_tz_value` TIMESTAMPTZ,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
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
    expected_partitions = [
        [start: "0000-01-01 00:00:00+00:00", end: partition_value0],
        [start: partition_value0, end: partition_value1],
        [start: partition_value1, end: partition_value2],
        [start: partition_value2, end: partition_value3],
        [start: partition_value3, end: "9999-12-31 23:59:59+08:00"],
    ]

    sql """
    INSERT INTO timestamptz_storage_dup_key_no_scale VALUES
        (null, null, 0),
        (null, '0000-01-01 00:00:00 +00:00', 1),
        (null, '2023-08-08 20:20:20 +08:00', 2),
        (null, '9999-12-31 23:59:59 +08:00', -1),
        ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
        ('0000-01-01 08:00:00 +08:00', '0000-01-01 08:00:00 +08:00', 1),
        ('2023-01-01 12:00:00 +08:00', '2023-01-01 12:00:00 +08:00', 0),
        ('2023-08-08 20:20:20 +08:00', '2023-08-08 20:20:20 +08:00', 1),
        ('2023-12-12 12:12:12 +08:00', '2023-12-12 12:12:12 +08:00', 2),
        ('9999-12-30 23:59:59 +08:00', '9999-12-30 23:59:59 +08:00', 2),
        ('9999-12-31 23:59:58 +08:00', '9999-12-31 23:59:59 +08:00', 2),
        ('9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00', 2);
    """

    qt_all1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale ORDER BY 1, 2, 3;
    """

    show_result = sql """
    show create table timestamptz_storage_dup_key_no_scale;
    """
    println "show create table result: ${show_result}"

    partitionRanges = []
    show_result[0][1].findAll(partitionPattern) { match, startTime, endTime ->
        partitionRanges << [start: startTime, end: endTime]
    }
    println "found partitions: ${partitionRanges}"
    /*
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
    */

    /*
explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '2023-01-01 12:00:00 +03:00' ORDER BY 1, 2, 3;
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                                                                                    |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                                                                                    |
|   OUTPUT EXPRS:                                                                                                                                    |
|     ts_tz[#3]                                                                                                                                      |
|     ts_tz_value[#4]                                                                                                                                |
|     VALUE[#5]                                                                                                                                      |
|   PARTITION: UNPARTITIONED                                                                                                                         |
|                                                                                                                                                    |
|   HAS_COLO_PLAN_NODE: false                                                                                                                        |
|                                                                                                                                                    |
|   VRESULT SINK                                                                                                                                     |
|      MYSQL_PROTOCOL                                                                                                                                |
|                                                                                                                                                    |
|   2:VMERGING-EXCHANGE                                                                                                                              |
|      offset: 0                                                                                                                                     |
|      distribute expr lists: ts_tz[#3]                                                                                                              |
|                                                                                                                                                    |
| PLAN FRAGMENT 1                                                                                                                                    |
|                                                                                                                                                    |
|   PARTITION: HASH_PARTITIONED: ts_tz[#0]                                                                                                           |
|                                                                                                                                                    |
|   HAS_COLO_PLAN_NODE: false                                                                                                                        |
|                                                                                                                                                    |
|   STREAM DATA SINK                                                                                                                                 |
|     EXCHANGE ID: 02                                                                                                                                |
|     UNPARTITIONED                                                                                                                                  |
| IS_MERGE: true                                                                                                                                     |
|                                                                                                                                                    |
|   1:VSORT(124)                                                                                                                                     |
|   |  order by: ts_tz_value[#4] ASC                                                                                                                 |
|   |  algorithm: full sort                                                                                                                          |
|   |  local merge sort                                                                                                                              |
|   |  merge by exchange                                                                                                                             |
|   |  offset: 0                                                                                                                                     |
|   |  distribute expr lists:                                                                                                                        |
|   |                                                                                                                                                |
|   0:VOlapScanNode(116)                                                                                                                             |
|      TABLE: regression_test_datatype_p0_timestamptz.timestamptz_storage_dup_key_no_scale(timestamptz_storage_dup_key_no_scale), PREAGGREGATION: ON |
|      PREDICATES: (ts_tz[#0] = '2023-01-01 09:00:00')                                                                                               |
|      partitions=1/12 (p2023_01)                                                                                                                    |
|      tablets=1/16, tabletList=1764212203833                                                                                                        |
|      cardinality=6, avgRowSize=711.6666, numNodes=1                                                                                                |
|      pushAggOp=NONE
    */
    // test =
    def ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    // assertTrue(ret.toString().contains("""= '0000-01-01 00:00:00'"""))
    qt_eq0 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_eq1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_eq2 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_eq3 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz = '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """

    // test !=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_neq """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_neq1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_neq2 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_neq3 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz != '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """

    // test >
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_gt0 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_gt1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_gt2 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_gt3 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz > '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """

    // test >=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_ge0 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_ge1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_ge2 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_ge3 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz >= '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """

    // test <
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_lt0 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_lt1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_lt2 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_lt3 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz < '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """

    // test <=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_le0 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_le1 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '0000-01-01 08:00:00 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_le2 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '2023-08-08 20:20:20 +08:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_le3 """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz <= '9999-12-31 23:59:58 +08:00' ORDER BY 1, 2, 3;
    """

    // test in
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz in('0000-01-01 08:00:00 +08:00', '2023-08-08 12:20:20 +00:00', '9999-12-31 23:59:58 +08:00') ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_in """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz in('0000-01-01 08:00:00 +08:00', '2023-08-08 12:20:20 +00:00', '9999-12-31 23:59:58 +08:00') ORDER BY 1, 2, 3;
    """

    // test not in
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz not in('0000-01-01 08:00:00 +08:00', '2023-08-08 12:20:20 +00:00', '9999-12-31 23:59:58 +08:00') ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_not_in """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz not in('0000-01-01 08:00:00 +08:00', '2023-08-08 12:20:20 +00:00', '9999-12-31 23:59:58 +08:00') ORDER BY 1, 2, 3;
    """

    // test is null
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz IS NULL ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("""IS NULL"""))
    qt_is_null """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz IS NULL ORDER BY 1, 2, 3;
    """

    // test is not null
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz IS NOT NULL ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    assertTrue(ret.toString().contains("""IS NOT NULL"""))
    qt_is_not_null """
        SELECT * FROM timestamptz_storage_dup_key_no_scale where ts_tz IS NOT NULL ORDER BY 1, 2, 3;
    """

    // agg
    qt_min_max_count """
        SELECT min(ts_tz), max(ts_tz), count(ts_tz), min(ts_tz_value), max(ts_tz_value), count(ts_tz_value) FROM timestamptz_storage_dup_key_no_scale group by value ORDER BY 1;
    """
    test {
        sql """
        SELECT sum(ts_tz) FROM timestamptz_storage_dup_key_no_scale group by value ORDER BY 1;
        """
        exception "sum"
    }
    test {
        sql """
        SELECT avg(ts_tz) FROM timestamptz_storage_dup_key_no_scale group by value ORDER BY 1;
        """
        exception "avg"
    }
    // qt_count_distinct """
    //     SELECT ts_tz, count(distinct ts_tz_value) FROM timestamptz_storage_dup_key_no_scale group by ts_tz ORDER BY 1;
    // """

    // multi key columns
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_multi_key_cols`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_multi_key_cols` (
          `ts_tz` TIMESTAMPTZ,
          `name` VARCHAR(20),
          `ts_tz_value` TIMESTAMPTZ,
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`, `name`)
        DISTRIBUTED BY HASH(`ts_tz`, `name`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """INSERT INTO timestamptz_storage_dup_key_multi_key_cols VALUES
    (null, null, '0000-01-01 00:00:00 +00:00', -1),
    (null, null, '9000-01-01 00:00:00 +00:00', -1),
    (null, 'jack', '0000-01-01 00:00:00 +00:00', -2),
    (null, 'jack', '9000-01-01 00:00:00 +00:00', -2),
    (null, 'rose', '0000-01-01 00:00:00 +00:00', -3),
    (null, 'rose', '9000-01-01 00:00:00 +00:00', -3),
    ('0000-01-01 00:00:00 +00:00', null,   '0000-01-01 00:00:00 +00:00', 1),
    ('0000-01-01 00:00:00 +00:00', null,   '9000-01-01 00:00:00 +00:00', 1),
    ('0000-01-01 00:00:00 +00:00', 'jack', '0000-01-01 00:00:00 +00:00', 2),
    ('0000-01-01 00:00:00 +00:00', 'jack', '9000-01-01 00:00:00 +00:00', 2),
    ('0000-01-01 00:00:00 +00:00', 'rose', '0000-01-01 00:00:00 +00:00', 3),
    ('0000-01-01 00:00:00 +00:00', 'rose', '9000-01-01 00:00:00 +00:00', 3),
    ('2023-01-01 15:00:00 -05:00', null,   '1000-01-01 00:00:00 +00:00', 4),
    ('2023-01-01 15:00:00 -05:00', null,   '9000-01-01 00:00:00 +00:00', 4),
    ('2023-01-01 15:00:00 -05:00', 'jack', '1000-01-01 00:00:00 +00:00', 5),
    ('2023-01-01 15:00:00 -05:00', 'jack', '9000-01-01 00:00:00 +00:00', 5),
    ('2023-01-01 15:00:00 -05:00', 'rose', '1000-01-01 00:00:00 +00:00', 6),
    ('2023-01-01 15:00:00 -05:00', 'rose', '9000-01-01 00:00:00 +00:00', 6),
    ('9999-12-31 23:59:59 +08:00', null,   '9999-12-31 23:59:59 +08:00', 7),
    ('9999-12-31 23:59:59 +08:00', null,   '1999-12-31 23:59:59 +08:00', 7),
    ('9999-12-31 23:59:59 +08:00', 'jack', '9999-12-31 23:59:59 +08:00', 8),
    ('9999-12-31 23:59:59 +08:00', 'jack', '1999-12-31 23:59:59 +08:00', 8),
    ('9999-12-31 23:59:59 +08:00', 'rose', '9999-12-31 23:59:59 +08:00', 9),
    ('9999-12-31 23:59:59 +08:00', 'rose', '1999-12-31 23:59:59 +08:00', 9)
    """

    qt_multi_key_cols_all """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols ORDER BY 1, 2, 3;
    """

    // test =
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz = '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_eq0 """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz = '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz = '2023-01-01 15:00:00 -05:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_eq1 """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz = '2023-01-01 15:00:00 -05:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz = '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_eq2 """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz = '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // test !=
    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz != '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_neq0 """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz != '0000-01-01 00:00:00 +00:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz != '2023-01-01 15:00:00 -05:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_neq1 """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz != '2023-01-01 15:00:00 -05:00' ORDER BY 1, 2, 3;
    """

    ret = sql """
        explain SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz != '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """
    assertFalse(ret.toString().contains("CAST")) && assertFalse(ret.toString().contains("cast"))
    qt_multi_key_neq2 """
        SELECT * FROM timestamptz_storage_dup_key_multi_key_cols where ts_tz != '9999-12-31 23:59:59 +08:00' ORDER BY 1, 2, 3;
    """

    // with scale
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
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_scale_no_max_partition`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_scale_no_max_partition` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_value` TIMESTAMPTZ(6),
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
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
    show create table timestamptz_storage_dup_key_scale_no_max_partition;
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
        INSERT INTO timestamptz_storage_dup_key_scale_no_max_partition VALUES ('9999-12-31 23:59:59.999999 +08:00', '2023-08-08 20:20:20.999999 +00:00', 9999);
        """
        exception "no partition for this tuple"
    }

    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_scale`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_scale` (
          `ts_tz` TIMESTAMPTZ(6),
          `ts_tz_value` TIMESTAMPTZ(6),
          `VALUE` INT
        ) DUPLICATE KEY(`ts_tz`)
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

    sql """INSERT INTO timestamptz_storage_dup_key_scale VALUES
    (null, null, -1),
    ('0000-01-01 00:00:00 +00:00', '0000-01-01 00:00:00 +00:00', 0),
    ('0000-01-01 00:00:00.000000 +00:00', '0000-01-01 00:00:00.000000 +00:00', 0),
    ('0000-01-01 00:00:00.000001 +00:00', '0000-01-01 00:00:00.000001 +00:00', 0),
    ('0000-01-01 00:00:00.123456 +00:00', '0000-01-01 00:00:00.123456 +00:00', 0),
    ('0000-01-01 00:00:00.999999 +00:00', '0000-01-01 00:00:00.999999 +00:00', 10),
    ('2023-08-08 20:20:20 +00:00', '2023-08-08 20:20:20 +00:00', 8),
    ('2023-08-08 20:20:20.000000 +00:00', '2023-08-08 20:20:20.000000 +00:00', 8),
    ('2023-08-08 20:20:20.000001 +00:00', '2023-08-08 20:20:20.000001 +00:00', 8),
    ('2023-08-08 20:20:20.123456 +00:00', '2023-08-08 20:20:20.123456 +00:00', 8),
    ('2023-08-08 20:20:20.999999 +00:00', '2023-08-08 20:20:20.999999 +00:00', 8),
    ('9999-12-31 23:59:59 +08:00', '9999-12-31 23:59:59 +08:00', 9998),
    ('9999-12-31 23:59:59.000000 +08:00', '9999-12-31 23:59:59.000000 +08:00', 9998),
    ('9999-12-31 23:59:59.000001 +08:00', '9999-12-31 23:59:59.000001 +08:00', 9998),
    ('9999-12-31 23:59:59.123456 +08:00', '9999-12-31 23:59:59.123456 +08:00', 9998),
    ('9999-12-31 23:59:59.999999 +08:00', '9999-12-31 23:59:59.999999 +08:00', 9999);
    """

    qt_scale_all """
        SELECT * FROM timestamptz_storage_dup_key_scale ORDER BY 1, 2, 3;
    """
    qt_scale_eq """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz = '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2, 3;
    """
    qt_scale_neq """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz != '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2, 3;
    """
    qt_scale_gt """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz > '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2, 3;
    """
    qt_scale_ge """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz >= '0000-01-01 00:00:00.000001 +00:00' ORDER BY 1, 2, 3;
    """
    qt_scale_lt """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz < '9999-12-30 23:59:59.999999 +00:00' ORDER BY 1, 2, 3;
    """
    //    SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz < '9999-12-31 23:59:59.999999 +00:00' ORDER BY 1, 2, 3;
    qt_scale_le """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz <= '2023-12-12 12:12:12.123461 +09:00' ORDER BY 1, 2, 3;
    """
    qt_scale_in """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz in('0000-01-01 00:00:00.000001 +00:00', '9999-12-31 23:59:59.999999 +08:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2, 3;
    """
    qt_scale_not_in """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz not in('0000-01-01 00:00:00.000001 +00:00', '9999-12-30 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2, 3;
    """
    //    SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz not in('0000-01-01 00:00:00.000001 +00:00', '9999-12-31 23:59:59.999999 +00:00', '2023-12-12 12:12:12.123461 +09:00') ORDER BY 1, 2, 3;
    qt_scale_is_null """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz IS NULL ORDER BY 1, 2, 3;
    """
    qt_scale_is_not_null """
        SELECT * FROM timestamptz_storage_dup_key_scale where ts_tz IS NOT NULL ORDER BY 1, 2, 3;
    """

    // list partition
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_scale_list_partition`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_scale_list_partition` (
          `ts_tz` TIMESTAMPTZ(6),
          `VALUE` INT
        ) duplicate KEY(`ts_tz`)
        partition by LIST(`ts_tz`) (
            PARTITION p2023_null VALUES IN (null),
            PARTITION p2023_h1 VALUES IN (
                '2023-01-01 12:00:00.123450+08:00',
                '2023-02-02 12:00:00.123451+08:00',
                '2023-03-03 12:00:00.123452+08:00',
                '2023-04-04 23:59:59.123453+08:00',
                '2023-05-05 00:00:00.123454+08:00',
                '2023-06-06 15:30:30.123455+08:00'),
            PARTITION p2023_h2 VALUES IN (
                '2023-07-07 07:07:07.123456+08:00',
                '2023-08-08 20:20:20.123457+08:00',
                '2023-09-09 09:09:09.123458+08:00',
                '2023-10-10 10:10:10.123459+08:00',
                '2023-11-11 11:11:11.123460+08:00',
                '2023-12-12 12:12:12.123461+08:00')
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    // '2023-12-12 12:12:12.999999')

    sql """INSERT INTO timestamptz_storage_dup_key_scale_list_partition VALUES
    (null, -1),
    ('2023-01-01 12:00:00.123450 +00:00', 1),
    ('2023-07-07 07:07:07.123456 +00:00', 7),
    ('2023-08-08 20:20:20.123457 +00:00', 8),
    ('2023-09-09 09:09:09.123458 +00:00', 9),
    ('2023-10-10 10:10:10.123459 +00:00', 10),
    ('2023-02-02 12:00:00.123451 +00:00', 2),
    ('2023-03-03 12:00:00.123452 +00:00', 3),
    ('2023-04-04 23:59:59.123453 +00:00', 4),
    ('2023-05-05 00:00:00.123454 +00:00', 5),
    ('2023-06-06 15:30:30.123455 +00:00', 6),
    ('2023-11-11 11:11:11.123460 +00:00', 11),
    ('2023-12-12 12:12:12.123461 +00:00', 12);
    """
    // ('2023-12-12 12:12:12.999999 +00:00', 13);
    qt_scale_list_partition0 """
        SELECT * FROM timestamptz_storage_dup_key_scale_list_partition ORDER BY 1, 2, 3;
    """

    // list partition, multi columns
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_dup_key_scale_list_partition_multi_cols`;
    """
    sql """
        CREATE TABLE `timestamptz_storage_dup_key_scale_list_partition_multi_cols` (
          `ts_tz` TIMESTAMPTZ(6),
          `name` VARCHAR(64),
          `VALUE` INT
        ) duplicate KEY(`ts_tz`, `name`)
        partition by LIST(`ts_tz`, `name`) (
            PARTITION p2023_null VALUES IN ((null, null)),
            PARTITION p2023_h1 VALUES IN (
                ('2023-01-01 12:00:00.123450+08:00', 'jack'),
                ('2023-02-02 12:00:00.123451+08:00', 'rose'),
                ('2023-03-03 12:00:00.123452+08:00', 'lily'),
                ('2023-04-04 23:59:59.123453+08:00', 'tulip'),
                ('2023-05-05 00:00:00.123454+08:00', 'daisy'),
                ('2023-06-06 15:30:30.123455+08:00', 'sunflower')),
            PARTITION p2023_h2 VALUES IN (
                ('2023-07-07 07:07:07.123456+08:00', 'jack'),
                ('2023-08-08 20:20:20.123457+08:00', 'rose'),
                ('2023-09-09 09:09:09.123458+08:00', 'lily'),
                ('2023-10-10 10:10:10.123459+08:00', 'tulip'),
                ('2023-11-11 11:11:11.123460+08:00', 'daisy'),
                ('2023-12-12 12:12:12.123461+08:00', 'sunflower'))
        )
        DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """INSERT INTO timestamptz_storage_dup_key_scale_list_partition_multi_cols VALUES
    (null, null, -1),
    ('2023-01-01 12:00:00.123450 +00:00', 'jack', 1),
    ('2023-07-07 07:07:07.123456 +00:00', 'jack', 7),
    ('2023-08-08 20:20:20.123457 +00:00', 'rose', 8),
    ('2023-09-09 09:09:09.123458 +00:00', 'lily', 9),
    ('2023-10-10 10:10:10.123459 +00:00', 'tulip', 10),
    ('2023-02-02 12:00:00.123451 +00:00', 'rose', 2),
    ('2023-03-03 12:00:00.123452 +00:00', 'lily', 3),
    ('2023-04-04 23:59:59.123453 +00:00', 'tulip', 4),
    ('2023-05-05 00:00:00.123454 +00:00', 'daisy', 5),
    ('2023-06-06 15:30:30.123455 +00:00', 'sunflower', 6),
    ('2023-11-11 11:11:11.123460 +00:00', 'daisy', 11),
    ('2023-12-12 12:12:12.123461 +00:00', 'sunflower', 12);
    """
    qt_scale_list_partition1 """
        SELECT * FROM timestamptz_storage_dup_key_scale_list_partition_multi_cols ORDER BY 1, 2, 3;
    """
}
