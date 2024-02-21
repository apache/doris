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

suite("test_datetime_precision") {
    sql "set enable_fallback_to_original_planner=false;"
    qt_cast_on_fe "select cast ('2024-02-21 01:23:45.123456' as Datetime(6));"
    qt_cast_on_fe "select cast ('2024-02-21 01:23:45.123456Z' as Datetime(6));"
    qt_cast_on_fe "select cast ('2024-02-21T01:23:45.123456' as Datetime(6));"
    qt_cast_on_fe "select cast ('2024-02-21T01:23:45.123456Z' as Datetime(6));"

    qt_cast_on_fe "select cast ('2024-02-21 01:23:45.1234567' as Datetime(6));"
    qt_cast_on_fe "select cast ('2024-02-21 01:23:45.1234567Z' as Datetime(6));"
    qt_cast_on_fe "select cast ('2024-02-21T01:23:45.1234567' as Datetime(6));"
    qt_cast_on_fe "select cast ('2024-02-21T01:23:45.1234567Z' as Datetime(6));"
    // make sure expr if folded on FE (result should not contain cast expr)
    qt_cast_on_fe "explain select cast ('2024-02-21T01:23:45.1234567Z' as Datetime(6));"


    sql "drop table if exists test_datetime_precision;"
    sql """
    CREATE TABLE IF NOT EXISTS test_datetime_precision (
      `rowid` int,
      `col1` datetimev2 NULL COMMENT "",
      `col2` datetimev2(6) NULL COMMENT "",
      `str` VARCHAR NOT NULL
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`rowid`) BUCKETS 2
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2");
    """

    // constants in values will be folded on FE
    sql """
    insert into test_datetime_precision values
    (0, '2024-02-21 11:11:11.123456', '2022-01-01 11:11:11.123456', '2024-02-06 03:37:07.123456Z'),
    (1, '2024-02-21 11:11:11.1234567', '2022-01-01 11:11:11.1234567', '2024-02-06 03:37:07.1234567Z'),
    (2, '2024-02-21 11:11:11.12345678', '2022-01-01 11:11:11.12345678', '2024-02-06 03:37:07.12345678Z');
    """

    qt_sql_all """
    select * from test_datetime_precision order by rowid, str desc;
    """

    qt_sql_cast_on_be """
    select t1.str as original_str, cast(str as Datetime(6)) from test_datetime_precision t1 order by str desc;
    """


}
