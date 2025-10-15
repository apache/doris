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

suite("test_date_function_v2") {
    sql """
    admin set frontend config ("enable_date_conversion"="true");
    """

    qt_sql_diff1 "select days_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select days_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")
    qt_sql_diff2 "select days_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select days_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")

    qt_sql_diff3 "select weeks_diff('2023-10-15 00:00:00', '2023-10-08 00:00:00.1');"
    testFoldConst("select weeks_diff('2023-10-15 00:00:00', '2023-10-08 00:00:00.1');")
    qt_sql_diff4 "select weeks_diff('2023-10-15 00:00:00', '2023-10-08 00:00:00');"
    testFoldConst("select weeks_diff('2023-10-15 00:00:00', '2023-10-08 00:00:00');")

    qt_sql_diff5 "select hours_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select hours_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")
    qt_sql_diff6 "select hours_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');"
    testFoldConst("select hours_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');")
    qt_sql_diff7 "select minutes_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select minutes_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")
    qt_sql_diff8 "select minutes_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');"
    testFoldConst("select minutes_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');")
    qt_sql_diff9 "select seconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select seconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")
    qt_sql_diff10 "select seconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');"
    testFoldConst("select seconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');")
    qt_sql_diff11 "select milliseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select milliseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")
    qt_sql_diff12 "select milliseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');"
    testFoldConst("select milliseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');")
    qt_sql_diff13 "select microseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');"
    testFoldConst("select microseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00.1');")
    qt_sql_diff14 "select microseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');"
    testFoldConst("select microseconds_diff('2023-10-15 00:00:00', '2023-10-14 00:00:00');")

    sql " drop table if exists my_doris_table;"
    sql """
    CREATE TABLE my_doris_table (
        `id` INT COMMENT '唯一ID',
        `event_date` DATETIME COMMENT '事件时间，精确到秒'
    )
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1"
    );
    """

    sql """
    INSERT INTO my_doris_table  VALUES
    (1, '2099-08-20 10:00:01');
    """

    def res = sql """ select hours_diff('2000-08-19 08:09:13', event_date) from my_doris_table; """
    assertTrue(res[0][0] < 0)
    res =  sql """ select hours_diff(now(), event_date) from my_doris_table;"""
    assertTrue(res[0][0] < 0)
}
