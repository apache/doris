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

    def tableName = "test_date_function_v2"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` INT,
                `name` varchar(32),
                `dt` varchar(32)
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
        """
    sql """ insert into ${tableName} values (3, 'Carl','2024-12-29 10:11:12') """
    def result1 = try_sql """
        select cast(str_to_date(dt, '%Y-%m-%d %H:%i:%s') as string) from ${tableName};
    """
    assertEquals(result1[0][0], "2024-12-29 10:11:12");

    def result2 = try_sql """
        select cast(str_to_date(dt, '%Y-%m-%d %H:%i:%s.%f') as string) from ${tableName};
    """
    assertEquals(result2[0][0], "2024-12-29 10:11:12.000000");

    def result3 = try_sql """
        select cast(str_to_date("2025-01-17 11:59:30", '%Y-%m-%d %H:%i:%s') as string);
    """
    assertEquals(result3[0][0], "2025-01-17 11:59:30");

    def result4 = try_sql """
        select cast(str_to_date("2025-01-17 11:59:30", '%Y-%m-%d %H:%i:%s.%f') as string);
    """
    assertEquals(result4[0][0], "2025-01-17 11:59:30.000000");

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
}
