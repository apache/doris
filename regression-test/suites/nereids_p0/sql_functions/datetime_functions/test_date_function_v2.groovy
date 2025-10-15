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
                `dt` varchar(32),
                `time_str` varchar(32),
                `datetime_val` datetime(6)
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
        """
    sql """ insert into ${tableName} values 
            (1, 'Carl', '2024-12-29 10:11:12', '12:34:56', '2024-12-29 10:11:12.123456'),
            (2, 'Bob', '2023-01-15 08:30:45', '23:59:59', '2023-01-15 08:30:45.000000'),
            (3, 'Alice', '2025-06-30 15:45:30', '-12:34:56', '2025-06-30 15:45:30.999999')
        """
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

    qt_sql_addtime1 "select add_time('2023-10-14 00:00:00', '22:35:22');"
    testFoldConst("select add_time('2023-10-14 00:00:00', '22:35:22');")
    qt_sql_addtime2 "select add_time('2023-10-14 00:00:00.12', '22:35:22.123456');"
    testFoldConst("select add_time('2023-10-14 00:00:00.12', '22:35:22.123456');")
    qt_sql_addtime3 "select add_time(dt, '122:35:22.123456') from ${tableName};"
    qt_sql_addtime4 "select add_time(cast('822:35:22.123456' as time(6)), cast('421:01:01' as time(6)));"
    qt_sql_addtime5 "select add_time(cast('-82:35:22.123456' as time(6)), cast('-421:01:01' as time(6)));"

    // test time string and datetime type in table
    qt_sql_addtime6 "select add_time(datetime_val, time_str) from ${tableName} order by id;"
    qt_sql_addtime7 "select add_time(cast(time_str as time), cast('02:00:00.123' as time(3))) from ${tableName} order by id;"
    qt_sql_addtime8 "select add_time(datetime_val, cast('01:30:00' as time)) from ${tableName} order by id;"

    test{
        sql("select add_time('9999-12-29 00:00:00', '122:35:22.123456');")
        exception "datetime value is out of range in function add_time"
    }

    qt_sql_subtime1("select sub_time('2023-10-14 00:00:00', '22:35:22');")
    testFoldConst("select sub_time('2023-10-14 00:00:00', '22:35:22');")
    qt_sql_subtime2("select sub_time('2023-10-14 00:00:00.12', '22:35:22.123456');")
    testFoldConst("select sub_time('2023-10-14 00:00:00.12', '22:35:22.123456');")
    qt_sql_subtime3("select sub_time(dt, '22:35:22.123456') from ${tableName};")
    qt_sql_subtime4("select sub_time('-421:01:01', '822:35:22');")
    qt_sql_subtime5("select sub_time('421:01:01', '-82:35:22.123456');")
    
    // test time string and datetime type in table
    qt_sql_subtime6("select sub_time(datetime_val, time_str) from ${tableName} order by id;")
    qt_sql_subtime7("select sub_time(cast(time_str as time), cast('02:00:00.123' as time(3))) from ${tableName} order by id;")
    qt_sql_subtime8("select sub_time(datetime_val, cast('01:30:00' as time)) from ${tableName} order by id;")

    test{
        sql("select sub_time('0000-01-01 00:00:00', '122:35:22.123456');")
        exception "datetime value is out of range in function sub_time"
    }

    //test computetimearithmetic regular

    qt_sql_addtime9 "select add_time('221222', cast('221010' as time));"
    qt_sql_addtime10 "select add_time('22:12:22', '221010');"
    qt_sql_addtime11 "select add_time('+22:12:22', '221010');"

    //datetime parameter
    qt_sql_addtime12 "select add_time('22/12/22', '221010');"

    qt_sql_subtime9 "select sub_time('221222', cast('221010' as time));"
    qt_sql_subtime10 "select sub_time('22:12:22', '221010');"
    qt_sql_subtime11 "select sub_time('-22:12:22', '221010');"
    //datetime parameter
    qt_sql_subtime12 "select sub_time('22/12/22', '221010');"
}
