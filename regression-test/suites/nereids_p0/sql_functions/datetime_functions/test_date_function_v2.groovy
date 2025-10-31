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

    def getFormatTable = "get_format_table_test"
    sql """ DROP TABLE IF EXISTS ${getFormatTable} """
    sql """ CREATE TABLE ${getFormatTable} (
                id INT,
                lc VARCHAR(10)
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ( "replication_num" = "1" ); """
    sql """ INSERT INTO ${getFormatTable} VALUES
            (1, 'USA'), (2, 'JIS'), (3, 'ISO'), (4, 'EUR'),
            (5, 'INTERNAL'), (6, 'Doris'); """

    qt_get_format_date_1 """ SELECT GET_FORMAT(DATE, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_date_2 """SELECT GET_FORMAT(DaTe, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_date_3 """SELECT GET_FORMAT(dATe, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_date_4 """SELECT GET_FORMAT(date, '你好');"""
    qt_get_format_datetime_1 """ SELECT GET_FORMAT(DATETIME, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_datetime_2 """SELECT GET_FORMAT(DaTETimE, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_datetime_3 """SELECT GET_FORMAT(dATetIMe, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_datetime_4 """SELECT GET_FORMAT(datetime, '你好');"""
    qt_get_format_time_1 """ SELECT GET_FORMAT(TIME, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_time_2 """ SELECT GET_FORMAT(TiMe, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_time_3 """ SELECT GET_FORMAT(tIME, lc) FROM ${getFormatTable} ORDER BY id; """
    qt_get_format_time_4 """ SELECT GET_FORMAT(time, '你好'); """
    test {
        sql """ SELECT GET_FORMAT(DATA, 'USA'); """
        exception "Format type only support DATE, DATETIME and TIME"
    }

    sql """ DROP TABLE IF EXISTS ${getFormatTable} """

    testFoldConst("SELECT GET_FORMAT(DATE, 'USA');")
    testFoldConst("SELECT GET_FORMAT(date, 'usa');")
    testFoldConst("SELECT GET_FORMAT(DATE, 'JIS');")
    testFoldConst("SELECT GET_FORMAT(Date, 'JiS');")
    testFoldConst("SELECT GET_FORMAT(DATE, 'ISO');")
    testFoldConst("SELECT GET_FORMAT(DaTe, 'iSo');")
    testFoldConst("SELECT GET_FORMAT(DATE, 'EUR');")
    testFoldConst("SELECT GET_FORMAT(daTE, 'EuR');")
    testFoldConst("SELECT GET_FORMAT(DATE, 'INTERNAL');")
    testFoldConst("SELECT GET_FORMAT(DaTE, 'InTERnAL');")
    testFoldConst("SELECT GET_FORMAT(DATE, 'Doris');")
    testFoldConst("SELECT GET_FORMAT(DATE, '你好');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, 'USA');")
    testFoldConst("SELECT GET_FORMAT(datetime, 'Usa');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, 'JIS');")
    testFoldConst("SELECT GET_FORMAT(DateTime, 'jis');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, 'ISO');")
    testFoldConst("SELECT GET_FORMAT(DaTeTiMe, 'IsO');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, 'EUR');")
    testFoldConst("SELECT GET_FORMAT(dateTIME, 'EuR');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, 'INTERNAL');")
    testFoldConst("SELECT GET_FORMAT(DaTeTimE, 'internal');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, 'Doris');")
    testFoldConst("SELECT GET_FORMAT(DATETIME, '你好');")
    testFoldConst("SELECT GET_FORMAT(TIME, 'USA');")
    testFoldConst("SELECT GET_FORMAT(time, 'USa');")
    testFoldConst("SELECT GET_FORMAT(TIME, 'JIS');")
    testFoldConst("SELECT GET_FORMAT(TiMe, 'jiS');")
    testFoldConst("SELECT GET_FORMAT(TIME, 'ISO');")
    testFoldConst("SELECT GET_FORMAT(TiME, 'iso');")
    testFoldConst("SELECT GET_FORMAT(TIME, 'EUR');")
    testFoldConst("SELECT GET_FORMAT(TimE, 'eur');")
    testFoldConst("SELECT GET_FORMAT(TIME, 'INTERNAL');")
    testFoldConst("SELECT GET_FORMAT(TImE, 'INTERNAL');")
    testFoldConst("SELECT GET_FORMAT(TIME, 'Doris');")
    testFoldConst("SELECT GET_FORMAT(TIME, '你好');")
}
