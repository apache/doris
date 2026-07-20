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

suite("test_select", "arrow_flight_sql") {
    def tableName = "test_select"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """INSERT INTO ${tableName} VALUES(111, "plsql111")"""
    sql """INSERT INTO ${tableName} VALUES(222, "plsql222")"""
    sql """INSERT INTO ${tableName} VALUES(333, "plsql333")"""
    sql """INSERT INTO ${tableName} VALUES(111, "plsql333")"""

    qt_arrow_flight_sql "select sum(id) as a, count(1) as b from ${tableName}"

    tableName = "test_select_datetime"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, name varchar(20), f_datetime_p datetime(6), f_datetime datetime) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """INSERT INTO ${tableName} VALUES(111, "plsql111","2024-07-19 12:00:00.123456","2024-07-19 12:00:00")"""
    sql """INSERT INTO ${tableName} VALUES(222, "plsql222","2024-07-20 12:00:00.123456","2024-07-20 12:00:00")"""
    sql """INSERT INTO ${tableName} VALUES(333, "plsql333","2024-07-21 12:00:00.123456","2024-07-21 12:00:00")"""

    qt_arrow_flight_sql_datetime "select * from ${tableName} order by id desc"

    // apache/doris#65741: by default (session variable enable_arrow_flight_datetime_naive = false)
    // DATETIME(V2) keeps the timezone-aware Arrow mapping, so the wall-clock value round-trips
    // unchanged over Arrow Flight. The opt-in timezone-naive mapping is verified in the BE unit
    // test DataTypeSerDeArrowTest.DateTimeV2ArrowTimezoneDependsOnNaiveFlag.
    qt_arrow_flight_sql_datetime_cast "select cast('2026-07-02 01:36:22.069504' as datetime(6)) as ts"

    tableName = "test_select_jsonb"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, payload jsonb) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        INSERT INTO ${tableName} VALUES
            (1, '{"k1": 1, "k2": "v2"}'),
            (2, '[1, 2, {"nested": true}]'),
            (3, NULL)
        """

    qt_arrow_flight_sql_jsonb "select id, payload from ${tableName} order by id"

    def largeJsonValueSize = 2100000
    sql """
        INSERT INTO ${tableName}
        SELECT 4, CAST(CONCAT('{"large":"', REPEAT('x', ${largeJsonValueSize}), '"}') AS JSONB)
        """

    // This row exceeds MAX_ARROW_UTF8 and exercises JSONB -> LargeString serialization.
    def largeJsonbResult = arrow_flight_sql """
        select payload, length(cast(payload as string)) from ${tableName} where id = 4
        """
    assertEquals(1, largeJsonbResult.size())
    assertEquals(2, largeJsonbResult[0].size())
    def expectedLargeJsonbSize = largeJsonValueSize + '{"large":""}'.length()
    def largeJsonb = largeJsonbResult[0][0].toString()
    assertEquals(expectedLargeJsonbSize, largeJsonb.length())
    assertEquals(expectedLargeJsonbSize, (largeJsonbResult[0][1] as Number).intValue())
    assertTrue(largeJsonb.startsWith('{"large":"'))
    assertTrue(largeJsonb.endsWith('"}'))
}
