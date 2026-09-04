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

import java.sql.Types
import java.time.LocalDateTime

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
        create table ${tableName} (id int, name varchar(20), f_datetime_p datetime(6),
            f_datetime datetime, f_timestamptz timestamptz(6))
        DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """INSERT INTO ${tableName} VALUES
        (111, "plsql111", "2024-07-19 12:00:00.123456", "2024-07-19 12:00:00",
            "2024-07-19 12:00:00.123456 +08:00"),
        (222, "plsql222", "2024-07-20 12:00:00.123456", "2024-07-20 12:00:00",
            "2024-07-20 12:00:00.123456 +08:00"),
        (333, "plsql333", "2024-07-21 12:00:00.123456", "2024-07-21 12:00:00",
            "2024-07-21 12:00:00.123456 +08:00")
    """

    // Arrow JDBC's untyped getObject() applies timezone conversion to a naive timestamp. Keep the
    // snapshot comparison textual and verify the timestamp schema and wall-clock values explicitly.
    qt_arrow_flight_sql_datetime """
        SELECT id, name, CAST(f_datetime_p AS STRING), CAST(f_datetime AS STRING)
        FROM ${tableName}
        ORDER BY id DESC
    """

    def discoveredColumnTypes = [:]
    context.getArrowFlightSqlConnection().getMetaData()
            .getColumns(null, context.dbName, tableName, null).withCloseable { columns ->
        while (columns.next()) {
            discoveredColumnTypes[columns.getString("COLUMN_NAME")] = columns.getInt("DATA_TYPE")
        }
    }
    assertEquals(Types.TIMESTAMP, discoveredColumnTypes["f_datetime_p"])
    assertEquals(Types.TIMESTAMP, discoveredColumnTypes["f_datetime"])
    assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, discoveredColumnTypes["f_timestamptz"])

    context.getArrowFlightSqlConnection().createStatement().withCloseable { statement ->
        statement.executeQuery("""
            USE ${context.dbName};
            SELECT
                CAST('2024-07-19 12:00:00' AS DATETIME(0)) AS datetime_s,
                CAST('2024-07-19 12:00:00.123' AS DATETIME(3)) AS datetime_ms,
                CAST('2024-07-19 12:00:00.123456' AS DATETIME(6)) AS datetime_us,
                CAST('2024-07-19 12:00:00.123456 +08:00' AS TIMESTAMPTZ(6)) AS timestamptz_us
        """).withCloseable { resultSet ->
            def metadata = resultSet.getMetaData()
            assertEquals(4, metadata.getColumnCount())
            for (int i = 1; i <= 3; i++) {
                assertEquals(Types.TIMESTAMP, metadata.getColumnType(i))
                assertEquals("TIMESTAMP", metadata.getColumnTypeName(i))
            }
            assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, metadata.getColumnType(4))
            assertEquals("TIMESTAMP_WITH_TIMEZONE", metadata.getColumnTypeName(4))

            assertTrue(resultSet.next())
            assertEquals(LocalDateTime.parse("2024-07-19T12:00:00"),
                    resultSet.getObject(1, LocalDateTime.class))
            assertEquals(LocalDateTime.parse("2024-07-19T12:00:00.123"),
                    resultSet.getObject(2, LocalDateTime.class))
            assertEquals(LocalDateTime.parse("2024-07-19T12:00:00.123456"),
                    resultSet.getObject(3, LocalDateTime.class))
        }
    }

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
