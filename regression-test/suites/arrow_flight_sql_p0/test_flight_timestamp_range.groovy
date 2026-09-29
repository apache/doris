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

import java.time.LocalDateTime

suite("test_flight_timestamp_range", "arrow_flight_sql") {
    sql "DROP TABLE IF EXISTS flight_timestamp_input"
    sql """CREATE TABLE flight_timestamp_input (id INT, value STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql """INSERT INTO flight_timestamp_input VALUES
           (1, '0001-01-01 00:00:00.000000'),
           (2, '9999-12-31 23:59:59.999999'),
           (3, '1969-12-31 23:59:59.999999'),
           (4, NULL), (5, '0000-12-31 00:00:00.000000')"""
    try {
        // Check that the source value reaches the date conversion instead of becoming SQL NULL.
        assertEquals([['0000-12-31 00:00:00.000000']],
                sql("SELECT CAST(CAST(value AS DATETIME(6)) AS STRING) FROM flight_timestamp_input WHERE id = 5"))
        def flightUrl = context.getArrowFlightSqlConnection().getMetaData().getURL()
        connect(context.config.jdbcUser, context.config.jdbcPassword, flightUrl) {
            def input = "${context.dbName}.flight_timestamp_input"
            for (def scale : [0, 3, 6]) {
                test {
                    sql "SELECT CAST(value AS DATETIME(${scale})) AS event_time FROM ${input} WHERE id = 5"
                    exception "outside the supported 0001-9999 range"
                }
            }
            test {
                sql "SELECT CAST('0000-12-31 00:00:00' AS DATETIME(6)) AS event_time"
                exception "outside the supported 0001-9999 range"
            }
            for (def expression : ["array(CAST(value AS DATETIME(6)))",
                                    "named_struct('child', CAST(value AS DATETIME(6)))",
                                    "map('key', CAST(value AS DATETIME(6)))",
                                    "map(CAST(value AS DATETIME(6)), 'value')"]) {
                test {
                    sql "SELECT ${expression} AS nested FROM ${input} WHERE id = 5"
                    exception "outside the supported 0001-9999 range"
                }
            }
            test {
                sql "SELECT CAST(value AS DATETIME(6)) AS event_time FROM ${input} ORDER BY id"
                exception "outside the supported 0001-9999 range"
            }
            context.getConnection().createStatement().withCloseable { statement ->
                statement.executeQuery("SELECT CAST(value AS DATETIME(6)) FROM ${input} WHERE id <= 4 ORDER BY id")
                        .withCloseable { rows ->
                    // Typed access preserves proleptic calendar dates and microseconds.
                    for (def expected : ["0001-01-01T00:00:00", "9999-12-31T23:59:59.999999",
                                          "1969-12-31T23:59:59.999999"]) {
                        assertTrue(rows.next())
                        assertEquals(LocalDateTime.parse(expected), rows.getObject(1, LocalDateTime.class))
                    }
                    assertTrue(rows.next())
                    assertNull(rows.getObject(1, LocalDateTime.class))
                    assertFalse(rows.next())
                }
            }
            test {
                sql "SELECT array(CAST(value AS DATETIME(6))) FROM ${input} WHERE id = 4"
                result [[[null]]]
            }
        }
    } finally {
        sql "DROP TABLE IF EXISTS flight_timestamp_input"
    }
}
