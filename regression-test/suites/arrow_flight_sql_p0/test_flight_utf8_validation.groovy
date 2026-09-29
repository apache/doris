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

suite("test_flight_utf8_validation", "arrow_flight_sql") {
    sql "DROP TABLE IF EXISTS flight_utf8_input"
    sql """CREATE TABLE flight_utf8_input (id INT, encoded STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    sql """INSERT INTO flight_utf8_input VALUES
           (1, '616263'), (2, 'E4B8ADF09F9880'), (3, ''), (4, NULL),
           (5, '84'), (6, 'C0AF'), (7, 'EDA080'), (8, 'F4908080'),
           (9, 'C2'), (10, 'A2')"""

    // TestAction uses the current JDBC connection, so explicitly bind it to Flight.
    def flightUrl = context.getArrowFlightSqlConnection().getMetaData().getURL()
    connect(context.config.jdbcUser, context.config.jdbcPassword, flightUrl) {
        def input = "${context.dbName}.flight_utf8_input"
        for (def id : [5, 6, 7, 8]) {
            test {
                sql "SELECT unhex(encoded) AS payload FROM ${input} WHERE id = ${id}"
                exception "Invalid UTF8"
            }
        }
        test {
            sql "SELECT unhex('84') AS payload"
            exception "Invalid UTF8"
        }
        // These bytes are valid only when concatenated; each row must be valid independently.
        test {
            sql "SELECT unhex(encoded) AS payload FROM ${input} WHERE id IN (9, 10) ORDER BY id"
            exception "Invalid UTF8"
        }
        for (def expression : ["array(unhex(encoded))",
                                "named_struct('child', unhex(encoded))",
                                "map('key', unhex(encoded))",
                                "map(unhex(encoded), 'value')"]) {
            test {
                sql "SELECT ${expression} AS nested FROM ${input} WHERE id = 5"
                exception "Invalid UTF8"
            }
        }
        // Reaching an invalid value after valid rows must fail rather than publish malformed text.
        test {
            sql "SELECT unhex(encoded) AS payload FROM ${input} ORDER BY id"
            exception "Invalid UTF8"
        }
        test {
            sql "SELECT unhex(encoded) FROM ${input} WHERE id <= 4 ORDER BY id"
            result [['abc'], ['中😀'], [''], [null]]
        }
        // Binary payloads may contain arbitrary bytes and must keep their binary Arrow type.
        context.getConnection().createStatement().withCloseable { statement ->
            statement.executeQuery("SELECT to_binary(encoded) AS payload FROM ${input} WHERE id = 5")
                    .withCloseable { rows ->
                assertTrue(rows.next())
                assertTrue(rows.getObject(1) instanceof byte[])
                assertEquals([0x84], rows.getBytes(1).collect { it & 0xff })
                assertFalse(rows.next())
            }
        }
    }
}
