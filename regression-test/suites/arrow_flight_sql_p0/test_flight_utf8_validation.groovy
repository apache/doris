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
    // Reuse Flight credentials and avoid the TLS-dependent early return in connect().
    def flight = context.getArrowFlightSqlConnection()
    def input = "${context.dbName}.flight_utf8_input"
    def expectInvalidUtf8 = { String query ->
        try {
            arrow_flight_sql(query)
            assertTrue(false, "Expected an invalid UTF-8 Flight result error")
        } catch (Exception error) {
            assertTrue(error.toString().contains("Invalid UTF8"), error.toString())
        }
    }
    arrow_flight_sql "DROP TABLE IF EXISTS ${input}"
    arrow_flight_sql """CREATE TABLE ${input} (id INT, encoded STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ("replication_num" = "1")"""
    try {
        arrow_flight_sql """INSERT INTO ${input} VALUES
               (1, '616263'), (2, 'E4B8ADF09F9880'), (3, ''), (4, NULL),
               (5, '84'), (6, 'C0AF'), (7, 'EDA080'), (8, 'F4908080'),
               (9, 'C2'), (10, 'A2')"""
        for (def id : [5, 6, 7, 8]) {
            expectInvalidUtf8("SELECT unhex(encoded) AS payload FROM ${input} WHERE id = ${id}")
        }
        // Constant folding can replace invalid bytes with valid text before Flight conversion.
        // Keep a table scan and skip folding so the constant is evaluated and validated on the BE.
        expectInvalidUtf8("""SELECT /*+ SET_VAR(debug_skip_fold_constant=true) */
                unhex('84') AS payload FROM ${input} WHERE id = 1""")
        // These bytes are valid only when concatenated; each row must be valid independently.
        expectInvalidUtf8("SELECT unhex(encoded) AS payload FROM ${input} WHERE id IN (9, 10) ORDER BY id")
        for (def expression : ["array(unhex(encoded))",
                                "named_struct('child', unhex(encoded))",
                                "map('key', unhex(encoded))",
                                "map(unhex(encoded), 'value')"]) {
            expectInvalidUtf8("SELECT ${expression} AS nested FROM ${input} WHERE id = 5")
        }
        // Reaching an invalid value after valid rows must fail rather than publish malformed text.
        expectInvalidUtf8("SELECT unhex(encoded) AS payload FROM ${input} ORDER BY id")
        assertEquals([['abc'], ['中😀'], [''], [null]],
                arrow_flight_sql("SELECT unhex(encoded) FROM ${input} WHERE id <= 4 ORDER BY id"))
        // Binary payloads may contain arbitrary bytes and must keep their binary Arrow type.
        flight.createStatement().withCloseable { statement ->
            statement.executeQuery("SELECT to_binary(encoded) AS payload FROM ${input} WHERE id = 5")
                    .withCloseable { rows ->
                assertTrue(rows.next())
                assertTrue(rows.getObject(1) instanceof byte[])
                assertEquals([0x84], rows.getBytes(1).collect { it & 0xff })
                assertFalse(rows.next())
            }
        }
    } finally {
        arrow_flight_sql "DROP TABLE IF EXISTS ${input}"
    }
}
