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

// Doris numbers DATE values in MySQL's calendar, where year 0 is not a leap year. Arrow `date32`
// is days since 1970-01-01 in the proleptic Gregorian calendar, where year 0 IS a leap year, so
// the two disagree for 0000-01-01 .. 0000-02-28. The MySQL protocol ships the year/month/day
// fields directly and therefore never depends on a calendar; it is the control path here. Any
// difference between the two protocols means the Arrow day ordinal is wrong.
// See https://github.com/apache/doris/issues/67366
suite("test_date_year_zero", "arrow_flight_sql") {
    sql "DROP TABLE IF EXISTS test_date_year_zero"
    sql """
        CREATE TABLE test_date_year_zero (
            id INT,
            d DATE,
            d_null DATE NULL,
            arr ARRAY<DATE>
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO test_date_year_zero VALUES
            (1, '0000-01-01', '0000-01-01', ['0000-01-01', '0000-02-28']),
            (2, '0000-02-28', NULL,         ['0000-03-01']),
            (3, '0000-03-01', '0000-03-01', ['0001-01-01']),
            (4, '0001-01-01', '0001-01-01', ['1969-12-31', '1970-01-01']),
            (5, '1969-12-31', '1969-12-31', ['2024-01-01']),
            (6, '1970-01-01', '1970-01-01', ['9999-12-31']),
            (7, '2024-01-01', '2024-01-01', ['0000-01-01']),
            (8, '9999-12-31', '9999-12-31', NULL);
    """

    // The absolute anchor. The MySQL protocol carries year/month/day verbatim, so these strings
    // are what the Arrow path below must reproduce. Cast to string: java.sql.Date runs year-zero
    // values through the Julian/Gregorian hybrid calendar.
    order_qt_source """
        SELECT CAST(id AS STRING), CAST(d AS STRING), CAST(d_null AS STRING)
        FROM test_date_year_zero
    """

    // Read through the driver's own rendering rather than getObject(), for the same reason.
    def fetchStrings = { java.sql.Connection conn, String stmtSql ->
        def rows = []
        conn.prepareStatement(stmtSql).withCloseable { st ->
            st.executeQuery().withCloseable { rs ->
                def columnCount = rs.metaData.columnCount
                while (rs.next()) {
                    def row = []
                    for (int i = 1; i <= columnCount; ++i) {
                        row.add(rs.getString(i))
                    }
                    rows.add(row)
                }
            }
        }
        return rows
    }

    def query = "SELECT id, d, d_null FROM test_date_year_zero ORDER BY id"
    def viaMysql = fetchStrings(context.getConnection(), query)
    def viaFlight = fetchStrings(context.getArrowFlightSqlConnection(),
                                 "USE ${context.dbName};" + query)
    assertEquals(8, viaMysql.size())
    // Before the fix, Arrow reported 0000-01-02 for row 1 and 0000-02-29 for row 2.
    assertEquals(viaMysql, viaFlight,
                 "the Arrow and MySQL protocols disagree on the DATE values")

    // ARRAY<DATE> has no encoding of its own, it delegates each element to the DATE SerDe. The
    // Arrow Flight JDBC driver renders a list<date32> as its raw day counts rather than as dates,
    // which makes this the strictest check available here: it pins the exact wire values.
    // 0000-01-01 is -719528 and 0000-02-28 is -719470 in the proleptic Gregorian calendar; the
    // pre-fix encoding produced -719527 and -719469.
    def arrayQuery = "SELECT arr FROM test_date_year_zero WHERE id = 1"
    def arrayViaFlight =
            fetchStrings(context.getArrowFlightSqlConnection(),
                         "USE ${context.dbName};" + arrayQuery)[0][0].toString()
    def arrayElements = arrayViaFlight.replaceAll(/[\[\]\s"]/, "").split(",") as List
    assertEquals(["-719528", "-719470"], arrayElements,
                 "ARRAY<DATE> elements are not proleptic Gregorian day counts, got: ${arrayViaFlight}")
}
