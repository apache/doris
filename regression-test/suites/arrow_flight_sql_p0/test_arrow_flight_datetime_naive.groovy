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

// apache/doris#65741: session variable enable_arrow_flight_datetime_naive.
// When enabled, DATETIME/DATETIMEV2 is returned over Arrow Flight as a timezone-naive Arrow
// timestamp carrying the wall-clock value. Read as a plain string (getString(), like pyarrow),
// the value is exact and independent of the client timezone. Note that reading it as
// java.sql.Timestamp (getObject()) anchors the naive value with the client JVM timezone, so that
// path is timezone-dependent by nature -- see the getObject on/off difference check below.
// The BE-side schema mapping for both switch states is asserted in the BE unit test
// DataTypeSerDeArrowTest.DateTimeV2ArrowTimezoneDependsOnNaiveFlag.
suite("test_arrow_flight_datetime_naive", "arrow_flight_sql") {
    def q = "select cast('2024-07-19 12:00:00.123456' as datetime(6)) as ts"
    // Use one Arrow Flight connection so the SET applies to the same session as the query.
    def conn = context.getArrowFlightSqlConnection()

    def setNaive = { boolean on ->
        conn.createStatement().withCloseable { st ->
            st.execute("set enable_arrow_flight_datetime_naive = " + on)
        }
    }
    def readString = {
        conn.createStatement().withCloseable { st ->
            def rs = st.executeQuery(q)
            assertTrue(rs.next())
            return rs.getString(1)
        }
    }
    def readObject = {
        conn.createStatement().withCloseable { st ->
            def rs = st.executeQuery(q)
            assertTrue(rs.next())
            return String.valueOf(rs.getObject(1))
        }
    }

    // With the switch ON, getString() returns the wall-clock value unchanged, in any client tz.
    setNaive(true)
    assertEquals("2024-07-19 12:00:00.123456", readString())

    // The switch changes the wire encoding. On a non-UTC client, the timezone-aware (off) and
    // timezone-naive (on) values differ when read as java.sql.Timestamp via getObject(). This also
    // confirms the SET actually took effect over Arrow Flight.
    if (java.util.TimeZone.getDefault().getRawOffset() != 0) {
        setNaive(false)
        def off = readObject()
        setNaive(true)
        def on = readObject()
        logger.info("arrow flight datetime getObject: off=${off}, on=${on}".toString())
        assertTrue(off != on)
    }
}
