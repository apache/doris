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

import org.apache.doris.regression.util.JdbcUtils

// Regression for https://github.com/apache/doris/issues/67368
//
// A UNIQUE KEY point query that matches the short circuit is executed by PointQueryExecutor instead of
// a Coordinator, and neither end of that path can serve an Arrow Flight result:
//
//  * Coordinator and NereidsCoordinator are the only places that register a FlightSqlEndpointsLocation,
//    so GetFlightInfo found no endpoint and failed with
//    "fetch arrow flight schema failed, no FlightSqlEndpointsLocations", dropping the row.
//  * The BE could not be pointed at either. tablet_fetch_data serializes with VMysqlResultWriter into
//    PTabletKeyLookupResponse.row_batch and runs no fragment, so the ArrowFlightResultBlockBuffer that
//    fetch_arrow_flight_schema looks up by finst id never exists.
//
// The fix keeps Arrow Flight SQL connections on the normal execution path, decided at plan time in
// LogicalResultSinkToShortCircuitPointQuery. The table below is the one from the issue.
suite("test_point_query_over_arrow_flight") {
    def mysqlConn = context.getConn()
    def flightConn = context.getArrowFlightSqlConnection()

    def runOnMysql = { String stmt ->
        def (result, meta) = JdbcUtils.executeToList(mysqlConn, stmt)
        return result
    }
    def runOnFlight = { String stmt ->
        def (result, meta) = JdbcUtils.executeToList(flightConn, stmt)
        return result
    }
    // The suite level explain{} action always runs on the MySQL connection, but the whole point here is
    // which protocol asked for the plan, so read the explain text off each connection explicitly.
    def explainOn = { conn, String stmt ->
        def (rows, meta) = JdbcUtils.executeToList(conn, "explain " + stmt)
        return rows.collect { row -> row.get(0).toString() }.join("\n")
    }

    def dbName = context.dbName
    runOnMysql "USE `${dbName}`"
    runOnFlight "USE `${dbName}`"

    def tblName = "test_point_query_over_arrow_flight_tbl"
    runOnMysql "DROP TABLE IF EXISTS ${tblName}"
    runOnMysql """
        CREATE TABLE ${tblName} (
            `col1` SMALLINT NOT NULL,
            `col2` INT NOT NULL,
            `loc3` CHAR(10) NOT NULL,
            `value` CHAR(10) NOT NULL,
            INDEX col3 (`loc3`) USING INVERTED,
            INDEX col2_idx (`col2`) USING INVERTED
        ) ENGINE=OLAP
        UNIQUE KEY(`col1`, `col2`, `loc3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `loc3`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "bloom_filter_columns" = "col1",
            "store_row_column" = "true",
            "enable_mow_light_delete" = "false"
        )
    """
    runOnMysql "INSERT INTO ${tblName} VALUES (10, 20, 'aabc', 'value')"

    def pointQuery = "SELECT * FROM ${tblName} WHERE col1 = 10 AND col2 = 20 AND loc3 = 'aabc'"

    // The short circuit is still taken on a MySQL connection: the fix is scoped to one protocol, it does
    // not disable the optimization. Assert this first, so a table that stopped qualifying for the short
    // circuit (schema or session variable drift) fails loudly here instead of making the flight
    // assertions below pass for the wrong reason.
    def mysqlExplain = explainOn(mysqlConn, pointQuery)
    assertTrue(mysqlExplain.contains("SHORT-CIRCUIT"),
            "the point query must still short circuit on a mysql connection, but got:\n" + mysqlExplain)

    // The same statement must be planned on the normal path over Arrow Flight SQL.
    def flightExplain = explainOn(flightConn, pointQuery)
    assertFalse(flightExplain.contains("SHORT-CIRCUIT"),
            "the point query must not short circuit on an arrow flight connection, but got:\n" + flightExplain)

    // This is the call that used to fail with "no FlightSqlEndpointsLocations".
    def (flightRows, flightMeta) = JdbcUtils.executeToList(flightConn, pointQuery)
    assertEquals(1, flightRows.size())
    assertEquals(10, flightRows[0][0] as int)
    assertEquals(20, flightRows[0][1] as int)
    assertEquals("aabc", flightRows[0][2].toString())
    assertEquals("value", flightRows[0][3].toString())

    // Both protocols must see the same row, one through the short circuit and one through the normal
    // plan.
    def mysqlRows = runOnMysql(pointQuery)
    assertEquals(1, mysqlRows.size())
    assertEquals(mysqlRows[0].collect { it.toString() }, flightRows[0].collect { it.toString() })

    // The BE produces the arrow batch, so the column types survive. Serving the point query result from
    // the FE instead would hand every column back as a string, because FlightSqlChannel.addResult builds
    // varchar vectors only.
    assertTrue(flightRows[0][0] instanceof Number,
            "col1 must stay numeric over arrow flight, but got: " + flightRows[0][0].getClass())
    assertTrue(flightRows[0][1] instanceof Number,
            "col2 must stay numeric over arrow flight, but got: " + flightRows[0][1].getClass())
    assertEquals(4, flightMeta.getColumnCount())

    // A key that matches no row is planned the same way and used to fail with the same error, so it is
    // not enough for the statement above to be the only shape that works.
    def emptyRows = runOnFlight("SELECT * FROM ${tblName} WHERE col1 = 11 AND col2 = 20 AND loc3 = 'aabc'")
    assertEquals(0, emptyRows.size())

    // The workaround reported in the issue keeps working, and a plain non point query on the same table
    // is unaffected.
    def hintRows = runOnFlight("SELECT /*+ SET_VAR(enable_short_circuit_query=false) */ * FROM ${tblName} "
            + "WHERE col1 = 10 AND col2 = 20 AND loc3 = 'aabc'")
    assertEquals(1, hintRows.size())
    assertEquals(1, runOnFlight("SELECT col1 FROM ${tblName} ORDER BY col1").size())

    runOnMysql "DROP TABLE IF EXISTS ${tblName}"
}
