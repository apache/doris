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

suite("test_table_types_over_arrow_flight", "arrow_flight_sql") {
    def flightConn = context.getArrowFlightSqlConnection()
    def runOnFlight = { String statement ->
        JdbcUtils.executeToList(flightConn, statement)
    }
    runOnFlight "USE `${context.dbName}`"
    runOnFlight "DROP VIEW IF EXISTS test_flight_table_types_view"
    runOnFlight "DROP TABLE IF EXISTS test_flight_table_types_base"
    runOnFlight """
        CREATE TABLE test_flight_table_types_base (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    runOnFlight """
        CREATE VIEW test_flight_table_types_view AS
        SELECT id FROM test_flight_table_types_base
    """

    // These JDBC metadata methods consume Flight GetTableTypes/GetTables streams.
    // SQL result snapshots alone do not exercise those metadata RPCs.
    def metadata = flightConn.getMetaData()
    def typeRows = metadata.getTableTypes()
    try {
        def types = []
        while (typeRows.next()) {
            types.add(typeRows.getString("TABLE_TYPE"))
        }
        assertEquals(["BASE TABLE", "SYSTEM VIEW", "VIEW"], types)
    } finally {
        typeRows.close()
    }

    def getTables = { List<String> types ->
        def rows = metadata.getTables("internal", context.dbName, "test_flight_table_types%",
                types == null ? null : types.toArray(new String[0]))
        try {
            def tables = []
            while (rows.next()) {
                assertEquals("internal", rows.getString("TABLE_CAT"))
                assertEquals(context.dbName, rows.getString("TABLE_SCHEM"))
                tables.add([rows.getString("TABLE_NAME"), rows.getString("TABLE_TYPE")])
            }
            return tables.sort { left, right -> left[0] <=> right[0] }
        } finally {
            rows.close()
        }
    }

    def baseTable = ["test_flight_table_types_base", "BASE TABLE"]
    def view = ["test_flight_table_types_view", "VIEW"]
    assertEquals([baseTable, view], getTables(null))
    assertEquals([baseTable, view], getTables([]))
    assertEquals([baseTable], getTables(["BASE TABLE"]))
    assertEquals([view], getTables(["VIEW"]))
    // Both orders must work: forwarding only the first type loses or leaks rows.
    assertEquals([baseTable, view], getTables(["VIEW", "BASE TABLE"]))
    assertEquals([baseTable, view], getTables(["BASE TABLE", "VIEW"]))
    assertEquals([], getTables(["SYSTEM VIEW"]))
    assertEquals([], getTables(["UNKNOWN"]))
    assertEquals([view], getTables(["UNKNOWN", "VIEW", "VIEW"]))
}
