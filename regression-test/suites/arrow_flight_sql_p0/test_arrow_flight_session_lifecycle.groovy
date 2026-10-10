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

import java.sql.Types

// One Arrow Flight SQL session, many requests. A statement the frontend answers itself (SHOW, SET,
// EXPLAIN, DESC, DDL) has its result cached on the session for the client's DoGet, every column as
// text; a query is run on the backend and pulled from there, typed; underneath, the session core
// (variables, current database) is one and the same. The session's protocol adapter decides per
// statement where the result is (FlightProtocolAdapter.beforeStatement / beforeQuery) and drops
// what the previous request left behind (beginRequest), so a session can alternate between the two
// kinds of statements without one leaking into the next.
//
// Not in the 'arrow_flight_sql' group on purpose: `sql` stays the MySQL control connection, and the
// statements under test go to the raw Flight connection so that nothing prepends `USE <db>;`.
suite("test_arrow_flight_session_lifecycle") {
    def tableName = "arrow_flight_session_lifecycle"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (k INT, v VARCHAR(20))
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO ${tableName} VALUES (1, 'a'), (2, 'b'), (3, 'c')"

    def flightConn = context.getArrowFlightSqlConnection()

    // The rows a statement returns over the Flight session, and the JDBC types of its columns.
    def flight = { String stmt ->
        logger.info("flight: ${stmt}".toString())
        def (rows, meta) = JdbcUtils.executeQueryToList(flightConn, stmt)
        def types = (1..meta.getColumnCount()).collect { meta.getColumnType(it) }
        def names = (1..meta.getColumnCount()).collect { meta.getColumnName(it) }
        return [rows: rows, types: types, names: names]
    }
    // Arrow JDBC hands a TINYINT back as a Byte and a BIGINT as a Long; compare the values, not the boxes.
    def nums = { List<List<Object>> rowList -> rowList.collect { row -> row.collect { it instanceof Number ? ((Number) it).longValue() : it } } }
    def flightFails = { String stmt, String fragment ->
        try {
            flight(stmt)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(fragment),
                    "expected the error of '${stmt}' to mention '${fragment}', got: ${e.getMessage()}")
            return
        }
        throw new AssertionError("'${stmt}' did not fail with '${fragment}'")
    }

    // The session's current database is set by a request of its own and holds for the rest.
    def use = flight("USE ${context.dbName}")
    assertEquals(["StatusResult"], use.names)

    // 1. A result the frontend materializes itself is delivered from the frontend, as text.
    def variables = flight("SHOW VARIABLES LIKE 'wait_timeout'")
    assertEquals(1, variables.rows.size())
    assertEquals("wait_timeout", variables.rows[0][0].toString())
    assertTrue(variables.types.every { it == Types.VARCHAR }, "expected text columns, got ${variables.types}")

    // 2. A query is run on the backend and pulled from there, typed. That includes a query the
    //    planner could answer on the frontend: a Flight client expects typed Arrow data, and the
    //    frontend-side result is text (FlightProtocolAdapter.supportsFeSideResult).
    def rows = flight("SELECT k, v FROM ${tableName} ORDER BY k")
    assertEquals([[1L, "a"], [2L, "b"], [3L, "c"]], nums(rows.rows))
    assertEquals([Types.INTEGER, Types.VARCHAR], rows.types)
    def literal = flight("SELECT 1")
    assertEquals([[1L]], nums(literal.rows))
    assertEquals(Types.TINYINT, literal.types[0])
    def sessionVar = flight("SELECT @@wait_timeout")
    assertTrue(sessionVar.types[0] in [Types.INTEGER, Types.BIGINT], "session variable came back as ${sessionVar.types}")

    // 3. The two kinds of statements alternate on one session; each request delivers exactly the
    //    result of its own statement, wherever the previous one left its result.
    assertEquals([[1L]], nums(flight("SELECT 1").rows))
    assertEquals("wait_timeout", flight("SHOW VARIABLES LIKE 'wait_timeout'").rows[0][0].toString())
    assertEquals([[2L]], nums(flight("SELECT k FROM ${tableName} WHERE k = 2").rows))
    flight("SET enable_profile = false")
    assertEquals([[3L]], nums(flight("SELECT count(*) FROM ${tableName}").rows))
    def explain = flight("EXPLAIN SELECT k FROM ${tableName}")
    assertTrue(explain.rows.collect { it[0].toString() }.join("\n").contains("OlapScanNode"),
            "EXPLAIN over Flight returned no plan: ${explain.rows}")
    assertEquals([Types.VARCHAR], explain.types)
    def desc = flight("DESC ${tableName}")
    assertEquals(["k", "v"], desc.rows.collect { it[0].toString() })
    assertEquals([[3L]], nums(flight("SELECT max(k) FROM ${tableName}").rows))

    // 4. EXPLAIN PLAN PROCESS has a result over Flight too; it used to return nothing because it
    //    bypassed the frontend-side result path.
    def process = flight("EXPLAIN PLAN PROCESS SELECT k FROM ${tableName}")
    assertEquals(["Rule", "Before", "After"], process.names)
    assertTrue(process.rows.size() > 0, "EXPLAIN PLAN PROCESS over Flight returned no rows")

    // 5. Session state set by one request is seen by the next, from either side: a user variable,
    //    and a session variable read back through a frontend-side SHOW and a backend query.
    flight("SET @lifecycle_var = 42")
    assertEquals([[42L]], nums(flight("SELECT @lifecycle_var").rows))
    flight("SET query_timeout = 1234")
    try {
        assertEquals("1234", flight("SHOW VARIABLES LIKE 'query_timeout'").rows[0][1].toString())
        assertEquals([[1234L]], nums(flight("SELECT @@query_timeout").rows))
    } finally {
        flight("SET query_timeout = DEFAULT")
    }

    // 6. A request may carry several statements; only the last one may return a result, wherever
    //    the result is. The request that ends in a query is fine; one that produced a result before
    //    its last statement is refused, whether the frontend cached that result or a backend holds
    //    it -- the endpoints of two queries would otherwise be delivered as one result, and a query
    //    before a SET would have its endpoints dropped for the SET's status.
    assertEquals([[2L]], nums(flight("SET @multi = 2; SELECT k FROM ${tableName} WHERE k = 2").rows))
    flightFails("SHOW VARIABLES LIKE 'wait_timeout'; SELECT 1",
            "Only be one stmt that returns the result and it is at the end")
    flightFails("SELECT k FROM ${tableName} WHERE k = 1; SELECT k FROM ${tableName} WHERE k = 2",
            "Only be one stmt that returns the result and it is at the end")
    flightFails("SELECT k FROM ${tableName} WHERE k = 1; SET @multi = 3",
            "Only be one stmt that returns the result and it is at the end")
    assertEquals([[2L]], nums(flight("SELECT @multi").rows))
    assertEquals([[3L]], nums(flight("SELECT count(*) FROM ${tableName}").rows))

    // 7. A failed statement leaves the session usable.
    flightFails("SELECT * FROM no_such_table_lifecycle", "does not exist")
    assertEquals([[3L]], nums(flight("SELECT count(*) FROM ${tableName}").rows))
    flightFails("SELECT k FROM ${tableName} WHERE", "mismatched input")
    assertEquals([[1L, "a"]], nums(flight("SELECT k, v FROM ${tableName} WHERE k = 1").rows))

    // 8. A DDL over Flight takes effect and answers with the synthesized status row.
    sql "DROP TABLE IF EXISTS ${tableName}_ddl"
    def ddl = flight("CREATE TABLE ${tableName}_ddl (k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1"
            + " PROPERTIES ('replication_num' = '1')")
    assertEquals(["StatusResult"], ddl.names)
    assertEquals(1, flight("SHOW TABLES LIKE '${tableName}_ddl'").rows.size())
    flight("DROP TABLE ${tableName}_ddl")
    assertEquals(0, flight("SHOW TABLES LIKE '${tableName}_ddl'").rows.size())

    sql "DROP TABLE IF EXISTS ${tableName}"
}
