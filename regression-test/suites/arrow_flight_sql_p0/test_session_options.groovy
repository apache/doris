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

import java.sql.DriverManager
import java.sql.SQLException

// The Flight SQL JDBC driver on the classpath shades Arrow Flight; its FlightSqlClient is the
// same client the ADBC and JDBC drivers speak the session actions with.
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightRuntimeException
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightStatusCode
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.GetSessionOptionsRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.NoOpSessionOptionValueVisitor
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.SessionOptionValueFactory
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.SetSessionOptionsRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator

// The session actions of Arrow Flight SQL on a Doris session: SetSessionOptions sets the current
// catalog (`catalog`, what the ADBC driver sends for adbc.connection.catalog and the JDBC driver for
// its catalog property), the current database (`schema`, adbc.connection.db_schema) and session
// variables (any other name), each answered on its own with INVALID_NAME / INVALID_VALUE / ERROR;
// GetSessionOptions reads them back as SHOW VARIABLES text; CloseSession invalidates the bearer
// token at once.
//
// Not in the 'arrow_flight_sql' group on purpose: `sql` stays the MySQL control connection, and the
// session under test is a raw Flight SQL client of its own.
suite("test_session_options") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    int port = context.config.otherConfigs.get("extArrowFlightSqlPort") as int
    String user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String password = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    sql "DROP TABLE IF EXISTS session_options_tbl"
    sql "CREATE TABLE session_options_tbl (k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')"

    def allocator = new RootAllocator()
    def client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
    try {
        // The bearer token the session is known by, obtained the way the drivers obtain it.
        def cred = client.authenticateBasicToken(user, password).get()
        def flight = new FlightSqlClient(client)

        def asString = new NoOpSessionOptionValueVisitor<String>() {
            @Override
            String visit(String value) { return value }
        }
        def options = {
            def result = flight.getSessionOptions(new GetSessionOptionsRequest(), cred).getSessionOptions()
            return result.collectEntries { name, value -> [(name): value.acceptVisitor(asString)] }
        }
        // The names that could not be set, with why, as the driver would report them.
        def set = { Map values ->
            def result = flight.setSessionOptions(new SetSessionOptionsRequest(values), cred)
            return result.getErrors().collectEntries { name, error -> [(name): error.value.name()] }
        }
        def str = { String value -> SessionOptionValueFactory.makeSessionOptionValue(value) }
        // The rows of a statement the frontend answers itself, pulled from the frontend over the
        // same session; every column of such a result is text.
        def rows = { String stmt ->
            logger.info("flight: ${stmt}".toString())
            def info = flight.execute(stmt, cred)
            def out = []
            info.getEndpoints().each { endpoint ->
                def stream = flight.getStream(endpoint.getTicket(), cred)
                try {
                    while (stream.next()) {
                        def root = stream.getRoot()
                        for (int i = 0; i < root.getRowCount(); i++) {
                            out << root.getFieldVectors().collect { it.isNull(i) ? null : it.getObject(i).toString() }
                        }
                    }
                } finally {
                    stream.close()
                }
            }
            return out
        }
        def statementFails = { String stmt, String fragment ->
            try {
                rows(stmt)
            } catch (FlightRuntimeException e) {
                assertTrue(e.getMessage().contains(fragment),
                        "expected '${stmt}' to fail with '${fragment}', got: ${e.getMessage()}")
                return
            }
            throw new AssertionError("'${stmt}' did not fail with '${fragment}'")
        }

        // 1. A new session: the internal catalog, no database, and every session variable as
        //    SHOW VARIABLES shows it.
        def initial = options()
        assertEquals("internal", initial["catalog"])
        assertEquals("", initial["schema"])
        def shown = rows("SHOW VARIABLES LIKE 'wait_timeout'")
        assertEquals(shown[0][1], initial["wait_timeout"])
        assertTrue(initial.size() > 100, "expected the session variables, got ${initial.size()} options")

        // 2. `schema` is the current database: the statements of the session run in it afterwards.
        statementFails("SHOW TABLES LIKE 'session_options_tbl'", "No database selected")
        assertEquals([:], set([schema: str(context.dbName)]))
        assertEquals(context.dbName, options()["schema"])
        assertEquals(1, rows("SHOW TABLES LIKE 'session_options_tbl'").size())

        // 3. `catalog` is the current catalog; switching to the one the session is in keeps its database.
        assertEquals([:], set([catalog: str("internal")]))
        assertEquals("internal", options()["catalog"])
        assertEquals(context.dbName, options()["schema"])

        // 4. A session variable takes a value of its own type or a string, reads back as text, and
        //    is what the session's statements see. The empty value sets it back to its default.
        def defaultTimeout = rows("SHOW VARIABLES LIKE 'query_timeout'")[0][2]
        assertEquals([:], set([query_timeout: SessionOptionValueFactory.makeSessionOptionValue(77L)]))
        assertEquals("77", options()["query_timeout"])
        assertEquals("77", rows("SHOW VARIABLES LIKE 'query_timeout'")[0][1])
        assertEquals([:], set([query_timeout: str("88"), enable_profile: SessionOptionValueFactory.makeSessionOptionValue(true)]))
        assertEquals("88", options()["query_timeout"])
        assertEquals("true", options()["enable_profile"])
        assertEquals([:], set([query_timeout: SessionOptionValueFactory.makeEmptySessionOptionValue()]))
        assertEquals(defaultTimeout, options()["query_timeout"])

        // 5. Each option of a request is set on its own and answered on its own. A variable is named
        //    as GetSessionOptions names it, and only so: not in another case, not without the prefix
        //    an experimental one is shown with, and a hidden or a retired one is no option at all.
        assertEquals([no_such_variable: "INVALID_NAME"], set([no_such_variable: str("1")]))
        assertEquals([Query_Timeout: "INVALID_NAME"], set([Query_Timeout: str("1")]))
        assertEquals([enable_shared_scan: "INVALID_NAME"], set([enable_shared_scan: str("true")]))
        assertEquals([:], set([experimental_enable_shared_scan: str("true")]))
        assertEquals("true", options()["experimental_enable_shared_scan"])
        assertEquals([enable_local_exchange: "INVALID_NAME", enable_nereids_dml: "INVALID_NAME"],
                set([enable_local_exchange: str("true"), enable_nereids_dml: str("true")]))
        assertEquals([schema: "INVALID_VALUE"], set([schema: str("no_such_db_for_session_options")]))
        assertEquals(context.dbName, options()["schema"])
        assertEquals([catalog: "INVALID_VALUE"], set([catalog: str("no_such_catalog_for_session_options")]))
        assertEquals([catalog: "INVALID_VALUE"], set([catalog: str("not.a.catalog.name")]))
        assertEquals("internal", options()["catalog"])
        assertEquals([query_timeout: "INVALID_VALUE"], set([query_timeout: str("not a number")]))
        assertEquals([net_buffer_length: "ERROR"], set([net_buffer_length: str("1")]))
        assertEquals([no_such_variable: "INVALID_NAME", time_zone: "INVALID_VALUE"],
                set([query_timeout: str("66"), no_such_variable: str("1"), time_zone: str("Mars/Olympus_Mons")]))
        assertEquals("66", options()["query_timeout"])

        // 6. A string value arrives as sent, quotes and backslashes included.
        String text = "it's \"quoted\" and back\\slashed"
        assertEquals([:], set([session_context: str(text)]))
        assertEquals(text, options()["session_context"])

        // 7. CloseSession invalidates the bearer token: the session is gone, and so is the token.
        assertEquals("CLOSED", flight.closeSession(new CloseSessionRequest(), cred).getStatus().name())
        try {
            flight.getSessionOptions(new GetSessionOptionsRequest(), cred)
            throw new AssertionError("the closed session's token is still accepted")
        } catch (FlightRuntimeException e) {
            assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code())
        }
    } finally {
        client.close()
        allocator.close()
    }

    // 8. What a real driver does with it: the Flight SQL JDBC driver sets the `catalog` session
    //    option while connecting when the URL names one, and refuses to connect when that fails.
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
    def jdbcUrl = { String catalog ->
        "jdbc:arrow-flight-sql://${host}:${port}/?catalog=${catalog}&useServerPrepStmts=false&useSSL=false&useEncryption=false"
    }
    def conn = DriverManager.getConnection(jdbcUrl("internal"), user, password)
    try {
        def rs = conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'wait_timeout'")
        assertTrue(rs.next())
    } finally {
        conn.close()
    }
    try {
        DriverManager.getConnection(jdbcUrl("no_such_catalog_for_session_options"), user, password).close()
        throw new AssertionError("connected although the catalog session option was refused")
    } catch (SQLException e) {
        def messages = []
        for (Throwable t = e; t != null; t = t.getCause()) {
            messages << t.getMessage()
        }
        assertTrue(messages.any { it != null && it.contains("Cannot set session option for catalog") },
                "expected the driver to report the refused catalog option, got: ${messages}")
    }
}
