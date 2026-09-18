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
import java.util.regex.Pattern
import java.util.concurrent.TimeUnit

import org.awaitility.Awaitility

// The Flight SQL JDBC driver on the classpath shades Arrow Flight; its FlightSqlClient is the
// one a test can drive directly (see test_session_options).
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightRuntimeException
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightStatusCode
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator

// An Arrow Flight SQL session is a connection of the same pool a MySQL connection is in: the
// user's max_user_connections counts both, whichever came first, and a refusal reads the same
// over either protocol - over Flight as the RESOURCE_EXHAUSTED status of the request that would
// have opened the session, since a session opens on its first request, not when the token is
// issued.
//
// The limit is 4 rather than 1 because the token manager still keeps at most
// max_user_connections / 2 bearer tokens per user and evicts the oldest - session included - when
// one more is issued; three MySQL connections take the slots a token cannot, so that the second
// Flight session is the one the pool refuses. (The per-user token cache goes with PR-3.2.)
//
// Not in the 'arrow_flight_sql' group on purpose: `sql` stays the MySQL control connection, and
// the sessions under test are raw Flight SQL clients and plain MySQL connections of their own.
suite("test_connection_quota") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    int port = context.config.otherConfigs.get("extArrowFlightSqlPort") as int

    String user = "flight_quota_user"
    String password = "Quota_12345"
    int limit = 4
    // What both protocols say when the user's limit is reached; only the count at the refusal varies.
    Pattern refusal = Pattern.compile("^Reach limit of connections\\. Total: (\\d+), User: ${limit}, Current: \\d+")

    sql "DROP USER IF EXISTS '${user}'"
    sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
    sql "GRANT SELECT_PRIV ON *.* TO '${user}'"
    // In cloud mode a query runs on a compute group; a user with no USAGE_PRIV on one is refused with
    // an INTERNAL error when it runs a statement (the Flight SELECT 1 below), before the pool's quota
    // is ever reached. Grant it the way the other cloud Flight suites do (see test_auth_remote_ip).
    if (isCloudMode()) {
        def computeGroups = sql "SHOW COMPUTE GROUPS"
        assertTrue(!computeGroups.isEmpty(), "cloud mode but SHOW COMPUTE GROUPS returned nothing")
        sql "GRANT USAGE_PRIV ON COMPUTE GROUP '${computeGroups[0][0]}' TO '${user}'"
    }
    sql "SET PROPERTY FOR '${user}' 'max_user_connections' = '${limit}'"

    // The connection quota is enforced per-FE, and information_schema.processlist is a cluster-wide
    // view when fetch_all_fe_for_system_table is on (the default): on a multi-FE cluster the count then
    // depends on which FE answers and can include connections on another FE, so a per-FE quota test
    // cannot rely on it. Anchor everything to the one FE that serves the configured Flight endpoint --
    // open the MySQL connections on that FE (its Flight host, the query port and options from jdbcUrl)
    // and count only that FE's own connections (fetch_all_fe_for_system_table = false). Without this the
    // MySQL connections, the Flight sessions and the counting query need not land on the same FE and the
    // count never settles (build 1049514).
    def jdbcMatcher = (context.config.jdbcUrl =~ /^(jdbc:mysql:\/\/)[^\/:@]+(:\d+.*)$/)
    assertTrue(jdbcMatcher.matches(),
            "cannot derive the Flight FE's MySQL url from jdbcUrl: ${context.config.jdbcUrl}")
    def feMysqlUrl = jdbcMatcher.replaceFirst("\$1${host}\$2")
    def allocator = new RootAllocator()
    def client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
    def flight = new FlightSqlClient(client)
    def mysqlConnections = []
    def openTokens = []
    def countConn = null
    try {
        // A dedicated connection on the Flight FE that reports only that FE's connections, so the count
        // is exactly the MySQL connections and Flight sessions this suite opened there.
        countConn = DriverManager.getConnection(feMysqlUrl, context.config.jdbcUser, context.config.jdbcPassword)
        countConn.createStatement().withCloseable { it.execute("SET fetch_all_fe_for_system_table = false") }
        // The anchoring above assumes the FE reached at feMysqlUrl is the one serving the configured
        // Flight endpoint (its host, jdbcUrl's query port). Assert it here -- otherwise the suite would
        // fail later at the quota checks with a confusing pool-pointing error -- by comparing this FE's
        // own arrow_flight_sql_port to the configured Flight port. forward_to_master keeps SHOW FRONTEND
        // CONFIG reporting this FE rather than the master.
        countConn.createStatement().withCloseable { it.execute("SET forward_to_master = false") }
        countConn.createStatement().withCloseable { st ->
            def rs = st.executeQuery("SHOW FRONTEND CONFIG LIKE 'arrow_flight_sql_port'")
            assertTrue(rs.next(), "SHOW FRONTEND CONFIG returned no arrow_flight_sql_port")
            assertEquals(port as String, rs.getString("Value"),
                    "the FE reached at ${feMysqlUrl} does not serve the Flight endpoint on port ${port}; "
                            + "this suite needs MySQL, Flight and the count query on the same FE")
        }
        // The user's connections as that FE's pool sees them: MySQL connections and Flight sessions alike.
        def connectionsOf = {
            def st = countConn.createStatement()
            try {
                def rs = st.executeQuery(
                        "SELECT COUNT(*) FROM information_schema.processlist WHERE User = '${user}'")
                rs.next()
                return rs.getInt(1)
            } finally {
                st.close()
            }
        }
        // A closed MySQL connection leaves the pool on the frontend's nio thread after the client's
        // COM_QUIT, and Connector/J does not wait for that; so wait here before the next connection
        // is opened against the count.
        def awaitConnections = { int expected ->
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                    .until { connectionsOf() == expected }
        }
        // Closes a session the way the drivers do, once; a token whose session is already gone is
        // UNAUTHENTICATED and needs nothing.
        def closeSession = { cred ->
            try {
                flight.closeSession(new CloseSessionRequest(), cred)
            } catch (FlightRuntimeException e) {
                assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code())
            }
            openTokens.remove(cred)
        }
        // Whether the pool refused this token's session: its RESOURCE_EXHAUSTED refusal message, or null
        // when the session was admitted. The quota is checked when the session is registered, before the
        // probe query runs (DorisFlightSqlProducer.getFlightInfoStatement gets the ConnectContext, which
        // registers it, and only then executes the statement). So a RESOURCE_EXHAUSTED is the refusal --
        // the session never entered the pool and the frontend invalidated the token -- while any other
        // outcome (success, or the statement failing for a reason outside this suite's scope, e.g. an
        // environment-specific query error) means the session was admitted and is in the pool; the
        // connectionsOf() checks below verify that count. The token is tracked before the request so a
        // session that opened is closed at the end.
        def refusalOf = { cred ->
            openTokens << cred
            try {
                flight.execute("SELECT 1", cred).getEndpoints()
                return null
            } catch (FlightRuntimeException e) {
                def code = e.status().code()
                if (code == FlightStatusCode.RESOURCE_EXHAUSTED) {
                    openTokens.remove(cred)
                    return e.status().description()
                }
                if (code == FlightStatusCode.INTERNAL) {
                    // Admitted: the pool check passed and the session was registered before the statement
                    // ran, so it counts (the connectionsOf() checks confirm it). The probe query then
                    // failed for a reason outside a connection-quota test's scope -- an environment-
                    // specific execution error -- which is logged here so it stays diagnosable.
                    logger.warn("test_connection_quota: an admitted Flight session's probe query failed "
                            + "with ${code}: ${e.status().description()}")
                    return null
                }
                // Neither a quota refusal nor an admitted session (e.g. UNAUTHENTICATED); let it surface.
                throw e
            }
        }
        // The pool's limit named by a refusal, asserting the refusal is worded as MySQL's is.
        def totalOf = { String message ->
            def m = refusal.matcher(message)
            assertTrue(m.find(), "expected the MySQL wording of the refusal, got: ${message}")
            return m.group(1)
        }
        // A MySQL connection of the user that could not open: the refusal, or null when it opened.
        def mysqlRefusal = {
            try {
                mysqlConnections << DriverManager.getConnection(feMysqlUrl, user, password)
                return null
            } catch (SQLException e) {
                return e.getMessage()
            }
        }

        // 1. Three MySQL connections, then the first Flight session: the user's four.
        (1..limit - 1).each { assertNull(mysqlRefusal(), "MySQL connection ${it} of ${limit - 1} was refused") }
        awaitConnections(limit - 1)
        def first = client.authenticateBasicToken(user, password).get()
        assertNull(refusalOf(first), "the Flight session should open as the user's last connection")
        assertEquals(limit, connectionsOf(), "the Flight session is a connection of the user's like the others")

        // 2. The second Flight session is refused, in MySQL's words...
        def second = client.authenticateBasicToken(user, password).get()
        String flightRefused = refusalOf(second)
        assertNotNull(flightRefused, "the second Flight session opened although the user's limit is reached")
        String total = totalOf(flightRefused)

        // 3. ...and so is a MySQL connection, in the same words.
        String mysqlRefused = mysqlRefusal()
        assertNotNull(mysqlRefused, "the MySQL connection opened although the user's limit is reached")
        assertEquals(total, totalOf(mysqlRefused))

        // 4. CloseSession releases the Flight session's connection: a MySQL connection opens now, and
        //    once it is closed again a Flight session does.
        assertEquals("CLOSED", flight.closeSession(new CloseSessionRequest(), first).getStatus().name())
        openTokens.remove(first)
        awaitConnections(limit - 1)
        assertNull(mysqlRefusal(), "the MySQL connection should open once the Flight session is closed")
        mysqlConnections.remove(mysqlConnections.size() - 1).close()
        awaitConnections(limit - 1)
        def third = client.authenticateBasicToken(user, password).get()
        assertNull(refusalOf(third), "the Flight session should open once the MySQL connection is closed")
        assertEquals(limit, connectionsOf(), "the reopened Flight session must be the user's ${limit}th connection")
        closeSession(third)
        awaitConnections(limit - 1)
    } finally {
        // Sessions outlive the client: close the ones still open, or a rerun on the same frontend
        // starts against a user whose slots they hold until wait_timeout (DROP USER does not end them).
        // Nothing here asserts or throws: the failure that brought the suite here, if any, is the one
        // reported, and every step of the cleanup runs.
        def quietly = { String what, Closure step ->
            try {
                step()
            } catch (Exception e) {
                logger.warn("cleanup of test_connection_quota: ${what} failed: ${e.message}")
            }
        }
        openTokens.each { cred ->
            quietly("closing a session left open") {
                try {
                    flight.closeSession(new CloseSessionRequest(), cred)
                } catch (FlightRuntimeException e) {
                    // A token whose session is already gone is UNAUTHENTICATED and needs nothing.
                    if (e.status().code() != FlightStatusCode.UNAUTHENTICATED) {
                        throw e
                    }
                }
            }
        }
        mysqlConnections.each { conn -> quietly("closing a MySQL connection") { conn.close() } }
        quietly("closing the count connection") { if (countConn != null) countConn.close() }
        quietly("closing the Flight client") { client.close() }
        quietly("closing the allocator") { allocator.close() }
        sql "DROP USER IF EXISTS '${user}'"
    }
}
