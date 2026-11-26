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

// A bearer token is the credential of exactly one Arrow Flight SQL session and is valid exactly as
// long as it: the session opens, and the token is issued, at the handshake that authenticates the
// password, and the session ends with CloseSession, with a KILL CONNECTION from another connection
// or with wait_timeout -- from that moment on, the very next call under the token is refused as
// UNAUTHENTICATED, and no other call of the session succeeds in between. There is no lifetime of
// the token's own: an active session is never cut off by a token expiry.
//
// Not in the 'arrow_flight_sql' group on purpose: `sql` stays the MySQL control connection, and
// the sessions under test are raw Flight SQL clients of their own.
suite("test_bearer_token_lifecycle") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    int port = context.config.otherConfigs.get("extArrowFlightSqlPort") as int

    String user = "flight_token_user"
    String password = "Token_12345"

    sql "DROP USER IF EXISTS '${user}'"
    sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
    sql "GRANT SELECT_PRIV ON *.* TO '${user}'"
    if (isCloudMode()) {
        def computeGroups = sql "SHOW COMPUTE GROUPS"
        assertTrue(!computeGroups.isEmpty(), "cloud mode but SHOW COMPUTE GROUPS returned nothing")
        sql "GRANT USAGE_PRIV ON COMPUTE GROUP '${computeGroups[0][0]}' TO '${user}'"
    }

    // Sessions live on the one FE that serves the configured Flight endpoint, and so must the
    // connection that lists and kills them: anchor a MySQL connection to that FE the way
    // test_connection_quota does, and count only that FE's own connections.
    def jdbcMatcher = (context.config.jdbcUrl =~ /^(jdbc:mysql:\/\/)[^\/:@]+(:\d+.*)$/)
    assertTrue(jdbcMatcher.matches(),
            "cannot derive the Flight FE's MySQL url from jdbcUrl: ${context.config.jdbcUrl}")
    def feMysqlUrl = jdbcMatcher.replaceFirst("\$1${host}\$2")
    def allocator = new RootAllocator()
    def client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
    def flight = new FlightSqlClient(client)
    def openTokens = []
    def feConn = null
    try {
        feConn = DriverManager.getConnection(feMysqlUrl, context.config.jdbcUser, context.config.jdbcPassword)
        feConn.createStatement().withCloseable { it.execute("SET fetch_all_fe_for_system_table = false") }
        feConn.createStatement().withCloseable { it.execute("SET forward_to_master = false") }
        feConn.createStatement().withCloseable { st ->
            def rs = st.executeQuery("SHOW FRONTEND CONFIG LIKE 'arrow_flight_sql_port'")
            assertTrue(rs.next(), "SHOW FRONTEND CONFIG returned no arrow_flight_sql_port")
            assertEquals(port as String, rs.getString("Value"),
                    "the FE reached at ${feMysqlUrl} does not serve the Flight endpoint on port ${port}; "
                            + "this suite needs the Flight sessions and the control connection on the same FE")
        }
        // The user's sessions as that FE's pool sees them: their connection ids.
        def sessionIds = {
            def ids = []
            feConn.createStatement().withCloseable { st ->
                def rs = st.executeQuery("SELECT Id FROM information_schema.processlist WHERE User = '${user}'")
                while (rs.next()) {
                    ids << rs.getLong(1)
                }
            }
            return ids
        }
        def awaitSessions = { int expected ->
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                    .until { sessionIds().size() == expected }
        }
        // Opens a session the way the drivers do: the handshake answers with its bearer token.
        def open = {
            def cred = client.authenticateBasicToken(user, password).get()
            openTokens << cred
            return cred
        }
        // A statement the frontend answers itself, run on the session and pulled from the frontend
        // over it (a query's rows would be on a backend, which this raw client does not connect to).
        def rows = { cred, String stmt ->
            def out = []
            def info = flight.execute(stmt, cred)
            info.getEndpoints().each { endpoint ->
                flight.getStream(endpoint.getTicket(), cred).withCloseable { stream ->
                    while (stream.next()) {
                        def root = stream.getRoot()
                        for (int i = 0; i < root.getRowCount(); i++) {
                            out << root.getFieldVectors().collect { it.isNull(i) ? null : it.getObject(i).toString() }
                        }
                    }
                }
            }
            return out
        }
        def works = { cred ->
            assertEquals([["wait_timeout"]], rows(cred, "SHOW VARIABLES LIKE 'wait_timeout'").collect { [it[0]] },
                    "the session should answer SHOW VARIABLES")
        }
        // Every kind of call is refused once the session has ended: a statement, a metadata request,
        // a session action.
        def unauthenticated = { cred, String why ->
            [
                { -> flight.execute("SHOW VARIABLES LIKE 'wait_timeout'", cred) },
                { -> flight.getCatalogs(cred) },
                { -> flight.closeSession(new CloseSessionRequest(), cred) },
            ].each { call ->
                try {
                    call()
                    throw new AssertionError("the token was still accepted after ${why}")
                } catch (FlightRuntimeException e) {
                    assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code(),
                            "after ${why}: ${e.status().description()}")
                    assertTrue(e.status().description().contains("no Arrow Flight SQL session is open under it"),
                            "after ${why}: ${e.status().description()}")
                }
            }
            openTokens.remove(cred)
        }

        // A session a failed earlier run left behind holds the user's name until wait_timeout (DROP
        // USER does not end it); end it here so the counts below are this run's.
        sessionIds().each { id -> feConn.createStatement().withCloseable { it.execute("KILL CONNECTION ${id}") } }

        // 1. Opening a session puts it in the pool from the handshake on, before it ran anything, and
        //    the token works for as long as the session is there.
        awaitSessions(0)
        def killed = open()
        awaitSessions(1)
        works(killed)

        // 2. KILL CONNECTION from another connection ends the session, and the token with it: the very
        //    next call is UNAUTHENTICATED.
        def killedId = sessionIds()[0]
        feConn.createStatement().withCloseable { it.execute("KILL CONNECTION ${killedId}") }
        awaitSessions(0)
        unauthenticated(killed, "KILL CONNECTION")

        // 3. wait_timeout ends an idle session the same way. The session sets its own, short, and the
        //    timeout checker (once a second) ends it once it has been idle that long; a session that
        //    keeps running statements is not ended (its idle time starts over with each).
        def idle = open()
        awaitSessions(1)
        // A SET is answered with a StatusResult of 0.
        assertEquals([["0"]], rows(idle, "SET wait_timeout = 3"))
        works(idle)
        awaitSessions(0)
        unauthenticated(idle, "wait_timeout")

        // 4. CloseSession, the client's own end of the session, ends the token at once; closing it
        //    again is refused like any other call under the token.
        def closed = open()
        awaitSessions(1)
        works(closed)
        assertEquals("CLOSED", flight.closeSession(new CloseSessionRequest(), closed).getStatus().name())
        awaitSessions(0)
        unauthenticated(closed, "CloseSession")

        // 5. A token of a session that ended never comes back: a later session of the same user does
        //    not revive it.
        def fresh = open()
        awaitSessions(1)
        works(fresh)
        unauthenticated(killed, "KILL CONNECTION, once a later session of the user was opened")
        works(fresh)
        assertEquals("CLOSED", flight.closeSession(new CloseSessionRequest(), fresh).getStatus().name())
        openTokens.remove(fresh)
        awaitSessions(0)
    } finally {
        def quietly = { String what, Closure step ->
            try {
                step()
            } catch (Exception e) {
                logger.warn("cleanup of test_bearer_token_lifecycle: ${what} failed: ${e.message}")
            }
        }
        openTokens.each { cred ->
            quietly("closing a session left open") {
                try {
                    flight.closeSession(new CloseSessionRequest(), cred)
                } catch (FlightRuntimeException e) {
                    if (e.status().code() != FlightStatusCode.UNAUTHENTICATED) {
                        throw e
                    }
                }
            }
        }
        quietly("closing the control connection") { if (feConn != null) feConn.close() }
        quietly("closing the Flight client") { client.close() }
        quietly("closing the allocator") { allocator.close() }
        sql "DROP USER IF EXISTS '${user}'"
    }
}
