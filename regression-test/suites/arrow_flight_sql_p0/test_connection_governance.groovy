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

// A MySQL connection and an Arrow Flight SQL session are connections of the one pool, governed
// alike and seen alike: both are rows of SHOW PROCESSLIST and of information_schema.processlist
// (whose columns are the same, ending with Protocol = MySQL / ArrowFlightSQL), whichever protocol
// asks; the user's max_user_connections counts both; KILL CONNECTION ends either from either; and
// wait_timeout ends an idle one. test_connection_quota and test_bearer_token_lifecycle go into the
// refusal and the token's fate; this suite is the operator's view of the two together.
//
// Not in the 'arrow_flight_sql' group on purpose: `sql` stays the MySQL control connection, and
// the connections under test are a raw Flight SQL client and a plain MySQL connection of their own.
suite("test_connection_governance") {
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    int port = context.config.otherConfigs.get("extArrowFlightSqlPort") as int

    String user = "flight_governance_user"
    String password = "Govern_12345"
    // One MySQL connection and one Flight session fill the user's quota.
    int limit = 2

    sql "DROP USER IF EXISTS '${user}'"
    sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
    sql "GRANT SELECT_PRIV ON *.* TO '${user}'"
    if (isCloudMode()) {
        def computeGroups = sql "SHOW COMPUTE GROUPS"
        assertTrue(!computeGroups.isEmpty(), "cloud mode but SHOW COMPUTE GROUPS returned nothing")
        sql "GRANT USAGE_PRIV ON COMPUTE GROUP '${computeGroups[0][0]}' TO '${user}'"
    }
    sql "SET PROPERTY FOR '${user}' 'max_user_connections' = '${limit}'"

    // Connections live on the one FE that serves the configured Flight endpoint, and so must the
    // connection that lists and kills them: anchor a MySQL connection to that FE the way
    // test_connection_quota does, and list only that FE's own connections.
    def jdbcMatcher = (context.config.jdbcUrl =~ /^(jdbc:mysql:\/\/)[^\/:@]+(:\d+.*)$/)
    assertTrue(jdbcMatcher.matches(),
            "cannot derive the Flight FE's MySQL url from jdbcUrl: ${context.config.jdbcUrl}")
    def feMysqlUrl = jdbcMatcher.replaceFirst("\$1${host}\$2")
    def allocator = new RootAllocator()
    def client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
    def flight = new FlightSqlClient(client)
    def openTokens = []
    def mysqlConnections = []
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
                            + "this suite needs the connections and the control connection on the same FE")
        }
        // A statement over the control connection: its rows as maps keyed by column name, and the
        // column names in order.
        def query = { String stmt ->
            def rows = []
            def names = []
            feConn.createStatement().withCloseable { st ->
                def rs = st.executeQuery(stmt)
                def meta = rs.getMetaData()
                (1..meta.getColumnCount()).each { names << meta.getColumnLabel(it) }
                while (rs.next()) {
                    def row = [:]
                    names.eachWithIndex { name, i -> row[name] = rs.getString(i + 1) }
                    rows << row
                }
            }
            return [names, rows]
        }
        // The user's connections as this FE's pool reports them to SHOW FULL PROCESSLIST.
        def shown = {
            def (names, rows) = query("SHOW FULL PROCESSLIST")
            return [names, rows.findAll { it["User"] == user }]
        }
        // ...and to information_schema.processlist, served by a backend's scanner that asks this
        // FE over RPC. An FE registered under more than one of its host's addresses answers once
        // per registration, so keep one row per connection id.
        def listed = {
            def (names, rows) = query("SELECT * FROM information_schema.processlist WHERE User = '${user}'")
            return [names, rows.unique { it["Id"] }]
        }
        def awaitConnections = { int expected ->
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                    .until { listed()[1].size() == expected }
        }
        // Opens a Flight session the way the drivers do: the handshake answers with its bearer token.
        def open = {
            def cred = client.authenticateBasicToken(user, password).get()
            openTokens << cred
            return cred
        }
        // A statement the frontend answers itself, run on a Flight session and pulled from the
        // frontend over it: its column names and its rows, every value as text.
        def flightQuery = { cred, String stmt ->
            def rows = []
            def info = flight.execute(stmt, cred)
            def names = info.getSchema().getFields().collect { it.getName() }
            info.getEndpoints().each { endpoint ->
                flight.getStream(endpoint.getTicket(), cred).withCloseable { stream ->
                    while (stream.next()) {
                        def root = stream.getRoot()
                        for (int i = 0; i < root.getRowCount(); i++) {
                            rows << root.getFieldVectors().collect { it.isNull(i) ? null : it.getObject(i).toString() }
                        }
                    }
                }
            }
            return [names, rows]
        }
        def unauthenticated = { cred, String why ->
            try {
                flight.execute("SHOW VARIABLES LIKE 'wait_timeout'", cred)
                throw new AssertionError("the token was still accepted after ${why}")
            } catch (FlightRuntimeException e) {
                assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code(),
                        "after ${why}: ${e.status().description()}")
            }
            openTokens.remove(cred)
        }

        // A connection a failed earlier run left behind holds the user's name until wait_timeout
        // (DROP USER does not end it); end it here so the counts below are this run's.
        listed()[1].each { row -> feConn.createStatement().withCloseable { it.execute("KILL CONNECTION ${row['Id']}") } }
        awaitConnections(0)

        // 1. One MySQL connection and one Flight session of the user. Both are in the pool from the
        //    handshake on, before either has run anything.
        def mysqlConn = DriverManager.getConnection(feMysqlUrl, user, password)
        mysqlConnections << mysqlConn
        def flightCred = open()
        awaitConnections(limit)

        // 2. The operator's view over MySQL. SHOW FULL PROCESSLIST and information_schema.processlist
        //    have the same columns, in the same order, ending with Protocol; both list the two
        //    connections, and Protocol tells them apart.
        def (shownNames, shownRows) = shown()
        def (listedNames, listedRows) = listed()
        assertEquals(shownNames, listedNames,
                "SHOW FULL PROCESSLIST and information_schema.processlist disagree on the columns")
        assertEquals("Protocol", shownNames[-1])
        assertEquals(limit, shownRows.size(), "SHOW FULL PROCESSLIST: ${shownRows}")
        assertEquals(limit, listedRows.size(), "information_schema.processlist: ${listedRows}")
        assertEquals(["ArrowFlightSQL", "MySQL"], shownRows.collect { it["Protocol"] }.sort())
        assertEquals(["ArrowFlightSQL", "MySQL"], listedRows.collect { it["Protocol"] }.sort())
        // Column for column, the two report the same connection -- the columns that move with time
        // and with the statement being run aside, and CurrentConnected, which marks the row of the
        // connection that asked and so is never set on a scanner's row.
        def stable = shownNames - ["CurrentConnected", "Command", "Time", "State", "QueryId", "TraceId", "Info"]
        shownRows.each { shownRow ->
            def listedRow = listedRows.find { it["Id"] == shownRow["Id"] }
            assertNotNull(listedRow, "connection ${shownRow['Id']} is missing from information_schema.processlist")
            stable.each { name ->
                assertEquals(shownRow[name], listedRow[name],
                        "column ${name} of connection ${shownRow['Id']} differs between the two")
            }
        }
        def mysqlId = shownRows.find { it["Protocol"] == "MySQL" }["Id"]
        def flightId = shownRows.find { it["Protocol"] == "ArrowFlightSQL" }["Id"]
        assertEquals(mysqlConn.createStatement().withCloseable { st ->
            def rs = st.executeQuery("SELECT CONNECTION_ID()")
            rs.next()
            rs.getString(1)
        }, mysqlId, "the MySQL row is not the MySQL connection's")

        // 3. The same view over Flight: the session's own SHOW PROCESSLIST lists its own row, marked
        //    as its own, and the user's MySQL connection, both under their protocol.
        def (flightNames, flightRows) = flightQuery(flightCred, "SHOW PROCESSLIST")
        assertEquals(shownNames, flightNames, "SHOW PROCESSLIST over Flight reports other columns")
        def ownRow = flightRows.find { it[0] == "Yes" }
        assertNotNull(ownRow, "the Flight session does not appear in its own SHOW PROCESSLIST: ${flightRows}")
        assertEquals(flightId, ownRow[1])
        assertEquals("ArrowFlightSQL", ownRow[-1])
        def mysqlRowOverFlight = flightRows.find { it[1] == mysqlId }
        assertNotNull(mysqlRowOverFlight, "the MySQL connection is missing from SHOW PROCESSLIST over Flight: ${flightRows}")
        assertEquals("No", mysqlRowOverFlight[0])
        assertEquals("MySQL", mysqlRowOverFlight[-1])

        // 4. The quota counts both: a third connection is refused over either protocol, in the
        //    words that name the user's limit (Current is the pool's count, other users included),
        //    and the two are untouched by the attempt.
        def refusal = ~/Reach limit of connections\. Total: \d+, User: ${limit}, Current: \d+/
        try {
            def refused = client.authenticateBasicToken(user, password).get()
            openTokens << refused
            throw new AssertionError("a third Flight session opened although the user's limit is reached")
        } catch (FlightRuntimeException e) {
            assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, e.status().code(), e.status().description())
            assertTrue(refusal.matcher(e.status().description()).find(), e.status().description())
        }
        try {
            mysqlConnections << DriverManager.getConnection(feMysqlUrl, user, password)
            throw new AssertionError("a second MySQL connection opened although the user's limit is reached")
        } catch (SQLException e) {
            assertTrue(refusal.matcher(e.getMessage()).find(), e.getMessage())
        }
        assertEquals([flightId, mysqlId].sort(), listed()[1].collect { it["Id"] }.sort())

        // 5. KILL CONNECTION from the Flight session ends the MySQL connection: it leaves both
        //    processlists, and its next statement finds it closed.
        flightQuery(flightCred, "KILL CONNECTION ${mysqlId}")
        awaitConnections(1)
        assertEquals([flightId], listed()[1].collect { it["Id"] })
        assertEquals([flightId], shown()[1].collect { it["Id"] })
        try {
            mysqlConn.createStatement().withCloseable { it.executeQuery("SELECT 1") }
            throw new AssertionError("the killed MySQL connection still ran a statement")
        } catch (SQLException e) {
            logger.info("the killed MySQL connection reports: ${e.message}")
        }

        // 6. KILL CONNECTION from MySQL ends the Flight session the same way, and with it its token.
        feConn.createStatement().withCloseable { it.execute("KILL CONNECTION ${flightId}") }
        awaitConnections(0)
        assertTrue(shown()[1].isEmpty(), "the killed Flight session is still shown: ${shown()[1]}")
        unauthenticated(flightCred, "KILL CONNECTION")

        // 7. wait_timeout ends an idle Flight session: its Time column counts the seconds since its
        //    last statement, and once they reach the session's wait_timeout (checked once a second)
        //    the row is gone, and the token with it.
        def idleCred = open()
        awaitConnections(1)
        flightQuery(idleCred, "SET wait_timeout = 8")
        sleep(2000)
        def idleRow = listed()[1][0]
        assertEquals("ArrowFlightSQL", idleRow["Protocol"])
        assertTrue((idleRow["Time"] as int) >= 1, "Time of the idle session: ${idleRow}")
        awaitConnections(0)
        unauthenticated(idleCred, "wait_timeout")
    } finally {
        def quietly = { String what, Closure step ->
            try {
                step()
            } catch (Exception e) {
                logger.warn("cleanup of test_connection_governance: ${what} failed: ${e.message}")
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
        mysqlConnections.each { conn -> quietly("closing a MySQL connection") { conn.close() } }
        quietly("closing the control connection") { if (feConn != null) feConn.close() }
        quietly("closing the Flight client") { client.close() }
        quietly("closing the allocator") { allocator.close() }
        sql "DROP USER IF EXISTS '${user}'"
    }
}
