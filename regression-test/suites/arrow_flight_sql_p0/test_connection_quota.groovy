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
    sql "SET PROPERTY FOR '${user}' 'max_user_connections' = '${limit}'"

    def allocator = new RootAllocator()
    def client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
    def mysqlConnections = []
    try {
        def flight = new FlightSqlClient(client)
        // A request over a token whose session cannot open: the refusal, or null when it opened.
        def refusalOf = { cred ->
            try {
                flight.execute("SELECT 1", cred).getEndpoints()
                return null
            } catch (FlightRuntimeException e) {
                assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, e.status().code())
                return e.status().description()
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
                mysqlConnections << DriverManager.getConnection(context.config.jdbcUrl, user, password)
                return null
            } catch (SQLException e) {
                return e.getMessage()
            }
        }

        // 1. Three MySQL connections, then the first Flight session: the user's four.
        (1..limit - 1).each { assertNull(mysqlRefusal(), "MySQL connection ${it} of ${limit - 1} was refused") }
        def first = client.authenticateBasicToken(user, password).get()
        assertNull(refusalOf(first), "the Flight session should open as the user's last connection")

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
        assertNull(mysqlRefusal(), "the MySQL connection should open once the Flight session is closed")
        mysqlConnections.remove(mysqlConnections.size() - 1).close()
        def third = client.authenticateBasicToken(user, password).get()
        assertNull(refusalOf(third), "the Flight session should open once the MySQL connection is closed")
        assertEquals("CLOSED", flight.closeSession(new CloseSessionRequest(), third).getStatus().name())
    } finally {
        mysqlConnections.each { it.close() }
        client.close()
        allocator.close()
        sql "DROP USER IF EXISTS '${user}'"
    }
}
