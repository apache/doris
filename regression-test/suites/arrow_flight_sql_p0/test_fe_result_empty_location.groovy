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

import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CallOption
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CallOptions
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.doris.regression.util.JdbcUtils

import java.util.concurrent.TimeUnit

// arrow_flight_fe_result_empty_location: with it on, a result only the FE can serve (a SHOW, a metadata
// request) is handed out with no location and redeemed on the connection that issued the ticket, while a
// query's result keeps its BE address. setFeConfigTemporary changes every FE, so the suite runs alone.
suite("test_fe_result_empty_location", "arrow_flight_sql,nonConcurrent") {
    def table = "test_fe_result_empty_location_t"
    sql "DROP TABLE IF EXISTS ${table}"
    sql """CREATE TABLE ${table} (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO ${table} VALUES (1), (2), (3)"

    def config = context.config.otherConfigs
    def feLocation = Location.forGrpcInsecure(config.get("extArrowFlightSqlHost"),
            Integer.parseInt(config.get("extArrowFlightSqlPort")))

    // The endpoints a plain Flight SQL client gets, and what it reads from them.
    def checkPlainClient = { boolean emptyLocation ->
        new RootAllocator().withCloseable { allocator ->
            FlightClient.builder(allocator, feLocation).build().withCloseable { flight ->
                def token = flight.authenticateBasicToken(config.get("extArrowFlightSqlUser"),
                        config.get("extArrowFlightSqlPassword")).get()
                CallOption[] options = [token, CallOptions.timeout(30, TimeUnit.SECONDS)]
                def client = new FlightSqlClient(flight)
                def countRows = { FlightClient reader, endpoint ->
                    int rows = 0
                    reader.getStream(endpoint.getTicket(), options).withCloseable { stream ->
                        while (stream.next()) {
                            rows += stream.getRoot().getRowCount()
                        }
                    }
                    rows
                }
                try {
                    // Results only this FE can serve: a metadata request and a SHOW.
                    [client.getCatalogs(options), client.execute("SHOW DATABASES", options)].each { info ->
                        assertEquals(1, info.getEndpoints().size())
                        def endpoint = info.getEndpoints()[0]
                        if (emptyLocation) {
                            assertTrue(endpoint.getLocations().isEmpty(), "locations ${endpoint.getLocations()}")
                        } else {
                            assertEquals(1, endpoint.getLocations().size(), "locations ${endpoint.getLocations()}")
                        }
                        // Redeemed on the client that issued the ticket.
                        assertTrue(countRows(flight, endpoint) > 0)
                    }
                    // A query's result stays on a BE and keeps the BE's address whatever the setting.
                    def query = client.execute("SELECT id FROM ${context.dbName}.${table}", options)
                    assertFalse(query.getEndpoints().isEmpty())
                    int queryRows = 0
                    query.getEndpoints().each { endpoint ->
                        assertFalse(endpoint.getLocations().isEmpty(), "a BE-held result must keep its location")
                        FlightClient.builder(allocator, endpoint.getLocations()[0]).build().withCloseable { be ->
                            queryRows += countRows(be, endpoint)
                        }
                    }
                    assertEquals(3, queryRows)
                } finally {
                    flight.closeSession(new CloseSessionRequest(), token)
                }
            }
        }
    }

    // What the Flight SQL JDBC driver reads: it follows a location, and redeems an endpoint that has
    // none on its own connection.
    def checkJdbcDriver = {
        def flightConn = context.getArrowFlightSqlConnection()
        assertFalse(JdbcUtils.executeToList(flightConn, "SHOW DATABASES").isEmpty())
        def typeRows = flightConn.getMetaData().getTableTypes()
        try {
            def types = []
            while (typeRows.next()) {
                types.add(typeRows.getString("TABLE_TYPE"))
            }
            assertEquals(["BASE TABLE", "SYSTEM VIEW", "VIEW"], types)
        } finally {
            typeRows.close()
        }
    }

    setFeConfigTemporary([arrow_flight_fe_result_empty_location: true]) {
        assertEquals("true", getFeConfig("arrow_flight_fe_result_empty_location"))
        checkPlainClient(true)
        checkJdbcDriver()
    }
    // Back to the default: the same requests name the FE's address again.
    assertEquals("false", getFeConfig("arrow_flight_fe_result_empty_location"))
    checkPlainClient(false)
    checkJdbcDriver()
    sql "DROP TABLE IF EXISTS ${table}"
}
