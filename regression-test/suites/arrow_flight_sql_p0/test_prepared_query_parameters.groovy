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
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightRuntimeException
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightStatusCode
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.BigIntVector
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.Float8Vector
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VarCharVector
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VectorSchemaRoot

import java.sql.DriverManager
import java.sql.Types

suite("test_prepared_query_parameters", "arrow_flight_sql") {
    def config = context.config.otherConfigs
    def location = Location.forGrpcInsecure(config.get("extArrowFlightSqlHost"),
            Integer.parseInt(config.get("extArrowFlightSqlPort")))
    Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
    def jdbcUrl = "jdbc:arrow-flight-sql://${config.get('extArrowFlightSqlHost')}:" +
            "${config.get('extArrowFlightSqlPort')}?useEncryption=false"
    DriverManager.getConnection(jdbcUrl, config.get("extArrowFlightSqlUser"),
            config.get("extArrowFlightSqlPassword")).withCloseable { connection ->
        // Exercise JDBC's schema-driven binder and query classifier, not only direct vector uploads.
        connection.prepareStatement("SELECT CAST(? AS BIGINT) AS value").withCloseable { statement ->
            assertEquals(1, statement.getMetaData().getColumnCount())
            assertEquals(Types.BIGINT, statement.getParameterMetaData().getParameterType(1))
            [42L, -7L, Long.MAX_VALUE].each { value ->
                statement.setLong(1, value)
                assertTrue(statement.execute())
                statement.getResultSet().withCloseable { result ->
                    assertTrue(result.next())
                    assertEquals(value, result.getLong(1))
                    assertTrue(!result.next())
                }
            }
            statement.setNull(1, Types.BIGINT)
            statement.executeQuery().withCloseable { result ->
                assertTrue(result.next())
                assertEquals(null, result.getObject(1))
                assertTrue(!result.next())
            }
        }
        connection.prepareStatement("SELECT CAST(? AS BIGINT), CAST(? AS STRING), CAST(? AS DOUBLE)")
                .withCloseable { statement ->
            statement.setLong(1, 17L)
            statement.setString(2, "quoted ' ? 中文")
            statement.setDouble(3, 2.5d)
            statement.executeQuery().withCloseable { result ->
                assertTrue(result.next())
                assertEquals(17L, result.getLong(1))
                assertEquals("quoted ' ? 中文", result.getString(2))
                assertEquals(2.5d, result.getDouble(3))
                assertTrue(!result.next())
            }
        }
        connection.prepareStatement('SELECT number FROM numbers("number"="10") '
                + 'WHERE number >= ? AND ? > number ORDER BY number').withCloseable { statement ->
            [[2L, 5L], [6L, 9L]].each { bounds ->
                statement.setLong(1, bounds[0])
                statement.setLong(2, bounds[1])
                statement.executeQuery().withCloseable { result ->
                    def rows = []
                    while (result.next()) {
                        rows.add(result.getLong(1))
                    }
                    assertEquals((bounds[0]..<bounds[1]).toList(), rows)
                }
            }
        }
    }
    new RootAllocator().withCloseable { allocator ->
        FlightClient.builder(allocator, location).build().withCloseable { flight ->
            def token = flight.authenticateBasicToken(config.get("extArrowFlightSqlUser"),
                    config.get("extArrowFlightSqlPassword")).get()
            CallOption[] options = [token, CallOptions.timeout(30, java.util.concurrent.TimeUnit.SECONDS)]
            def client = new FlightSqlClient(flight)
            def fetch = { info ->
                def rows = []
                info.getEndpoints().each { endpoint ->
                    def resultLocation = endpoint.getLocations().isEmpty() ? location : endpoint.getLocations()[0]
                    FlightClient.builder(allocator, resultLocation).build().withCloseable { resultClient ->
                        resultClient.getStream(endpoint.getTicket(), options).withCloseable { stream ->
                            while (stream.next()) {
                                def root = stream.getRoot()
                                for (int row = 0; row < root.getRowCount(); row++) {
                                    rows.add(root.getFieldVectors().collect { vector ->
                                        def value = vector.getObject(row)
                                        value == null ? null : value.toString()
                                    })
                                }
                            }
                        }
                    }
                }
                rows
            }
            try {
                def prepared = client.prepare("SELECT CAST(? AS BIGINT) AS value", options)
                try {
                    new BigIntVector("value", allocator).withCloseable { value ->
                        VectorSchemaRoot.of(value).withCloseable { root ->
                            prepared.setParameters(root)
                            [42L, -7L, Long.MAX_VALUE].each { expected ->
                                value.setSafe(0, expected)
                                root.setRowCount(1)
                                assertEquals([[expected.toString()]], fetch(prepared.execute(options)))
                            }
                            value.setNull(0)
                            assertEquals([[null]], fetch(prepared.execute(options)))
                            value.setSafe(0, 1L)
                            value.setSafe(1, 2L)
                            root.setRowCount(2)
                            try {
                                prepared.execute(options)
                                assertTrue(false, "Multiple parameter rows must not silently execute only the first")
                            } catch (FlightRuntimeException e) {
                                assertEquals(FlightStatusCode.UNIMPLEMENTED, e.status().code())
                            }
                            root.setRowCount(1)
                            value.setSafe(0, 3L)
                            assertEquals([["3"]], fetch(prepared.execute(options)))
                        }
                    }
                } finally {
                    // Close must use the same authenticated session that owns the prepared handle.
                    prepared.close(options)
                }
                prepared = client.prepare("SELECT CAST(? AS BIGINT), CAST(? AS STRING), CAST(? AS DOUBLE)", options)
                try {
                    VectorSchemaRoot.of(new BigIntVector("n", allocator), new VarCharVector("s", allocator),
                            new Float8Vector("f", allocator)).withCloseable { root ->
                        String text = "quoted ' text ? with Unicode 中文"
                        root.getVector(0).setSafe(0, 17L)
                        root.getVector(1).setSafe(0, text.getBytes("UTF-8"))
                        root.getVector(2).setSafe(0, 2.5d)
                        root.setRowCount(1)
                        prepared.setParameters(root)
                        assertEquals([["17", text, "2.5"]], fetch(prepared.execute(options)))
                    }
                } finally {
                    prepared.close(options)
                }
                prepared = client.prepare('SELECT number FROM numbers("number"="10") '
                        + 'WHERE number >= ? AND number < ? ORDER BY number', options)
                try {
                    VectorSchemaRoot.of(new BigIntVector("lower", allocator),
                            new BigIntVector("upper", allocator)).withCloseable { root ->
                        root.setRowCount(1)
                        prepared.setParameters(root)
                        [[2L, 5L], [6L, 9L]].each { bounds ->
                            root.getVector(0).setSafe(0, bounds[0])
                            root.getVector(1).setSafe(0, bounds[1])
                            root.setRowCount(1)
                            assertEquals((bounds[0]..<bounds[1]).collect { [it.toString()] },
                                    fetch(prepared.execute(options)))
                        }
                    }
                } finally {
                    prepared.close(options)
                }
                assertEquals([["1"]], fetch(client.execute("SELECT 1", options)))
            } finally {
                flight.closeSession(new CloseSessionRequest(), options)
            }
        }
    }
}
