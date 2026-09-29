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
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.ArrowType

suite("test_schema_only", "arrow_flight_sql") {
    def config = context.config.otherConfigs
    def location = Location.forGrpcInsecure(config.get("extArrowFlightSqlHost"),
            Integer.parseInt(config.get("extArrowFlightSqlPort")))
    new RootAllocator().withCloseable { allocator ->
        FlightClient.builder(allocator, location).build().withCloseable { flight ->
            def token = flight.authenticateBasicToken(config.get("extArrowFlightSqlUser"),
                    config.get("extArrowFlightSqlPassword")).get()
            CallOption[] options = [token, CallOptions.timeout(30, java.util.concurrent.TimeUnit.SECONDS)]
            def client = new FlightSqlClient(flight)
            String showTimeout = "SHOW VARIABLES LIKE 'query_timeout'"
            def readTimeout = {
                def info = client.execute(showTimeout, options)
                assertEquals(1, info.getEndpoints().size())
                def values = []
                flight.getStream(info.getEndpoints().get(0).getTicket(), options).withCloseable { stream ->
                    while (stream.next()) {
                        def root = stream.getRoot()
                        for (int i = 0; i < root.getRowCount(); ++i) {
                            values.add(root.getVector("Value").getObject(i).toString())
                        }
                    }
                }
                assertEquals(1, values.size())
                return values[0]
            }
            def prepareSchema = { String query ->
                def prepared = client.prepare(query, options)
                try {
                    // ADBC ExecuteSchema consumes this schema before executing or fetching FlightInfo.
                    def schema = prepared.getResultSetSchema()
                    assertTrue(schema.getFields()*.getName() != ["ResultMeta"])
                    assertEquals(schema, prepared.fetchSchema(options).getSchema())
                    return schema
                } finally {
                    prepared.close(options)
                }
            }
            try {
                def cases = [
                        ["SELECT CAST(1 AS BIGINT) AS id, CAST(NULL AS VARCHAR(20)) AS name",
                         ["id", "name"], [new ArrowType.Int(64, true), new ArrowType.Utf8()]],
                        ["""SELECT CAST('2025-01-02 03:04:05.123456' AS DATETIME(6)) AS event_time,
                                  ARRAY('first', 'second') AS tags,
                                  MAP('key', CAST(1 AS BIGINT)) AS properties,
                                  NAMED_STRUCT('amount', CAST(1 AS DECIMAL(18,4))) AS record""",
                         ["event_time", "tags", "properties", "record"],
                         [new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), new ArrowType.List(),
                          new ArrowType.Map(false), new ArrowType.Struct()]],
                        ["SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE 1=0",
                         ["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME"],
                         [new ArrowType.Utf8(), new ArrowType.Utf8(), new ArrowType.Utf8()]],
                        ["SELECT group_concat_state('x') AS state", ["state"], [new ArrowType.Utf8()]],
                        [showTimeout, ["Variable_name", "Value", "Default_Value", "Changed"],
                         [new ArrowType.Utf8(), new ArrowType.Utf8(), new ArrowType.Utf8(), new ArrowType.Utf8()]]
                ]
                cases.eachWithIndex { entry, index ->
                    def schema = prepareSchema(entry[0])
                    assertEquals(entry[1], schema.getFields()*.getName())
                    assertEquals(entry[2], schema.getFields()*.getType())
                    assertEquals(schema, client.getExecuteSchema(entry[0], options).getSchema())
                    if (index == 0) {
                        assertTrue(!schema.getFields()[0].isNullable())
                        assertTrue(schema.getFields()[1].isNullable())
                    } else if (index == 1) {
                        assertEquals(new ArrowType.Utf8(), schema.getFields()[1].getChildren()[0].getType())
                        def entries = schema.getFields()[2].getChildren()[0]
                        assertTrue(!entries.isNullable())
                        assertTrue(!entries.getChildren()[0].isNullable())
                        assertEquals(new ArrowType.Int(64, true), entries.getChildren()[1].getType())
                        assertEquals(new ArrowType.Decimal(18, 4, 128),
                                schema.getFields()[3].getChildren()[0].getType())
                    }
                }

                // Resolving command metadata must not execute SET or leak query-scoped SET_VAR hints.
                def originalTimeout = readTimeout()
                def changedTimeout = originalTimeout == "17" ? "19" : "17"
                def setQuery = "SET query_timeout=${changedTimeout}".toString()
                assertEquals(["StatusResult"], prepareSchema(setQuery).getFields()*.getName())
                client.getExecuteSchema(setQuery, options)
                assertEquals(originalTimeout, readTimeout())
                def hintedQuery = "SELECT /*+ SET_VAR(query_timeout=${changedTimeout}) */ 1 AS id".toString()
                prepareSchema(hintedQuery)
                client.getExecuteSchema(hintedQuery, options)
                assertEquals(originalTimeout, readTimeout())

                ["SELEC 1", "SELECT definitely_missing_schema_column"].each { query ->
                    [true, false].each { preparedRoute ->
                        FlightRuntimeException failure = null
                        try {
                            if (preparedRoute) {
                                prepareSchema(query)
                            } else {
                                client.getExecuteSchema(query, options)
                            }
                        } catch (FlightRuntimeException e) {
                            failure = e
                        }
                        assertTrue(failure != null, "Schema discovery accepted invalid SQL: ${query}")
                        assertEquals(FlightStatusCode.INVALID_ARGUMENT, failure.status().code())
                        assertEquals(["id", "name"], prepareSchema(cases[0][0]).getFields()*.getName())
                        assertEquals(originalTimeout, readTimeout())
                    }
                }

                assertEquals(6, prepareSchema("SHOW FRONTEND CONFIG").getFields().size())
                assertEquals(prepareSchema("SHOW PROC '/'"), client.getExecuteSchema("SHOW PROC '/'", options).getSchema())
                ["SHOW PYTHON PACKAGES IN '3.11'", "SHOW QUERY STATS"].each { query ->
                    [true, false].each { preparedRoute ->
                        FlightRuntimeException failure = null
                        try {
                            if (preparedRoute) {
                                prepareSchema(query)
                            } else {
                                client.getExecuteSchema(query, options)
                            }
                        } catch (FlightRuntimeException e) {
                            failure = e
                        }
                        assertTrue(failure != null)
                        assertEquals(FlightStatusCode.UNIMPLEMENTED, failure.status().code())
                    }
                }

                client.execute("USE information_schema", options)
                def prepared = client.prepare("SELECT TABLE_NAME FROM tables WHERE 1=0", options)
                try {
                    client.execute("USE mysql", options)
                    // A prepared header must not silently describe a different database's table.
                    [true, false].each { schemaRoute ->
                        FlightRuntimeException failure = null
                        try {
                            if (schemaRoute) {
                                prepared.fetchSchema(options)
                            } else {
                                prepared.execute(options)
                            }
                        } catch (FlightRuntimeException e) {
                            failure = e
                        }
                        assertTrue(failure != null)
                        assertEquals(FlightStatusCode.NOT_FOUND, failure.status().code())
                    }
                } finally {
                    prepared.close(options)
                }

            } finally {
                client.closeSession(new CloseSessionRequest(), options)
            }
        }
    }
}
