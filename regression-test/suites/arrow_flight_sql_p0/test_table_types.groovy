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
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.types.pojo.ArrowType

suite("test_table_types", "arrow_flight_sql") {
    def expected = ["BASE TABLE", "SYSTEM VIEW", "VIEW"]
    def config = context.config.otherConfigs
    def location = Location.forGrpcInsecure(config.get("extArrowFlightSqlHost"),
            Integer.parseInt(config.get("extArrowFlightSqlPort")))
    new RootAllocator().withCloseable { allocator ->
        FlightClient.builder(allocator, location).build().withCloseable { flight ->
            def token = flight.authenticateBasicToken(config.get("extArrowFlightSqlUser"),
                    config.get("extArrowFlightSqlPassword")).get()
            CallOption[] options = [token, CallOptions.timeout(30, java.util.concurrent.TimeUnit.SECONDS)]
            def client = new FlightSqlClient(flight)
            3.times {
                def info = client.getTableTypes(options)
                assertEquals(["table_type"], info.getSchema().getFields()*.getName())
                assertEquals(new ArrowType.Utf8(), info.getSchema().getFields()[0].getType())
                assertFalse(info.getSchema().getFields()[0].isNullable())
                assertEquals(1, info.getEndpoints().size())
                def types = []
                // GetFlightInfo already succeeded before this fix; consume DoGet to detect missing data support.
                flight.getStream(info.getEndpoints()[0].getTicket(), options).withCloseable { stream ->
                    assertEquals(info.getSchema(), stream.getSchema())
                    while (stream.next()) {
                        def root = stream.getRoot()
                        for (int row = 0; row < root.getRowCount(); ++row) {
                            types.add(root.getVector("table_type").getObject(row).toString())
                        }
                    }
                }
                assertEquals(expected, types)
            }
            def info = client.execute("SHOW VARIABLES LIKE 'query_timeout'", options)
            int rows = 0
            flight.getStream(info.getEndpoints()[0].getTicket(), options).withCloseable { stream ->
                while (stream.next()) {
                    rows += stream.getRoot().getRowCount()
                }
            }
            assertEquals(1, rows)
        }
    }
    def jdbcTypes = []
    context.getArrowFlightSqlConnection().getMetaData().getTableTypes().withCloseable { rows ->
        while (rows.next()) {
            jdbcTypes.add(rows.getString(1))
        }
    }
    assertEquals(expected, jdbcTypes)
}
