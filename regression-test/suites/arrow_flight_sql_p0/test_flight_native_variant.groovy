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

import org.apache.arrow.driver.jdbc.shaded.com.google.protobuf.Any
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CallOptions
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator

import java.util.concurrent.TimeUnit

suite("test_flight_native_variant", "arrow_flight_sql") {
    def frontend = jdbc_sql_return_maparray("SHOW FRONTENDS").find {
        it.IsMaster.toString().equalsIgnoreCase("true") && it.Alive.toString().equalsIgnoreCase("true")
    }
    assertNotNull(frontend)
    assertTrue(frontend.ArrowFlightSqlPort.toString().toInteger() > 0)
    def database = jdbc_sql("SELECT DATABASE()")[0][0]
    // Match ingestion to the configured Variant representation; legacy expression roots
    // cannot be cast to a table Variant with a different subcolumn limit.
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    def table = "${database}.flight_native_variant_input"
    def allocator = new RootAllocator(Long.MAX_VALUE)
    def feClient = FlightClient.builder(allocator,
            Location.forGrpcInsecure(frontend.Host.toString(), frontend.ArrowFlightSqlPort.toString().toInteger())).build()
    def client = new FlightSqlClient(feClient)
    def auth
    def read = { String query, Closure inspect, boolean parallel = false, int resultBackendCount = 1, def prepared = null ->
        int count = 0
        def info = prepared == null ? client.execute(query, auth) : prepared.execute(auth)
        assertFalse(info.endpoints.isEmpty())
        // Multiple buckets alone do not prove coverage of native output from different result BEs.
        if (parallel) {
            def resultAddresses = info.endpoints.collect { endpoint ->
                def fields = Any.parseFrom(endpoint.ticket.bytes).unpack(FlightSql.TicketStatementQuery.class)
                        .statementHandle.toStringUtf8().split("&")
                "${fields[1]}:${fields[2]}".toString()
            }
            assertEquals(info.endpoints.size(), resultAddresses.toSet().size(), "Duplicate result backends")
            if (resultBackendCount > 1) {
                assertTrue(info.endpoints.size() > 1, "Expected multiple native Variant result backends")
            }
        }
        info.endpoints.each { endpoint ->
            FlightClient.builder(allocator, endpoint.locations[0]).build().withCloseable { beClient ->
                beClient.getStream(endpoint.ticket, auth, CallOptions.timeout(30, TimeUnit.SECONDS)).withCloseable { stream ->
                    assertEquals(info.schema, stream.schema, "Result endpoints must share the published schema")
                    while (stream.next()) {
                        inspect(stream.root)
                        count += stream.root.rowCount
                    }
                }
            }
        }
        count
    }
    def executeSetting = { String query -> read(query, { root -> }) }
    try {
        auth = feClient.authenticateBasicToken(context.config.otherConfigs.get("extArrowFlightSqlUser"),
                context.config.otherConfigs.get("extArrowFlightSqlPassword")).get()
        executeSetting("SET enable_sql_cache=false")
        executeSetting("SET enable_nereids_distribute_planner=true")
        executeSetting("SET parallel_pipeline_task_num=8")
        jdbc_sql("DROP TABLE IF EXISTS ${table}")
        jdbc_sql("""CREATE TABLE ${table} (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 60
            PROPERTIES("replication_num"="1")""")
        jdbc_sql("""INSERT INTO ${table}
            SELECT number + 1, ${variantV2Function}(CASE number % 4
                WHEN 0 THEN '42' WHEN 1 THEN '"text"'
                WHEN 2 THEN '{"a":[1,null,"x"]}' ELSE NULL END)
            FROM numbers("number"="60")""")
        def resultBackendCount = jdbc_sql_return_maparray("SHOW TABLETS FROM ${table}")
                .collect { it.BackendId }.unique().size()
        [false, true].each { parallel ->
            executeSetting("SET enable_parallel_result_sink=${parallel}")
            [false, true, false].each { nativeVariant ->
                executeSetting("SET enable_arrow_flight_sql_native_variant=${nativeVariant}")
                def seen = []
                assertEquals(60, read("SELECT id, v FROM ${table}", { root ->
                    def vector = root.getVector(1)
                    def field = vector.field
                    if (nativeVariant) {
                        assertEquals("arrow.parquet.variant", field.metadata.get("ARROW:extension:name"))
                        assertEquals("Struct", field.type.toString())
                        assertEquals(["metadata", "value"], field.children.collect { it.name })
                        field.children.each { child ->
                            assertFalse(child.nullable)
                            assertEquals("Binary", child.type.toString())
                        }
                    } else {
                        assertEquals("Utf8", field.type.toString())
                    }
                    for (int i = 0; i < root.rowCount; i++) {
                        int id = root.getVector(0).get(i)
                        seen.add(id)
                        assertEquals(id % 4 == 0, vector.isNull(i))
                        if (nativeVariant && id % 4 != 0) {
                            assertTrue(vector.getChild("metadata").get(i).length > 0)
                            assertTrue(vector.getChild("value").get(i).length > 0)
                            if (id % 4 == 1) {
                                assertEquals([12, 42], vector.getChild("value").get(i).collect { it & 0xff })
                            }
                        } else if (!nativeVariant && id % 4 == 1) {
                            assertEquals("42", vector.getObject(i).toString())
                        }
                    }
                }, parallel, resultBackendCount))
                assertEquals((1..60).toList(), seen.sort())
                // Prepare and GetSchema must advertise the same Variant leaves as execution.
                ["SELECT CAST(42 AS VARIANT) AS v",
                 "SELECT v, ARRAY(v) AS a FROM ${table} WHERE id = 1",
                 "SELECT v FROM ${table} WHERE id < 0"].eachWithIndex { query, index ->
                    def prepared = client.prepare(query.toString(), auth)
                    try {
                        def schema = prepared.resultSetSchema
                        assertEquals(schema, client.getExecuteSchema(query.toString(), auth).schema)
                        assertEquals(schema, prepared.fetchSchema(auth).schema)
                        def leaves = [schema.fields[0]]
                        if (index == 1) {
                            leaves.add(schema.fields[1].children[0])
                        }
                        leaves.each { field ->
                            assertEquals(nativeVariant ? "Struct" : "Utf8", field.type.toString())
                            if (nativeVariant) {
                                assertEquals("arrow.parquet.variant", field.metadata.get("ARROW:extension:name"))
                            }
                        }
                        assertEquals(index == 2 ? 0 : 1, read(query.toString(), { root -> }, false, 1, prepared))
                    } finally {
                        prepared.close(auth)
                    }
                }
            }
        }
        executeSetting("SET enable_arrow_flight_sql_native_variant=true")
        // A folded constant must use the same wire representation as a scanned Variant column.
        assertEquals(1, read("SELECT CAST(42 AS VARIANT) AS v", { root ->
            assertEquals("arrow.parquet.variant", root.getVector(0).field.metadata.get("ARROW:extension:name"))
        }))
        assertEquals(0, read("SELECT v FROM ${table} WHERE id < 0", { root -> }))
    } finally {
        try {
            if (auth != null) {
                feClient.closeSession(new org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest(), auth)
            }
        } finally {
            try {
                client.close()
            } finally {
                try {
                    allocator.close()
                } finally {
                    jdbc_sql("DROP TABLE IF EXISTS ${table}")
                }
            }
        }
    }
}
