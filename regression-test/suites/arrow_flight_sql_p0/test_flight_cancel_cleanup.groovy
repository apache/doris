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
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.impl.FlightSql
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator

suite("test_flight_cancel_cleanup", "arrow_flight_sql") {
    def frontend = jdbc_sql_return_maparray("SHOW FRONTENDS").find {
        it.IsMaster.toString().equalsIgnoreCase("true") && it.Alive.toString().equalsIgnoreCase("true")
    }
    assertNotNull(frontend)
    assertTrue(frontend.ArrowFlightSqlPort.toString().toInteger() > 0)
    def backends = jdbc_sql_return_maparray("SHOW BACKENDS").findAll {
        it.Alive.toString().equalsIgnoreCase("true")
    }
    assertTrue(!backends.isEmpty())
    def database = jdbc_sql("SELECT DATABASE()")[0][0]
    def table = "${database}.flight_cancel_cleanup_source"
    def allocator = new RootAllocator(Long.MAX_VALUE)
    def feClient = FlightClient.builder(allocator,
            Location.forGrpcInsecure(frontend.Host.toString(), frontend.ArrowFlightSqlPort.toString().toInteger())).build()
    def client = new FlightSqlClient(feClient)
    def auth
    def consume = { String query ->
        def info = client.execute(query, auth)
        def rows = 0
        info.endpoints.each { endpoint ->
            FlightClient.builder(allocator, endpoint.locations[0]).build().withCloseable { beClient ->
                beClient.getStream(endpoint.ticket, auth).withCloseable { stream ->
                    while (stream.next()) {
                        rows += stream.root.rowCount
                    }
                }
            }
        }
        rows
    }
    try {
        auth = feClient.authenticateBasicToken(context.config.otherConfigs.get("extArrowFlightSqlUser"),
                context.config.otherConfigs.get("extArrowFlightSqlPassword")).get()
        consume("SET enable_sql_cache=false")
        // The ticket is a query id in parallel mode, so inspect this query instead of global task counts.
        consume("SET enable_parallel_result_sink=true")
        consume("SET query_timeout=120")
        jdbc_sql("DROP TABLE IF EXISTS ${table}")
        jdbc_sql("CREATE TABLE ${table} (id BIGINT) DISTRIBUTED BY HASH(id) BUCKETS 8 " +
                "PROPERTIES(\"replication_num\"=\"1\")")
        jdbc_sql("INSERT INTO ${table} SELECT number FROM numbers(\"number\"=\"1024\")")
        def tabletBackends = jdbc_sql_return_maparray("SHOW TABLETS FROM ${table}")
                .collect { it.BackendId }.unique().size()
        [false, true].each { explicitCancel ->
            def info = client.execute("SELECT id, n FROM ${table} " +
                    "LATERAL VIEW explode_numbers(100000000) expanded AS n", auth)
            if (tabletBackends > 1) {
                assertTrue(info.endpoints.size() > 1, "Expected distributed Flight result endpoints")
            }
            def ids = info.endpoints.collect { endpoint ->
                def queryId = Any.parseFrom(endpoint.ticket.bytes).unpack(FlightSql.TicketStatementQuery.class)
                        .statementHandle.toStringUtf8().split("&")[0]
                // Flight tickets omit leading zeroes; the BE diagnostic API requires two 16-digit halves.
                queryId.split("-").collect { it.padLeft(16, "0") }.join("-")
            }.unique()
            // Abort only one endpoint; the cancellation must reach every participating BE.
            [info.endpoints[0]].each { endpoint ->
                FlightClient.builder(allocator, endpoint.locations[0]).build().withCloseable { beClient ->
                    beClient.getStream(endpoint.ticket, auth).withCloseable { stream ->
                        assertTrue(stream.next())
                        assertTrue(stream.root.rowCount > 0)
                        if (explicitCancel) {
                            stream.cancel("test cancellation", null)
                        }
                        // Closing after one batch must also cancel the unfinished producer.
                    }
                }
            }
            def deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20)
            def remaining = []
            while (true) {
                remaining = []
                backends.each { backend ->
                    ids.each { id ->
                        def body = new URL("http://${backend.Host}:${backend.HttpPort}/api/query_pipeline_tasks/${id}")
                                .getText(connectTimeout: 3000, readTimeout: 3000)
                        if (!body.contains("not found")) {
                            remaining.add("${id}: ${body}")
                        }
                    }
                }
                if (remaining.isEmpty() || System.nanoTime() >= deadline) {
                    break
                }
                sleep(100)
            }
            assertTrue(remaining.isEmpty(), "Cancelled Flight query retained pipelines: ${remaining}")
            assertEquals(1, consume("SELECT 1 AS healthy"))
        }
        assertEquals(10, consume("SELECT number FROM numbers(\"number\"=\"10\")"))
    } finally {
        try {
            if (auth != null) {
                feClient.closeSession(new org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest(), auth)
            }
        } finally {
            try {
                client.close()
                allocator.close()
            } finally {
                jdbc_sql("DROP TABLE IF EXISTS ${table}")
            }
        }
    }
}
