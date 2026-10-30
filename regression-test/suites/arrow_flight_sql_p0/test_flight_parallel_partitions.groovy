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

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

suite("test_flight_parallel_partitions", "arrow_flight_sql") {
    def frontend = jdbc_sql_return_maparray("SHOW FRONTENDS").find {
        it.IsMaster.toString().equalsIgnoreCase("true") && it.Alive.toString().equalsIgnoreCase("true")
    }
    assertNotNull(frontend)
    assertTrue(frontend.ArrowFlightSqlPort.toString().toInteger() > 0)
    def database = jdbc_sql("SELECT DATABASE()")[0][0]
    def table = "${database}.flight_parallel_partition_source"
    def allocator = new RootAllocator(Long.MAX_VALUE)
    def feClient = FlightClient.builder(allocator,
            Location.forGrpcInsecure(frontend.Host.toString(), frontend.ArrowFlightSqlPort.toString().toInteger())).build()
    def client = new FlightSqlClient(feClient)
    def auth
    def readEndpoint = { endpoint, boolean collectRows ->
        def rows = []
        FlightClient.builder(allocator, endpoint.locations[0]).build().withCloseable { beClient ->
            beClient.getStream(endpoint.ticket, auth, CallOptions.timeout(30, TimeUnit.SECONDS)).withCloseable { stream ->
                while (stream.next()) {
                    if (collectRows) {
                        def vector = stream.root.getVector(0)
                        for (int i = 0; i < stream.root.rowCount; i++) {
                            rows.add(((Number) vector.getObject(i)).longValue())
                        }
                    }
                }
            }
        }
        rows
    }
    def executeSetting = { String statement ->
        client.execute(statement, auth).endpoints.each { readEndpoint(it, false) }
    }
    try {
        auth = feClient.authenticateBasicToken(context.config.otherConfigs.get("extArrowFlightSqlUser"),
                context.config.otherConfigs.get("extArrowFlightSqlPassword")).get()
        executeSetting("SET enable_sql_cache=false")
        executeSetting("SET enable_nereids_distribute_planner=true")
        executeSetting("SET parallel_pipeline_task_num=8")
        executeSetting("SET query_timeout=60")
        jdbc_sql("DROP TABLE IF EXISTS ${table}")
        jdbc_sql("CREATE TABLE ${table} (id BIGINT NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 60 " +
                "PROPERTIES(\"replication_num\"=\"1\")")
        jdbc_sql("INSERT INTO ${table} SELECT number FROM numbers(\"number\"=\"60\")")
        def resultBackendCount = jdbc_sql_return_maparray("SHOW TABLETS FROM ${table}")
                .collect { it.BackendId }.unique().size()
        [true, false].each { parallel ->
            executeSetting("SET enable_parallel_result_sink=${parallel}")
            [false, true].each { concurrent ->
                // Each execution owns fresh tickets; consuming a ticket does not create a replayable partition.
                def info = client.execute("SELECT id * 1000 + n AS sequence_id FROM ${table} " +
                        "LATERAL VIEW explode_numbers(1000) expanded AS n", auth)
                assertTrue(!info.endpoints.isEmpty())
                def tickets = info.endpoints.collect { Base64.encoder.encodeToString(it.ticket.bytes) }
                assertEquals(tickets.size(), tickets.toSet().size(), "Duplicate Flight result tickets")
                if (parallel) {
                    def resultAddresses = info.endpoints.collect { endpoint ->
                        def fields = Any.parseFrom(endpoint.ticket.bytes).unpack(FlightSql.TicketStatementQuery.class)
                                .statementHandle.toStringUtf8().split("&")
                        "${fields[1]}:${fields[2]}".toString()
                    }
                    // Instance parallelism must not publish multiple readers for the same BE result buffer.
                    assertEquals(info.endpoints.size(), resultAddresses.toSet().size())
                    if (resultBackendCount > 1) {
                        assertTrue(info.endpoints.size() > 1, "Expected multiple result backends")
                    }
                } else {
                    assertEquals(1, info.endpoints.size())
                }
                def rows = []
                if (concurrent) {
                    def executor = Executors.newFixedThreadPool(Math.min(8, info.endpoints.size()))
                    try {
                        def futures = info.endpoints.collect { endpoint ->
                            executor.submit({ readEndpoint(endpoint, true) } as Callable)
                        }
                        futures.each { rows.addAll(it.get(60, TimeUnit.SECONDS)) }
                    } finally {
                        executor.shutdownNow()
                        assertTrue(executor.awaitTermination(35, TimeUnit.SECONDS))
                    }
                } else {
                    info.endpoints.each { rows.addAll(readEndpoint(it, true)) }
                }
                assertEquals(60000, rows.size())
                assertEquals((0L..<60000L).toList(), rows.sort())
            }
        }
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
