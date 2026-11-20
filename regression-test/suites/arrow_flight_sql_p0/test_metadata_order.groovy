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
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.message.MessageSerializer

import java.nio.channels.Channels
import java.util.concurrent.TimeUnit

suite("test_metadata_order", "p0,nonConcurrent") {
    // Catalog discovery must not initialize these catalogs or contact a metastore.
    sql "DROP CATALOG IF EXISTS flight_order_z"
    sql "DROP CATALOG IF EXISTS flight_order_a"
    sql '''CREATE CATALOG flight_order_z PROPERTIES
        ("type"="hms", "hive.metastore.uris"="thrift://127.0.0.1:1")'''
    sql '''CREATE CATALOG flight_order_a PROPERTIES
        ("type"="hms", "hive.metastore.uris"="thrift://127.0.0.1:1")'''
    sql "CREATE DATABASE IF NOT EXISTS flight_order_z_db"
    sql "CREATE DATABASE IF NOT EXISTS flight_order_a_db"
    for (String db : ["flight_order_z_db", "flight_order_a_db"]) {
        sql "DROP VIEW IF EXISTS ${db}.m_view"
        sql "DROP TABLE IF EXISTS ${db}.z_table"
        sql "DROP TABLE IF EXISTS ${db}.a_table"
        sql """CREATE TABLE ${db}.z_table (z INT) DISTRIBUTED BY HASH(z) BUCKETS 1
            PROPERTIES ("replication_num"="1")"""
        sql """CREATE TABLE ${db}.a_table (a INT) DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES ("replication_num"="1")"""
        sql "CREATE VIEW ${db}.m_view AS SELECT a FROM ${db}.a_table"
    }
    sql "DROP USER IF EXISTS flight_order_user"
    sql "CREATE USER flight_order_user IDENTIFIED BY 'FlightMetadata123'"
    sql "GRANT SELECT_PRIV ON flight_order_a.*.* TO flight_order_user"
    sql "GRANT SELECT_PRIV ON internal.flight_order_a_db.a_table TO flight_order_user"

    def config = context.config.otherConfigs
    def location = Location.forGrpcInsecure(config.get("extArrowFlightSqlHost"),
            Integer.parseInt(config.get("extArrowFlightSqlPort")))
    def check = { String username, String password, String prefix ->
        new RootAllocator().withCloseable { allocator ->
            FlightClient.builder(allocator, location).build().withCloseable { client ->
                def token = client.authenticateBasicToken(username, password).get()
                CallOption[] options = [token, CallOptions.timeout(30, TimeUnit.SECONDS)]
                def flight = new FlightSqlClient(client)
                def read = { info ->
                    def rows = []
                    info.getEndpoints().each { endpoint ->
                        client.getStream(endpoint.getTicket(), options).withCloseable { stream ->
                            while (stream.next()) {
                                def root = stream.getRoot()
                                for (int row = 0; row < root.getRowCount(); row++) {
                                    def values = root.getFieldVectors().findAll { it.getName() != "table_schema" }
                                            .collect { it.getObject(row).toString() }
                                    if (root.getVector("table_schema") != null) {
                                        byte[] bytes = ((VarBinaryVector) root.getVector("table_schema")).get(row)
                                        def schema = MessageSerializer.deserializeSchema(new ReadChannel(
                                                Channels.newChannel(new ByteArrayInputStream(bytes))))
                                        values.add(schema.getFields()*.getName().join(","))
                                    }
                                    rows.add(values)
                                }
                            }
                        }
                    }
                    return rows
                }
                // Snapshot the wire order. Sorting the collected rows would conceal this bug.
                // SQL literals let the normal qt output writer record non-SQL metadata as well.
                def snapshot = { String tag, List rows ->
                    String value = rows.collect { it.join(":") }.join(";")
                    quickTest(prefix + tag, "SELECT '${value}' ORDER BY 1")
                }
                try {
                    snapshot("catalogs", read(flight.getCatalogs(options)).findAll {
                        it[0] in ["flight_order_a", "internal", "flight_order_z"]
                    })
                    snapshot("schemas", read(flight.getSchemas("internal", "flight_order_%_db", options)))
                    snapshot("tables", read(flight.getTables("internal", "flight_order_%_db", "%",
                            null, false, options)))
                    snapshot("table_schemas", read(flight.getTables("internal", "flight_order_%_db", "%",
                            ["table"], true, options)))
                    snapshot("empty", read(flight.getTables("internal", "flight_order_missing", "%",
                            null, true, options)))
                } finally {
                    client.closeSession(new CloseSessionRequest(), token)
                }
            }
        }
    }
    try {
        check(config.get("extArrowFlightSqlUser"), config.get("extArrowFlightSqlPassword"), "admin_")
        check("flight_order_user", "FlightMetadata123", "restricted_")
    } finally {
        sql "DROP USER flight_order_user"
        sql "DROP CATALOG flight_order_a"
        sql "DROP CATALOG flight_order_z"
    }
}
