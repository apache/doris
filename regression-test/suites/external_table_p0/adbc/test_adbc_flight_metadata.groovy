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

import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CallOptions
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator

import java.util.concurrent.TimeUnit

suite("test_adbc_flight_metadata", "p0,external") {
    String driver = context.config.otherConfigs.get("adbcDriverPath")
    if (driver == null || driver.isEmpty()) {
        String thirdparty = System.getenv("DORIS_THIRDPARTY")
        if (thirdparty == null || thirdparty.isEmpty()) {
            thirdparty = new File(context.config.suitePath).getParentFile().getParentFile()
                    .getAbsolutePath() + "/thirdparty"
        }
        driver = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }
    int port = (sql "SHOW FRONTENDS")[0][6] as int
    if (port <= 0 || !new File(driver).canRead()) {
        logger.info("SKIPPED test_adbc_flight_metadata: Flight SQL and its ADBC driver are required")
        return
    }
    String catalog = "test_adbc_flight_metadata_catalog"
    String database = "test_adbc_flight_metadata_db"
    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql "DROP DATABASE IF EXISTS internal.${database} FORCE"
    sql "CREATE DATABASE internal.${database}"
    sql """CREATE TABLE internal.${database}.base_table (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql "CREATE VIEW internal.${database}.view_table AS SELECT id FROM internal.${database}.base_table"

    // Leave sql_dialect unset so vendor detection also reaches the scoped Doris metadata path.
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        "type" = "adbc", "driver_url" = "${driver}",
        "uri" = "grpc://127.0.0.1:${port}", "user" = "root", "password" = "")"""

    new RootAllocator().withCloseable { allocator ->
        FlightClient.builder(allocator, Location.forGrpcInsecure("127.0.0.1", port))
                .build().withCloseable { client ->
            def credential = client.authenticateBasicToken("root", "").get()
            def deadline = CallOptions.timeout(30, TimeUnit.SECONDS)
            def flight = new FlightSqlClient(client)
            try {
                def read = { info, List<String> columns ->
                    def rows = []
                    info.getEndpoints().each { endpoint ->
                        flight.getStream(endpoint.getTicket(), credential, deadline).withCloseable { stream ->
                            while (stream.next()) {
                                def root = stream.getRoot()
                                for (int row = 0; row < root.getRowCount(); row++) {
                                    rows.add(columns.collect { root.getVector(it).getObject(row)?.toString() })
                                }
                            }
                        }
                    }
                    return rows
                }
                def expectedSchemas = (sql "SHOW DATABASES FROM internal LIKE '${database}'")
                        .collect { [catalog, it[0].toString()] }
                // Run unrestricted discovery before warming the ADBC cache: it must not recursively load itself.
                def schemas = read(flight.getSchemas(null, database, credential, deadline),
                        ["catalog_name", "db_schema_name"])
                assertEquals(expectedSchemas, schemas.findAll { it[0] == catalog && it[1] == database })

                def expectedTables = (sql "SHOW FULL TABLES FROM internal.${database}")
                        .findAll { it[1].toString() == "BASE TABLE" }
                        .collect { [catalog, database, it[0].toString()] }
                for (boolean includeSchema : [false, true]) {
                    def tables = read(flight.getTables(null, database, "base_table", null,
                            includeSchema, credential, deadline),
                            ["catalog_name", "db_schema_name", "table_name"])
                    assertEquals(expectedTables, tables.findAll { it[0] == catalog && it[1] == database })
                }
                assertEquals(
                        (sql "SHOW DATABASES FROM internal LIKE '${database}'").toString(),
                        (sql "SHOW DATABASES FROM ${catalog} LIKE '${database}'").toString())
                assertEquals(
                        (sql "SHOW TABLES FROM internal.${database} LIKE 'base_table'").toString(),
                        (sql "SHOW TABLES FROM ${catalog}.${database}").toString())
                assertEquals(
                        (sql "DESC internal.${database}.base_table").collect { it[0..2] },
                        (sql "DESC ${catalog}.${database}.base_table").collect { it[0..2] })
            } finally {
                flight.closeSession(new CloseSessionRequest(), credential, deadline)
            }
        }
    }
}
