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

import java.nio.channels.Channels

import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.CloseSessionRequest
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.FlightClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.Location
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.memory.RootAllocator
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.driver.jdbc.shaded.org.apache.arrow.vector.ipc.message.MessageSerializer

suite("test_flight_metadata_catalog", "p0,external,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    if (host == null || host.isEmpty()) {
        logger.info("Arrow Flight SQL is not configured, skip the test.")
        return
    }
    // Use the live port, since an external environment's configured Flight port may be stale.
    int port = (sql "SHOW FRONTENDS")[0][6] as int
    if (port <= 0) {
        logger.info("Arrow Flight SQL is disabled, skip the test.")
        return
    }
    String catalog = "test_flight_metadata_catalog"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String password = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type' = 'hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
    )"""
    String originalShowFullName = (sql "SHOW GLOBAL VARIABLES LIKE 'show_full_dbname_in_info_schema_db'")[0][1]
    try {
        def expectedSchemas = (sql "SHOW DATABASES FROM ${catalog} LIKE 'multi_catalog'")
                .collect { [catalog, it[0].toString()] }
        assertFalse(expectedSchemas.isEmpty(), "Hive metadata fixture is missing")

        new RootAllocator().withCloseable { allocator ->
            FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build().withCloseable { client ->
                def credential = client.authenticateBasicToken(user, password).get()
                def flight = new FlightSqlClient(client)
                try {
                    def read = { info, List<String> columns ->
                        def rows = []
                        info.getEndpoints().each { endpoint ->
                            flight.getStream(endpoint.getTicket(), credential).withCloseable { stream ->
                                while (stream.next()) {
                                    def root = stream.getRoot()
                                    for (int row = 0; row < root.getRowCount(); row++) {
                                        rows.add(columns.collect { name ->
                                            if (name == "table_schema") {
                                                byte[] bytes = ((VarBinaryVector) root.getVector(name)).get(row)
                                                return MessageSerializer.deserializeSchema(new ReadChannel(
                                                        Channels.newChannel(new ByteArrayInputStream(bytes))))
                                                        .getFields().collect { it.getName() }
                                            }
                                            return root.getVector(name).getObject(row)?.toString()
                                        })
                                    }
                                }
                            }
                        }
                        return rows
                    }
                    for (boolean showFullName : [false, true]) {
                        // Flight carries catalog and schema separately, regardless of MySQL display settings.
                        sql "SET GLOBAL show_full_dbname_in_info_schema_db = ${showFullName}"
                        def schemas = { String filter, String pattern ->
                            read(flight.getSchemas(filter, pattern, credential),
                                    ["catalog_name", "db_schema_name"])
                        }
                        for (String pattern : ["multi_catalog", "multi%", null]) {
                            String like = pattern == null ? "" : " LIKE '${pattern}'"
                            def expected = (sql "SHOW DATABASES FROM ${catalog}${like}")
                                    .collect { [catalog, it[0].toString()] }.sort()
                            assertEquals(expected, schemas(catalog, pattern).sort())
                            // Omission must discover the external catalog even when the session starts in internal.
                            if (pattern != null) {
                                assertEquals(expected, schemas(null, pattern).findAll { it[0] == catalog }.sort())
                            }
                            assertTrue(schemas("", pattern).isEmpty())
                        }
                        assertTrue(schemas(catalog, "missing_flight_schema%").isEmpty())

                        for (boolean includeSchema : [false, true]) {
                            for (String table : ["region", "hive_text_complex_type2"]) {
                                def expectedTables = (sql "SHOW FULL TABLES FROM ${catalog}.multi_catalog LIKE '${table}'")
                                        .findAll { it[0].toString() == table }
                                        .collect { [catalog, "multi_catalog", it[0].toString(), it[1].toString()] }
                                assertFalse(expectedTables.isEmpty(), "Hive table fixture is missing: ${table}")
                                def columns = ["catalog_name", "db_schema_name", "table_name", "table_type"]
                                if (includeSchema) {
                                    def fieldNames = (sql "DESC ${catalog}.multi_catalog.${table}")
                                            .collect { it[0].toString() }
                                    expectedTables.each { it.add(fieldNames) }
                                    columns.add("table_schema")
                                }
                                def tables = { String filter, String pattern ->
                                    read(flight.getTables(filter, pattern, table, null, includeSchema, credential),
                                            columns).findAll { it[1] == "multi_catalog" && it[2] == table }
                                }
                                for (String pattern : ["multi_catalog", "multi%"]) {
                                    assertEquals(expectedTables, tables(catalog, pattern))
                                    assertEquals(expectedTables, tables(null, pattern).findAll { it[0] == catalog })
                                    assertTrue(tables("", pattern).isEmpty())
                                }
                            }
                        }
                        def internalSchemas = read(flight.getSchemas("internal", "information_schema", credential),
                                ["catalog_name", "db_schema_name"])
                        def allSchemas = read(flight.getSchemas(null, "information_schema", credential),
                                ["catalog_name", "db_schema_name"])
                        assertEquals(internalSchemas, allSchemas.findAll { it[0] == "internal" })
                    }
                } finally {
                    flight.closeSession(new CloseSessionRequest(), credential)
                }
            }
        }
    } finally {
        sql "SET GLOBAL show_full_dbname_in_info_schema_db = ${originalShowFullName}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
    }
}
