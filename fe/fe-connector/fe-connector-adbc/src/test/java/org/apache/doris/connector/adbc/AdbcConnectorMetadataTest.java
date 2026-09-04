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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.arrow.adbc.core.AdbcStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The metadata surface end to end against the real SQLite ADBC driver: the same Java -> JNI -> C driver
 * manager -> driver .so path FE takes in production, with only the source swapped for a temporary file.
 *
 * <p>Skips loudly when thirdparty's native libraries are absent -- see {@link AdbcNativeTestSupport}. A
 * skipped run has verified nothing here.
 */
class AdbcConnectorMetadataTest {

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    /**
     * A fresh cache per call, so each test here reads the source rather than the previous test's answers --
     * what the cache does across statements is {@link AdbcMetadataCacheNativeTest}'s subject, not this one's.
     */
    private static AdbcConnectorMetadata metadataOn(AdbcClient client) {
        return new AdbcConnectorMetadata(client, new AdbcSchemaStrategy(),
                AdbcDialectRegistry::defaultDialect, new AdbcMetadataCache(Map.of()));
    }

    /**
     * The INSERT is load-bearing, not decoration. SQLite is dynamically typed and its ADBC driver derives
     * the Arrow schema from the values present, not from the declared column types: on an empty t1 it
     * reports all four columns as int64. So a fixture with no rows would assert a type mapping the driver
     * never actually produced.
     */
    private static void seed(AdbcClient client) {
        client.withConnection(connection -> {
            for (String sql : new String[] {
                    "CREATE TABLE IF NOT EXISTS t1 (c_int INTEGER, c_dbl REAL, c_txt TEXT, c_blob BLOB)",
                    "INSERT INTO t1 VALUES (1, 1.5, 'a', x'00ff')",
                    "CREATE TABLE IF NOT EXISTS t2 (a INTEGER)",
                    "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t1"}) {
                try (AdbcStatement statement = connection.createStatement()) {
                    statement.setSqlQuery(sql);
                    statement.executeUpdate();
                }
            }
            return null;
        });
    }

    @Test
    void showDatabasesReturnsTheFlattenedNamespace(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            AdbcConnectorMetadata metadata = metadataOn(client);

            Assertions.assertEquals(List.of("main"), metadata.listDatabaseNames(null));
            Assertions.assertTrue(metadata.databaseExists(null, "main"));
            Assertions.assertFalse(metadata.databaseExists(null, "no_such_db"));
        }
    }

    @Test
    void showTablesExcludesViews(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);

            // Doris presents no views for an ADBC catalog, so listing v1 would produce a name that DESC and
            // SELECT would both then fail on.
            Assertions.assertEquals(List.of("t1", "t2"), metadataOn(client).listTableNames(null, "main"));
        }
    }

    @Test
    void listingTablesOfAnUnknownDatabaseIsEmptyNotAnError(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            Assertions.assertEquals(List.of(), metadataOn(client).listTableNames(null, "no_such_db"));
        }
    }

    @Test
    void tableHandleCarriesTheRemoteCoordinates(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);

            Optional<ConnectorTableHandle> handle = metadataOn(client).getTableHandle(null, "main", "t1");

            Assertions.assertTrue(handle.isPresent());
            AdbcTableHandle adbc = (AdbcTableHandle) handle.get();
            // SQLite names a catalog but no schema, so the Doris database name came from the catalog level.
            // The handle must still record which level it came from, or the pushed-down SQL later cannot
            // qualify the table.
            Assertions.assertEquals("main", adbc.getRemoteCatalog());
            Assertions.assertEquals("", adbc.getRemoteDbSchema());
            Assertions.assertEquals("t1", adbc.getRemoteTable());
            Assertions.assertEquals("main", adbc.getDorisDbName());
        }
    }

    @Test
    void unknownTableAndUnknownDatabaseBothYieldNoHandle(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            AdbcConnectorMetadata metadata = metadataOn(client);

            Assertions.assertEquals(Optional.empty(), metadata.getTableHandle(null, "main", "no_such"));
            Assertions.assertEquals(Optional.empty(), metadata.getTableHandle(null, "no_such", "t1"));
            // A view is not a table here; handing back a handle for it would defer the failure to DESC.
            Assertions.assertEquals(Optional.empty(), metadata.getTableHandle(null, "main", "v1"));
        }
    }

    @Test
    void descMapsTheRealArrowSchema(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            AdbcConnectorMetadata metadata = metadataOn(client);
            ConnectorTableHandle handle = metadata.getTableHandle(null, "main", "t1").orElseThrow();

            ConnectorTableSchema schema = metadata.getTableSchema(null, handle);

            // Expected values come from what the driver actually reports: SQLite INTEGER arrives as int64,
            // REAL as float64, TEXT as utf8, BLOB as binary. Doris has no binary column type here, so BLOB
            // lands on STRING.
            List<String> names = new ArrayList<>();
            List<ConnectorType> types = new ArrayList<>();
            for (ConnectorColumn column : schema.getColumns()) {
                names.add(column.getName());
                types.add(column.getType());
            }
            Assertions.assertEquals(List.of("c_int", "c_dbl", "c_txt", "c_blob"), names);
            Assertions.assertEquals(List.of(ConnectorType.of("BIGINT"), ConnectorType.of("DOUBLE"),
                    ConnectorType.of("STRING"), ConnectorType.of("STRING")), types);
            Assertions.assertEquals("t1", schema.getTableName());
        }
    }

    @Test
    void columnHandlesMatchTheSchemaAndItsOrder(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            AdbcConnectorMetadata metadata = metadataOn(client);
            ConnectorTableHandle handle = metadata.getTableHandle(null, "main", "t1").orElseThrow();

            Map<String, ConnectorColumnHandle> handles = metadata.getColumnHandles(null, handle);

            // Order matters: the scan path pairs handles with schema columns positionally.
            Assertions.assertEquals(List.of("c_int", "c_dbl", "c_txt", "c_blob"),
                    new ArrayList<>(handles.keySet()));
        }
    }

    @Test
    void missingTableIsReportedAsSuchNotAsADriverGap(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);
            AdbcConnectorMetadata metadata = metadataOn(client);
            // Build a handle for a table that does not exist, bypassing getTableHandle's existence check.
            AdbcTableHandle ghost = new AdbcTableHandle(new AdbcNamespace("main", ""), "no_such_table");

            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadata.getTableSchema(null, ghost));

            // The fallback to executeSchema must fire only on NOT_IMPLEMENTED. Falling back on every error
            // would answer a plain missing table with "this driver implements neither method", sending the
            // user to look at their driver instead of their table name.
            Assertions.assertTrue(e.getMessage().contains("no_such_table"), e.getMessage());
            Assertions.assertFalse(e.getMessage().contains("implements neither"), e.getMessage());
        }
    }

    @Test
    void tableDescriptorIsTypedForTheScanPath(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("meta.db"))) {
            seed(client);

            TTableDescriptor descriptor = metadataOn(client)
                    .buildTableDescriptor(null, 7L, "t1", "main", "t1", 4, 42L);

            // Returning null (the SPI default) would let fe-core fall back to SCHEMA_TABLE, and BE would
            // then build a SchemaTableDescriptor rather than the one the file-scan path expects.
            Assertions.assertEquals(TTableType.HIVE_TABLE, descriptor.getTableType());
            Assertions.assertTrue(descriptor.isSetHiveTable());
        }
    }
}
