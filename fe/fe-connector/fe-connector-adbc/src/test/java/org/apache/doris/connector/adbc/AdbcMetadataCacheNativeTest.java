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
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

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
 * The metadata path with a catalog-level cache in front of it, against the real SQLite driver.
 *
 * <p>Each test changes the source behind Doris's back and then asks what Doris sees. That is the only
 * evidence that says whether an answer came from memory or from the driver, and unlike a call counter it
 * cannot be satisfied by a cache that stores things it never reads.
 *
 * <p>Every {@code metadata()} call stands for one statement: the engine builds a fresh
 * {@link AdbcConnectorMetadata} per statement, and the cache is what they share.
 *
 * <p>Skips loudly when thirdparty's native libraries are absent -- see {@link AdbcNativeTestSupport}.
 */
class AdbcMetadataCacheNativeTest {

    private final AdbcMetadataCache cache = new AdbcMetadataCache(Map.of());

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    /** One statement's view of the catalog. Separate objects, one shared cache -- as in production. */
    private AdbcConnectorMetadata metadata(AdbcClient client) {
        return new AdbcConnectorMetadata(client, new AdbcSchemaStrategy(),
                AdbcDialectRegistry::defaultDialect, cache);
    }

    private static void execute(AdbcClient client, String... statements) {
        client.withConnection(connection -> {
            for (String sql : statements) {
                try (AdbcStatement statement = connection.createStatement()) {
                    statement.setSqlQuery(sql);
                    statement.executeUpdate();
                }
            }
            return null;
        });
    }

    /** SQLite derives its Arrow types from the values present, so a row is needed for the types to be real. */
    private static void seed(AdbcClient client) {
        execute(client,
                "CREATE TABLE t1 (c_int INTEGER, c_txt TEXT)",
                "INSERT INTO t1 VALUES (1, 'a')");
    }

    private static List<String> columnNames(ConnectorTableSchema schema) {
        List<String> names = new ArrayList<>();
        for (ConnectorColumn column : schema.getColumns()) {
            names.add(column.getName());
        }
        return names;
    }

    private List<String> columnsOf(AdbcClient client, String table) {
        ConnectorTableHandle handle = metadata(client).getTableHandle(null, "main", table).orElseThrow();
        return columnNames(metadata(client).getTableSchema(null, handle));
    }

    @Test
    void theNextStatementReadsTheSchemaTheLastOneAlreadyPaidFor(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("cache.db"))) {
            seed(client);
            Assertions.assertEquals(List.of("c_int", "c_txt"), columnsOf(client, "t1"));

            execute(client, "ALTER TABLE t1 ADD COLUMN c_added INTEGER");

            // The column really is there now -- the source changed and Doris was not told. Serving the
            // remembered shape is the whole point; noticing the change here would mean nothing was cached.
            Assertions.assertEquals(List.of("c_int", "c_txt"), columnsOf(client, "t1"));
        }
    }

    @Test
    void refreshTableIsWhatMakesTheAlteredColumnsVisible(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("cache.db"))) {
            seed(client);
            columnsOf(client, "t1");
            execute(client, "ALTER TABLE t1 ADD COLUMN c_added INTEGER");

            cache.invalidateTable("main", "t1");

            Assertions.assertEquals(List.of("c_int", "c_txt", "c_added"), columnsOf(client, "t1"));
        }
    }

    /**
     * Decision C. Reading the listing from memory is fine; concluding from memory that a name does not exist
     * is not. A user who just created a table and is told it is not there has no way to tell that from a
     * typo, and no reason to suspect a cache.
     */
    @Test
    void tableCreatedAfterTheListingWasCachedIsStillFound(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("cache.db"))) {
            seed(client);
            metadata(client).listTableNames(null, "main");

            execute(client, "CREATE TABLE t_new (a INTEGER)", "INSERT INTO t_new VALUES (1)");

            Optional<ConnectorTableHandle> handle = metadata(client).getTableHandle(null, "main", "t_new");
            Assertions.assertTrue(handle.isPresent(), "a table created after the listing was cached must"
                    + " still be reachable by name");
            Assertions.assertEquals(List.of("a"), columnNames(metadata(client)
                    .getTableSchema(null, handle.get())));
        }
    }

    @Test
    void missingTableIsStillMissingAfterTheListingIsReRead(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("cache.db"))) {
            seed(client);
            metadata(client).listTableNames(null, "main");

            // Re-reading the listing is a last chance to find the name, not a way to accept any name.
            Assertions.assertEquals(Optional.empty(),
                    metadata(client).getTableHandle(null, "main", "no_such_table"));
        }
    }

    /**
     * The listing methods stay live however much is remembered. They read like reports, but the engine loads
     * its own name cache from them and then decides from that whether a table exists at all -- including the
     * re-list it does as a last chance for a name it has never seen. A cached answer here would turn that
     * re-check into a formality and leave a table created a moment ago unreachable.
     */
    @Test
    void listingTheTablesAlwaysAsksTheSource(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("cache.db"))) {
            seed(client);
            Assertions.assertEquals(List.of("t1"), metadata(client).listTableNames(null, "main"));

            execute(client, "CREATE TABLE t_new (a INTEGER)");

            Assertions.assertEquals(List.of("t1", "t_new"), metadata(client).listTableNames(null, "main"));
        }
    }

    @Test
    void listingTheDatabasesAlwaysAsksTheSource(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("cache.db"))) {
            seed(client);
            // Plant a database the source does not have. Creating one behind Doris's back is what this
            // stands in for -- SQLite gains a catalog only by ATTACH, which does not outlive the connection
            // it ran on -- and it fails the same way: anything answered from memory shows the ghost.
            cache.namespaces(() -> List.of(new AdbcNamespace("ghost", "")));

            Assertions.assertEquals(List.of("main"), metadata(client).listDatabaseNames(null));
        }
    }
}
