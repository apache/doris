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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Reads {@code getObjects} results produced by the real SQLite driver, so the nested-Arrow parsing is
 * checked against a shape a driver actually emits rather than one this test invented. A hand-built result
 * covers the case a real driver will not produce on demand: a non-standard schema.
 */
class AdbcObjectsReaderTest {

    private static AdbcClient sqliteClient(Path dbFile) {
        return new AdbcClient(AdbcNativeTestSupport.sqliteDriver(), "libadbc_driver_sqlite.so",
                null, "file:" + dbFile, null, null, Map.of());
    }

    private static void seed(AdbcClient client) {
        client.withConnection(connection -> {
            for (String sql : new String[] {
                    "CREATE TABLE IF NOT EXISTS t1 (c_int INTEGER, c_txt TEXT)",
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
    void readsTheNamespaceASourceWithNoSchemaLayerReports(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("objects.db"))) {
            seed(client);

            List<AdbcNamespace> namespaces = client.withConnection(connection -> {
                try (ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.DB_SCHEMAS, null, null, null, null, null)) {
                    return AdbcObjectsReader.readNamespaces(reader);
                }
            });

            // SQLite has catalogs but no schema layer, and reports the missing level as an EMPTY STRING
            // rather than null -- treating only null as "absent" would produce a Doris database named "".
            Assertions.assertEquals(1, namespaces.size(), namespaces.toString());
            Assertions.assertEquals("main", namespaces.get(0).getRemoteCatalog());
            Assertions.assertEquals("", namespaces.get(0).getRemoteDbSchema());
            Assertions.assertEquals("main", namespaces.get(0).dorisDatabaseName());
        }
    }

    @Test
    void readsTableNamesAndHonoursTheTableTypeFilter(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("objects.db"))) {
            seed(client);
            AdbcNamespace main = new AdbcNamespace("main", "");

            List<String> tables = client.withConnection(connection -> {
                try (ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES, null, null, null,
                        new String[] {"table"}, null)) {
                    return AdbcObjectsReader.readTableNames(reader, main);
                }
            });

            // v1 is a view. Doris presents no views for ADBC catalogs, so listing it would produce a table
            // that DESC and SELECT both fail on.
            Assertions.assertEquals(List.of("t1", "t2"), tables);
        }
    }

    @Test
    void viewsAreDroppedEvenWhenTheSourceIgnoresTheTypeFilter(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("objects.db"))) {
            seed(client);
            AdbcNamespace main = new AdbcNamespace("main", "");

            // Asking with no type filter reproduces, through a real driver, what a Doris source does to the
            // filter the connector does send: its Flight SQL endpoint recognises only the literal "VIEW" and
            // answers "table" with everything. The guarantee has to survive that, so it cannot live in the
            // request -- v1 must be gone because of the table_type that came back with it.
            List<String> tables = client.withConnection(connection -> {
                try (ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)) {
                    return AdbcObjectsReader.readTableNames(reader, main);
                }
            });

            Assertions.assertEquals(List.of("t1", "t2"), tables);
        }
    }

    @Test
    void tablesOfOtherNamespacesAreNotListed(@TempDir Path tempDir) {
        try (AdbcClient client = sqliteClient(tempDir.resolve("objects.db"))) {
            seed(client);
            // getObjects filters are advisory: a driver may answer with everything it has. Returning rows
            // that belong to a different namespace would list those tables under the wrong database.
            AdbcNamespace other = new AdbcNamespace("someother", "public");

            List<String> tables = client.withConnection(connection -> {
                try (ArrowReader reader = connection.getObjects(
                        AdbcConnection.GetObjectsDepth.TABLES, null, null, null,
                        new String[] {"table"}, null)) {
                    return AdbcObjectsReader.readTableNames(reader, other);
                }
            });

            Assertions.assertEquals(List.of(), tables);
        }
    }

    @Test
    void theTypeNamesRealSourcesUseAreClassifiedTheWayTheyMean() {
        // A Doris source spells a table "BASE TABLE" -- getting that one wrong does not hide a view, it
        // hides EVERY table of every ADBC catalog pointed at Doris, and the catalog just looks empty.
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable("BASE TABLE"));
        // ...and its materialized views come back under the same name, which is right: those are storage
        // that can be scanned, not a query wearing a table's name.
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable("table"));

        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("VIEW"));
        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("view"));
        // What Doris calls its information_schema tables. Reading one through ADBC is not supported either.
        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("SYSTEM VIEW"));

        // Dropped, deliberately. The forgiving rule -- keep what is not recognised -- is the wrong one here:
        // a leaked view scans fine through ADBC, so it never announces itself. A source whose tables land
        // here lists nothing instead, which does.
        Assertions.assertFalse(AdbcObjectsReader.isBaseTable("OLAP"));

        // Saying nothing is not the same as saying something unrecognised: a source that omits the column
        // stays exactly as usable as it was before this filter existed.
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable(null));
        Assertions.assertTrue(AdbcObjectsReader.isBaseTable(""));
    }

    @Test
    void resultWithoutTheStandardColumnsIsRejectedByName() throws Exception {
        // A driver that answers getObjects with its own shape must fail with something that says so; the
        // alternative is a NullPointerException from deep inside the reader.
        Schema schema = new Schema(List.of(
                new Field("something_else", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        try (BufferAllocator allocator = new RootAllocator();
                ArrowReader reader = oneRowReader(allocator, schema)) {
            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> AdbcObjectsReader.readNamespaces(reader));
            Assertions.assertTrue(e.getMessage().contains("catalog_name"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("something_else"), e.getMessage());
        }
    }

    private static ArrowReader oneRowReader(BufferAllocator allocator, Schema schema) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
            VarCharVector vector = (VarCharVector) root.getVector(0);
            vector.allocateNew(1);
            vector.setSafe(0, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            root.setRowCount(1);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator);
    }
}
