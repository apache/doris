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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code ALTER TABLE ... SET (...)} on a Paimon table: every property becomes a {@link SchemaChange#setOption} in
 * ONE remote {@code alterTable}, so paimon validates the whole list before committing any of it and the table gets
 * one new schema version per statement.
 */
public class PaimonConnectorMetadataTablePropertiesTest {

    private static final Identifier TABLE_ID = Identifier.create("db", "t");

    @Test
    public void setTablePropertiesSendsEveryOptionInOneAuthenticatedAlter() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext context = new RecordingConnectorContext();
        PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                ops, PaimonCatalogProperties.of(Collections.emptyMap()), context);

        metadata.setTableProperties(null, handle(), options("2", "5"));

        Assertions.assertEquals(Collections.singletonList("alterTable:db.t"), ops.log);
        Assertions.assertEquals(TABLE_ID, ops.lastAlteredTableId);
        Assertions.assertEquals(Arrays.asList(
                        SchemaChange.setOption("snapshot.num-retained.min", "2"),
                        SchemaChange.setOption("snapshot.num-retained.max", "5")),
                ops.lastSchemaChanges);
        Assertions.assertEquals(1, context.authCount, "the remote alter must run inside executeAuthenticated");
    }

    @Test
    public void setTablePropertiesPersistsEveryOptionAsOneSchemaVersion(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            catalog.createTable(TABLE_ID, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .option("snapshot.num-retained.min", "1")
                    .option("snapshot.num-retained.max", "8")
                    .option("commit.max-retries", "3")
                    .build(), false);
            long schemaId = schemaId(catalog);

            metadataOver(catalog).setTableProperties(null, handle(), options("2", "5"));

            Map<String, String> actual = catalog.getTable(TABLE_ID).options();
            Assertions.assertEquals("2", actual.get("snapshot.num-retained.min"));
            Assertions.assertEquals("5", actual.get("snapshot.num-retained.max"));
            Assertions.assertEquals("3", actual.get("commit.max-retries"), "an option not named must be kept");
            Assertions.assertEquals(schemaId + 1, schemaId(catalog), "one statement must be one schema version");
        }
    }

    @Test
    public void setTablePropertiesRejectedListCommitsNothing(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            catalog.createTable(TABLE_ID, Schema.newBuilder()
                    .column("id", DataTypes.INT().notNull())
                    .column("seq", DataTypes.BIGINT())
                    .primaryKey("id")
                    .option("snapshot.num-retained.min", "2")
                    .option("snapshot.num-retained.max", "5")
                    .build(), false);
            long schemaId = schemaId(catalog);
            // The valid entry goes first: applying the entries one at a time would commit it before the
            // invalid one fails.
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("snapshot.num-retained.max", "10");
            properties.put("fields.missing.sequence-group", "seq");

            DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadataOver(catalog).setTableProperties(null, handle(), properties));

            Assertions.assertTrue(e.getMessage().contains("missing"), e.getMessage());
            Map<String, String> actual = catalog.getTable(TABLE_ID).options();
            Assertions.assertEquals("5", actual.get("snapshot.num-retained.max"),
                    "the valid entry of a rejected list must not be committed on its own");
            Assertions.assertFalse(actual.containsKey("fields.missing.sequence-group"));
            Assertions.assertEquals(schemaId, schemaId(catalog));
        }
    }

    @Test
    public void setTablePropertiesRejectsPathBeforeRemoteAlter() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        RecordingConnectorContext context = new RecordingConnectorContext();
        PaimonConnectorMetadata metadata = new PaimonConnectorMetadata(
                ops, PaimonCatalogProperties.of(Collections.emptyMap()), context);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("snapshot.num-retained.max", "10");
        properties.put("PATH", "s3://warehouse/relocated_table");

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.setTableProperties(null, handle(), properties));

        Assertions.assertTrue(e.getMessage().contains("Change path is not supported yet"), e.getMessage());
        Assertions.assertTrue(ops.log.isEmpty(), ops.log.toString());
        Assertions.assertEquals(0, context.authCount);
    }

    @Test
    public void setTablePropertiesPathLeavesTableUnchanged(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = localCatalog(warehouse)) {
            catalog.createTable(TABLE_ID, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .option("snapshot.num-retained.min", "1")
                    .option("snapshot.num-retained.max", "8")
                    .build(), false);
            long schemaId = schemaId(catalog);
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("snapshot.num-retained.max", "10");
            properties.put("path", warehouse.resolve("relocated_table").toUri().toString());

            // Paimon itself would store path in a new schema version without moving the table; the
            // catalog then overrides it on load, so check the stored schema rather than the loaded table.
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> metadataOver(catalog).setTableProperties(null, handle(), properties));

            Map<String, String> stored = storedSchemaOptions(catalog);
            Assertions.assertEquals("8", stored.get("snapshot.num-retained.max"));
            Assertions.assertFalse(stored.containsKey("path"), stored.toString());
            Assertions.assertEquals(schemaId, schemaId(catalog));
        }
    }

    private static Map<String, String> storedSchemaOptions(Catalog catalog) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(TABLE_ID);
        return new FileSystemSchemaManager(LocalFileIO.create(), table.location()).latest().get().options();
    }

    private static Map<String, String> options(String min, String max) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("snapshot.num-retained.min", min);
        properties.put("snapshot.num-retained.max", max);
        return properties;
    }

    private static PaimonTableHandle handle() {
        return new PaimonTableHandle("db", "t", Collections.emptyList(), Collections.emptyList());
    }

    private static Catalog localCatalog(Path warehouse) throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new org.apache.paimon.fs.Path(warehouse.toUri()));
        catalog.createDatabase("db", false);
        return catalog;
    }

    private static PaimonConnectorMetadata metadataOver(Catalog catalog) {
        return new PaimonConnectorMetadata(new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog),
                PaimonCatalogProperties.of(Collections.emptyMap()), new RecordingConnectorContext());
    }

    private static long schemaId(Catalog catalog) throws Exception {
        return ((FileStoreTable) catalog.getTable(TABLE_ID)).schema().id();
    }
}
