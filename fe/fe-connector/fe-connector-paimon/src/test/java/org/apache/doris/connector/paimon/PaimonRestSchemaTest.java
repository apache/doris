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

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class PaimonRestSchemaTest {
    private static final Identifier TABLE = Identifier.create("db", "events");

    @TempDir
    java.nio.file.Path directory;

    @Test
    void restSchemaCanBePinnedWithCacheEnabledOrDisabled() throws Exception {
        for (boolean cacheEnabled : new boolean[] {true, false}) {
            FileStoreTable physical = physicalTable("table-" + cacheEnabled, "value", 1000L);
            FileStoreTable restTable = restTable(physical, cacheEnabled ? "rest-table-id" : null);
            Assertions.assertNotEquals(physical.schema().timeMillis(), restTable.schema().timeMillis());
            Assertions.assertNotEquals(physical.schema().highestFieldId(), restTable.schema().highestFieldId());
            Assertions.assertNotEquals(physical.schema().version(), restTable.schema().version());
            AtomicReference<Table> response = new AtomicReference<>(restTable);
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                    RESTCatalog rest = restCatalog(response, new AtomicInteger())) {
                Catalog cached = cached(rest, owner, cacheEnabled);
                FileStoreTable loaded = (FileStoreTable) cached.getTable(TABLE);
                Map<String, String> pin = pin(loaded);
                FileStoreTable scan = PaimonScanParams.applyOptionsWithoutTimeTravel(loaded, pin);
                PaimonSchemaPin.validateRetainedSchema(scan.schema(), physical.schema());
                Assertions.assertEquals("false", scan.options().get("file-index.read.enabled"));
                Assertions.assertSame(restTable.fileIO(), scan.fileIO());
                Assertions.assertSame(restTable.catalogEnvironment(), scan.catalogEnvironment());
            }
        }
    }

    @Test
    void cachedRestHandleRejectsSameShapeReplacementEvenWhenPinDescribesReplacement() throws Exception {
        FileStoreTable physical = physicalTable("replaced", "value", 1000L);
        AtomicReference<Table> response = new AtomicReference<>(restTable(physical));
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(response, loads)) {
            Catalog cached = cached(rest, owner, true);
            FileStoreTable retained = (FileStoreTable) cached.getTable(TABLE);
            physical.schemaManager().deleteSchema(0L);
            FileStoreTable replacement = physicalTable("replaced", "value", 2000L);
            response.set(restTable(replacement));
            Assertions.assertSame(retained, cached.getTable(TABLE));
            Assertions.assertEquals(2, loads.get());
            Map<String, String> pin = pin(retained);
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> PaimonScanParams.applyOptionsWithoutTimeTravel(retained, pin));
            cached.invalidateTable(TABLE);
            FileStoreTable fresh = (FileStoreTable) cached.getTable(TABLE);
            Assertions.assertDoesNotThrow(() -> PaimonScanParams.applyOptionsWithoutTimeTravel(fresh, pin));
            Assertions.assertEquals(4, loads.get());
        }
    }

    @Test
    void restAndPhysicalFieldsMustAgreeBeforeBinding() throws Exception {
        FileStoreTable physical = physicalTable("mismatch", "old_value", 1000L);
        FileStoreTable staleRest = restTable(physical);
        physical.schemaManager().deleteSchema(0L);
        physicalTable("mismatch", "new_value", 2000L);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(new AtomicReference<>(staleRest), new AtomicInteger())) {
            Catalog cached = cached(rest, owner, true);
            Assertions.assertThrows(DorisConnectorException.class, () -> cached.getTable(TABLE));
        }
    }

    @Test
    void fallbackBranchesKeepTheirOwnPhysicalSchemas() throws Exception {
        FileStoreTable main = physicalTable("main", "value", 1000L);
        FileStoreTable fallback = physicalTable("fallback", "value", 2000L);
        FallbackReadFileStoreTable pair = new FallbackReadFileStoreTable(restTable(main), restTable(fallback), true);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(new AtomicReference<>(pair), new AtomicInteger())) {
            FallbackReadFileStoreTable loaded = (FallbackReadFileStoreTable) cached(rest, owner, true).getTable(TABLE);
            PaimonSchemaPin.validateRetainedSchema(loaded.wrapped().schema(), main.schema());
            PaimonSchemaPin.validateRetainedSchema(loaded.other().schema(), fallback.schema());
            Assertions.assertSame(pair.wrapped().catalogEnvironment(), loaded.wrapped().catalogEnvironment());
            Assertions.assertSame(pair.other().catalogEnvironment(), loaded.other().catalogEnvironment());
            Assertions.assertTrue(PaimonReaderOptions.isWrappedFirst(loaded));
        }
    }

    @Test
    void replacementBeforeFirstSchemaReadUsesFreshRestOptions() throws Exception {
        FileStoreTable original = bucketTable("before-read");
        FileStoreTable replacement = replacement(original, 2000L);
        FileStoreTable oldRest = restTable(original);
        FileStoreTable newRest = restTable(replacement);
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(() -> {
                    if (loads.get() == 1) {
                        replaceSchema(replacement);
                        writeBucketRows(replacement);
                        return oldRest;
                    }
                    return newRest;
                }, loads)) {
            FileStoreTable loaded = (FileStoreTable) cached(rest, owner, true).getTable(TABLE);
            FileStoreTable mixed = oldRest.copy(replacement.schema().copy(oldRest.options()));
            Assertions.assertTrue(matchingRows(mixed).size() < 16,
                    "the fixture must expose bucket pruning with mixed-generation options");
            Assertions.assertEquals(16, matchingRows(loaded).size(),
                    "WHERE x = 1 must retain rows from every replacement bucket keyed by y");
            Assertions.assertEquals("y", loaded.options().get("bucket-key"),
                    "a replacement schema must never be combined with the original bucket key");
            PaimonSchemaPin.validateRetainedSchema(loaded.schema(), replacement.schema());
        }
    }

    @Test
    void replacementDuringRestReloadRejectsTheLoadWithoutCachingIt() throws Exception {
        for (long replacementTime : new long[] {1000L, 2000L}) {
            FileStoreTable original = bucketTable("during-reload-" + replacementTime);
            FileStoreTable replacement = replacement(original, replacementTime);
            FileStoreTable oldRest = restTable(original);
            FileStoreTable newRest = restTable(replacement);
            AtomicInteger loads = new AtomicInteger();
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                    RESTCatalog rest = restCatalog(() -> {
                        if (loads.get() == 2) {
                            replaceSchema(replacement);
                        }
                        return loads.get() <= 2 ? oldRest : newRest;
                    }, loads)) {
                Catalog cached = cached(rest, owner, true);
                Assertions.assertThrows(DorisConnectorException.class, () -> cached.getTable(TABLE));
                FileStoreTable retried = (FileStoreTable) cached.getTable(TABLE);
                Assertions.assertEquals("y", retried.options().get("bucket-key"));
                PaimonSchemaPin.validateRetainedSchema(retried.schema(), replacement.schema());
            }
        }
    }

    @Test
    void fallbackReplacementDuringRestReloadIsAlsoRejected() throws Exception {
        FileStoreTable main = bucketTable("stable-main");
        FileStoreTable fallback = bucketTable("changing-fallback");
        FileStoreTable replacement = replacement(fallback, 2000L);
        FallbackReadFileStoreTable pair = new FallbackReadFileStoreTable(restTable(main), restTable(fallback), true);
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(() -> {
                    if (loads.get() == 2) {
                        replaceSchema(replacement);
                    }
                    return pair;
                }, loads)) {
            Catalog cached = cached(rest, owner, true);
            Assertions.assertThrows(DorisConnectorException.class, () -> cached.getTable(TABLE));
        }
    }

    @Test
    void fallbackShapeChangeDuringReloadIsRejected() throws Exception {
        FileStoreTable main = restTable(bucketTable("shape-main"));
        FileStoreTable fallback = restTable(bucketTable("shape-fallback"));
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(() -> loads.get() == 1
                        ? new FallbackReadFileStoreTable(main, fallback, true) : main, loads)) {
            Assertions.assertThrows(DorisConnectorException.class, () -> cached(rest, owner, true).getTable(TABLE));
        }
    }

    @Test
    void changedLocationWithIdenticalSchemaDuringReloadIsRejected() throws Exception {
        FileStoreTable original = restTable(bucketTable("original-location"));
        FileStoreTable relocated = restTable(bucketTable("new-location"));
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged();
                RESTCatalog rest = restCatalog(() -> loads.get() == 1 ? original : relocated, loads)) {
            Assertions.assertThrows(DorisConnectorException.class, () -> cached(rest, owner, true).getTable(TABLE));
        }
    }

    private static void writeBucketRows(FileStoreTable table) {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            for (int y = 0; y < 16; y++) {
                write.write(GenericRow.of(1, y));
            }
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<Integer> matchingRows(FileStoreTable table) throws Exception {
        ReadBuilder read = table.newReadBuilder().withFilter(new PredicateBuilder(table.rowType()).equal(0, 1));
        List<Integer> values = new ArrayList<>();
        for (Split split : read.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.newRead().createReader(split)) {
                reader.forEachRemaining(row -> values.add(row.getInt(1)));
            }
        }
        return values;
    }

    private FileStoreTable bucketTable(String name) throws Exception {
        LocalFileIO io = LocalFileIO.create();
        org.apache.paimon.fs.Path location = new org.apache.paimon.fs.Path(directory.resolve(name).toUri());
        Schema schema = Schema.newBuilder().column("x", DataTypes.INT().notNull())
                .column("y", DataTypes.INT().notNull()).primaryKey("x", "y")
                .option("bucket", "2").option("bucket-key", "x").build();
        TableSchema fields = TableSchema.create(0L, schema);
        TableSchema persisted = new TableSchema(fields.version(), 0L, fields.fields(), fields.highestFieldId(),
                fields.partitionKeys(), fields.primaryKeys(), fields.options(), fields.comment(), 1000L);
        Assertions.assertTrue(new SchemaManager(io, location).commit(persisted));
        return FileStoreTableFactory.create(io, location, persisted);
    }

    private static FileStoreTable replacement(FileStoreTable original, long time) {
        TableSchema schema = original.schema();
        Map<String, String> options = new HashMap<>(schema.options());
        options.put("bucket-key", "y");
        TableSchema replaced = new TableSchema(schema.version(), schema.id(), schema.fields(), schema.highestFieldId(),
                schema.partitionKeys(), schema.primaryKeys(), options, schema.comment(), time);
        return FileStoreTableFactory.create(original.fileIO(), original.location(), replaced);
    }

    private static void replaceSchema(FileStoreTable replacement) {
        try {
            replacement.fileIO().overwriteFileUtf8(replacement.schemaManager().toSchemaPath(replacement.schema().id()),
                    replacement.schema().toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FileStoreTable physicalTable(String name, String valueColumn, long creationTime) throws Exception {
        LocalFileIO io = LocalFileIO.create();
        org.apache.paimon.fs.Path location = new org.apache.paimon.fs.Path(directory.resolve(name).toUri());
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT())
                .column(valueColumn, DataTypes.STRING()).option("bucket", "-1").build();
        TableSchema fields = TableSchema.create(0L, schema);
        // Older schema formats and removed field IDs are also absent from REST's Schema payload.
        TableSchema persisted = new TableSchema(TableSchema.PAIMON_07_VERSION, 0L, fields.fields(), 10,
                fields.partitionKeys(), fields.primaryKeys(), fields.options(), fields.comment(), creationTime);
        Assertions.assertTrue(new SchemaManager(io, location).commit(persisted));
        return FileStoreTableFactory.create(io, location, persisted);
    }

    private static FileStoreTable restTable(FileStoreTable physical) {
        return restTable(physical, "rest-table-id");
    }

    private static FileStoreTable restTable(FileStoreTable physical, String uuid) {
        TableSchema reconstructed = TableSchema.create(physical.schema().id(), physical.schema().toSchema());
        Map<String, String> options = new HashMap<>(reconstructed.options());
        options.put("file-index.read.enabled", "false");
        return FileStoreTableFactory.create(physical.fileIO(), physical.location(), reconstructed.copy(options),
                new CatalogEnvironment(TABLE, uuid, null, null, null, null, false, true));
    }

    private static Map<String, String> pin(FileStoreTable table) {
        Map<String, String> options = PaimonScanParams.withBoundSchema(
                PaimonScanParams.pinOptionsToSnapshot(Collections.emptyMap(), -1L), table.schema().id());
        options.putAll(PaimonSchemaPin.capture(table, table.schema().id(), -1L));
        return options;
    }

    private static Catalog cached(RESTCatalog rest, CatalogMetaCache owner, boolean enabled) {
        return PaimonMetaCacheCatalog.tryToCreate(rest, owner, 100, 100, new Options(),
                enabled, true, catalog -> catalog);
    }

    private static RESTCatalog restCatalog(AtomicReference<Table> response, AtomicInteger loads) {
        return restCatalog(response::get, loads);
    }

    private static RESTCatalog restCatalog(Supplier<Table> response, AtomicInteger loads) {
        Options options = new Options();
        options.set("uri", "http://localhost:1");
        options.set("prefix", "test-prefix");
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        return new RESTCatalog(CatalogContext.create(options), false) {
            @Override
            public Table getTable(Identifier identifier) {
                loads.incrementAndGet();
                return response.get();
            }
        };
    }
}
