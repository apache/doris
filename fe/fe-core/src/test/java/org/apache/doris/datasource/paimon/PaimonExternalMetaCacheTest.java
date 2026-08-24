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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.metacache.paimon.PaimonLatestSnapshotProjectionLoader;
import org.apache.doris.datasource.metacache.paimon.PaimonPartitionInfoLoader;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PaimonExternalMetaCacheTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testLatestSnapshotUsesLatestSchemaForPinnedRead() {
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(),
                (nameMapping, schemaId) -> new PaimonSchemaCacheValue(
                        Collections.emptyList(), Collections.emptyList(), null));
        NameMapping nameMapping = new NameMapping(1L, "db", "table", "remote_db", "remote_table");
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable pinnedTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable latestSchemaTable = Mockito.mock(FileStoreTable.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableSchema latestSchema = Mockito.mock(TableSchema.class);
        Mockito.when(snapshot.id()).thenReturn(12L);
        Mockito.when(baseTable.copyWithLatestSchema()).thenReturn(latestSchemaTable);
        Mockito.when(latestSchemaTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(latestSchemaTable.latestSnapshot()).thenReturn(Optional.of(snapshot));
        Mockito.when(latestSchemaTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(pinnedTable);
        Mockito.when(latestSchemaTable.schemaManager()).thenReturn(schemaManager);
        Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
        Mockito.when(latestSchema.id()).thenReturn(4L);

        PaimonSnapshotCacheValue value = loader.load(nameMapping, baseTable);

        Assert.assertEquals(12L, value.getSnapshot().getSnapshotId());
        Assert.assertEquals(4L, value.getSnapshot().getSchemaId());
        Assert.assertSame(pinnedTable, value.getSnapshot().getTable());
        Mockito.verify(latestSchemaTable).copyWithoutTimeTravel(
                Mockito.argThat(options -> "12".equals(options.get(CoreOptions.SCAN_SNAPSHOT_ID.key()))
                        && options.entrySet().stream()
                                .filter(entry -> entry.getValue() != null)
                                .count() == 1));
    }

    @Test
    public void testFullLatestProjectionCapsManifestParallelismBeforePartitionLoad() throws AnalysisException {
        // Keep the test aligned with the loader contract: partition loading may report analysis failures.
        int localCapacity = Runtime.getRuntime().availableProcessors();
        Assume.assumeTrue(localCapacity < 256);
        PaimonPartitionInfoLoader partitionLoader = Mockito.mock(PaimonPartitionInfoLoader.class);
        Mockito.when(partitionLoader.load(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(PaimonPartitionInfo.EMPTY);
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                partitionLoader,
                (nameMapping, schemaId) -> new PaimonSchemaCacheValue(
                        Collections.emptyList(), Collections.emptyList(), null));
        NameMapping nameMapping = new NameMapping(1L, "db", "table", "remote_db", "remote_table");
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable latestSchemaTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable pinnedTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable cappedTable = Mockito.mock(FileStoreTable.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableSchema latestSchema = Mockito.mock(TableSchema.class);
        Mockito.when(baseTable.copyWithLatestSchema()).thenReturn(latestSchemaTable);
        Mockito.when(latestSchemaTable.options()).thenReturn(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "256"));
        Mockito.when(latestSchemaTable.latestSnapshot()).thenReturn(Optional.of(snapshot));
        Mockito.when(snapshot.id()).thenReturn(12L);
        Mockito.when(latestSchemaTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(pinnedTable);
        // Model Paimon's option inheritance after snapshot pinning. The cap must be applied to the
        // resulting planning leaf so fallback branches keep their independent lower limits.
        Mockito.when(pinnedTable.options()).thenReturn(Collections.singletonMap(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "256"));
        Mockito.when(pinnedTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(cappedTable);
        Mockito.when(latestSchemaTable.schemaManager()).thenReturn(schemaManager);
        Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
        Mockito.when(latestSchema.id()).thenReturn(4L);

        loader.load(nameMapping, baseTable);

        Mockito.verify(latestSchemaTable).copyWithoutTimeTravel(Mockito.argThat(options ->
                "12".equals(options.get(CoreOptions.SCAN_SNAPSHOT_ID.key()))));
        Mockito.verify(pinnedTable).copyWithoutTimeTravel(Mockito.argThat(options ->
                String.valueOf(localCapacity).equals(
                        options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()))));
        Mockito.verify(partitionLoader).load(nameMapping, cappedTable, Collections.emptyList());
    }

    @Test
    public void testLatestFenceDoesNotLoadSchemaOrPartitions() {
        PaimonPartitionInfoLoader partitionLoader = Mockito.mock(PaimonPartitionInfoLoader.class);
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                partitionLoader,
                (nameMapping, schemaId) -> {
                    throw new AssertionError("a version-only fence must not load schema metadata");
                });
        NameMapping nameMapping = new NameMapping(1L, "db", "table", "remote_db", "remote_table");
        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable latestSchemaTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable pinnedTable = Mockito.mock(FileStoreTable.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableSchema latestSchema = Mockito.mock(TableSchema.class);
        Mockito.when(baseTable.copyWithLatestSchema()).thenReturn(latestSchemaTable);
        Mockito.when(latestSchemaTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(latestSchemaTable.latestSnapshot()).thenReturn(Optional.of(snapshot));
        Mockito.when(snapshot.id()).thenReturn(12L);
        Mockito.when(latestSchemaTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(pinnedTable);
        Mockito.when(latestSchemaTable.schemaManager()).thenReturn(schemaManager);
        Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
        Mockito.when(latestSchema.id()).thenReturn(4L);

        PaimonSnapshotCacheValue fence = loader.loadFence(nameMapping, baseTable);

        Assert.assertEquals(12L, fence.getSnapshot().getSnapshotId());
        Assert.assertEquals(4L, fence.getSnapshot().getSchemaId());
        Assert.assertSame(PaimonPartitionInfo.EMPTY, fence.getPartitionInfo());
        Mockito.verifyNoInteractions(partitionLoader);
    }

    @Test
    public void testFenceHydrationKeepsCapturedTableGeneration() throws Exception {
        PaimonPartitionInfoLoader partitionLoader = Mockito.mock(PaimonPartitionInfoLoader.class);
        Mockito.when(partitionLoader.load(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(PaimonPartitionInfo.EMPTY);
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                partitionLoader,
                (nameMapping, schemaId) -> new PaimonSchemaCacheValue(
                        Collections.emptyList(), Collections.emptyList(), null));
        NameMapping nameMapping = new NameMapping(1L, "db", "table", "remote_db", "remote_table");
        FileStoreTable captured = Mockito.mock(FileStoreTable.class);
        FileStoreTable capturedPinned = Mockito.mock(FileStoreTable.class);
        Mockito.when(captured.options()).thenReturn(Collections.emptyMap());
        Mockito.when(captured.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(capturedPinned);
        PaimonSnapshot fence = new PaimonSnapshot(12L, 4L, captured);

        PaimonSnapshotCacheValue hydrated = loader.loadAtFence(nameMapping, fence);

        Assert.assertSame(
                "hydration must derive from the fence's captured table, not a reloaded generation",
                capturedPinned, hydrated.getSnapshot().getTable());
        Mockito.verify(captured, Mockito.never()).copyWithLatestSchema();
    }

    @Test
    public void testTagProjectionKeepsOnlyRepinnedSnapshotSelector() throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new Path(temporaryFolder.newFolder("tag_projection").toURI()));
        catalog.createDatabase("db", false);
        Identifier identifier = Identifier.create("db", "table");
        catalog.createTable(identifier, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        try (StreamTableWrite write = table.newWrite("test");
                StreamTableCommit commit = table.newCommit("test")) {
            write.write(BinaryRow.singleColumn(1));
            commit.commit(0, write.prepareCommit(false, 1));
        }
        table.createTag("stable", 1L);
        Table selected = PaimonScanParams.applyOptions(
                table, Collections.singletonMap(CoreOptions.SCAN_TAG_NAME.key(), "stable"));
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(),
                (nameMapping, schemaId) -> new PaimonSchemaCacheValue(
                        Collections.emptyList(), Collections.emptyList(), null));

        PaimonSnapshotCacheValue value = loader.load(
                new NameMapping(1L, "db", "table", "db", "table"), selected);

        Assert.assertEquals("1", value.getSnapshot().getTable().options()
                .get(CoreOptions.SCAN_SNAPSHOT_ID.key()));
        Assert.assertFalse(value.getSnapshot().getTable().options()
                .containsKey(CoreOptions.SCAN_TAG_NAME.key()));
    }

    @Test
    public void testNeutralCacheLoadDefersPhysicalManifestValidation() throws Exception {
        java.io.File warehouse = temporaryFolder.newFolder("neutral_cache");
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "paimon");
        properties.put(PaimonExternalCatalog.PAIMON_CATALOG_TYPE,
                PaimonExternalCatalog.PAIMON_FILESYSTEM);
        properties.put("warehouse", warehouse.toURI().toString());
        PaimonExternalCatalog dorisCatalog = new PaimonExternalCatalog(
                91L, "paimon_test", null, properties, "");
        dorisCatalog.makeSureInitialized();
        dorisCatalog.catalog.createDatabase("db", false);
        Identifier identifier = Identifier.create("db", "table");
        dorisCatalog.catalog.createTable(identifier, Schema.newBuilder()
                .column("id", DataTypes.INT())
                .option(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "0")
                .build(), false);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.doReturn(dorisCatalog).when(catalogMgr)
                .getCatalogOrException(Mockito.eq(91L), Mockito.any());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            cache.initCatalog(91L, Collections.emptyMap());

            Table table = cache.getPaimonTable(
                    new NameMapping(91L, "db", "table", "db", "table"));

            Assert.assertEquals("0", table.options().get(
                    CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testReloadInvalidatesPaimonTableCacheBeforeLoad() throws Exception {
        java.io.File warehouse = temporaryFolder.newFolder("reload_table");
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "paimon");
        properties.put(PaimonExternalCatalog.PAIMON_CATALOG_TYPE,
                PaimonExternalCatalog.PAIMON_FILESYSTEM);
        properties.put("warehouse", warehouse.toURI().toString());
        PaimonExternalCatalog dorisCatalog = new PaimonExternalCatalog(
                92L, "paimon_reload_test", null, properties, "");
        dorisCatalog.makeSureInitialized();
        dorisCatalog.catalog.close();

        Catalog paimonCatalog = Mockito.mock(Catalog.class);
        Table expected = Mockito.mock(Table.class);
        Identifier identifier = Identifier.create("remote_db", "remote_table");
        Mockito.when(paimonCatalog.getTable(identifier)).thenReturn(expected);
        dorisCatalog.catalog = paimonCatalog;

        Table actual = dorisCatalog.reloadPaimonTable(
                new NameMapping(92L, "local_db", "local_table", "remote_db", "remote_table"));

        Assert.assertSame(expected, actual);
        InOrder inOrder = Mockito.inOrder(paimonCatalog);
        inOrder.verify(paimonCatalog).invalidateTable(identifier);
        inOrder.verify(paimonCatalog).getTable(identifier);
    }

    @Test
    public void testDorisTableCacheMissReloadsPaimonCatalogOnce() throws Exception {
        long catalogId = 93L;
        NameMapping nameMapping = new NameMapping(
                catalogId, "local_db", "local_table", "remote_db", "remote_table");
        Table expected = Mockito.mock(Table.class);
        PaimonExternalCatalog dorisCatalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(dorisCatalog.reloadPaimonTable(nameMapping)).thenReturn(expected);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.doReturn(dorisCatalog).when(catalogMgr)
                .getCatalogOrException(Mockito.eq(catalogId), Mockito.any());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            cache.initCatalog(catalogId, Collections.emptyMap());

            Assert.assertSame(expected, cache.getPaimonTable(nameMapping));
            Assert.assertSame(expected, cache.getPaimonTable(nameMapping));

            Mockito.verify(dorisCatalog, Mockito.times(1)).reloadPaimonTable(nameMapping);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testPartitionProjectionRejectsUnsafeEffectiveTableBeforeEnumeration() throws Exception {
        FileStoreTable unsafeTable = newPartitionedTable(
                "unsafe", Collections.singletonMap("scan.manifest.parallelism", "0"));
        PaimonPartitionInfoLoader loader = new PaimonPartitionInfoLoader();

        try {
            loader.load(new NameMapping(1L, "db", "table", "db", "table"), unsafeTable,
                    Collections.singletonList(new Column("part", Type.INT)));
            Assert.fail("partition projection must reject unsafe manifest parallelism before enumeration");
        } catch (CacheException e) {
            Assert.assertTrue(e.getMessage().contains("scan.manifest.parallelism"));
        }
    }

    @Test
    public void testPartitionProjectionUsesSafeCopiedTableInsteadOfRawCatalogReload() throws Exception {
        FileStoreTable unsafeTable = newPartitionedTable(
                "safe_override", Collections.singletonMap("scan.manifest.parallelism", "0"));
        FileStoreTable safeTable = unsafeTable.copy(
                Collections.singletonMap("scan.manifest.parallelism", "1"));
        PaimonPartitionInfoLoader loader = new PaimonPartitionInfoLoader();

        PaimonPartitionInfo partitionInfo = loader.load(
                new NameMapping(1L, "db", "table", "db", "table"), safeTable,
                Collections.singletonList(new Column("part", Type.INT)));

        Assert.assertTrue(partitionInfo.getNameToPartition().isEmpty());
    }

    @Test
    public void testPartitionProjectionIgnoresReaderOnlyPhysicalOptions() throws Exception {
        FileStoreTable table = newPartitionedTable(
                "reader_only", Collections.singletonMap("read.batch-size", "0"));
        PaimonPartitionInfoLoader loader = new PaimonPartitionInfoLoader();

        // Partition metadata does not use the data-reader batch size. The final relation copy is
        // validated separately before scanning, so a safe override can reuse this projection.
        PaimonPartitionInfo partitionInfo = loader.load(
                new NameMapping(1L, "db", "table", "db", "table"), table,
                Collections.singletonList(new Column("part", Type.INT)));

        Assert.assertTrue(partitionInfo.getNameToPartition().isEmpty());
    }

    private FileStoreTable newPartitionedTable(String name, Map<String, String> options) throws Exception {
        TableSchema schema = new TableSchema(
                0,
                java.util.Arrays.asList(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", new IntType())),
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                options,
                null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(),
                new Path(temporaryFolder.newFolder(name).toURI()),
                schema,
                CatalogEnvironment.empty());
    }

    @Test
    public void testInvalidateTablePrecise() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping t1 = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping t2 = new NameMapping(catalogId, "db1", "tbl2", "rdb1", "rtbl2");

            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tableEntry =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tableEntry.put(t1, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 1L, null))));
            tableEntry.put(t2, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(2L, 2L, null))));

            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(tableEntry.getIfPresent(t1));
            Assert.assertNotNull(tableEntry.getIfPresent(t2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateDbAndStats() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping db1Table = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping db2Table = new NameMapping(catalogId, "db2", "tbl1", "rdb2", "rtbl1");

            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tableEntry =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tableEntry.put(db1Table, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 1L, null))));
            tableEntry.put(db2Table, new PaimonTableCacheValue(null,
                    () -> new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, new PaimonSnapshot(2L, 2L, null))));

            org.apache.doris.datasource.metacache.MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemaEntry =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class);
            PaimonSchemaCacheKey db1Schema = new PaimonSchemaCacheKey(db1Table, 1L);
            PaimonSchemaCacheKey db2Schema = new PaimonSchemaCacheKey(db2Table, 2L);
            schemaEntry.put(db1Schema, new SchemaCacheValue(Collections.emptyList()));
            schemaEntry.put(db2Schema, new SchemaCacheValue(Collections.emptyList()));

            cache.invalidateDb(catalogId, "db1");

            Assert.assertNull(tableEntry.getIfPresent(db1Table));
            Assert.assertNotNull(tableEntry.getIfPresent(db2Table));
            Assert.assertNull(schemaEntry.getIfPresent(db1Schema));
            Assert.assertNotNull(schemaEntry.getIfPresent(db2Schema));

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            Assert.assertTrue(stats.containsKey(PaimonExternalMetaCache.ENTRY_TABLE));
            Assert.assertTrue(stats.containsKey(PaimonExternalMetaCache.ENTRY_SCHEMA));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSchemaStatsWhenSchemaCacheDisabled() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "0");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats schemaStats = stats.get(PaimonExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(0L, schemaStats.getTtlSecond());
            Assert.assertTrue(schemaStats.isConfigEnabled());
            Assert.assertFalse(schemaStats.isEffectiveEnabled());
        } finally {
            executor.shutdownNow();
        }
    }
}
