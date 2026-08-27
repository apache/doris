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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.iceberg.source.IcebergTableQueryInfo;
import org.apache.doris.datasource.metacache.EstimatorCalibrationAssertions;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IcebergExternalMetaCacheTest {
    // U+0130 (LATIN CAPITAL LETTER I WITH DOT ABOVE) lower-cases to two characters in Locale.ROOT.
    private static final String DOTTED_CAPITAL_I = String.valueOf((char) 0x0130);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSnapshotAndManifestWeightsScaleLinearlyToOneHundredThousandItems() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(
                mapping, tableWithMetadataLocation("/metadata/linear-v1.json")).get();
        long snapshotBase = snapshotWeight(snapshotKey, 0);
        long snapshotOneThousand = snapshotWeight(snapshotKey, 1_000);
        assertLinearScale(snapshotBase, snapshotOneThousand,
                snapshotWeight(snapshotKey, 10_000), snapshotWeight(snapshotKey, 100_000));

        IcebergManifestEntryKey manifestKey = new IcebergManifestEntryKey(
                "/manifest/linear.avro", ManifestContent.DATA);
        long manifestBase = manifestWeight(manifestKey, 0);
        long manifestOneThousand = manifestWeight(manifestKey, 1_000);
        assertLinearScale(manifestBase, manifestOneThousand,
                manifestWeight(manifestKey, 10_000), manifestWeight(manifestKey, 100_000));
    }

    @Test
    public void testWeightedEntriesAreRegistered() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.table.max-weight", "4MB");
            properties.put("meta.cache.iceberg.snapshot.max-weight", "8MB");
            properties.put("meta.cache.iceberg.manifest.enable", "true");
            properties.put("meta.cache.iceberg.manifest.max-weight", "16MB");
            cache.initCatalog(1L, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(1L);
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_TABLE).isWeightBounded());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_SNAPSHOT).isWeightBounded());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isWeightBounded());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotInheritsLegacyTableCountSettingsButNotWeight() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.table.enable", "false");
            properties.put("meta.cache.iceberg.table.ttl-second", "17");
            properties.put("meta.cache.iceberg.table.capacity", "23");
            cache.initCatalog(1L, properties);

            MetaCacheEntryStats snapshot = cache.stats(1L).get(IcebergExternalMetaCache.ENTRY_SNAPSHOT);
            Assert.assertFalse(snapshot.isConfigEnabled());
            Assert.assertEquals(17L, snapshot.getTtlSecond());
            Assert.assertEquals(23L, snapshot.getCapacity());
            Assert.assertFalse(snapshot.isWeightBounded());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotKeyIncludesMetadataGeneration() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Table first = tableWithMetadataLocation("/metadata/v1.json");
        Table second = tableWithMetadataLocation("/metadata/v2.json");
        Table recreated = tableWithMetadataLocation("/metadata/v1.json");

        IcebergSnapshotEntryKey firstKey = IcebergSnapshotEntryKey.tryCreate(mapping, first).get();
        IcebergSnapshotEntryKey sameKey = IcebergSnapshotEntryKey.tryCreate(mapping, first).get();
        IcebergSnapshotEntryKey secondKey = IcebergSnapshotEntryKey.tryCreate(mapping, second).get();
        IcebergSnapshotEntryKey recreatedKey = IcebergSnapshotEntryKey.tryCreate(mapping, recreated).get();

        Assert.assertEquals(firstKey, sameKey);
        Assert.assertNotEquals(firstKey, secondKey);
        Assert.assertNotEquals("drop/recreate may reuse HadoopCatalog's v1 path", firstKey, recreatedKey);
        Assert.assertEquals("/metadata/v1.json", firstKey.getMetadataFileLocation());
        Assert.assertNotEquals(firstKey.getTableUuid(), recreatedKey.getTableUuid());
        Assert.assertFalse(IcebergSnapshotEntryKey.tryCreate(mapping,
                newInterfaceProxy(Table.class)).isPresent());
    }

    @Test
    public void testReplacingTableGenerationRetiresSnapshotAndSchemaProjection() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping mapping = NameMapping.createForTest(catalogId, "db", "tbl");
            IcebergTableCacheValue first = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/retire-v1.json"));
            IcebergTableCacheValue second = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/retire-v2.json"));
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);
            tables.put(mapping, first);
            IcebergSnapshotEntryKey oldSnapshotKey = IcebergSnapshotEntryKey.tryCreate(
                    mapping, first.getRetainedIcebergTable()).get();
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            snapshots.put(oldSnapshotKey, new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L)));
            IcebergSchemaCacheKey oldSchemaKey = new IcebergSchemaCacheKey(
                    mapping, first.getTableUuid().get(), 0L);
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SCHEMA,
                    IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            schemas.put(oldSchemaKey, new SchemaCacheValue(Collections.emptyList()));

            // Simulate expiry/invalidation before the next table generation is published.
            tables.invalidateKey(mapping);
            tables.put(mapping, second);

            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
            while ((snapshots.peekIfPresent(oldSnapshotKey) != null
                    || schemas.peekIfPresent(oldSchemaKey) != null)
                    && System.nanoTime() < deadlineNanos) {
                TimeUnit.MILLISECONDS.sleep(10L);
            }
            Assert.assertNull(snapshots.peekIfPresent(oldSnapshotKey));
            Assert.assertNull(schemas.peekIfPresent(oldSchemaKey));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testCatalogInvalidationDropsManifestCacheForSnapshotOnlyFileIo() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping sharedMapping = NameMapping.createForTest(catalogId, "db", "shared");
            NameMapping evictedMapping = NameMapping.createForTest(catalogId, "db", "evicted");
            PropertiesFileIO sharedIo = new PropertiesFileIO("token", "shared");
            PropertiesFileIO snapshotOnlyIo = new PropertiesFileIO("token", "snapshot-only");
            Table sharedTable = tableWithMetadata(
                    metadataWithLocation("/metadata/shared-v1.json"), sharedIo);
            Table evictedTable = tableWithMetadata(
                    metadataWithLocation("/metadata/evicted-v1.json"), snapshotOnlyIo);

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);

            // The shared generation is retained by both the table entry and its projection: the
            // reset must drop its per-FileIO manifest cache exactly once.
            tables.put(sharedMapping, new IcebergTableCacheValue(sharedTable));
            snapshots.put(IcebergSnapshotEntryKey.tryCreate(sharedMapping, sharedTable).get(),
                    new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(),
                            new IcebergSnapshot(-1L, 0L), Optional.empty(), sharedTable));
            // The second generation survives only in its independently admitted snapshot
            // projection, mimicking weight/TTL eviction of the base table entry before reset.
            snapshots.put(IcebergSnapshotEntryKey.tryCreate(evictedMapping, evictedTable).get(),
                    new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(),
                            new IcebergSnapshot(-1L, 0L), Optional.empty(), evictedTable));

            try (MockedStatic<ManifestFiles> manifestFiles = Mockito.mockStatic(ManifestFiles.class)) {
                cache.invalidateCatalog(catalogId);
                manifestFiles.verify(() -> ManifestFiles.dropCache(sharedIo), Mockito.times(1));
                manifestFiles.verify(() -> ManifestFiles.dropCache(snapshotOnlyIo), Mockito.times(1));
                manifestFiles.verifyNoMoreInteractions();
            }
            Assert.assertNull(tables.peekIfPresent(sharedMapping));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testSameGenerationRefreshWithRenewedFileIoRetiresSnapshotProjection() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping mapping = NameMapping.createForTest(catalogId, "db", "tbl");
            TableMetadata metadata = metadataWithLocation("/metadata/rotate-v1.json");
            // Same metadata file, but each catalog reload vends a FileIO with fresh credentials.
            IcebergTableCacheValue first = new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "one")));
            IcebergTableCacheValue rotated = new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "two")));
            IcebergTableCacheValue equivalent = new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "two")));
            Assert.assertTrue(first.isSamePhysicalGeneration(rotated));
            Assert.assertFalse(first.isSameOperationalGeneration(rotated));
            Assert.assertTrue(rotated.isSameOperationalGeneration(equivalent));

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SCHEMA,
                    IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            tables.put(mapping, first);
            IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(
                    mapping, first.getRetainedIcebergTable()).get();
            IcebergSnapshotCacheValue firstProjection = new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                    Optional.empty(), first.getRetainedIcebergTable());
            snapshots.put(snapshotKey, firstProjection);
            IcebergSchemaCacheKey schemaKey = new IcebergSchemaCacheKey(mapping, first.getTableUuid().get(), 0L);
            schemas.put(schemaKey, new SchemaCacheValue(Collections.emptyList()));

            // Rotated credentials: the projection frozen on the previous handle must not survive,
            // schema projections are generation-keyed and stay.
            tables.put(mapping, rotated);
            Assert.assertNull(snapshots.peekIfPresent(snapshotKey));
            Assert.assertNotNull(schemas.peekIfPresent(schemaKey));

            IcebergSnapshotCacheValue rotatedProjection = new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                    Optional.empty(), rotated.getRetainedIcebergTable());
            snapshots.put(snapshotKey, rotatedProjection);
            // An equivalent reload (same credentials, new FileIO instance) keeps the projection.
            tables.put(mapping, equivalent);
            Assert.assertSame(rotatedProjection, snapshots.peekIfPresent(snapshotKey));

            // A count-mode projection retains no handle and is never bound to credentials.
            IcebergSnapshotCacheValue countProjection = new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L));
            snapshots.put(snapshotKey, countProjection);
            tables.put(mapping, new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "three"))));
            Assert.assertSame(countProjection, snapshots.peekIfPresent(snapshotKey));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testAcquisitionsFailWhenCatalogResetsMidFlight() {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps opsOne = Mockito.mock(IcebergMetadataOps.class);
        IcebergMetadataOps opsTwo = Mockito.mock(IcebergMetadataOps.class);
        java.util.concurrent.atomic.AtomicReference<IcebergMetadataOps> currentOps =
                new java.util.concurrent.atomic.AtomicReference<>(opsOne);
        ExecutionAuthenticator generationOne = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        ExecutionAuthenticator generationTwo = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        java.util.concurrent.atomic.AtomicReference<ExecutionAuthenticator> currentAuthenticator =
                new java.util.concurrent.atomic.AtomicReference<>(generationOne);
        java.util.concurrent.atomic.AtomicBoolean alterCompletesDuringLoad =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        Mockito.when(catalog.getMetadataOps()).thenAnswer(invocation -> currentOps.get());
        Mockito.when(catalog.getExecutionAuthenticator()).thenAnswer(
                invocation -> currentAuthenticator.get());
        stubTableLoadContext(catalog);
        Table table = tableWithMetadataLocation("/metadata/coherent-acquisition-v1.json");
        org.mockito.stubbing.Answer<Table> loadFlipsGeneration = invocation -> {
            if (alterCompletesDuringLoad.get()) {
                // The concurrent property/credential ALTER reinitializes the catalog while the
                // external load is still in flight.
                currentAuthenticator.set(generationTwo);
                currentOps.set(opsTwo);
            }
            return table;
        };
        Mockito.when(opsOne.loadTable("remote_db", "remote_tbl")).thenAnswer(loadFlipsGeneration);
        Mockito.when(opsTwo.loadTable("remote_db", "remote_tbl")).thenAnswer(loadFlipsGeneration);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }
        };
        try {
            cache.initCatalog(1L, Collections.emptyMap());
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);

            // Miss-load boundary: the spliced acquisition must fail instead of publishing.
            try {
                tables.get(mapping);
                Assert.fail("a table load spliced by a mid-flight catalog reset must not publish");
            } catch (RuntimeException e) {
                Assert.assertTrue(String.valueOf(e.getMessage()),
                        exceptionChainContains(e, "was reset while acquiring iceberg table"));
            }
            Assert.assertNull(tables.peekIfPresent(mapping));

            // Writable-handle boundary: same acquisition contract for DDL handles.
            currentAuthenticator.set(generationOne);
            currentOps.set(opsOne);
            try {
                cache.getWritableIcebergTable(dorisTable);
                Assert.fail("a writable handle spliced by a mid-flight catalog reset must not be served");
            } catch (RuntimeException e) {
                Assert.assertTrue(String.valueOf(e.getMessage()),
                        exceptionChainContains(e, "was reset while acquiring iceberg table"));
            }

            // Retries run entirely against the settled second generation and stay coherent.
            alterCompletesDuringLoad.set(false);
            IcebergTableCacheValue published = tables.get(mapping);
            Assert.assertSame(currentAuthenticator.get(), published.getAuthenticator());
            Assert.assertSame(table, cache.getWritableIcebergTable(dorisTable));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testResetCatalogReinitializesBeforeCaptureAndWritableStaysOnDispatchGeneration() {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps retainedOps = Mockito.mock(IcebergMetadataOps.class);
        IcebergMetadataOps currentOps = Mockito.mock(IcebergMetadataOps.class);
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        java.util.concurrent.atomic.AtomicBoolean initialized =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        Mockito.doAnswer(invocation -> {
            initialized.set(true);
            return null;
        }).when(catalog).makeSureInitialized();
        Mockito.when(catalog.getExecutionAuthenticator()).thenAnswer(invocation -> {
            if (!initialized.get()) {
                throw new RuntimeException(
                        "ExecutionAuthenticator is null, please confirm it is initialized.");
            }
            return authenticator;
        });
        Mockito.when(catalog.getMetadataOps()).thenReturn(currentOps);
        stubTableLoadContext(catalog);
        Table table = tableWithMetadataLocation("/metadata/reset-before-capture-v1.json");
        Mockito.when(currentOps.loadTable("remote_db", "remote_tbl")).thenReturn(table);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }
        };
        try {
            cache.initCatalog(1L, Collections.emptyMap());
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);

            // A reset-to-uninitialized catalog must be initialized before the miss captures its
            // execution context, exactly as the load used to trigger implicitly.
            IcebergTableCacheValue published = tables.get(mapping);
            Assert.assertSame(authenticator, published.getAuthenticator());
            // Projections built from the published generation carry its execution context for
            // later planning-time validation.
            Assert.assertSame(authenticator,
                    cache.getSnapshotCache(dorisTable).getCapturedAuthenticator());

            initialized.set(false);
            Assert.assertSame(table, cache.getWritableIcebergTable(dorisTable));
            Assert.assertTrue(initialized.get());

            // A caller that retains the ops of an earlier dispatch generation must not be handed
            // the reinitialized generation's writable handle.
            try {
                cache.getWritableIcebergTable(dorisTable, retainedOps);
                Assert.fail("a writable acquisition must stay on the caller's dispatch generation");
            } catch (RuntimeException e) {
                Assert.assertTrue(String.valueOf(e.getMessage()),
                        exceptionChainContains(e, "was reset while acquiring iceberg table"));
            }
            Assert.assertSame(table, cache.getWritableIcebergTable(dorisTable, currentOps));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotPartitionLoadUsesCapturedAuthenticator() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.isValidRelatedTable()).thenReturn(true);
        Table projectionTable = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Schema schema = Mockito.mock(Schema.class);
        Mockito.when(projectionTable.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(11L);
        Mockito.when(projectionTable.schema()).thenReturn(schema);
        Mockito.when(schema.schemaId()).thenReturn(3);
        ExecutionAuthenticator capturedAuthenticator = new ExecutionAuthenticator() {
        };
        Method loader = IcebergExternalMetaCache.class.getDeclaredMethod(
                "loadSnapshotProjection", ExternalTable.class, Table.class, Table.class,
                String.class, boolean.class, ExecutionAuthenticator.class);
        loader.setAccessible(true);
        try (MockedStatic<IcebergUtils> icebergUtils = Mockito.mockStatic(
                IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            icebergUtils.when(() -> IcebergUtils.loadPartitionInfo(
                            dorisTable, projectionTable, 11L, 3L, capturedAuthenticator))
                    .thenReturn(IcebergPartitionInfo.empty());
            icebergUtils.when(() -> IcebergUtils.getNameMapping(projectionTable))
                    .thenReturn(Optional.empty());
            icebergUtils.clearInvocations();

            loader.invoke(cache, dorisTable, projectionTable, projectionTable,
                    null, false, capturedAuthenticator);

            icebergUtils.verify(() -> IcebergUtils.loadPartitionInfo(
                    dorisTable, projectionTable, 11L, 3L, capturedAuthenticator));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testRetainedExecutionContextAllowanceAndPlanningFence() {
        ExecutionAuthenticator captured = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        ExecutionAuthenticator replaced = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Table tableA = tableWithMetadataLocation("/metadata/auth-allowance-v1.json");
        Table tableB = tableWithMetadataLocation("/metadata/auth-allowance-v1.json");
        IcebergTableCacheValue unbound = new IcebergTableCacheValue(tableA);
        IcebergTableCacheValue bound = new IcebergTableCacheValue(tableB);
        bound.bindAuthenticator(captured);
        Assert.assertEquals("bound values carry the retained-context allowance", 16L * 1024L,
                IcebergCacheSizeEstimator.estimateTableEntry(mapping, bound).getBytes()
                        - IcebergCacheSizeEstimator.estimateTableEntry(mapping, unbound).getBytes());

        IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(mapping, tableA).get();
        IcebergSnapshotCacheValue plain = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L), Optional.empty(), tableA);
        IcebergSnapshotCacheValue boundSnapshot = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L), Optional.empty(), tableA)
                .bindCapturedAuthenticator(captured);
        Assert.assertEquals(16L * 1024L,
                IcebergCacheSizeEstimator.estimateSnapshotEntry(key, boundSnapshot).getBytes()
                        - IcebergCacheSizeEstimator.estimateSnapshotEntry(key, plain).getBytes());

        // The pinned generation is plannable only under its captured execution context.
        boundSnapshot.ensurePlannableUnder(captured, "tbl");
        boundSnapshot.ensurePlannableUnder(null, "tbl");
        plain.ensurePlannableUnder(replaced, "tbl");
        try {
            boundSnapshot.ensurePlannableUnder(replaced, "tbl");
            Assert.fail("planning a pinned generation under a replaced catalog context must fail");
        } catch (IllegalStateException e) {
            Assert.assertTrue(String.valueOf(e.getMessage()),
                    e.getMessage().contains("please retry"));
        }
    }

    @Test
    public void testExplicitSnapshotValueCarriesItsGenerationContext() {
        ExecutionAuthenticator captured = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        ExecutionAuthenticator replaced = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        Table table = tableWithMetadataLocation("/metadata/explicit-snapshot-v1.json");
        IcebergTableCacheValue generation = new IcebergTableCacheValue(table);
        generation.bindAuthenticator(captured);

        // VERSION/TIME and branch/tag relations retain the query-scoped table of the generation
        // they were bound on; the constructed value must carry that generation's context so the
        // planning fence can reject a catalog that was reset in between.
        IcebergSnapshotCacheValue value = IcebergUtils.newExplicitSnapshotValue(
                new IcebergTableQueryInfo(1L, "main", 0), table, generation);
        Assert.assertSame(captured, value.getCapturedAuthenticator());
        value.ensurePlannableUnder(captured, "tbl");
        try {
            value.ensurePlannableUnder(replaced, "tbl");
            Assert.fail("an explicit snapshot bound before a catalog reset must not plan under the new context");
        } catch (IllegalStateException e) {
            Assert.assertTrue(String.valueOf(e.getMessage()), e.getMessage().contains("please retry"));
        }
    }

    @Test
    public void testAuthOnlyAlterRetiresProjectionsOfTheOldContext() {
        ExecutionAuthenticator oldContext = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        ExecutionAuthenticator newContext = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(java.util.concurrent.Callable<T> task) throws Exception {
                return task.call();
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping mapping = NameMapping.createForTest(catalogId, "db", "tbl");
            TableMetadata metadata = metadataWithLocation("/metadata/auth-only-alter-v1.json");
            // Same metadata file and operationally equivalent FileIO resources; only the
            // captured execution context differs, as after an auth-only catalog ALTER.
            IcebergTableCacheValue first = new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "same")));
            first.bindAuthenticator(oldContext);
            IcebergTableCacheValue refreshed = new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "same")));
            refreshed.bindAuthenticator(newContext);
            Assert.assertTrue(first.isSamePhysicalGeneration(refreshed));
            Assert.assertFalse("a replaced execution context is a new operational generation",
                    first.isSameOperationalGeneration(refreshed));
            IcebergTableCacheValue sameContext = new IcebergTableCacheValue(
                    tableWithMetadata(metadata, new PropertiesFileIO("token", "same")));
            sameContext.bindAuthenticator(oldContext);
            Assert.assertTrue(first.isSameOperationalGeneration(sameContext));

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            tables.put(mapping, first);
            IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(
                    mapping, first.getRetainedIcebergTable()).get();
            snapshots.put(key, new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L), Optional.empty(),
                    first.getRetainedIcebergTable())
                    .bindCapturedAuthenticator(oldContext));

            // The same-metadata refresh under the new context must retire the old projection so
            // the next lookup rebuilds one that is plannable again.
            tables.put(mapping, refreshed);
            Assert.assertNull("projections of the replaced execution context must be retired",
                    snapshots.peekIfPresent(key));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    private static boolean exceptionChainContains(Throwable throwable, String fragment) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(fragment)) {
                return true;
            }
        }
        return false;
    }

    private static void stubTableLoadContext(IcebergExternalCatalog catalog) {
        Mockito.when(catalog.beginTableLoad()).thenAnswer(invocation -> {
            catalog.makeSureInitialized();
            IcebergMetadataOps ops = (IcebergMetadataOps) catalog.getMetadataOps();
            ExecutionAuthenticator authenticator = catalog.getExecutionAuthenticator();
            String catalogType = catalog.getIcebergCatalogType();
            IcebergExternalCatalog.TableLoadContext context =
                    Mockito.mock(IcebergExternalCatalog.TableLoadContext.class);
            Mockito.when(context.getOps()).thenReturn(ops);
            Mockito.when(context.getAuthenticator()).thenReturn(authenticator);
            Mockito.when(context.getCatalogType()).thenReturn(catalogType);
            Mockito.when(context.loadTable(Mockito.anyString(), Mockito.anyString())).thenAnswer(load ->
                    authenticator.execute(() -> ops.loadTable(load.getArgument(0), load.getArgument(1))));
            IcebergCatalogResourceTracker.ResourceLease lease =
                    Mockito.mock(IcebergCatalogResourceTracker.ResourceLease.class);
            Mockito.when(context.promote()).thenReturn(lease);
            return context;
        });
    }

    @Test
    public void testRejectedTableGenerationsDoNotAccumulateSnapshotOrSchemaProjections() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                return task.call();
            }
        });
        stubTableLoadContext(catalog);
        // Every reload advances the metadata location; every publication is rejected.
        Mockito.when(metadataOps.loadTable("remote_db", "remote_tbl")).thenReturn(
                tableWithMetadataLocation("/metadata/rejected-v1.json"),
                tableWithMetadataLocation("/metadata/rejected-v2.json"),
                tableWithMetadataLocation("/metadata/rejected-v3.json"));
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }

            @Override
            MetaCacheSizeEstimate prepareTableForCachePublication(
                    NameMapping nameMapping, IcebergTableCacheValue value) {
                return MetaCacheSizeEstimate.incomplete("test_rejection");
            }
        };
        try {
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_SCHEMA,
                    IcebergSchemaCacheKey.class, SchemaCacheValue.class);

            for (int i = 1; i <= 3; i++) {
                IcebergSnapshotCacheValue projection = cache.getSnapshotCache(dorisTable);
                Assert.assertNotNull(projection);
                Assert.assertNull("rejected table handle must not be published", tables.peekIfPresent(mapping));
                Assert.assertEquals("projections of an unpublished generation must be retired",
                        0L, snapshots.stats().getEstimatedSize());
                Assert.assertEquals(i, tables.stats().getWeightAdmissionRejectedCount());
            }
            Mockito.verify(metadataOps, Mockito.times(3)).loadTable("remote_db", "remote_tbl");

            // Schema projections keyed by a rejected generation are not kept either.
            IcebergTableCacheValue rejected = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/rejected-schema.json"));
            IcebergSchemaCacheKey schemaKey = new IcebergSchemaCacheKey(
                    mapping, rejected.getTableUuid().get(), 0L);
            IcebergSchemaCacheValue schemaValue = new IcebergSchemaCacheValue(
                    Collections.emptyList(), Collections.emptyList());
            schemas.put(schemaKey, schemaValue);
            Assert.assertSame(schemaValue, cache.getIcebergSchemaCacheValue(
                    mapping, 0L, rejected.getRetainedIcebergTable()));
            Assert.assertNull(schemas.peekIfPresent(schemaKey));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testRejectedTableRefreshKeepsCurrentGenerationProjections() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                return task.call();
            }
        });
        stubTableLoadContext(catalog);
        Mockito.when(metadataOps.loadTable("remote_db", "remote_tbl"))
                .thenReturn(tableWithMetadataLocation("/metadata/rejected-refresh.json"));
        AtomicBoolean rejectRefresh = new AtomicBoolean(false);
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }

            @Override
            MetaCacheSizeEstimate prepareTableForCachePublication(
                    NameMapping nameMapping, IcebergTableCacheValue value) {
                if (rejectRefresh.get()) {
                    return MetaCacheSizeEstimate.incomplete("test_refresh_rejection");
                }
                return super.prepareTableForCachePublication(nameMapping, value);
            }
        };
        try {
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            IcebergTableCacheValue current = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/current.json"));
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_SCHEMA,
                    IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            tables.put(mapping, current);
            IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(
                    mapping, current.getRetainedIcebergTable()).get();
            IcebergSnapshotCacheValue snapshotValue = new IcebergSnapshotCacheValue(
                    IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L));
            snapshots.put(snapshotKey, snapshotValue);
            IcebergSchemaCacheKey schemaKey = new IcebergSchemaCacheKey(
                    mapping, current.getTableUuid().get(), 0L);
            SchemaCacheValue schemaValue = new SchemaCacheValue(Collections.emptyList());
            schemas.put(schemaKey, schemaValue);

            rejectRefresh.set(true);
            Method triggerRefresh = MetaCacheEntry.class.getDeclaredMethod("triggerRefreshForTest", Object.class);
            triggerRefresh.setAccessible(true);
            triggerRefresh.invoke(tables, mapping);
            executor.submit(() -> { }).get(3L, TimeUnit.SECONDS);

            Assert.assertSame(current, tables.peekIfPresent(mapping));
            Assert.assertSame(snapshotValue, snapshots.peekIfPresent(snapshotKey));
            Assert.assertSame(schemaValue, schemas.peekIfPresent(schemaKey));
            Assert.assertEquals(1L, tables.stats().getWeightAdmissionRejectedCount());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testIneffectiveTableEntryRevalidatesSnapshotResourcesOnEveryLookup() {
        // A base entry can be ineffective while the snapshot entry caches: the physical key hits,
        // but the projection must be rebuilt as soon as the fresh handle rotates credentials.
        assertIneffectiveBaseRebindsRotatedCredentials(
                Collections.singletonMap("meta.cache.iceberg.table.max-weight", "0"));
        Map<String, String> explicitSnapshot = new HashMap<>();
        explicitSnapshot.put("meta.cache.iceberg.table.enable", "false");
        explicitSnapshot.put("meta.cache.iceberg.snapshot.enable", "true");
        assertIneffectiveBaseRebindsRotatedCredentials(explicitSnapshot);
    }

    private void assertIneffectiveBaseRebindsRotatedCredentials(Map<String, String> catalogProperties) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                return task.call();
            }
        });
        stubTableLoadContext(catalog);
        TableMetadata metadata = metadataWithLocation("/metadata/ineffective-base.json");
        Table firstHandle = tableWithMetadata(metadata, new PropertiesFileIO("token", "one"));
        Table sameCredentials = tableWithMetadata(metadata, new PropertiesFileIO("token", "one"));
        Table rotatedHandle = tableWithMetadata(metadata, new PropertiesFileIO("token", "two"));
        Mockito.when(metadataOps.loadTable("remote_db", "remote_tbl"))
                .thenReturn(firstHandle, sameCredentials, rotatedHandle);
        AtomicInteger preparations = new AtomicInteger();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }

            @Override
            MetaCacheSizeEstimate prepareTableForCachePublication(
                    NameMapping nameMapping, IcebergTableCacheValue value) {
                preparations.incrementAndGet();
                return super.prepareTableForCachePublication(nameMapping, value);
            }
        };
        try {
            cache.initCatalog(1L, catalogProperties);
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tables = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshots = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            Assert.assertFalse(tables.isEffectivelyEnabled());
            Assert.assertTrue(snapshots.isEffectivelyEnabled());
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);

            IcebergSnapshotCacheValue first = cache.getSnapshotCache(dorisTable);
            Assert.assertNull(tables.peekIfPresent(mapping));
            Assert.assertEquals(1L, snapshots.stats().getEstimatedSize());
            // Same credentials on a new handle instance: the physically keyed projection is reused.
            Assert.assertSame(first, cache.getSnapshotCache(dorisTable));
            // Rotated credentials: the projection frozen on the first handle is rebuilt.
            IcebergSnapshotCacheValue rebound = cache.getSnapshotCache(dorisTable);
            Assert.assertNotSame(first, rebound);
            Assert.assertSame(rotatedHandle.io(), rebound.getIcebergTable().get().io());
            Assert.assertEquals(1L, snapshots.stats().getEstimatedSize());
            Mockito.verify(metadataOps, Mockito.times(3)).loadTable("remote_db", "remote_tbl");
            // An ineffective weighted entry never admits, so publication sizing is skipped.
            Assert.assertEquals(0, preparations.get());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testIneffectiveWeightedEntriesSkipPublicationSizing() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                return task.call();
            }
        });
        stubTableLoadContext(catalog);
        Mockito.when(metadataOps.loadTable("remote_db", "remote_tbl")).thenAnswer(
                invocation -> tableWithMetadataLocation("/metadata/ineffective-weighted.json"));
        AtomicInteger preparations = new AtomicInteger();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }

            @Override
            MetaCacheSizeEstimate prepareTableForCachePublication(
                    NameMapping nameMapping, IcebergTableCacheValue value) {
                preparations.incrementAndGet();
                return super.prepareTableForCachePublication(nameMapping, value);
            }
        };
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put("meta.cache.iceberg.table.max-weight", "0");
            properties.put("meta.cache.iceberg.snapshot.max-weight", "0");
            properties.put("meta.cache.iceberg.manifest.enable", "true");
            properties.put("meta.cache.iceberg.manifest.max-weight", "0");
            cache.initCatalog(1L, properties);
            for (String entryName : new String[] {IcebergExternalMetaCache.ENTRY_TABLE,
                    IcebergExternalMetaCache.ENTRY_SNAPSHOT, IcebergExternalMetaCache.ENTRY_MANIFEST}) {
                MetaCacheEntryStats stats = cache.stats(1L).get(entryName);
                Assert.assertTrue(entryName, stats.isWeightBounded());
                Assert.assertFalse(entryName, stats.isEffectiveEnabled());
            }
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);

            IcebergSnapshotCacheValue projection = cache.getSnapshotCache(dorisTable);
            IcebergTableCacheValue tableValue = cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).get(mapping);
            Assert.assertEquals(0, preparations.get());
            Assert.assertFalse(tableValue.isQueryIsolationPrepared());
            Assert.assertFalse(projection.getSizeEstimate().isComplete());
            Assert.assertEquals("not_prepared", projection.getSizeEstimate().getIncompleteReason());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testFileIoCredentialPayloadScalesWithConfiguration() {
        // The frozen operations strongly own the handle's FileIO configuration; with fixed
        // metadata the estimate must grow with the credential/property payload.
        TableMetadata metadata = metadataWithLocation("/metadata/file-io-payload.json");
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue smallIo = new IcebergTableCacheValue(
                tableWithMetadata(metadata, new PropertiesFileIO("token", "short")));
        IcebergTableCacheValue largeIo = new IcebergTableCacheValue(
                tableWithMetadata(metadata, new PropertiesFileIO(
                        "token", repeatedCharacter('c', 64 * 1024))));
        long smallEstimate = smallIo.prepareForCachePublication(mapping).getBytes();
        long largeEstimate = largeIo.prepareForCachePublication(mapping).getBytes();
        Assert.assertTrue("credential payload must be charged: small=" + smallEstimate
                + ", large=" + largeEstimate, largeEstimate - smallEstimate >= 64 * 1024 - 16);
    }

    @Test
    public void testFieldDefaultPayloadScalesWithLength() {
        // v3 field defaults retain arbitrary scalar payloads; with a fixed field count the
        // estimate must grow with the default length.
        long shortEstimate = fieldDefaultEstimate(16);
        long longEstimate = fieldDefaultEstimate(64 * 1024);
        Assert.assertTrue("default payload must be charged: short=" + shortEstimate
                + ", long=" + longEstimate, longEstimate - shortEstimate >= 60 * 1024);
    }

    private long fieldDefaultEstimate(int defaultLength) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional("defaulted").withId(2)
                        .ofType(Types.StringType.get())
                        .withInitialDefault(repeatedCharacter('d', defaultLength))
                        .withWriteDefault(repeatedCharacter('w', defaultLength))
                        .build());
        TableMetadata metadata = TableMetadata.newTableMetadata(schema,
                PartitionSpec.unpartitioned(), "file:/warehouse/db/tbl",
                Collections.singletonMap("format-version", "3"));
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation("/metadata/field-defaults.json").build();
        MetaCacheSizeEstimate estimate = new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, null), "db.tbl"))
                .prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    @Test
    public void testUnknownTransformPayloadScalesWithTokenLength() {
        // Unrecognized transform tokens are preserved verbatim; with a fixed field count the
        // estimate must grow with the token length and stay inside the character budget.
        long shortEstimate = unknownTransformEstimate(64);
        long longEstimate = unknownTransformEstimate(64 * 1024);
        Assert.assertTrue("transform payload must be charged: short=" + shortEstimate
                + ", long=" + longEstimate, longEstimate - shortEstimate >= 60 * 1024);
    }

    private long unknownTransformEstimate(int tokenLength) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema,
                PartitionSpec.builderFor(schema).identity("id").build(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        String token = repeatedCharacter('u', tokenLength);
        String json = TableMetadataParser.toJson(metadata)
                .replace("\"transform\" : \"identity\"", "\"transform\" : \"" + token + "\"")
                .replace("\"transform\":\"identity\"", "\"transform\":\"" + token + "\"");
        Assert.assertTrue("fixture must replace the transform token", json.contains(token));
        TableMetadata parsed = TableMetadataParser.fromJson("/metadata/unknown-transform.json", json);
        MetaCacheSizeEstimate estimate = new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(parsed, null), "db.tbl"))
                .prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    @Test
    public void testSameClassResourceReplacementsAreNotEquatedForEncryption() {
        // Two same-class encryption managers can hold different KMS state: only plaintext
        // managers are stateless enough to be equivalent across instances.
        Table first = Mockito.mock(Table.class);
        Table second = Mockito.mock(Table.class);
        FileIO sharedIo = Mockito.mock(FileIO.class);
        Mockito.when(sharedIo.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(first.io()).thenReturn(sharedIo);
        Mockito.when(second.io()).thenReturn(sharedIo);
        org.apache.iceberg.encryption.EncryptionManager firstManager =
                Mockito.mock(org.apache.iceberg.encryption.EncryptionManager.class);
        org.apache.iceberg.encryption.EncryptionManager secondManager =
                Mockito.mock(org.apache.iceberg.encryption.EncryptionManager.class);
        Mockito.when(first.encryption()).thenReturn(firstManager);
        Mockito.when(second.encryption()).thenReturn(secondManager);
        Assert.assertFalse("distinct stateful managers must not be equated",
                IcebergTableCacheValue.sharesOperationalResources(first, second));

        Mockito.when(second.encryption()).thenReturn(firstManager);
        Assert.assertTrue("identical managers are equivalent",
                IcebergTableCacheValue.sharesOperationalResources(first, second));

        Mockito.when(first.encryption()).thenReturn(
                org.apache.iceberg.encryption.PlaintextEncryptionManager.instance());
        Mockito.when(second.encryption()).thenReturn(
                Mockito.mock(org.apache.iceberg.encryption.PlaintextEncryptionManager.class));
        Assert.assertTrue("plaintext managers are stateless and equivalent across instances",
                IcebergTableCacheValue.sharesOperationalResources(first, second));
    }

    @Test
    public void testSchemaEntryDoesNotAutoRefreshOutsideAuthenticatorScope() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            cache.initCatalog(1L, Collections.emptyMap());
            // A (table uuid, schemaId) key is immutable, and a timed Caffeine refresh would run
            // the default loader outside the catalog execution authenticator scope.
            Assert.assertFalse(cache.stats(1L)
                    .get(IcebergExternalMetaCache.ENTRY_SCHEMA).isAutoRefresh());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testDisabledTableCacheKeepsPhysicallyKeyedSchemaProjections() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.singletonMap("meta.cache.iceberg.table.enable", "false"));
            NameMapping mapping = NameMapping.createForTest(catalogId, "db", "tbl");
            IcebergTableCacheValue table = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/disabled-table-cache.json"));
            IcebergSchemaCacheKey schemaKey = new IcebergSchemaCacheKey(mapping, table.getTableUuid().get(), 0L);
            IcebergSchemaCacheValue schemaValue = new IcebergSchemaCacheValue(
                    Collections.emptyList(), Collections.emptyList());
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SCHEMA,
                    IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            schemas.put(schemaKey, schemaValue);

            // No table handle is ever published, but the projection is keyed by the table UUID and
            // stays valid for every reload of that table.
            Assert.assertSame(schemaValue, cache.getIcebergSchemaCacheValue(
                    mapping, 0L, table.getRetainedIcebergTable()));
            Assert.assertSame(schemaValue, schemas.peekIfPresent(schemaKey));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testOldGenerationSchemaLoadCannotRepopulateAfterTableReplacement() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping mapping = NameMapping.createForTest(catalogId, "db", "tbl");
            IcebergTableCacheValue oldTable = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/schema-race-old.json"));
            IcebergTableCacheValue newTable = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/schema-race-new.json"));
            cache.entry(catalogId, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).put(mapping, newTable);
            IcebergSchemaCacheKey staleKey = new IcebergSchemaCacheKey(
                    mapping, oldTable.getTableUuid().get(), 0L);
            IcebergSchemaCacheValue staleValue = new IcebergSchemaCacheValue(
                    Collections.emptyList(), Collections.emptyList());
            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemas = cache.entry(
                    catalogId, IcebergExternalMetaCache.ENTRY_SCHEMA,
                    IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            schemas.put(staleKey, staleValue);

            Assert.assertSame(staleValue, cache.getIcebergSchemaCacheValue(
                    mapping, 0L, oldTable.getRetainedIcebergTable()));
            Assert.assertNull(schemas.peekIfPresent(staleKey));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testFrozenGenerationPreservesSparseEquivalentSchemaIds() {
        TableMetadata metadata = TableMetadataParser.fromJson("/metadata/sparse.json", "{"
                + "\"format-version\":2,\"table-uuid\":\"sparse-schema-table\","
                + "\"location\":\"file:/warehouse/sparse\",\"last-sequence-number\":0,"
                + "\"last-updated-ms\":1,\"last-column-id\":2,\"current-schema-id\":2,"
                + "\"schemas\":["
                + "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
                + "{\"id\":1,\"name\":\"a\",\"required\":false,\"type\":\"int\"}]},"
                + "{\"type\":\"struct\",\"schema-id\":1,\"fields\":["
                + "{\"id\":1,\"name\":\"a\",\"required\":false,\"type\":\"int\"},"
                + "{\"id\":2,\"name\":\"b\",\"required\":false,\"type\":\"string\"}]},"
                + "{\"type\":\"struct\",\"schema-id\":2,\"fields\":["
                + "{\"id\":1,\"name\":\"a\",\"required\":false,\"type\":\"int\"}]}],"
                + "\"default-spec-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],"
                + "\"last-partition-id\":999,\"default-sort-order-id\":0,"
                + "\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{},"
                + "\"current-snapshot-id\":-1,\"refs\":{},\"snapshots\":[],"
                + "\"statistics\":[],\"partition-statistics\":[],"
                + "\"snapshot-log\":[],\"metadata-log\":[]}");
        Table retained = IcebergSnapshotCacheValue.retainNonGrowingGeneration(
                IcebergSnapshotCacheValue.retainTableGeneration(
                        new BaseTable(new StaticTableOperations(metadata, null), "db.tbl")));
        TableMetadata retainedMetadata = ((HasTableOperations) retained).operations().current();

        Assert.assertEquals(2, retainedMetadata.currentSchemaId());
        Assert.assertEquals(java.util.Arrays.asList(0, 1, 2), retainedMetadata.schemas().stream()
                .map(Schema::schemaId).collect(Collectors.toList()));
        Assert.assertEquals(1, retainedMetadata.schemas().stream()
                .filter(schema -> schema.schemaId() == 2).findFirst().get().columns().size());
    }

    @Test
    public void testFrozenGenerationAcceptsSnapshotCreatedBeforeV3Upgrade() {
        TableMetadata metadata = TableMetadataParser.fromJson("/metadata/upgraded-v3.json", "{"
                + "\"format-version\":3,\"table-uuid\":\"upgraded-v3-table\","
                + "\"location\":\"file:/warehouse/v3\",\"last-sequence-number\":1,"
                + "\"last-updated-ms\":2,\"last-column-id\":1,\"current-schema-id\":0,"
                + "\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
                + "{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"int\"}]}],"
                + "\"default-spec-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],"
                + "\"last-partition-id\":999,\"default-sort-order-id\":0,"
                + "\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{},"
                + "\"current-snapshot-id\":7,\"next-row-id\":0,"
                + "\"refs\":{\"main\":{\"snapshot-id\":7,\"type\":\"branch\"}},"
                + "\"snapshots\":[{\"sequence-number\":0,\"snapshot-id\":7,"
                + "\"timestamp-ms\":1,\"summary\":{\"operation\":\"append\"},"
                + "\"manifests\":[],\"schema-id\":0}],"
                + "\"statistics\":[],\"partition-statistics\":[],"
                + "\"snapshot-log\":[{\"timestamp-ms\":1,\"snapshot-id\":7}],"
                + "\"metadata-log\":[]}");
        Table retained = IcebergSnapshotCacheValue.retainNonGrowingGeneration(
                IcebergSnapshotCacheValue.retainTableGeneration(
                        new BaseTable(new StaticTableOperations(metadata, null), "db.tbl")));

        Assert.assertEquals(7L, retained.currentSnapshot().snapshotId());
        Assert.assertEquals(3, ((HasTableOperations) retained).operations().current().formatVersion());
    }

    @Test
    public void testTableSnapshotAndManifestEstimatesArePrecomputed() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Table table = tableWithMetadataLocation("/metadata/v1.json");
        IcebergTableCacheValue tableValue = new IcebergTableCacheValue(table);
        tableValue.prepareForCachePublication(mapping);
        Assert.assertTrue(tableValue.getSizeEstimate().isComplete());
        Assert.assertTrue(tableValue.getSizeEstimate().getBytes() > 0L);

        IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(mapping, table).get();
        IcebergSnapshotCacheValue snapshotValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L), Optional.empty(), table);
        snapshotValue.prepareForCachePublication(snapshotKey);
        Assert.assertTrue(snapshotValue.getSizeEstimate().getIncompleteReason(),
                snapshotValue.getSizeEstimate().isComplete());
        Assert.assertTrue(snapshotValue.getSizeEstimate().getBytes() > 0L);

        IcebergManifestEntryKey manifestKey = new IcebergManifestEntryKey("/manifest/a.avro", ManifestContent.DATA);
        ManifestCacheValue manifestValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/a.parquet").withFileSizeInBytes(10L).withRecordCount(1L).build()));
        MetaCacheSizeEstimate manifestEstimate =
                IcebergCacheSizeEstimator.estimateManifestEntry(manifestKey, manifestValue);
        Assert.assertTrue(manifestEstimate.getIncompleteReason(), manifestEstimate.isComplete());
        Assert.assertTrue(manifestEstimate.getBytes() > 0L);

        Table unsupportedTable = newInterfaceProxy(Table.class);
        MetaCacheSizeEstimate unsupported = IcebergCacheSizeEstimator.estimateTableEntry(
                mapping, new IcebergTableCacheValue(unsupportedTable));
        Assert.assertFalse(unsupported.isComplete());
        Assert.assertTrue(unsupported.getIncompleteReason().startsWith("unsupported_iceberg_table:"));
    }

    @Test
    public void testIcebergPreparationFailureIsFailClosed() {
        TableMetadata brokenMetadata = Mockito.mock(TableMetadata.class);
        Mockito.when(brokenMetadata.currentSnapshot())
                .thenThrow(new IllegalStateException("unsupported snapshot state"));
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(brokenMetadata);
        Table brokenTable = new BaseTable(operations, "db.tbl");
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");

        IcebergTableCacheValue tableValue = new IcebergTableCacheValue(brokenTable);
        MetaCacheSizeEstimate tableEstimate = tableValue.prepareForCachePublication(mapping);

        Assert.assertFalse(tableEstimate.isComplete());
        Assert.assertTrue(tableEstimate.getIncompleteReason()
                .startsWith("iceberg_table_preparation_failed:"));
        Assert.assertSame(tableValue.getRetainedIcebergTable(), tableValue.newQueryScopedTable());

        Table healthyTable = tableWithMetadataLocation("/metadata/fail-closed-key.json");
        IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(mapping, healthyTable).get();
        IcebergSnapshotCacheValue snapshotValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                Optional.empty(), brokenTable);
        MetaCacheSizeEstimate snapshotEstimate = snapshotValue.prepareForCachePublication(key);

        Assert.assertFalse(snapshotEstimate.isComplete());
        Assert.assertTrue(snapshotEstimate.getIncompleteReason()
                .startsWith("iceberg_snapshot_preparation_failed:"));
    }

    @Test
    public void testManifestAccountingAcceptsThirdPartyContentFiles() {
        DataFile copied = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/data/copied.parquet").withFileSizeInBytes(10L).withRecordCount(1L)
                .build().copy();
        ManifestCacheValue supported = ManifestCacheValue.forDataFiles(
                Collections.singletonList(copied));
        Assert.assertTrue(supported.isAccountingComplete());
        Assert.assertTrue(IcebergCacheSizeEstimator.estimateManifestEntry(
                new IcebergManifestEntryKey("/manifest/copied.avro", ManifestContent.DATA),
                supported).isComplete());

        // A proxy, mock or third-party ContentFile exposes its counts and payload through the
        // public ContentFile API; it is accounted generically instead of disabling caching.
        DataFile proxy = newInterfaceProxy(DataFile.class);
        ManifestCacheValue thirdParty = ManifestCacheValue.forDataFiles(
                Collections.singletonList(proxy));
        Assert.assertEquals(Collections.singletonList(proxy), thirdParty.getDataFiles());
        Assert.assertTrue(thirdParty.isAccountingComplete());
        Assert.assertTrue(IcebergCacheSizeEstimator.estimateManifestEntry(
                new IcebergManifestEntryKey("/manifest/proxy.avro", ManifestContent.DATA),
                thirdParty).isComplete());

        ManifestCacheValue.Builder deleteBuilder = ManifestCacheValue.deleteFilesBuilder();
        deleteBuilder.addDeleteFile(newInterfaceProxy(DeleteFile.class));
        Assert.assertTrue(deleteBuilder.build().isAccountingComplete());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testManifestAccountingFailureKeepsFilesAndRejectsWeightedAdmission() {
        Map<Integer, Long> brokenColumnSizes = Mockito.mock(Map.class);
        Mockito.when(brokenColumnSizes.size())
                .thenThrow(new IllegalStateException("new metrics representation"));
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/data/broken-metrics.parquet").withFileSizeInBytes(10L)
                .withMetrics(new Metrics(1L, brokenColumnSizes, null, null, null))
                .build();

        ManifestCacheValue value = ManifestCacheValue.forDataFiles(Collections.singletonList(file));
        MetaCacheSizeEstimate estimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                new IcebergManifestEntryKey("/manifest/fail-closed.avro", ManifestContent.DATA), value);

        Assert.assertEquals(Collections.singletonList(file), value.getDataFiles());
        Assert.assertFalse(value.isAccountingComplete());
        Assert.assertFalse(estimate.isComplete());
        Assert.assertEquals("iceberg_manifest_accounting_incomplete", estimate.getIncompleteReason());
    }

    @Test
    public void testTableEstimateAccountsForNestedSchemaAndPropertyPayload() {
        String largePayload = repeatedCharacter('x', 64 * 1024);
        Table smallTable = tableWithNestedSchemaAndProperty("x", "x");
        Table largeTable = tableWithNestedSchemaAndProperty(largePayload, largePayload);
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue smallValue = new IcebergTableCacheValue(smallTable);
        IcebergTableCacheValue largeValue = new IcebergTableCacheValue(largeTable);

        smallValue.prepareForCachePublication(mapping);
        largeValue.prepareForCachePublication(mapping);

        long expectedPayloadDelta = (MetaCacheWeightUtils.estimatedStringBytes(largePayload)
                - MetaCacheWeightUtils.estimatedStringBytes("x")) * 2L;
        Assert.assertTrue(largeValue.getSizeEstimate().getBytes()
                - smallValue.getSizeEstimate().getBytes() >= expectedPayloadDelta);
    }

    @Test
    public void testTablePayloadCountsHistoricalSchemaSpecAndSortFields() {
        List<Types.NestedField> fields = IntStream.range(0, 100)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 1, "field_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        Schema largeSchema = new Schema(0, fields);
        Schema smallSchema = new Schema(1, fields.get(0));
        TableMetadata schemaHistory = TableMetadata.newTableMetadata(
                largeSchema, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-history", Collections.emptyMap());
        schemaHistory = TableMetadata.buildFrom(schemaHistory)
                .addSchema(smallSchema)
                .setCurrentSchema(smallSchema.schemaId())
                .discardChanges()
                .build();
        TableMetadata smallSchemaOnly = TableMetadata.newTableMetadata(
                smallSchema, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-history", Collections.emptyMap());

        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(largeSchema).withSpecId(0);
        SortOrder.Builder sortBuilder = SortOrder.builderFor(largeSchema).withOrderId(1);
        for (Types.NestedField field : fields) {
            specBuilder.identity(field.name());
            sortBuilder.asc(field.name());
        }
        PartitionSpec populatedSpec = specBuilder.build();
        SortOrder populatedSortOrder = sortBuilder.build();
        TableMetadata fieldHistory = TableMetadata.newTableMetadata(
                largeSchema, populatedSpec, populatedSortOrder,
                "file:/warehouse/field-history", Collections.emptyMap());
        fieldHistory = TableMetadata.buildFrom(fieldHistory)
                .setDefaultPartitionSpec(
                        PartitionSpec.builderFor(largeSchema).withSpecId(1).build())
                .setDefaultSortOrder(SortOrder.unsorted())
                .discardChanges()
                .build();
        TableMetadata emptyFields = TableMetadata.newTableMetadata(
                largeSchema, PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                "file:/warehouse/field-history", Collections.emptyMap());
        TableMetadata partitionFields = TableMetadata.newTableMetadata(
                largeSchema, populatedSpec, SortOrder.unsorted(),
                "file:/warehouse/field-history", Collections.emptyMap());
        TableMetadata sortFields = TableMetadata.newTableMetadata(
                largeSchema, PartitionSpec.unpartitioned(), populatedSortOrder,
                "file:/warehouse/field-history", Collections.emptyMap());

        // Cache values come from Iceberg's parser. Round-trip builder fixtures so JOL measures
        // the same canonical ownership graph used in production instead of write-side builders.
        // Metadata locations of compared fixtures have equal length: that String is not part of
        // retainedTablePayloadBytes and must not leak into the JOL delta.
        schemaHistory = roundTripMetadata(schemaHistory, "/metadata/jol-schema-large.json");
        smallSchemaOnly = roundTripMetadata(smallSchemaOnly, "/metadata/jol-schema-small.json");
        fieldHistory = roundTripMetadata(fieldHistory, "/metadata/jol-fields-both.json");
        emptyFields = roundTripMetadata(emptyFields, "/metadata/jol-fields-none.json");
        partitionFields = roundTripMetadata(partitionFields, "/metadata/jol-fields-spec.json");
        sortFields = roundTripMetadata(sortFields, "/metadata/jol-fields-sort.json");

        long schemaDelta = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(schemaHistory))
                - IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        tableWithMetadata(smallSchemaOnly));
        long specAndSortDelta = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(fieldHistory))
                - IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        tableWithMetadata(emptyFields));
        long partitionFieldDelta = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(partitionFields))
                - IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        tableWithMetadata(emptyFields));
        long sortFieldDelta = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(sortFields))
                - IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        tableWithMetadata(emptyFields));

        materializeAllLazyState(schemaHistory);
        materializeAllLazyState(smallSchemaOnly);
        materializeAllLazyState(fieldHistory);
        materializeAllLazyState(emptyFields);
        materializeAllLazyState(partitionFields);
        materializeAllLazyState(sortFields);

        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg schema history", 0L, schemaDelta,
                smallSchemaOnly, schemaHistory);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg partition fields", 0L, partitionFieldDelta,
                emptyFields, partitionFields);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg sort fields", 0L, sortFieldDelta,
                emptyFields, sortFields);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg spec and sort fields", 0L, specAndSortDelta,
                emptyFields, fieldHistory);
    }

    @Test
    public void testSchemaLookupFormulaScalesWithSchemaWidth() {
        for (int fieldCount : new int[] {3, 32, 100, 1000}) {
            List<Types.NestedField> fields = IntStream.range(0, fieldCount)
                    .mapToObj(index -> Types.NestedField.optional(
                            index + 1, "field_" + index, Types.StringType.get()))
                    .collect(Collectors.toList());
            assertSchemaLookupFormula(
                    new Schema(0, fields), "schema lookup width " + fieldCount);
        }
        // Single-column schemas cannot grow; check their partition-spec graph instead.
        assertPartitionSpecFormula(new Schema(0, Types.NestedField.optional(
                1, "field_0", Types.StringType.get())), "partition spec width 1");
    }

    @Test
    public void testSchemaLookupFormulaCountsListAndMapSyntheticFields() {
        List<Types.NestedField> listFields = new ArrayList<>();
        listFields.add(Types.NestedField.optional(
                1, "identity_field", Types.IntegerType.get()));
        for (int index = 0; index < 100; index++) {
            listFields.add(Types.NestedField.optional(
                    index + 2, "list_" + index,
                    Types.ListType.ofOptional(10_000 + index, Types.StringType.get())));
        }
        assertSchemaLookupFormula(new Schema(0, listFields), "schema lookup list synthetic fields");

        List<Types.NestedField> mapFields = new ArrayList<>();
        mapFields.add(Types.NestedField.optional(
                1, "identity_field", Types.IntegerType.get()));
        for (int index = 0; index < 100; index++) {
            mapFields.add(Types.NestedField.optional(
                    index + 2, "map_" + index,
                    Types.MapType.ofOptional(
                            10_000 + index * 2, 10_001 + index * 2,
                            Types.StringType.get(), Types.LongType.get())));
        }
        assertSchemaLookupFormula(new Schema(0, mapFields), "schema lookup map synthetic fields");
    }

    @Test
    public void testSchemaLookupFormulaCountsNestedStructShortAliases() {
        List<Types.NestedField> listFields = new ArrayList<>();
        listFields.add(Types.NestedField.optional(
                1, "identity_field", Types.IntegerType.get()));
        for (int index = 0; index < 100; index++) {
            Types.StructType elementType = Types.StructType.of(Types.NestedField.optional(
                    20_000 + index, "leaf", Types.StringType.get()));
            listFields.add(Types.NestedField.optional(
                    index + 2, "list_" + index,
                    Types.ListType.ofOptional(10_000 + index, elementType)));
        }
        assertSchemaLookupFormula(
                new Schema(0, listFields), "list struct short aliases");

        List<Types.NestedField> mapFields = new ArrayList<>();
        mapFields.add(Types.NestedField.optional(
                1, "identity_field", Types.IntegerType.get()));
        for (int index = 0; index < 100; index++) {
            Types.StructType valueType = Types.StructType.of(Types.NestedField.optional(
                    30_000 + index, "leaf", Types.LongType.get()));
            mapFields.add(Types.NestedField.optional(
                    index + 2, "map_" + index,
                    Types.MapType.ofOptional(
                            10_000 + index * 2, 10_001 + index * 2,
                            Types.StringType.get(), valueType)));
        }
        assertSchemaLookupFormula(
                new Schema(0, mapFields), "map struct short aliases");
    }

    @Test
    public void testSchemaIdentifierFieldFormulaScalesWithWidth() {
        for (int fieldCount : new int[] {1, 32, 100, 1000}) {
            List<Types.NestedField> fields = IntStream.range(0, fieldCount)
                    .mapToObj(index -> Types.NestedField.required(
                            index + 1, "identifier_" + index, Types.LongType.get()))
                    .collect(Collectors.toList());
            Set<Integer> identifierIds = fields.stream()
                    .map(Types.NestedField::fieldId)
                    .collect(Collectors.toSet());
            Schema withoutIdentifiers = new Schema(0, fields);
            Schema withIdentifiers = new Schema(0, fields, identifierIds);
            TableMetadata empty = roundTripMetadata(TableMetadata.newTableMetadata(
                    withoutIdentifiers, PartitionSpec.unpartitioned(),
                    "file:/warehouse/schema-identifiers", Collections.emptyMap()),
                    "/metadata/jol-schema-identifiers-none-" + fieldCount + ".json");
            TableMetadata populated = roundTripMetadata(TableMetadata.newTableMetadata(
                    withIdentifiers, PartitionSpec.unpartitioned(),
                    "file:/warehouse/schema-identifiers", Collections.emptyMap()),
                    "/metadata/jol-schema-identifiers-with-" + fieldCount + ".json");

            long emptyEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                    tableWithMetadata(empty));
            long populatedEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                    tableWithMetadata(populated));
            materializeAllLazyState(empty);
            materializeAllLazyState(populated);
            EstimatorCalibrationAssertions.assertConservativeDelta(
                    "iceberg identifier fields " + fieldCount,
                    emptyEstimate, populatedEstimate, empty, populated);
        }
    }

    @Test
    public void testUnicodeLowerCaseSchemaFormulaAgainstJolOwnedGraph() {
        // U+0130 lower-cases to "i" plus U+0307 in Locale.ROOT: the generated lower-case index
        // keys are longer than their sources and switch the String coder to UTF-16.
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue small = tableValueWithSchemaAndProperties(
                unicodeNestedSchema(1), Collections.emptyMap());
        IcebergTableCacheValue populated = tableValueWithSchemaAndProperties(
                unicodeNestedSchema(33), Collections.emptyMap());

        long smallEstimate = small.prepareForCachePublication(mapping).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(mapping).getBytes();
        materializeAllLazyState(small);
        materializeAllLazyState(populated);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg unicode lower-case nested fields",
                smallEstimate, populatedEstimate, small, populated);
    }

    @Test
    public void testUnicodeLowerCasePartitionNameFormulaAgainstJolOwnedGraph() {
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.optional(1, DOTTED_CAPITAL_I + "dentity_Key", Types.StringType.get()));
        for (int index = 0; index < 8; index++) {
            fields.add(Types.NestedField.optional(index + 2, DOTTED_CAPITAL_I + "_field_" + index,
                    Types.StringType.get()));
        }
        // The identity partition name is lower-cased three times: by the partition StructType,
        // by the secondary Schema and by the secondary StructType.
        assertPartitionSpecFormula(new Schema(0, fields), "unicode partition name");
    }

    @Test
    public void testTableAccountingCharacterBudgetFailsClosedWithoutFailingLoad() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        String hugeName = repeatedCharacter('x', 1 << 20);
        List<Types.NestedField> fields = IntStream.range(0, 5)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 1, hugeName + index, Types.StringType.get()))
                .collect(Collectors.toList());
        IcebergTableCacheValue value = tableValueWithSchemaAndProperties(
                new Schema(0, fields), Collections.emptyMap());

        IllegalStateException budgetFailure = Assert.assertThrows(IllegalStateException.class,
                () -> IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        value.getRetainedIcebergTable()));
        Assert.assertTrue(budgetFailure.getMessage(),
                budgetFailure.getMessage().contains("character budget"));

        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(mapping);

        Assert.assertFalse(estimate.isComplete());
        Assert.assertTrue(estimate.getIncompleteReason(),
                estimate.getIncompleteReason().startsWith("iceberg_table_preparation_failed:"));
        Assert.assertNotNull(value.getRetainedIcebergTable());
        Assert.assertEquals(5, value.getRetainedIcebergTable().schema().columns().size());
        Assert.assertSame(value.getRetainedIcebergTable(), value.newQueryScopedTable());
    }

    @Test
    public void testTableAccountingElementBudgetFailsClosedWithoutFailingLoad() {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        @SuppressWarnings("unchecked")
        Map<String, String> oversizedProperties = Mockito.mock(Map.class);
        Mockito.when(oversizedProperties.size()).thenReturn(2_000_001);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(oversizedProperties);
        Table table = tableWithMetadata(metadata);

        IllegalStateException budgetFailure = Assert.assertThrows(IllegalStateException.class,
                () -> IcebergCacheSizeEstimator.retainedTablePayloadBytes(table));
        Assert.assertTrue(budgetFailure.getMessage(),
                budgetFailure.getMessage().contains("work budget"));
        Mockito.verify(oversizedProperties, Mockito.never()).entrySet();

        IcebergTableCacheValue value = new IcebergTableCacheValue(table);
        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(
                NameMapping.createForTest(1L, "db", "tbl"));
        Assert.assertFalse(estimate.isComplete());
        Assert.assertTrue(estimate.getIncompleteReason(),
                estimate.getIncompleteReason().startsWith("iceberg_table_preparation_failed:"));
        Assert.assertSame(value.getRetainedIcebergTable(), value.newQueryScopedTable());
    }

    @Test
    public void testAdmittedTableEstimateCoversFullyMaterializedRetainedGraph() {
        // Whole-entry oracle: the admitted weight must cover the complete retained graph after
        // every lazy Schema/StructType/PartitionSpec index a scan can create has materialized,
        // including the O(distinctSources * fields) fieldsBySourceId graph. Component deltas can
        // miss a shared baseline; this compares absolute sizes.
        // The tight bound applies once the variable payload dominates the fixed per-table base
        // (TABLE_BASE_BYTES); small tables are deliberately covered by that base.
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        for (int width : new int[] {1, 100, 1000}) {
            assertAdmittedEstimateCoversRetainedGraph(
                    "flat identity-partitioned " + width, mapping,
                    tableValueWithIdentityPartitionedFields(width), width >= 100);
            assertAdmittedEstimateCoversRetainedGraph(
                    "nested mixed-case " + width, mapping,
                    tableValueWithNestedMixedCaseFields(width), width >= 1000);
        }
        // The lazy name indexes retain fully qualified dotted paths: a deep chain of long
        // names is dominated by the generated ancestor path copies.
        assertAdmittedEstimateCoversRetainedGraph(
                "deep long-name chain", mapping, tableValueWithDeepLongNameFields(24), false);
    }

    private IcebergTableCacheValue tableValueWithDeepLongNameFields(int depth) {
        String longName = repeatedCharacter('n', 64);
        Types.NestedField leaf = Types.NestedField.optional(
                1000 + depth, longName + "_leaf", Types.StringType.get());
        Type type = Types.StructType.of(leaf);
        for (int level = depth - 1; level >= 1; level--) {
            type = Types.StructType.of(Types.NestedField.optional(
                    1000 + level, longName + "_" + level, type));
        }
        Schema schema = new Schema(Types.NestedField.optional(1, "root", type));
        return tableValueWithSchemaAndSpec(schema, PartitionSpec.unpartitioned());
    }

    private void assertAdmittedEstimateCoversRetainedGraph(
            String fixture, NameMapping mapping, IcebergTableCacheValue value,
            boolean requireTightBound) {
        long estimate = value.prepareForCachePublication(mapping).getBytes();
        long before = EstimatorCalibrationAssertions.graphSize(value);
        materializeAllLazyState(value);
        long after = EstimatorCalibrationAssertions.graphSize(value);
        Assert.assertTrue(fixture + " lazy state must grow the retained graph", after > before);
        Assert.assertTrue(fixture + " underestimates the materialized entry: estimate="
                + estimate + ", retained=" + after, estimate >= after);
        if (requireTightBound) {
            // Coarse weights are upward-rounded; only guard against absurd over-estimation.
            Assert.assertTrue(fixture + " is excessively conservative: estimate=" + estimate
                    + ", retained=" + after, estimate <= Math.ceil(after * 8.0D));
        }
    }

    @Test
    public void testSchemaFormulaCountsBoxedIdsOfUncachedFieldIds() {
        // TableMetadata.newTableMetadata() reassigns fresh ids from 1, which the JVM Integer
        // cache serves for free. Add the schema to existing metadata instead so ids above 127
        // survive and every lookup map really boxes its keys and values.
        List<Types.NestedField> oneFlat = Collections.singletonList(
                Types.NestedField.optional(10_000, "Field_0", Types.StringType.get()));
        List<Types.NestedField> manyFlat = IntStream.range(0, 32)
                .mapToObj(index -> Types.NestedField.optional(
                        10_000 + index, "Field_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        assertRetainedPayloadDelta("uncached flat field ids",
                metadataWithAddedSchema(new Schema(1, oneFlat)),
                metadataWithAddedSchema(new Schema(1, manyFlat)), "jol-uncached-ids");

        List<Types.NestedField> nestedFields = IntStream.range(0, 32)
                .mapToObj(index -> Types.NestedField.optional(
                        20_000 + index, "Nested_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        Schema oneNested = new Schema(1, Types.NestedField.optional(10_000, "payload",
                Types.StructType.of(nestedFields.get(0))));
        Schema manyNested = new Schema(1, Types.NestedField.optional(10_000, "payload",
                Types.StructType.of(nestedFields)));
        assertRetainedPayloadDelta("uncached nested field ids",
                metadataWithAddedSchema(oneNested), metadataWithAddedSchema(manyNested),
                "jol-uncached-ids");
    }

    private TableMetadata metadataWithAddedSchema(Schema schema) {
        return metadataWithAddedSchema(schema, 2);
    }

    private TableMetadata metadataWithAddedSchema(Schema schema, int formatVersion) {
        Schema base = new Schema(0, Types.NestedField.optional(1, "base", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(
                base, PartitionSpec.unpartitioned(), SortOrder.unsorted(), "file:/warehouse/uncached-ids",
                Collections.singletonMap(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion)));
        return TableMetadata.buildFrom(metadata).addSchema(schema)
                .setCurrentSchema(schema.schemaId()).discardChanges().build();
    }

    @Test
    public void testSchemaFormulaCountsFieldDefaultLiterals() {
        // v3 field defaults are retained as Literal wrappers around boxed or String values.
        assertRetainedPayloadDelta("defaulted fields",
                metadataWithAddedSchema(new Schema(1, defaultedFields(1)), 3),
                metadataWithAddedSchema(new Schema(1, defaultedFields(32)), 3), "jol-defaults");
    }

    private List<Types.NestedField> defaultedFields(int fieldCount) {
        List<Types.NestedField> fields = new ArrayList<>();
        for (int index = 0; index < fieldCount; index++) {
            fields.add(Types.NestedField.optional("text_" + index).withId(10_000 + index)
                    .ofType(Types.StringType.get())
                    .withInitialDefault(Expressions.lit("initial_" + index))
                    .withWriteDefault(Expressions.lit("write_" + index))
                    .build());
            fields.add(Types.NestedField.optional("number_" + index).withId(20_000 + index)
                    .ofType(Types.LongType.get())
                    .withInitialDefault(Expressions.lit(100_000L + index))
                    .withWriteDefault(Expressions.lit(200_000L + index))
                    .build());
        }
        return fields;
    }

    @Test
    public void testTablePayloadAccountsForRetainedHistoricalMetadata() {
        String largePayload = repeatedCharacter('x', 64 * 1024);
        long smallBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithMaterializedPayload("x", 32)));
        long largeBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithMaterializedPayload(largePayload, 64 * 1024)));

        Assert.assertTrue(largeBytes - smallBytes >= 64L * 1024L - 32L);
    }

    @Test
    public void testStatisticsBlobFormulaAgainstJolOwnedGraph() {
        GenericStatisticsFile empty = new GenericStatisticsFile(
                1L, "/stats/file.puffin", 1L, 1L, Collections.emptyList());
        List<org.apache.iceberg.BlobMetadata> blobs = IntStream.range(0, 32)
                .mapToObj(index -> new GenericBlobMetadata(
                        "blob-type-" + index,
                        1L,
                        1L,
                        java.util.Arrays.asList(10_000 + index, 20_000 + index),
                        Collections.singletonMap(
                                "property-" + index, "value-" + index)))
                .collect(Collectors.toList());
        GenericStatisticsFile populated = new GenericStatisticsFile(
                1L, "/stats/file.puffin", 1L, 1L, blobs);
        TableMetadata emptyMetadata = metadataWithStatisticsFile(empty);
        TableMetadata populatedMetadata = metadataWithStatisticsFile(populated);

        long emptyEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(emptyMetadata));
        long populatedEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(populatedMetadata));

        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg statistics blobs", emptyEstimate, populatedEstimate, empty, populated);

        // With a fixed blob count, growing only the property count must charge the per-entry
        // map structure, not just the property strings.
        GenericStatisticsFile fewProperties = statisticsFileWithProperties(4);
        GenericStatisticsFile manyProperties = statisticsFileWithProperties(256);
        long fewEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithStatisticsFile(fewProperties)));
        long manyEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithStatisticsFile(manyProperties)));
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg blob properties", fewEstimate, manyEstimate,
                fewProperties, manyProperties);
    }

    private GenericStatisticsFile statisticsFileWithProperties(int propertyCount) {
        Map<String, String> properties = new HashMap<>();
        for (int index = 0; index < propertyCount; index++) {
            properties.put("p" + index, "v" + index);
        }
        return new GenericStatisticsFile(1L, "/stats/file.puffin", 1L, 1L,
                Collections.singletonList(new GenericBlobMetadata(
                        "blob-type", 1L, 1L, Collections.singletonList(1), properties)));
    }

    @Test
    public void testTableEstimateAccountsForRetainedBranchHistory() {
        TableMetadata oneCommit = metadataWithSnapshotSequence(1L);
        TableMetadata tenThousandCommits = metadataWithSnapshotSequence(10_000L);
        IcebergTableCacheValue smallValue = new IcebergTableCacheValue(tableWithMetadata(oneCommit));
        IcebergTableCacheValue largeValue = new IcebergTableCacheValue(
                tableWithMetadata(tenThousandCommits));
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");

        smallValue.prepareForCachePublication(mapping);
        largeValue.prepareForCachePublication(mapping);

        Assert.assertTrue(smallValue.getSizeEstimate().getIncompleteReason(),
                smallValue.getSizeEstimate().isComplete());
        Assert.assertTrue(largeValue.getSizeEstimate().getIncompleteReason(),
                largeValue.getSizeEstimate().isComplete());
        Assert.assertEquals(smallValue.getSizeEstimate().getBytes(),
                largeValue.getSizeEstimate().getBytes());
        Mockito.verify(oneCommit, Mockito.atLeastOnce()).snapshots();
        Mockito.verify(tenThousandCommits, Mockito.atLeastOnce()).snapshots();
        Mockito.verify(oneCommit, Mockito.never()).lastSequenceNumber();
        Mockito.verify(tenThousandCommits, Mockito.never()).lastSequenceNumber();
        Mockito.verify(oneCommit, Mockito.atLeastOnce()).snapshotLog();
        Mockito.verify(tenThousandCommits, Mockito.atLeastOnce()).snapshotLog();
        Mockito.verify(oneCommit, Mockito.atLeastOnce()).refs();
        Mockito.verify(tenThousandCommits, Mockito.atLeastOnce()).refs();
    }

    @Test
    public void testWeightedTablePreparationRunsInsideCatalogAuthenticator() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean authenticated = new AtomicBoolean();
        AtomicBoolean firstPreparation = new AtomicBoolean(true);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Table table = tableWithMetadataLocation("/metadata/authenticated-v1.json");
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(metadataOps.loadTable("remote_db", "remote_tbl")).thenReturn(table);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                Assert.assertTrue(authenticated.compareAndSet(false, true));
                try {
                    return task.call();
                } finally {
                    authenticated.set(false);
                }
            }
        });
        stubTableLoadContext(catalog);
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }

            @Override
            MetaCacheSizeEstimate prepareTableForCachePublication(
                    NameMapping nameMapping, IcebergTableCacheValue value) {
                if (firstPreparation.compareAndSet(true, false)) {
                    Assert.assertTrue("publication preparation must retain Kerberos scope",
                            authenticated.get());
                }
                return super.prepareTableForCachePublication(nameMapping, value);
            }
        };
        try {
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = new NameMapping(
                    1L, "db", "tbl", "remote_db", "remote_tbl");

            IcebergTableCacheValue value = cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).get(mapping);
            IcebergTableCacheValue cached = cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).get(mapping);

            Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                    value.getSizeEstimate().isComplete());
            Assert.assertSame(value, cached);
            Mockito.verify(metadataOps, Mockito.times(1)).loadTable("remote_db", "remote_tbl");
            Assert.assertFalse(authenticated.get());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testQueryScopedMetadataReusesFrozenGenerationWithoutFileIo() throws Exception {
        String tableLocation = temporaryFolder.newFolder("authenticated-metadata").toURI().toString();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table liveTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), tableLocation);
        AtomicInteger metadataReads = new AtomicInteger();
        FileIO trackingFileIO = Mockito.mock(FileIO.class);
        Mockito.when(trackingFileIO.newInputFile(Mockito.anyString())).thenAnswer(invocation -> {
            metadataReads.incrementAndGet();
            return liveTable.io().newInputFile((String) invocation.getArgument(0));
        });
        TableMetadata metadata = ((HasTableOperations) liveTable).operations().current();
        Table trackedTable = new BaseTable(
                new StaticTableOperations(metadata, trackingFileIO), liveTable.name());
        IcebergTableCacheValue countValue = new IcebergTableCacheValue(trackedTable);
        countValue.getWritableIcebergTable(liveTable);
        Assert.assertEquals("count-based writes must not add metadata FileIO", 0, metadataReads.get());
        IcebergTableCacheValue value = new IcebergTableCacheValue(trackedTable);
        value.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));

        Table statementTable = value.newQueryScopedTable();
        IcebergSnapshotCacheValue.loadQueryMetadataForStatement(statementTable);
        IcebergSnapshotCacheValue statementValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                Optional.empty(), statementTable);
        Assert.assertEquals(statementTable.schema().asStruct(),
                statementValue.getIcebergTable().get().schema().asStruct());
        com.google.common.collect.Lists.newArrayList(
                statementValue.getIcebergTable().get().snapshots());
        Assert.assertEquals("statement handoff must reuse frozen metadata", 0, metadataReads.get());
        value.getWritableIcebergTable(liveTable);

        Assert.assertEquals(0, metadataReads.get());
    }

    @Test
    public void testCountModeTimeTravelDoesNotEnableQueryIsolation() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            cache.initCatalog(1L, Collections.emptyMap());
            NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
            IcebergTableCacheValue value = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/count-v1.json"));
            cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).put(mapping, value);
            ExternalTable table = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(table.getOrBuildNameMapping()).thenReturn(mapping);

            Table queryTable = cache.getQueryScopedIcebergTable(table);

            Assert.assertSame(value.getRetainedIcebergTable(), queryTable);
            Assert.assertFalse(value.isQueryIsolationPrepared());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTimeTravelGenerationBundleDoesNotMixReplacedTableValue() throws Exception {
        String firstLocation = temporaryFolder.newFolder("bundle-first").toURI().toString();
        String secondLocation = temporaryFolder.newFolder("bundle-second").toURI().toString();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table firstTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), firstLocation);
        firstTable.newAppend().appendFile(DataFiles.builder(firstTable.spec())
                .withPath(firstLocation + "/data/a.parquet")
                .withFileSizeInBytes(10L).withRecordCount(1L).build()).commit();
        Table secondTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), secondLocation);
        secondTable.newAppend().appendFile(DataFiles.builder(secondTable.spec())
                .withPath(secondLocation + "/data/b.parquet")
                .withFileSizeInBytes(20L).withRecordCount(2L).build()).commit();
        long firstSnapshotId = firstTable.currentSnapshot().snapshotId();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> entry = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);
            entry.put(mapping, new IcebergTableCacheValue(firstTable));
            ExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);

            Table queryTable = cache.getQueryScopedIcebergTable(dorisTable);
            entry.put(mapping, new IcebergTableCacheValue(secondTable));

            Assert.assertEquals(firstSnapshotId,
                    queryTable.currentSnapshot().snapshotId());
            Assert.assertEquals(firstTable.location(),
                    queryTable.location());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testPinnedGenerationSurvivesMetadataFileRetirement() throws Exception {
        String staleLocation = temporaryFolder.newFolder("stale-metadata").toURI().toString();
        String freshLocation = temporaryFolder.newFolder("fresh-metadata").toURI().toString();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table staleTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), staleLocation);
        Table freshTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), freshLocation);
        String staleMetadataLocation = ((HasTableOperations) staleTable)
                .operations().current().metadataFileLocation();
        IcebergTableCacheValue staleValue = new IcebergTableCacheValue(staleTable);
        staleValue.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));
        staleTable.io().deleteFile(staleMetadataLocation);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        stubTableLoadContext(catalog);
        Mockito.when(metadataOps.loadTable("db", "tbl")).thenReturn(freshTable);
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }
        };
        try {
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
            cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).put(mapping, staleValue);
            ExternalTable table = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(table.getOrBuildNameMapping()).thenReturn(mapping);

            Table queryTable = cache.getQueryScopedIcebergTable(table);

            Assert.assertEquals(staleTable.schema().asStruct(), queryTable.schema().asStruct());
            Mockito.verify(metadataOps, Mockito.never()).loadTable("db", "tbl");
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testWeightedV1TablePublicationFailsClosedWithoutIo() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        Snapshot currentSnapshot = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":2,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifests\":[\"/manifest/current-a.avro\","
                + "\"/manifest/current-b.avro\"],\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata)
                .setBranchSnapshot(currentSnapshot, SnapshotRef.MAIN_BRANCH)
                .discardChanges().withMetadataLocation("/metadata/v1.json").build();
        FileIO fileIO = Mockito.mock(FileIO.class);
        Mockito.when(fileIO.newInputFile(Mockito.anyString())).thenAnswer(invocation -> {
            InputFile inputFile = Mockito.mock(InputFile.class);
            Mockito.when(inputFile.location()).thenReturn(invocation.getArgument(0));
            return inputFile;
        });
        Table liveTable = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.tbl");
        IcebergTableCacheValue value = new IcebergTableCacheValue(liveTable);

        value.prepareForCachePublication(mapping);

        Assert.assertFalse(value.getSizeEstimate().isComplete());
        Table retained = value.getRetainedIcebergTable();
        Assert.assertTrue(IcebergSnapshotCacheValue.isFrozenGeneration(retained));
        Mockito.verifyNoInteractions(fileIO);
        retained.currentSnapshot().allManifests(fileIO);
        Mockito.verify(fileIO, Mockito.atLeastOnce()).newInputFile("/manifest/current-a.avro");
        Mockito.verify(fileIO, Mockito.atLeastOnce()).newInputFile("/manifest/current-b.avro");
    }

    @Test
    public void testWeightedV2ManifestListMaterializesOnlyInQueryView() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        String tableLocation = temporaryFolder.newFolder("v2-table").toURI().toString();
        Table liveTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), tableLocation);
        liveTable.newAppend().appendFile(
                DataFiles.builder(liveTable.spec())
                        .withPath(tableLocation + "/data/a.parquet")
                        .withFileSizeInBytes(10L)
                        .withRecordCount(1L)
                        .build()).commit();
        Assert.assertNotNull(liveTable.currentSnapshot().manifestListLocation());
        IcebergTableCacheValue value = new IcebergTableCacheValue(liveTable);

        value.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                value.getSizeEstimate().isComplete());
        Table retained = value.getRetainedIcebergTable();
        Table firstQuery = value.getIcebergTable();
        Table secondQuery = value.getIcebergTable();
        List<ManifestFile> firstManifests =
                firstQuery.currentSnapshot().dataManifests(firstQuery.io());
        List<ManifestFile> secondManifests =
                secondQuery.currentSnapshot().dataManifests(secondQuery.io());
        Assert.assertEquals(1, firstManifests.size());
        Assert.assertEquals(1, secondManifests.size());
        Assert.assertNotSame(firstQuery.currentSnapshot(), secondQuery.currentSnapshot());
        Assert.assertNotSame(firstManifests, secondManifests);
        Assert.assertNotSame(retained.currentSnapshot(), firstQuery.currentSnapshot());

        // A time-travel projection built from the query-scoped view keeps that isolation: a
        // historical manifest-list read must not touch the cached generation's snapshots.
        long snapshotId = liveTable.currentSnapshot().snapshotId();
        IcebergSnapshotCacheValue historical = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(snapshotId, 0L),
                Optional.empty(), value.getIcebergTable());
        Table historicalTable = historical.getIcebergTable().get();
        Assert.assertEquals(1, historicalTable.snapshot(snapshotId).dataManifests(historicalTable.io()).size());
        Assert.assertNotSame(retained.snapshot(snapshotId), historicalTable.snapshot(snapshotId));
        Snapshot cachedSnapshot = retained.snapshot(snapshotId);
        for (Field retainedField : cachedSnapshot.getClass().getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isTransient(retainedField.getModifiers())
                    && !retainedField.getType().isPrimitive()) {
                retainedField.setAccessible(true);
                Assert.assertNull(retainedField.getName() + " must stay unmaterialized in the cache",
                        retainedField.get(cachedSnapshot));
            }
        }
    }

    @Test
    public void testTablePublicationDoesNotReadHistoricalManifestLists() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        Snapshot historical = SnapshotParser.fromJson("{\"snapshot-id\":6,\"timestamp-ms\":1,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"/manifest-list/history.avro\",\"schema-id\":0}");
        Snapshot current = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":2,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"/manifest-list/current.avro\",\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata)
                .addSnapshot(historical)
                .setBranchSnapshot(current, SnapshotRef.MAIN_BRANCH)
                .discardChanges().withMetadataLocation("/metadata/v2.json").build();
        FileIO fileIO = Mockito.mock(FileIO.class);
        IcebergTableCacheValue value = new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, fileIO), "db.tbl"));

        value.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(), value.getSizeEstimate().isComplete());
        Mockito.verify(fileIO, Mockito.never()).newInputFile("/manifest-list/history.avro");
    }

    @Test
    public void testManifestEstimateScalesWithFileCount() {
        ManifestCacheValue oneFile = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/one.parquet").withFileSizeInBytes(10L).withRecordCount(1L).build()));
        ManifestCacheValue twoFiles = ManifestCacheValue.forDataFiles(java.util.Arrays.asList(
                oneFile.getDataFiles().get(0), oneFile.getDataFiles().get(0)));
        IcebergManifestEntryKey key = new IcebergManifestEntryKey("/manifest/data.avro", ManifestContent.DATA);

        long oneFileBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, oneFile).getBytes();
        long twoFileBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, twoFiles).getBytes();

        Assert.assertTrue(twoFileBytes > oneFileBytes);
    }

    @Test
    public void testManifestFormulaAgainstJolOwnedGraph() {
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/jol.avro", ManifestContent.DATA);
        ManifestCacheValue empty = ManifestCacheValue.forDataFiles(Collections.emptyList());
        ManifestCacheValue populated = ManifestCacheValue.forDataFiles(
                IntStream.range(0, 32).mapToObj(this::dataFileWithMetrics)
                        .collect(Collectors.toList()));
        ManifestCacheValue shortTail = ManifestCacheValue.forDataFiles(
                Collections.singletonList(dataFileWithPathPayload(16)));
        ManifestCacheValue longTail = ManifestCacheValue.forDataFiles(
                Collections.singletonList(dataFileWithPathPayload(4096)));
        ManifestCacheValue emptyDeletes = ManifestCacheValue.forDeleteFiles(Collections.emptyList());
        ManifestCacheValue populatedDeletes = ManifestCacheValue.forDeleteFiles(
                IntStream.range(0, 32).mapToObj(this::deleteFileWithMetrics)
                        .collect(Collectors.toList()));

        long emptyEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, empty).getBytes();
        long populatedEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, populated).getBytes();
        long shortTailEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, shortTail).getBytes();
        long longTailEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, longTail).getBytes();
        IcebergManifestEntryKey deleteKey = new IcebergManifestEntryKey(
                "/manifest/jol-delete.avro", ManifestContent.DELETES);
        long emptyDeleteEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                deleteKey, emptyDeletes).getBytes();
        long populatedDeleteEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                deleteKey, populatedDeletes).getBytes();

        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg manifest files", emptyEstimate, populatedEstimate, empty, populated);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg long-tail path", shortTailEstimate, longTailEstimate, shortTail, longTail);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg manifest delete files", emptyDeleteEstimate, populatedDeleteEstimate,
                emptyDeletes, populatedDeletes);
    }

    @Test
    public void testManifestPartitionDataFormulaAgainstJolOwnedGraph() {
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/jol-partitioned.avro", ManifestContent.DATA);
        for (int fieldCount : new int[] {1, 8, 32, 100}) {
            ManifestCacheValue unpartitioned = ManifestCacheValue.forDataFiles(
                    manifestFilesWithIntegerPartitions(32, 0));
            ManifestCacheValue partitioned = ManifestCacheValue.forDataFiles(
                    manifestFilesWithIntegerPartitions(32, fieldCount));
            long unpartitionedEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                    key, unpartitioned).getBytes();
            long partitionedEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                    key, partitioned).getBytes();

            EstimatorCalibrationAssertions.assertConservativeDelta(
                    "iceberg manifest partition fields " + fieldCount,
                    unpartitionedEstimate, partitionedEstimate,
                    unpartitioned, partitioned);
        }
    }

    @Test
    public void testManifestVariablePartitionValuesAgainstJolOwnedGraph() {
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/jol-variable-partition.avro", ManifestContent.DATA);
        ManifestCacheValue unpartitioned = ManifestCacheValue.forDataFiles(
                manifestFilesWithIntegerPartitions(32, 0));
        long unpartitionedEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                key, unpartitioned).getBytes();

        ManifestCacheValue strings = ManifestCacheValue.forDataFiles(
                manifestFilesWithPartitions(32, 8, Types.StringType.get(),
                        (fileIndex, fieldIndex) -> repeatedCharacter('s', 64)
                                + fileIndex + "_" + fieldIndex));
        long stringEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                key, strings).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg manifest string partitions",
                unpartitionedEstimate, stringEstimate, unpartitioned, strings);

        ManifestCacheValue binary = ManifestCacheValue.forDataFiles(
                manifestFilesWithPartitions(32, 8, Types.BinaryType.get(),
                        (fileIndex, fieldIndex) -> ByteBuffer.allocate(64)));
        long binaryEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(
                key, binary).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg manifest binary partitions",
                unpartitionedEstimate, binaryEstimate, unpartitioned, binary);
    }

    @Test
    public void testManifestAccountingFindsPathTailAtAnyPosition() {
        List<DataFile> baselineFiles = new ArrayList<>();
        List<DataFile> tailFiles = new ArrayList<>();
        String largePath = "/data/" + repeatedCharacter('x', 64 * 1024) + ".parquet";
        for (int index = 0; index < 101; index++) {
            baselineFiles.add(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("/data/file-" + index + ".parquet")
                    .withFileSizeInBytes(10L).withRecordCount(1L).build());
            tailFiles.add(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(index == 57 ? largePath : "/data/file-" + index + ".parquet")
                    .withFileSizeInBytes(10L).withRecordCount(1L).build());
        }
        ManifestCacheValue baseline = ManifestCacheValue.forDataFiles(baselineFiles);
        ManifestCacheValue withTail = ManifestCacheValue.forDataFiles(tailFiles);
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/path-tail.avro", ManifestContent.DATA);

        long baselineBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, baseline).getBytes();
        long tailBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, withTail).getBytes();

        Assert.assertTrue(tailBytes - baselineBytes >= largePath.length() - 32L);
        Assert.assertTrue(tailBytes - baselineBytes < largePath.length() * 2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testManifestAccountingFailsClosedBeyondWorkBudget() {
        Map<Integer, ByteBuffer> oversizedBounds = Mockito.mock(Map.class);
        Mockito.when(oversizedBounds.size()).thenReturn(8_000_001);
        // GenericDataFile wraps the bounds map without copying it, so the oversized size is
        // observed by accounting without allocating the entries.
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/data/oversized-bounds.parquet").withFileSizeInBytes(10L)
                .withMetrics(new Metrics(1L, null, null, null, null, oversizedBounds, null))
                .build();

        ManifestCacheValue value = ManifestCacheValue.forDataFiles(
                Collections.singletonList(file));

        Assert.assertFalse(value.isAccountingComplete());
        Assert.assertFalse(IcebergCacheSizeEstimator.estimateManifestEntry(
                new IcebergManifestEntryKey("/manifest/oversized.avro", ManifestContent.DATA),
                value).isComplete());
    }

    @Test
    public void testManifestAccountingChargesUnknownPartitionValuesGenerically() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        DataFile file = DataFiles.builder(spec)
                .withPath("/data/unreadable-partition.parquet").withFileSizeInBytes(10L)
                .withRecordCount(1L).withPartitionPath("id=1").build();
        // A partition value of a class the accounting does not know gets a generic weight.
        ((PartitionData) file.partition()).set(0, new Object());

        ManifestCacheValue value = ManifestCacheValue.forDataFiles(
                Collections.singletonList(file));

        Assert.assertTrue(value.isAccountingComplete());
        Assert.assertTrue(value.getRetainedPayloadBytes() > 0L);
    }

    @Test
    public void testTableAndSnapshotFormulasAgainstJolOwnedGraphs() throws Exception {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        // Compare two non-empty schemas: an empty schema never materializes any lookup index,
        // so it is not a fair baseline for the per-field formula.
        IcebergTableCacheValue emptyTable = tableValueWithFields(1);
        IcebergTableCacheValue populatedTable = tableValueWithFields(32);
        long emptyTableEstimate = emptyTable.prepareForCachePublication(mapping).getBytes();
        long populatedTableEstimate = populatedTable.prepareForCachePublication(mapping).getBytes();
        materializeAllLazyState(emptyTable);
        materializeAllLazyState(populatedTable);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg table fields", emptyTableEstimate, populatedTableEstimate,
                emptyTable, populatedTable);

        Table keyTable = tableWithMetadataLocation("/metadata/jol-snapshot-v1.json");
        IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(mapping, keyTable).get();
        IcebergSnapshotCacheValue emptySnapshot = new IcebergSnapshotCacheValue(
                realPartitionInfo(0), new IcebergSnapshot(-1L, 0L));
        IcebergSnapshotCacheValue populatedSnapshot = new IcebergSnapshotCacheValue(
                realPartitionInfo(32), new IcebergSnapshot(-1L, 0L));
        long emptySnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, emptySnapshot).getBytes();
        long populatedSnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, populatedSnapshot).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg snapshot partitions", emptySnapshotEstimate, populatedSnapshotEstimate,
                emptySnapshot, populatedSnapshot);

        // A spec that widened after the related-table check retains one more literal per range
        // endpoint and one more value/transform per partition; the projection charges the width
        // it actually loaded instead of the single field the check assumed.
        // (Both fixtures already carry the shared MIN-literal type singleton of the second column.)
        IcebergSnapshotCacheValue twoColumnSnapshot = new IcebergSnapshotCacheValue(
                realPartitionInfo(32, 2), new IcebergSnapshot(-1L, 0L));
        IcebergSnapshotCacheValue wideSnapshot = new IcebergSnapshotCacheValue(
                realPartitionInfo(32, 3), new IcebergSnapshot(-1L, 0L));
        long twoColumnSnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, twoColumnSnapshot).getBytes();
        long wideSnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, wideSnapshot).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg wide snapshot partitions", twoColumnSnapshotEstimate, wideSnapshotEstimate,
                twoColumnSnapshot, wideSnapshot);

        // Overlapping physical partitions merge into one Doris partition that keeps every
        // enclosed name in a HashSet; the weight follows the set cardinality, not the group count.
        IcebergSnapshotCacheValue aliasedSnapshot = new IcebergSnapshotCacheValue(
                realPartitionInfo(32, 1, true), new IcebergSnapshot(-1L, 0L));
        long aliasedSnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, aliasedSnapshot).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg partition aliases", populatedSnapshotEstimate, aliasedSnapshotEstimate,
                populatedSnapshot, aliasedSnapshot);

        // When mergeOverlapPartitions() really drops the enclosed day ranges, only the surviving
        // Doris partition keeps range endpoints: widening the spec must charge the extra
        // endpoint columns once, not once per enclosed physical partition.
        IcebergSnapshotCacheValue mergedSnapshot = new IcebergSnapshotCacheValue(
                mergedPartitionInfo(32, 2), new IcebergSnapshot(-1L, 0L));
        IcebergSnapshotCacheValue mergedWideSnapshot = new IcebergSnapshotCacheValue(
                mergedPartitionInfo(32, 3), new IcebergSnapshot(-1L, 0L));
        Assert.assertEquals(1, mergedWideSnapshot.getPartitionInfo().getNameToPartitionItem().size());
        Assert.assertEquals(32, mergedWideSnapshot.getPartitionInfo().getNameToIcebergPartition().size());
        long mergedSnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, mergedSnapshot).getBytes();
        long mergedWideSnapshotEstimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(
                key, mergedWideSnapshot).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg merged wide partitions", mergedSnapshotEstimate, mergedWideSnapshotEstimate,
                mergedSnapshot, mergedWideSnapshot);

        // A name mapping retains an element array per field once it has several historical names.
        IcebergSnapshotCacheValue singleNames = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                Optional.of(nameMappingWithAliases(32, 1)));
        IcebergSnapshotCacheValue manyNames = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                Optional.of(nameMappingWithAliases(32, 8)));
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg name mapping aliases",
                IcebergCacheSizeEstimator.estimateSnapshotEntry(key, singleNames).getBytes(),
                IcebergCacheSizeEstimator.estimateSnapshotEntry(key, manyNames).getBytes(),
                singleNames, manyNames);
    }

    private Map<Integer, List<String>> nameMappingWithAliases(int fieldCount, int aliasesPerField) {
        Map<Integer, List<String>> mapping = new java.util.HashMap<>();
        for (int field = 0; field < fieldCount; field++) {
            List<String> names = new ArrayList<>();
            for (int alias = 0; alias < aliasesPerField; alias++) {
                names.add("field_" + field + "_v" + alias);
            }
            mapping.put(1000 + field, names);
        }
        return mapping;
    }

    @Test
    public void testNestedSchemaFormulaAgainstJolOwnedGraph() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue small = tableValueWithNestedFields(1);
        IcebergTableCacheValue populated = tableValueWithNestedFields(33);

        long smallEstimate = small.prepareForCachePublication(mapping).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(mapping).getBytes();
        materializeAllLazyState(small);
        materializeAllLazyState(populated);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg nested fields", smallEstimate, populatedEstimate, small, populated);
    }

    @Test
    public void testTablePropertyFormulaAgainstJolOwnedGraph() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue empty = tableValueWithProperties(0);
        IcebergTableCacheValue populated = tableValueWithProperties(32);

        long emptyEstimate = empty.prepareForCachePublication(mapping).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(mapping).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg table properties", emptyEstimate, populatedEstimate, empty, populated);
    }

    @Test
    public void testSnapshotHistoryFormulaAgainstJolOwnedGraph() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue small = tableValueWithSnapshotHistory(1, false);
        IcebergTableCacheValue populated = tableValueWithSnapshotHistory(33, false);
        long smallEstimate = small.prepareForCachePublication(mapping).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(mapping).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg snapshot history", smallEstimate, populatedEstimate, small, populated);

        IcebergTableCacheValue withoutLog = tableValueWithSnapshotHistory(33, false);
        IcebergTableCacheValue withLog = tableValueWithSnapshotHistory(33, true);
        long withoutLogEstimate = withoutLog.prepareForCachePublication(mapping).getBytes();
        long withLogEstimate = withLog.prepareForCachePublication(mapping).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg snapshot log", withoutLogEstimate, withLogEstimate, withoutLog, withLog);
    }

    @Test
    public void testV1SnapshotAccountingFailsClosedWithoutIo() {
        Snapshot snapshot = SnapshotParser.fromJson(
                "{\"snapshot-id\":1,\"timestamp-ms\":1,"
                        + "\"manifests\":[\"/manifest/v1-a.avro\",\"/manifest/v1-b.avro\"]}");
        IcebergTableCacheValue value = new IcebergTableCacheValue(
                tableWithMetadata(metadataWithSnapshots(snapshot)));

        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(
                NameMapping.createForTest(1L, "db", "tbl"));

        Assert.assertFalse(estimate.isComplete());
    }

    @Test
    public void testSnapshotWithoutSummaryRemainsCacheable() {
        Snapshot snapshot = SnapshotParser.fromJson(
                "{\"snapshot-id\":1,\"timestamp-ms\":1,"
                        + "\"manifest-list\":\"/manifest/list.avro\"}");
        IcebergTableCacheValue value = new IcebergTableCacheValue(
                tableWithMetadata(metadataWithSnapshots(snapshot)));

        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(
                NameMapping.createForTest(1L, "db", "tbl"));

        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
    }


    @Test
    public void testSnapshotKeyIdPayloadIsAccounted() {
        String longKeyId = repeatedCharacter('k', 64 * 1024);
        Snapshot shortSnapshot = SnapshotParser.fromJson(
                "{\"snapshot-id\":1,\"timestamp-ms\":1,"
                        + "\"manifest-list\":\"/manifest/list.avro\",\"key-id\":\"k\"}");
        Snapshot longSnapshot = SnapshotParser.fromJson(
                "{\"snapshot-id\":1,\"timestamp-ms\":1,"
                        + "\"manifest-list\":\"/manifest/list.avro\",\"key-id\":\""
                        + longKeyId + "\"}");

        long shortBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithSnapshots(shortSnapshot)));
        long longBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithSnapshots(longSnapshot)));

        Assert.assertTrue(longBytes - shortBytes >= longKeyId.length() - 8L);
    }

    @Test
    public void testManifestEstimateAccountsForSkewedFilePaths() {
        String largePath = "/data/" + repeatedCharacter('x', 64 * 1024) + ".parquet";
        ManifestCacheValue smallValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/x.parquet")
                        .withFileSizeInBytes(10L)
                        .withRecordCount(1L)
                        .build()));
        ManifestCacheValue largeValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(largePath)
                        .withFileSizeInBytes(10L)
                        .withRecordCount(1L)
                        .build()));
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/path-skew.avro", ManifestContent.DATA);

        long smallBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, smallValue).getBytes();
        long largeBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, largeValue).getBytes();

        Assert.assertTrue(largeBytes - smallBytes >= largePath.length() - "/data/x.parquet".length());
    }

    @Test
    public void testManifestEstimateAccountsForSkewedBufferPayload() {
        Metrics smallMetrics = new Metrics(1L, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.singletonMap(1, ByteBuffer.allocateDirect(32)), Collections.emptyMap());
        Metrics largeMetrics = new Metrics(1L, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.singletonMap(1, ByteBuffer.allocateDirect(64 * 1024)), Collections.emptyMap());
        ManifestCacheValue smallValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/encrypted.parquet")
                        .withFileSizeInBytes(10L)
                        .withMetrics(smallMetrics)
                        .build()));
        ManifestCacheValue largeValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/encrypted.parquet")
                        .withFileSizeInBytes(10L)
                        .withMetrics(largeMetrics)
                        .build()));

        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/encrypted.avro", ManifestContent.DATA);
        MetaCacheSizeEstimate smallEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, smallValue);
        MetaCacheSizeEstimate largeEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, largeValue);

        Assert.assertTrue(smallEstimate.getIncompleteReason(), smallEstimate.isComplete());
        Assert.assertTrue(largeEstimate.getIncompleteReason(), largeEstimate.isComplete());
        Assert.assertEquals(1L, smallValue.getDataFileMetricEntryCount());
        Assert.assertEquals(1L, largeValue.getDataFileMetricEntryCount());
        Assert.assertTrue(largeEstimate.getBytes() - smallEstimate.getBytes() >= 64 * 1024 - 32);
    }

    @Test
    public void testManifestEstimateAccountsForDeleteFileAuxiliaryPayload() {
        String largeReference = "/data/" + repeatedCharacter('x', 64 * 1024) + ".parquet";
        List<Long> largeOffsets = IntStream.range(0, 4096)
                .mapToObj(index -> (long) index).collect(Collectors.toList());
        DeleteFile smallPositionDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/delete/position.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .withReferencedDataFile("/data/x.parquet")
                .withSplitOffsets(Collections.singletonList(0L))
                .build();
        DeleteFile largePositionDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/delete/position.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .withReferencedDataFile(largeReference)
                .withSplitOffsets(largeOffsets)
                .build();
        int[] largeEqualityIds = IntStream.range(0, 4096).toArray();
        DeleteFile smallEqualityDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(1)
                .withPath("/delete/equality.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .build();
        DeleteFile largeEqualityDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(largeEqualityIds)
                .withPath("/delete/equality.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .build();
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/delete.avro", ManifestContent.DELETES);

        long smallPositionBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(smallPositionDelete))).getBytes();
        long largePositionBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(largePositionDelete))).getBytes();
        long smallEqualityBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(smallEqualityDelete))).getBytes();
        long largeEqualityBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(largeEqualityDelete))).getBytes();

        Assert.assertTrue(largePositionBytes > smallPositionBytes);
        Assert.assertTrue(largeEqualityBytes > smallEqualityBytes);
    }

    @Test
    public void testV1SnapshotPublicationDoesNotPolluteManifestIo() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        Snapshot snapshot = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":1,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifests\":[\"/manifest/a.avro\",\"/manifest/b.avro\"],\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata).setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
                .discardChanges().withMetadataLocation("/metadata/v1.json").build();
        FileIO fileIO = Mockito.mock(FileIO.class);
        Mockito.when(fileIO.newInputFile(Mockito.anyString())).thenAnswer(invocation -> {
            InputFile inputFile = Mockito.mock(InputFile.class);
            Mockito.when(inputFile.location()).thenReturn(invocation.getArgument(0));
            return inputFile;
        });
        Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.tbl");
        IcebergSnapshotCacheValue value = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(7L, 0L), Optional.empty(), table);
        IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(
                NameMapping.createForTest(1L, "db", "tbl"), table).get();

        value.prepareForCachePublication(key);

        Assert.assertFalse(value.getSizeEstimate().isComplete());
        Table queryTable = value.getIcebergTable().get();
        Assert.assertSame(table.currentSnapshot(), queryTable.currentSnapshot());
        Mockito.verifyNoInteractions(fileIO);
        queryTable.currentSnapshot().allManifests(fileIO);
        Mockito.verify(fileIO, Mockito.atLeastOnce()).newInputFile("/manifest/a.avro");
        Mockito.verify(fileIO, Mockito.atLeastOnce()).newInputFile("/manifest/b.avro");
    }

    @Test
    public void testInvalidateTableKeepsManifestCache() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            NameMapping t1 = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping t2 = new NameMapping(catalogId, "db1", "tbl2", "rdb1", "rtbl2");

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tableEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            tableEntry.put(t1, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));
            tableEntry.put(t2, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));

            Table snapshotTable = tableWithMetadataLocation("/metadata/invalidate-v1.json");
            IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(t1, snapshotTable).get();
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshotEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            snapshotEntry.put(snapshotKey,
                    new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L)));

            MetaCacheEntry<NameMapping, org.apache.iceberg.view.View> viewEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_VIEW, NameMapping.class, org.apache.iceberg.view.View.class);
            viewEntry.put(t1, newInterfaceProxy(org.apache.iceberg.view.View.class));
            viewEntry.put(t2, newInterfaceProxy(org.apache.iceberg.view.View.class));

            String sharedManifestPath = "/tmp/shared.avro";
            IcebergManifestEntryKey m1 = mockManifestKey(sharedManifestPath);
            IcebergManifestEntryKey m2 = mockManifestKey(sharedManifestPath);
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            Assert.assertEquals(m1, m2);
            manifestEntry.put(m1, ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));
            manifestEntry.put(m2, ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));

            Assert.assertNotNull(manifestEntry.getIfPresent(m1));
            Assert.assertNotNull(manifestEntry.getIfPresent(m2));
            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(tableEntry.getIfPresent(t1));
            Assert.assertNotNull(tableEntry.getIfPresent(t2));
            Assert.assertNull(snapshotEntry.getIfPresent(snapshotKey));
            Assert.assertNull(viewEntry.getIfPresent(t1));
            Assert.assertNotNull(viewEntry.getIfPresent(t2));
            Assert.assertNotNull(manifestEntry.getIfPresent(m1));
            Assert.assertNotNull(manifestEntry.getIfPresent(m2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateDbAndStats() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            NameMapping db1Table = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping db2Table = new NameMapping(catalogId, "db2", "tbl1", "rdb2", "rtbl1");

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tableEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            tableEntry.put(db1Table, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));
            tableEntry.put(db2Table, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));

            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_SCHEMA, IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            IcebergSchemaCacheKey db1Schema = new IcebergSchemaCacheKey(db1Table, 1L);
            IcebergSchemaCacheKey db2Schema = new IcebergSchemaCacheKey(db2Table, 2L);
            schemaEntry.put(db1Schema, new SchemaCacheValue(Collections.emptyList()));
            schemaEntry.put(db2Schema, new SchemaCacheValue(Collections.emptyList()));
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            IcebergManifestEntryKey manifestKey = mockManifestKey("/tmp/db-invalidate.avro");
            manifestEntry.put(manifestKey,
                    ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));

            cache.invalidateDb(catalogId, "db1");

            Assert.assertNull(tableEntry.getIfPresent(db1Table));
            Assert.assertNotNull(tableEntry.getIfPresent(db2Table));
            Assert.assertNull(schemaEntry.getIfPresent(db1Schema));
            Assert.assertNotNull(schemaEntry.getIfPresent(db2Schema));
            Assert.assertNotNull(manifestEntry.getIfPresent(manifestKey));

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            Assert.assertTrue(stats.containsKey(IcebergExternalMetaCache.ENTRY_TABLE));
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_TABLE).isAutoRefresh());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isConfigEnabled());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isEffectiveEnabled());
            Assert.assertFalse(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isAutoRefresh());
            Assert.assertEquals(-1L, stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).getTtlSecond());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSchemaStatsWhenSchemaCacheDisabled() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "0");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats schemaStats = stats.get(IcebergExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(0L, schemaStats.getTtlSecond());
            Assert.assertTrue(schemaStats.isConfigEnabled());
            Assert.assertFalse(schemaStats.isEffectiveEnabled());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestStatsDisabledByDefault() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats manifestStats = stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST);
            Assert.assertNotNull(manifestStats);
            Assert.assertFalse(manifestStats.isConfigEnabled());
            Assert.assertFalse(manifestStats.isEffectiveEnabled());
            Assert.assertFalse(manifestStats.isAutoRefresh());
            Assert.assertEquals(-1L, manifestStats.getTtlSecond());
            Assert.assertEquals(100000L, manifestStats.getCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestEntryRequiresContextualLoader() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            IcebergManifestEntryKey manifestKey = mockManifestKey("/tmp/contextual-only.avro");

            UnsupportedOperationException exception = Assert.assertThrows(UnsupportedOperationException.class,
                    () -> manifestEntry.get(manifestKey));
            Assert.assertTrue(exception.getMessage().contains("contextual miss loader"));

            ManifestCacheValue value = manifestEntry.get(manifestKey,
                    ignored -> ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));
            Assert.assertNotNull(value);
            Assert.assertSame(value, manifestEntry.getIfPresent(manifestKey));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestEnableUsesDefaultCapacity() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.manifest.enable", "true");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats manifestStats = stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST);
            Assert.assertNotNull(manifestStats);
            Assert.assertTrue(manifestStats.isConfigEnabled());
            Assert.assertTrue(manifestStats.isEffectiveEnabled());
            Assert.assertEquals(-1L, manifestStats.getTtlSecond());
            Assert.assertEquals(100000L, manifestStats.getCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    private Map<String, String> manifestCacheEnabledProperties() {
        Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
        properties.put("meta.cache.iceberg.manifest.enable", "true");
        return properties;
    }

    private long snapshotWeight(IcebergSnapshotEntryKey key, int partitionCount) {
        IcebergPartitionInfo partitionInfo = Mockito.mock(IcebergPartitionInfo.class);
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = sizeOnlyMap(partitionCount);
        Map<String, IcebergPartition> partitions = sizeOnlyMap(partitionCount);
        Map<String, java.util.Set<String>> aliases = sizeOnlyMap(partitionCount);
        Mockito.when(partitionInfo.getNameToPartitionItem()).thenReturn(partitionItems);
        Mockito.when(partitionInfo.getNameToIcebergPartition()).thenReturn(partitions);
        Mockito.when(partitionInfo.getNameToIcebergPartitionNames()).thenReturn(aliases);
        IcebergSnapshotCacheValue value = new IcebergSnapshotCacheValue(
                partitionInfo, new IcebergSnapshot(key.getSnapshotId(), key.getSchemaId()));
        MetaCacheSizeEstimate estimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(key, value);
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    private long manifestWeight(IcebergManifestEntryKey key, int fileCount) {
        ManifestCacheValue value = Mockito.mock(ManifestCacheValue.class);
        List<org.apache.iceberg.DataFile> dataFiles = sizeOnlyList(fileCount);
        Mockito.when(value.getDataFiles()).thenReturn(dataFiles);
        Mockito.when(value.getDeleteFiles()).thenReturn(Collections.emptyList());
        Mockito.when(value.isAccountingComplete()).thenReturn(true);
        MetaCacheSizeEstimate estimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, value);
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    private void assertLinearScale(long base, long oneThousand, long tenThousand, long oneHundredThousand) {
        long oneThousandPayload = oneThousand - base;
        Assert.assertTrue(oneThousandPayload > 0L);
        Assert.assertEquals(oneThousandPayload * 10L, tenThousand - base);
        Assert.assertEquals(oneThousandPayload * 100L, oneHundredThousand - base);
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> sizeOnlyMap(int size) {
        Map<K, V> map = Mockito.mock(Map.class);
        Mockito.when(map.size()).thenReturn(size);
        return map;
    }

    @SuppressWarnings("unchecked")
    private <V> List<V> sizeOnlyList(int size) {
        List<V> list = Mockito.mock(List.class);
        Mockito.when(list.size()).thenReturn(size);
        return list;
    }

    private static String repeatedCharacter(char character, int count) {
        char[] characters = new char[count];
        java.util.Arrays.fill(characters, character);
        return new String(characters);
    }

    private Table tableWithMetadataLocation(String metadataLocation) {
        return tableWithMetadata(metadataWithLocation(metadataLocation), null);
    }

    private TableMetadata metadataWithLocation(String metadataLocation) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        return TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation(metadataLocation).build();
    }

    private Table tableWithMetadata(TableMetadata metadata, FileIO io) {
        return new BaseTable(new StaticTableOperations(metadata, io), "db.tbl");
    }

    /** A FileIO whose identity is its configuration, like a catalog-vended S3 FileIO. */
    private static final class PropertiesFileIO implements FileIO {
        private final Map<String, String> properties;

        private PropertiesFileIO(String key, String value) {
            this.properties = Collections.singletonMap(key, value);
        }

        @Override
        public Map<String, String> properties() {
            return properties;
        }

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }

    private IcebergTableCacheValue tableValueWithFields(int fieldCount) {
        List<Types.NestedField> fields = IntStream.range(0, fieldCount)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 1, "field_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(fields), PartitionSpec.unpartitioned(),
                "file:/warehouse/jol-table", Collections.emptyMap());
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation("/metadata/jol-table-v1.json").build();
        metadata = roundTripMetadata(metadata, "/metadata/jol-table-v1.json");
        return new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, null), "db.tbl"));
    }

    private IcebergTableCacheValue tableValueWithNestedFields(int nestedFieldCount) {
        List<Types.NestedField> nestedFields = IntStream.range(0, nestedFieldCount)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 2, "nested_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        Schema schema = new Schema(Types.NestedField.optional(
                1, "payload", Types.StructType.of(nestedFields)));
        return tableValueWithSchemaAndProperties(schema, Collections.emptyMap());
    }

    private Schema unicodeNestedSchema(int nestedFieldCount) {
        List<Types.NestedField> nestedFields = new ArrayList<>();
        for (int index = 0; index < nestedFieldCount; index++) {
            nestedFields.add(Types.NestedField.optional(
                    index + 10, "Nested_" + DOTTED_CAPITAL_I + "_" + index, Types.StringType.get()));
        }
        Types.StructType element = Types.StructType.of(Types.NestedField.optional(
                3, "Leaf_" + DOTTED_CAPITAL_I, Types.StringType.get()));
        return new Schema(
                Types.NestedField.optional(1, "Payload_" + DOTTED_CAPITAL_I, Types.StructType.of(nestedFields)),
                Types.NestedField.optional(2, "List_" + DOTTED_CAPITAL_I,
                        Types.ListType.ofOptional(4, element)),
                Types.NestedField.optional(5, "Map_" + DOTTED_CAPITAL_I, Types.MapType.ofOptional(
                        6, 7, Types.StringType.get(), Types.StringType.get())));
    }

    private IcebergTableCacheValue tableValueWithIdentityPartitionedFields(int fieldCount) {
        List<Types.NestedField> fields = IntStream.range(0, fieldCount)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 1, "field_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        Schema schema = new Schema(fields);
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        for (Types.NestedField field : fields) {
            specBuilder.identity(field.name());
        }
        return tableValueWithSchemaAndSpec(schema, specBuilder.build());
    }

    private IcebergTableCacheValue tableValueWithNestedMixedCaseFields(int nestedFieldCount) {
        // Uncached field ids, upper-case names and list/map synthetic fields exercise the boxed
        // key, lower-case String and short-alias terms of the schema formula.
        List<Types.NestedField> nestedFields = IntStream.range(0, nestedFieldCount)
                .mapToObj(index -> Types.NestedField.optional(
                        1000 + index, "Nested_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        Schema schema = new Schema(
                Types.NestedField.optional(1, "payload", Types.StructType.of(nestedFields)),
                Types.NestedField.optional(2, "list", Types.ListType.ofOptional(3,
                        Types.StructType.of(Types.NestedField.optional(
                                4, "leaf", Types.StringType.get())))),
                Types.NestedField.optional(5, "map", Types.MapType.ofOptional(
                        6, 7, Types.StringType.get(), Types.LongType.get())),
                Types.NestedField.optional(8, "id", Types.LongType.get()));
        return tableValueWithSchemaAndSpec(
                schema, PartitionSpec.builderFor(schema).identity("id").build());
    }

    private IcebergTableCacheValue tableValueWithSchemaAndSpec(Schema schema, PartitionSpec spec) {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, spec, "file:/warehouse/jol-table", Collections.emptyMap());
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation("/metadata/jol-table-v1.json").build();
        metadata = roundTripMetadata(metadata, "/metadata/jol-table-v1.json");
        return new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, null), "db.tbl"));
    }

    private IcebergTableCacheValue tableValueWithProperties(int propertyCount) {
        Map<String, String> properties = IntStream.range(0, propertyCount).boxed()
                .collect(Collectors.toMap(index -> "key_" + index, index -> "value_" + index));
        Schema schema = new Schema(Types.NestedField.required(
                1, "id", Types.IntegerType.get()));
        return tableValueWithSchemaAndProperties(schema, properties);
    }

    private IcebergTableCacheValue tableValueWithSchemaAndProperties(
            Schema schema, Map<String, String> properties) {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/jol-table", properties);
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation("/metadata/jol-table-v1.json").build();
        metadata = roundTripMetadata(metadata, "/metadata/jol-table-v1.json");
        return new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, null), "db.tbl"));
    }

    private IcebergTableCacheValue tableValueWithSnapshotHistory(
            int snapshotCount, boolean includeSnapshotLog) {
        long currentSnapshotId = 1000L + snapshotCount - 1L;
        StringBuilder json = new StringBuilder()
                .append("{\"format-version\":2,\"table-uuid\":\"jol-table\",")
                .append("\"location\":\"file:/warehouse/jol-table\",\"last-sequence-number\":")
                .append(snapshotCount).append(",\"last-updated-ms\":").append(snapshotCount)
                .append(",\"last-column-id\":1,\"current-schema-id\":0,")
                .append("\"schemas\":[{\"type\":\"struct\",\"schema-id\":0,\"fields\":[")
                .append("{\"id\":1,\"name\":\"field\",\"required\":false,\"type\":\"string\"}]}],")
                .append("\"default-spec-id\":0,\"partition-specs\":[{\"spec-id\":0,\"fields\":[]}],")
                .append("\"last-partition-id\":999,\"default-sort-order-id\":0,")
                .append("\"sort-orders\":[{\"order-id\":0,\"fields\":[]}],\"properties\":{},")
                .append("\"current-snapshot-id\":").append(currentSnapshotId)
                .append(",\"refs\":{\"main\":{\"snapshot-id\":").append(currentSnapshotId)
                .append(",\"type\":\"branch\"}},\"snapshots\":[");
        for (int index = 0; index < snapshotCount; index++) {
            if (index > 0) {
                json.append(',');
            }
            json.append("{\"sequence-number\":").append(index + 1L)
                    .append(",\"snapshot-id\":").append(1000L + index);
            if (index > 0) {
                json.append(",\"parent-snapshot-id\":").append(1000L + index - 1L);
            }
            json.append(",\"timestamp-ms\":").append(index + 1L)
                    .append(",\"summary\":{\"operation\":\"append\"},")
                    .append("\"manifest-list\":\"/jol/list-").append(index)
                    .append(".avro\",\"schema-id\":0}");
        }
        json.append("],\"statistics\":[],\"partition-statistics\":[],\"snapshot-log\":[");
        if (includeSnapshotLog) {
            for (int index = 0; index < snapshotCount; index++) {
                if (index > 0) {
                    json.append(',');
                }
                json.append("{\"timestamp-ms\":").append(index + 1L)
                        .append(",\"snapshot-id\":").append(1000L + index).append('}');
            }
        }
        json.append("],\"metadata-log\":[]}");
        TableMetadata metadata = TableMetadataParser.fromJson(
                "/metadata/jol-history-v1.json", json.toString());
        return new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, null), "db.tbl"));
    }

    private IcebergPartitionInfo realPartitionInfo(int partitionCount) throws Exception {
        return realPartitionInfo(partitionCount, 1);
    }

    private IcebergPartitionInfo realPartitionInfo(int partitionCount, int partitionColumnCount)
            throws Exception {
        return realPartitionInfo(partitionCount, partitionColumnCount, false);
    }

    private IcebergPartitionInfo realPartitionInfo(
            int partitionCount, int partitionColumnCount, boolean mergeAllIntoFirst) throws Exception {
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = new java.util.HashMap<>();
        Map<String, IcebergPartition> partitions = new java.util.HashMap<>();
        List<org.apache.doris.catalog.Column> partitionColumns = new ArrayList<>();
        for (int column = 0; column < partitionColumnCount; column++) {
            partitionColumns.add(new org.apache.doris.catalog.Column(
                    "part" + column, org.apache.doris.catalog.PrimitiveType.DATETIMEV2));
        }
        for (int index = 0; index < partitionCount; index++) {
            String value = Integer.toString(index);
            String name = "part=" + value;
            partitionItems.put(name, new org.apache.doris.catalog.RangePartitionItem(
                    IcebergUtils.getPartitionRange(value, "day", partitionColumns)));
            // Loaded partitions own one String per value; bucket/truncate transforms own their
            // strings too (year/month/day literals are shared and only leave the estimate
            // more conservative).
            List<String> values = new ArrayList<>();
            List<String> transforms = new ArrayList<>();
            for (int column = 0; column < partitionColumnCount; column++) {
                values.add(new String(value.toCharArray()));
                transforms.add(new String("day".toCharArray()));
            }
            partitions.put(name, new IcebergPartition(name, 0, 1L, 1L, 1L, 1L, 1L, values, transforms));
        }
        Map<String, Set<String>> aliases = Collections.emptyMap();
        if (mergeAllIntoFirst && partitionCount > 0) {
            // mergeOverlapPartitions() shape: the surviving name owns a set of every enclosed name.
            aliases = Collections.singletonMap("part=0", new java.util.HashSet<>(partitions.keySet()));
        }
        return new IcebergPartitionInfo(partitionItems, partitions, aliases);
    }

    /**
     * One "year" partition enclosing {@code partitionCount - 1} "day" partitions of that year;
     * mergeOverlapPartitions() keeps a single Doris partition item owning every physical name.
     */
    private IcebergPartitionInfo mergedPartitionInfo(int partitionCount, int partitionColumnCount)
            throws Exception {
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = new java.util.HashMap<>();
        Map<String, IcebergPartition> partitions = new java.util.HashMap<>();
        List<org.apache.doris.catalog.Column> partitionColumns = new ArrayList<>();
        for (int column = 0; column < partitionColumnCount; column++) {
            partitionColumns.add(new org.apache.doris.catalog.Column(
                    "part" + column, org.apache.doris.catalog.PrimitiveType.DATETIMEV2));
        }
        for (int index = 0; index < partitionCount; index++) {
            String transform = index == 0 ? "year" : "day";
            String value = index == 0 ? "0" : Integer.toString(index);
            String name = "part=" + transform + "-" + value;
            partitionItems.put(name, new org.apache.doris.catalog.RangePartitionItem(
                    IcebergUtils.getPartitionRange(value, transform, partitionColumns)));
            List<String> values = new ArrayList<>();
            List<String> transforms = new ArrayList<>();
            for (int column = 0; column < partitionColumnCount; column++) {
                values.add(new String(value.toCharArray()));
                transforms.add(new String(transform.toCharArray()));
            }
            partitions.put(name, new IcebergPartition(name, 0, 1L, 1L, 1L, 1L, 1L, values, transforms));
        }
        Map<String, Set<String>> aliases = IcebergUtils.mergeOverlapPartitions(partitionItems);
        return new IcebergPartitionInfo(partitionItems, partitions, aliases);
    }

    private org.apache.iceberg.DataFile dataFileWithMetrics(int index) {
        Map<Integer, Long> columnSizes = metricLongMap(index, 0);
        Map<Integer, Long> valueCounts = metricLongMap(index, 1);
        Map<Integer, Long> nullCounts = metricLongMap(index, 2);
        Map<Integer, Long> nanCounts = metricLongMap(index, 3);
        Map<Integer, ByteBuffer> lowerBounds = metricBufferMap();
        Map<Integer, ByteBuffer> upperBounds = metricBufferMap();
        Metrics metrics = new Metrics(
                100L, columnSizes, valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds);
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/data/jol-" + index + ".parquet")
                .withFileSizeInBytes(1024L)
                .withMetrics(metrics)
                .build()
                .copy();
    }

    private org.apache.iceberg.DataFile dataFileWithPathPayload(int pathLength) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/data/" + repeatedCharacter('x', pathLength) + ".parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(1L)
                .build();
    }

    private DeleteFile deleteFileWithMetrics(int index) {
        Map<Integer, Long> columnSizes = metricLongMap(index, 0);
        Map<Integer, Long> valueCounts = metricLongMap(index, 1);
        Map<Integer, Long> nullCounts = metricLongMap(index, 2);
        Map<Integer, Long> nanCounts = metricLongMap(index, 3);
        Map<Integer, ByteBuffer> lowerBounds = metricBufferMap();
        Map<Integer, ByteBuffer> upperBounds = metricBufferMap();
        Metrics metrics = new Metrics(
                100L, columnSizes, valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds);
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/delete/jol-" + index + ".parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(1L)
                .withReferencedDataFile("/data/jol-" + index + ".parquet")
                .withMetrics(metrics)
                .build()
                .copy();
    }

    private Map<Integer, Long> metricLongMap(int fileIndex, int mapIndex) {
        Map<Integer, Long> values = new java.util.HashMap<>();
        for (int column = 0; column < 8; column++) {
            values.put(Integer.valueOf(10_000 + column),
                    Long.valueOf(10_000L + fileIndex * 100L + mapIndex * 10L + column));
        }
        return values;
    }

    private Map<Integer, ByteBuffer> metricBufferMap() {
        Map<Integer, ByteBuffer> values = new java.util.HashMap<>();
        for (int column = 0; column < 8; column++) {
            values.put(Integer.valueOf(10_000 + column), ByteBuffer.allocate(32));
        }
        return values;
    }

    private List<DataFile> manifestFilesWithIntegerPartitions(
            int fileCount, int partitionFieldCount) {
        return manifestFilesWithPartitions(
                fileCount, partitionFieldCount, Types.IntegerType.get(),
                (fileIndex, fieldIndex) -> Integer.valueOf(
                        10_000 + fileIndex * partitionFieldCount + fieldIndex));
    }

    private List<DataFile> manifestFilesWithPartitions(
            int fileCount, int partitionFieldCount, Type.PrimitiveType partitionType,
            BiFunction<Integer, Integer, Object> valueFactory) {
        if (partitionFieldCount == 0) {
            return IntStream.range(0, fileCount)
                    .mapToObj(index -> DataFiles.builder(PartitionSpec.unpartitioned())
                            .withPath("/partition-data/file-" + index + ".parquet")
                            .withFileSizeInBytes(10L)
                            .withRecordCount(1L)
                            .build())
                    .collect(Collectors.toList());
        }
        List<Types.NestedField> fields = IntStream.range(0, partitionFieldCount)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 1, "partition_" + index, partitionType))
                .collect(Collectors.toList());
        Schema schema = new Schema(fields);
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        fields.forEach(field -> specBuilder.identity(field.name()));
        PartitionSpec spec = specBuilder.build();
        DataFiles.Builder fileBuilder = DataFiles.builder(spec);
        PartitionData partitionData = new PartitionData(spec.partitionType());
        List<DataFile> files = new ArrayList<>(fileCount);
        for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
            for (int fieldIndex = 0; fieldIndex < partitionFieldCount; fieldIndex++) {
                partitionData.set(fieldIndex, valueFactory.apply(fileIndex, fieldIndex));
            }
            files.add(fileBuilder.withPartition(partitionData)
                    .withPath("/partition-data/file-" + fileIndex + ".parquet")
                    .withFileSizeInBytes(10L)
                    .withRecordCount(1L)
                    .build());
        }
        return files;
    }

    private Table tableWithMetadata(TableMetadata metadata) {
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        return new BaseTable(operations, "db.tbl");
    }

    private TableMetadata roundTripMetadata(TableMetadata metadata, String metadataLocation) {
        return TableMetadataParser.fromJson(metadataLocation, TableMetadataParser.toJson(metadata));
    }

    private void materializeAllLazyState(IcebergTableCacheValue value) {
        Table table = value.getRetainedIcebergTable();
        materializeAllLazyState(((HasTableOperations) table).operations().current());
    }

    private void materializeAllLazyState(TableMetadata metadata) {
        for (Schema schema : metadata.schemas()) {
            materializeSchemaAndStruct(schema);
        }
        for (PartitionSpec spec : metadata.specs()) {
            spec.fields();
            spec.javaClasses();
            Types.StructType partitionType = spec.partitionType();
            spec.rawPartitionType();
            if (!spec.fields().isEmpty()) {
                spec.getFieldsBySourceId(spec.fields().get(0).sourceId());
            }
            materializeStructAndSecondarySchema(partitionType);
        }
    }

    private void materializeSchemaAndStruct(Schema schema) {
        if (schema.columns().isEmpty()) {
            return;
        }
        Types.NestedField first = schema.columns().get(0);
        materializeSchemaIndexes(schema, first);
        materializeStructAndSecondarySchema(schema.asStruct());
    }

    private void materializeStructAndSecondarySchema(Types.StructType struct) {
        if (struct.fields().isEmpty()) {
            return;
        }
        Types.NestedField first = struct.fields().get(0);
        materializeStructIndexes(struct, first);
        Schema secondary = struct.asSchema();
        materializeSchemaIndexes(secondary, first);
        materializeStructIndexes(secondary.asStruct(), first);
    }

    private void materializeSchemaIndexes(Schema schema, Types.NestedField first) {
        schema.findField(first.name());
        schema.findField(first.fieldId());
        schema.caseInsensitiveFindField(first.name().toUpperCase(java.util.Locale.ROOT));
        schema.idToName();
        schema.identifierFieldIds();
        schema.accessorForField(first.fieldId());
    }

    private void materializeStructIndexes(Types.StructType struct, Types.NestedField first) {
        struct.fields();
        struct.field(first.name());
        struct.caseInsensitiveField(first.name().toUpperCase(java.util.Locale.ROOT));
        struct.field(first.fieldId());
    }

    /**
     * Delta between the first two columns and the whole schema: measures the schema graph. Both
     * sides reach the same shared type singletons (StringType, ListType element names, ...) so
     * only per-column growth is compared.
     */
    private void assertSchemaLookupFormula(Schema schema, String fixture) {
        Schema firstColumns = new Schema(schema.schemaId(), schema.columns().subList(0, 2));
        TableMetadata empty = TableMetadata.newTableMetadata(
                firstColumns, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-lookup", Collections.emptyMap());
        TableMetadata populated = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-lookup", Collections.emptyMap());
        assertRetainedPayloadDelta(fixture, empty, populated, "jol-schema-lookup");
    }

    /** Delta between an unpartitioned spec and one identity field: measures the spec graph. */
    private void assertPartitionSpecFormula(Schema schema, String fixture) {
        TableMetadata empty = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-lookup", Collections.emptyMap());
        TableMetadata populated = TableMetadata.newTableMetadata(
                schema, PartitionSpec.builderFor(schema).identity(schema.columns().get(0).name()).build(),
                "file:/warehouse/schema-lookup", Collections.emptyMap());
        assertRetainedPayloadDelta(fixture, empty, populated, "jol-partition-spec");
    }

    private void assertRetainedPayloadDelta(
            String fixture, TableMetadata empty, TableMetadata populated, String locationPrefix) {
        empty = roundTripMetadata(empty,
                "/metadata/" + locationPrefix + "-none-" + fixture.hashCode() + ".json");
        populated = roundTripMetadata(populated,
                "/metadata/" + locationPrefix + "-with-" + fixture.hashCode() + ".json");

        long emptyEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(empty));
        long populatedEstimate = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(populated));
        materializeAllLazyState(empty);
        materializeAllLazyState(populated);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "iceberg " + fixture, emptyEstimate, populatedEstimate, empty, populated);
    }

    private TableMetadata metadataWithMaterializedPayload(String payload, int bufferBytes) {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.uuid()).thenReturn("stable-uuid");
        Mockito.when(metadata.refs()).thenReturn(Collections.singletonMap(
                payload, Mockito.mock(SnapshotRef.class)));
        TableMetadata.MetadataLogEntry metadataLogEntry =
                Mockito.mock(TableMetadata.MetadataLogEntry.class);
        Mockito.when(metadataLogEntry.file()).thenReturn(payload);
        Mockito.when(metadata.previousFiles()).thenReturn(
                Collections.singletonList(metadataLogEntry));
        GenericBlobMetadata blob = new GenericBlobMetadata(
                payload, 1L, 1L, Collections.singletonList(1),
                Collections.singletonMap(payload, payload));
        Mockito.when(metadata.statisticsFiles()).thenReturn(Collections.singletonList(
                new GenericStatisticsFile(1L, payload, 1L, 1L,
                        Collections.singletonList(blob))));
        org.apache.iceberg.PartitionStatisticsFile partitionStatistics =
                Mockito.mock(org.apache.iceberg.PartitionStatisticsFile.class);
        Mockito.when(partitionStatistics.path()).thenReturn(payload);
        Mockito.when(metadata.partitionStatisticsFiles()).thenReturn(
                Collections.singletonList(partitionStatistics));
        EncryptedKey encryptedKey = Mockito.mock(EncryptedKey.class);
        Mockito.when(encryptedKey.keyId()).thenReturn(payload);
        Mockito.when(encryptedKey.encryptedById()).thenReturn(payload);
        Mockito.when(encryptedKey.encryptedKeyMetadata()).thenReturn(
                ByteBuffer.allocateDirect(bufferBytes));
        Mockito.when(encryptedKey.properties()).thenReturn(
                Collections.singletonMap(payload, payload));
        Mockito.when(metadata.encryptionKeys()).thenReturn(Collections.singletonList(encryptedKey));
        return metadata;
    }

    private TableMetadata metadataWithSnapshots(Snapshot snapshot) {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(metadata.currentSnapshot()).thenReturn(snapshot);
        Mockito.when(metadata.snapshotLog()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.previousFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.refs()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.statisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.partitionStatisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.encryptionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.metadataFileLocation()).thenReturn("/metadata/snapshot.json");
        return metadata;
    }

    private TableMetadata metadataWithStatisticsFile(StatisticsFile statisticsFile) {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.snapshots()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.snapshotLog()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.previousFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.refs()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.statisticsFiles()).thenReturn(
                Collections.singletonList(statisticsFile));
        Mockito.when(metadata.partitionStatisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.encryptionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.metadataFileLocation()).thenReturn("/metadata/statistics.json");
        return metadata;
    }

    private TableMetadata metadataWithSnapshotSequence(long lastSequenceNumber) {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.snapshotLog()).thenReturn(Collections.singletonList(
                Mockito.mock(org.apache.iceberg.HistoryEntry.class)));
        Mockito.when(metadata.refs()).thenReturn(Collections.singletonMap(
                "branch-tip", Mockito.mock(SnapshotRef.class)));
        Mockito.when(metadata.previousFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.statisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.partitionStatisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.encryptionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.lastSequenceNumber()).thenReturn(lastSequenceNumber);
        Mockito.when(metadata.metadataFileLocation()).thenReturn("/metadata/sequence.json");
        return metadata;
    }

    private Table tableWithNestedSchemaAndProperty(String nestedFieldName, String propertyValue) {
        Schema schema = new Schema(Types.NestedField.optional(1, "payload",
                Types.StructType.of(Types.NestedField.optional(
                        2, nestedFieldName, Types.StringType.get()))));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.singletonMap("payload", propertyValue));
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation("/metadata/nested.json").build();
        return new BaseTable(new StaticTableOperations(metadata, null), "db.tbl");
    }

    private IcebergManifestEntryKey mockManifestKey(String path) {
        return IcebergManifestEntryKey.of(new TestingManifestFile(path, ManifestContent.DATA));
    }

    private <T> T newInterfaceProxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return type.getSimpleName() + "Proxy";
                    default:
                        return null;
                }
            }
            return defaultValue(method.getReturnType());
        }));
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("unsupported primitive type: " + type);
    }

    private static final class TestingManifestFile implements ManifestFile {
        private final String path;
        private final ManifestContent content;

        private TestingManifestFile(String path, ManifestContent content) {
            this.path = path;
            this.content = content;
        }

        @Override
        public String path() {
            return path;
        }

        @Override
        public ManifestContent content() {
            return content;
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public int partitionSpecId() {
            return 0;
        }

        @Override
        public long sequenceNumber() {
            return 0;
        }

        @Override
        public long minSequenceNumber() {
            return 0;
        }

        @Override
        public Long snapshotId() {
            return null;
        }

        @Override
        public Integer addedFilesCount() {
            return null;
        }

        @Override
        public Long addedRowsCount() {
            return null;
        }

        @Override
        public Integer existingFilesCount() {
            return null;
        }

        @Override
        public Long existingRowsCount() {
            return null;
        }

        @Override
        public Integer deletedFilesCount() {
            return null;
        }

        @Override
        public Long deletedRowsCount() {
            return null;
        }

        @Override
        public List<PartitionFieldSummary> partitions() {
            return null;
        }

        @Override
        public ByteBuffer keyMetadata() {
            return null;
        }

        @Override
        public ManifestFile copy() {
            return new TestingManifestFile(path, content);
        }
    }
}
