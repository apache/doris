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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.EstimatorCalibrationAssertions;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;
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
import org.apache.paimon.privilege.PrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PaimonExternalMetaCacheTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSnapshotWeightScalesLinearlyToOneHundredThousandPartitions() throws Exception {
        FileStoreTable table = newPartitionedTable("linear_snapshot_estimate", Collections.emptyMap());
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(
                mapping, 1L, table.schema().id(), 1L);
        long base = snapshotWeight(key, table, 0);
        long oneThousand = snapshotWeight(key, table, 1_000);
        long tenThousand = snapshotWeight(key, table, 10_000);
        long oneHundredThousand = snapshotWeight(key, table, 100_000);

        long oneThousandPayload = oneThousand - base;
        Assert.assertTrue(oneThousandPayload > 0L);
        Assert.assertEquals(oneThousandPayload * 10L, tenThousand - base);
        Assert.assertEquals(oneThousandPayload * 100L, oneHundredThousand - base);
    }

    @Test
    public void testSnapshotWeightAccountsForSkewedTableOptions() throws Exception {
        FileStoreTable smallTable = newPartitionedTable(
                "option_small", Collections.singletonMap("payload", "x"));
        String largePayload = repeatedCharacter('x', 64 * 1024);
        FileStoreTable largeTable = newPartitionedTable(
                "option_large", Collections.singletonMap("payload", largePayload));
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey smallKey = new PaimonSnapshotEntryKey(
                mapping, 1L, smallTable.schema().id(), 1L);
        PaimonSnapshotEntryKey largeKey = new PaimonSnapshotEntryKey(
                mapping, 1L, largeTable.schema().id(), 1L);

        long smallBytes = snapshotWeight(smallKey, smallTable, 0);
        long largeBytes = snapshotWeight(largeKey, largeTable, 0);

        Assert.assertTrue(largeBytes - smallBytes
                >= MetaCacheWeightUtils.estimatedStringBytes(largePayload)
                        - MetaCacheWeightUtils.estimatedStringBytes("x"));
    }

    @Test
    public void testSnapshotWeightAccountsForNestedSchemaPayload() throws Exception {
        String largeFieldName = repeatedCharacter('x', 64 * 1024);
        FileStoreTable smallTable = newPartitionedTableWithNestedField("nested_small", "x");
        FileStoreTable largeTable = newPartitionedTableWithNestedField(
                "nested_large", largeFieldName);
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey smallKey = new PaimonSnapshotEntryKey(
                mapping, 1L, smallTable.schema().id(), 1L);
        PaimonSnapshotEntryKey largeKey = new PaimonSnapshotEntryKey(
                mapping, 1L, largeTable.schema().id(), 1L);

        long smallBytes = snapshotWeight(smallKey, smallTable, 0);
        long largeBytes = snapshotWeight(largeKey, largeTable, 0);

        Assert.assertTrue(largeBytes - smallBytes
                >= MetaCacheWeightUtils.estimatedStringBytes(largeFieldName)
                        - MetaCacheWeightUtils.estimatedStringBytes("x"));
    }

    @Test
    public void testSnapshotWeightAccountsForTableComment() throws Exception {
        String largeComment = repeatedCharacter('x', 64 * 1024);
        FileStoreTable smallTable = newPartitionedTable(
                "comment_small", Collections.emptyMap(), "x");
        FileStoreTable largeTable = newPartitionedTable(
                "comment_large", Collections.emptyMap(), largeComment);
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");

        long smallBytes = snapshotWeight(new PaimonSnapshotEntryKey(
                mapping, 1L, smallTable.schema().id(), 1L), smallTable, 0);
        long largeBytes = snapshotWeight(new PaimonSnapshotEntryKey(
                mapping, 1L, largeTable.schema().id(), 1L), largeTable, 0);

        Assert.assertTrue(largeBytes - smallBytes
                >= MetaCacheWeightUtils.estimatedStringBytes(largeComment)
                        - MetaCacheWeightUtils.estimatedStringBytes("x"));
    }

    @Test
    public void testSnapshotFormulaAgainstJolOwnedGraph() throws Exception {
        FileStoreTable table = newStringPartitionedTable("jol_snapshot");
        FileStoreTable intTable = newPartitionedTable("jol_int_snapshot", Collections.emptyMap());
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(
                mapping, 1L, table.schema().id(), 1L);
        PaimonSnapshotCacheValue empty = snapshotValueWithRealPartitions(table, 0, 16, Type.STRING);
        PaimonSnapshotCacheValue populated = snapshotValueWithRealPartitions(
                table, 32, 16, Type.STRING);
        PaimonSnapshotCacheValue shortTail = snapshotValueWithRealPartitions(
                table, 1, 16, Type.STRING);
        PaimonSnapshotCacheValue longTail = snapshotValueWithRealPartitions(
                table, 1, 4096, Type.STRING);

        long emptyEstimate = empty.prepareForCachePublication(key).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(key).getBytes();
        long shortTailEstimate = shortTail.prepareForCachePublication(key).getBytes();
        long longTailEstimate = longTail.prepareForCachePublication(key).getBytes();

        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon snapshot partitions", emptyEstimate, populatedEstimate, empty, populated);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon long-tail partition", shortTailEstimate, longTailEstimate, shortTail, longTail);

        PaimonSnapshotEntryKey intKey = new PaimonSnapshotEntryKey(
                mapping, 1L, intTable.schema().id(), 1L);
        PaimonSnapshotCacheValue emptyInts = snapshotValueWithRealPartitions(
                intTable, 0, 0, Type.INT);
        PaimonSnapshotCacheValue populatedInts = snapshotValueWithRealPartitions(
                intTable, 32, 0, Type.INT);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon int snapshot partitions",
                emptyInts.prepareForCachePublication(intKey).getBytes(),
                populatedInts.prepareForCachePublication(intKey).getBytes(),
                emptyInts, populatedInts);
    }

    @Test
    public void testTableSchemaFormulaAgainstJolOwnedGraph() throws Exception {
        FileStoreTable emptyTable = newTableWithExtraFields("jol_empty_schema", 0);
        FileStoreTable populatedTable = newTableWithExtraFields("jol_populated_schema", 32);
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey emptyKey = new PaimonSnapshotEntryKey(
                mapping, 1L, emptyTable.schema().id(), 1L);
        PaimonSnapshotEntryKey populatedKey = new PaimonSnapshotEntryKey(
                mapping, 1L, populatedTable.schema().id(), 1L);
        PaimonSnapshotCacheValue empty = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY,
                new PaimonSnapshot(1L, emptyTable.schema().id(), emptyTable));
        PaimonSnapshotCacheValue populated = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY,
                new PaimonSnapshot(1L, populatedTable.schema().id(), populatedTable));

        long emptyEstimate = empty.prepareForCachePublication(emptyKey).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(populatedKey).getBytes();
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon table fields", emptyEstimate, populatedEstimate, empty, populated);
    }

    @Test
    public void testNestedTableSchemaFormulaAgainstJolOwnedGraph() throws Exception {
        FileStoreTable smallTable = newTableWithNestedFields("jol_nested_small", 1);
        FileStoreTable populatedTable = newTableWithNestedFields("jol_nested_large", 33);
        assertTableDeltaAgainstJol("paimon nested fields", smallTable, populatedTable);
    }

    @Test
    public void testTableOptionFormulaAgainstJolOwnedGraph() throws Exception {
        FileStoreTable emptyTable = newTableWithOptions("jol_options_empty", 0);
        FileStoreTable populatedTable = newTableWithOptions("jol_options_large", 32);
        assertTableDeltaAgainstJol("paimon table options", emptyTable, populatedTable);
    }

    @Test
    public void testCompositeTypeFormulaAgainstJolOwnedGraph() {
        assertTableDeltaAgainstJol("paimon array type",
                newTableWithPayloadType("array", nestedArrayType(1)),
                newTableWithPayloadType("array", nestedArrayType(100)));
        assertTableDeltaAgainstJol("paimon map type",
                newTableWithPayloadType("map", nestedMapType(1)),
                newTableWithPayloadType("map", nestedMapType(100)));
        assertTableDeltaAgainstJol("paimon multiset type",
                newTableWithPayloadType("multiset", nestedMultisetType(1)),
                newTableWithPayloadType("multiset", nestedMultisetType(100)));
        assertTableDeltaAgainstJol("paimon row type",
                newTableWithPayloadType("row", nestedRowType(1)),
                newTableWithPayloadType("row", nestedRowType(100)));
        assertTableDeltaAgainstJol("paimon vector type",
                newTableWithPayloadType("vector", rowOfLeafTypes(1, VectorType.class)),
                newTableWithPayloadType("vector", rowOfLeafTypes(100, VectorType.class)));
        assertTableDeltaAgainstJol("paimon decimal type",
                newTableWithPayloadType("decimal", rowOfLeafTypes(1, DecimalType.class)),
                newTableWithPayloadType("decimal", rowOfLeafTypes(100, DecimalType.class)));
    }

    @Test
    public void testUnknownDataTypeFailsClosedWithoutFailingLoad() {
        DataType unknownType = new DataType(true, DataTypeRoot.INTEGER) {
            @Override
            public int defaultSize() {
                return Integer.BYTES;
            }

            @Override
            public DataType copy(boolean isNullable) {
                return this;
            }

            @Override
            public String asSQLString() {
                return "UNKNOWN";
            }

            @Override
            public <R> R accept(DataTypeVisitor<R> visitor) {
                return new IntType().accept(visitor);
            }
        };
        FileStoreTable table = newTableWithPayloadType("unknown-type", unknownType);
        Assert.assertThrows(IllegalStateException.class,
                () -> PaimonCacheSizeEstimator.retainedTablePayloadBytes(table));

        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(mapping, 1L, table.schema().id(), 1L);
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, table.schema().id(), table));
        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(key);

        Assert.assertFalse(estimate.isComplete());
        Assert.assertSame(table, value.getSnapshot().getTable());
    }

    @Test
    public void testSnapshotWeightEntryAndPrecomputedEstimate() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.paimon.snapshot.max-weight", "8MB"));
            Assert.assertTrue(cache.stats(1L).get(PaimonExternalMetaCache.ENTRY_SNAPSHOT).isWeightBounded());

            NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
            FileStoreTable table = newPartitionedTable("snapshot_estimate", Collections.emptyMap());
            Object lazyStoreBefore = readField(table, table.getClass(), "lazyStore");
            PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(mapping, 1L, table.schema().id(), 1L);
            PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, table.schema().id(), table));
            value.prepareForCachePublication(key);

            Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                    value.getSizeEstimate().isComplete());
            Assert.assertTrue(value.getSizeEstimate().getBytes() > 0L);
            Assert.assertSame("cache admission must not materialize FileStoreTable.store()",
                    lazyStoreBefore, readField(table, table.getClass(), "lazyStore"));

            PaimonSnapshotCacheValue unsupportedValue = new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 1L, Mockito.mock(Table.class)));
            unsupportedValue.prepareForCachePublication(new PaimonSnapshotEntryKey(mapping, 1L, 1L, 1L));
            Assert.assertFalse(unsupportedValue.getSizeEstimate().isComplete());
            Assert.assertTrue(unsupportedValue.getSizeEstimate().getIncompleteReason()
                    .startsWith("unsupported_paimon_table:"));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTablePayloadAccountingWorkIsBounded() {
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        TableSchema schema = Mockito.mock(TableSchema.class);
        @SuppressWarnings("unchecked")
        Map<String, String> oversizedOptions = Mockito.mock(Map.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(schema.fields()).thenReturn(Collections.emptyList());
        Mockito.when(schema.options()).thenReturn(oversizedOptions);
        Mockito.when(oversizedOptions.size()).thenReturn(50_001);

        Assert.assertThrows(IllegalStateException.class,
                () -> PaimonCacheSizeEstimator.retainedTablePayloadBytes(table));
    }

    @Test
    public void testRowTypeLazyLookupReservationCoversPostAdmissionGrowth() throws Exception {
        // Field ids above the Integer cache make every lazy map box its keys and values.
        RowType small = wideRowType(1);
        RowType populated = wideRowType(200);
        FileStoreTable smallTable = newTableWithPayloadType("row-lazy-small", small);
        FileStoreTable populatedTable = newTableWithPayloadType("row-lazy-large", populated);
        for (String fieldName : ROW_TYPE_LAZY_FIELDS) {
            Assert.assertNull(fieldName, readField(populated, fieldName));
        }

        // The oracle inside assertTableDeltaAgainstJol materializes the four maps after the
        // estimate is taken; the estimate reserved at admission must already cover them.
        assertTableDeltaAgainstJol("paimon row lazy lookup maps", smallTable, populatedTable);
        for (String fieldName : ROW_TYPE_LAZY_FIELDS) {
            Assert.assertNotNull(fieldName, readField(populated, fieldName));
        }

        // A RowType whose maps were materialized before admission is estimated identically.
        RowType preloaded = wideRowType(200);
        long unloadedEstimate = PaimonCacheSizeEstimator.retainedTablePayloadBytes(
                newTableWithPayloadType("row-unloaded", wideRowType(200)));
        materializeRowTypeIndexes(preloaded);
        Assert.assertEquals(unloadedEstimate, PaimonCacheSizeEstimator.retainedTablePayloadBytes(
                newTableWithPayloadType("row-loaded", preloaded)));
    }

    @Test
    public void testSnapshotKeySeparatesReloadedTableGenerations() {
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshot fence = new PaimonSnapshot(7L, 3L, null);
        PaimonSnapshotCacheValue fenceValue = new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY, fence);
        PaimonTableCacheValue first = new PaimonTableCacheValue(null, fenceValue);
        PaimonTableCacheValue reloaded = new PaimonTableCacheValue(null, fenceValue);

        PaimonSnapshotEntryKey firstKey = PaimonSnapshotEntryKey.of(
                mapping, fence, first.getGeneration());
        PaimonSnapshotEntryKey reloadedKey = PaimonSnapshotEntryKey.of(
                mapping, fence, reloaded.getGeneration());

        Assert.assertNotEquals(firstKey, reloadedKey);
        Assert.assertNotEquals(firstKey.getTableGeneration(), reloadedKey.getTableGeneration());
    }

    @Test
    public void testReplacingTableGenerationRetiresSnapshotAndSchemaProjection() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping mapping = new NameMapping(catalogId, "db", "tbl", "db", "tbl");
            PaimonTableCacheValue first = new PaimonTableCacheValue(Mockito.mock(Table.class));
            PaimonTableCacheValue second = new PaimonTableCacheValue(Mockito.mock(Table.class));
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tables.put(mapping, first);
            PaimonSnapshotEntryKey oldSnapshotKey = new PaimonSnapshotEntryKey(
                    mapping, 1L, 2L, first.getGeneration());
            org.apache.doris.datasource.metacache.MetaCacheEntry<
                    PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots = cache.entry(
                    catalogId, PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class);
            snapshots.put(oldSnapshotKey, new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 2L, first.getPaimonTable())));
            PaimonSchemaCacheKey oldSchemaKey = new PaimonSchemaCacheKey(
                    mapping, first.getGeneration(), 2L);
            org.apache.doris.datasource.metacache.MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemas =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class);
            schemas.put(oldSchemaKey, new SchemaCacheValue(Collections.emptyList()));

            // Simulate expiry/invalidation before the next table generation is published.
            tables.invalidateKey(mapping);
            tables.put(mapping, second);

            Assert.assertNull(snapshots.peekIfPresent(oldSnapshotKey));
            Assert.assertNull(schemas.peekIfPresent(oldSchemaKey));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotHitRefreshesFenceWithoutReloadingProjection() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.doReturn(catalog).when(catalogMgr)
                .getCatalogOrException(Mockito.eq(1L), Mockito.any());
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                return task.call();
            }
        });
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());
            NameMapping mapping = new NameMapping(catalogId, "db", "tbl", "remote_db", "remote_tbl");
            FileStoreTable table = Mockito.mock(FileStoreTable.class);
            FileStoreTable pinnedTable = Mockito.mock(FileStoreTable.class);
            Snapshot latestSnapshot = Mockito.mock(Snapshot.class);
            SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
            TableSchema latestSchema = Mockito.mock(TableSchema.class);
            Mockito.when(table.copyWithLatestSchema()).thenReturn(table);
            Mockito.when(table.latestSnapshot()).thenReturn(Optional.of(latestSnapshot));
            Mockito.when(latestSnapshot.id()).thenReturn(7L);
            Mockito.when(table.schemaManager()).thenReturn(schemaManager);
            Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
            Mockito.when(latestSchema.id()).thenReturn(3L);
            Mockito.when(table.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(pinnedTable);
            PaimonSnapshot fence = new PaimonSnapshot(7L, 3L, pinnedTable);
            PaimonSnapshotCacheValue snapshotValue = new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, fence);
            PaimonTableCacheValue tableValue = new PaimonTableCacheValue(table);
            PaimonSnapshotEntryKey key = PaimonSnapshotEntryKey.of(
                    mapping, fence, tableValue.getGeneration());
            cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, PaimonTableCacheValue.class).put(mapping, tableValue);
            cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class).put(key, snapshotValue);
            ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);

            Assert.assertSame(snapshotValue, cache.getSnapshotCache(dorisTable));
            Assert.assertSame(snapshotValue, cache.getSnapshotCache(dorisTable));
            Assert.assertEquals(7L, cache.loadLatestSnapshotFence(dorisTable).getSnapshot().getSnapshotId());
            Assert.assertEquals(7L, cache.loadLatestSnapshotFence(dorisTable).getSnapshot().getSnapshotId());

            Mockito.verify(table, Mockito.times(4)).copyWithLatestSchema();
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testContextualSnapshotAndSchemaMissesRunAuthenticated() {
        AtomicInteger authenticationDepth = new AtomicInteger();
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                authenticationDepth.incrementAndGet();
                try {
                    return task.call();
                } finally {
                    authenticationDepth.decrementAndGet();
                }
            }
        };
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.doReturn(catalog).when(catalogMgr)
                .getCatalogOrException(Mockito.eq(1L), Mockito.any());
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(1L);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(authenticator);
        Mockito.doReturn(database).when(catalog).getDbNullable("db");
        Mockito.when(database.getTableNullable("tbl")).thenReturn(externalTable);
        Mockito.doReturn(Optional.of(database)).when(catalog).getDb("db");
        Mockito.doReturn(Optional.of(externalTable)).when(database).getTable("tbl");
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue("schema history must be read under authentication",
                    authenticationDepth.get() > 0);
            Column partitionColumn = new Column("part", Type.INT);
            return new PaimonSchemaCacheValue(
                    Collections.singletonList(partitionColumn),
                    Collections.singletonList(partitionColumn), null);
        }).when(externalTable).loadSchemaForCache(Mockito.any(), Mockito.anyLong());

        FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable latestSchemaTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable fenceTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable snapshotTable = Mockito.mock(FileStoreTable.class);
        Snapshot latestSnapshot = Mockito.mock(Snapshot.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        TableSchema latestSchema = Mockito.mock(TableSchema.class);
        ReadBuilder readBuilder = Mockito.mock(ReadBuilder.class);
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(baseTable.copyWithLatestSchema()).thenAnswer(invocation -> {
            Assert.assertTrue("snapshot fence must be read under authentication",
                    authenticationDepth.get() > 0);
            return latestSchemaTable;
        });
        Mockito.when(latestSchemaTable.latestSnapshot()).thenReturn(Optional.of(latestSnapshot));
        Mockito.when(latestSnapshot.id()).thenReturn(7L);
        Mockito.when(latestSchemaTable.schemaManager()).thenReturn(schemaManager);
        Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
        Mockito.when(latestSchema.id()).thenReturn(3L);
        Mockito.when(latestSchemaTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(fenceTable);
        Mockito.when(fenceTable.copyWithoutTimeTravel(Mockito.anyMap())).thenAnswer(invocation -> {
            Assert.assertTrue("snapshot pinning must run under authentication",
                    authenticationDepth.get() > 0);
            return snapshotTable;
        });
        Mockito.when(snapshotTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(snapshotTable.newReadBuilder()).thenAnswer(invocation -> {
            Assert.assertTrue("partition enumeration must run under authentication",
                    authenticationDepth.get() > 0);
            return readBuilder;
        });
        Mockito.when(readBuilder.newScan()).thenReturn(tableScan);
        Mockito.when(tableScan.listPartitionEntries()).thenAnswer(invocation -> {
            Assert.assertTrue("partition manifest access must run under authentication",
                    authenticationDepth.get() > 0);
            return Collections.emptyList();
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            cache.initCatalog(1L, Collections.emptyMap());
            NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
            PaimonTableCacheValue first = new PaimonTableCacheValue(baseTable);
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tables.put(mapping, first);
            ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);

            PaimonSnapshotCacheValue snapshot = cache.getSnapshotCache(dorisTable);

            Assert.assertEquals(7L, snapshot.getSnapshot().getSnapshotId());
            Assert.assertEquals(0, authenticationDepth.get());

            PaimonTableCacheValue second = new PaimonTableCacheValue(baseTable);
            tables.put(mapping, second);
            PaimonSchemaCacheKey staleKey = new PaimonSchemaCacheKey(
                    mapping, first.getGeneration(), 99L);
            cache.getPaimonSchemaCacheValue(mapping, 99L, first.getGeneration(), baseTable);
            Assert.assertNull("a concurrent old-generation schema load must not repopulate the cache",
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class).peekIfPresent(staleKey));
            Assert.assertEquals(0, authenticationDepth.get());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotEstimateSupportsPrivilegedTableWrapper() throws Exception {
        FileStoreTable table = newPartitionedTable("privileged_estimate", Collections.emptyMap());
        FileStoreTable privileged = PrivilegedFileStoreTable.wrap(
                table, Mockito.mock(PrivilegeChecker.class), Identifier.create("db", "tbl"));
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(
                mapping, 1L, table.schema().id(), 1L);
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY,
                new PaimonSnapshot(1L, table.schema().id(), privileged));

        value.prepareForCachePublication(key);

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                value.getSizeEstimate().isComplete());
    }

    @Test
    public void testSnapshotEstimateDoesNotMaterializeNestedRowTypeIndexes() throws Exception {
        RowType nested = DataTypes.ROW(
                DataTypes.FIELD(10, "nested_id", DataTypes.INT()),
                DataTypes.FIELD(11, "nested_name", DataTypes.STRING()));
        TableSchema schema = new TableSchema(
                0,
                java.util.Arrays.asList(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "payload", nested)),
                11,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
        FileStoreTable table = new AppendOnlyFileStoreTable(
                LocalFileIO.create(),
                new Path(temporaryFolder.newFolder("nested_row_estimate").toURI()),
                schema,
                CatalogEnvironment.empty());
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(mapping, 1L, schema.id(), 1L);
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, schema.id(), table));

        Map<String, Object> stateBefore = new HashMap<>();
        for (String fieldName : java.util.Arrays.asList(
                "laziedNameToField", "laziedNameToIndex", "laziedFieldIdToField", "laziedFieldIdToIndex")) {
            stateBefore.put(fieldName, readField(nested, fieldName));
        }
        value.prepareForCachePublication(key);

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(), value.getSizeEstimate().isComplete());
        for (Map.Entry<String, Object> entry : stateBefore.entrySet()) {
            Assert.assertSame(entry.getKey() + " must not be changed by cache admission",
                    entry.getValue(), readField(nested, entry.getKey()));
        }
    }

    @Test
    public void testSnapshotInheritsLegacyTableCountSettingsButNotWeight() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
            Map<String, String> properties = new HashMap<>();
            properties.put("meta.cache.paimon.table.enable", "false");
            properties.put("meta.cache.paimon.table.ttl-second", "17");
            properties.put("meta.cache.paimon.table.capacity", "23");
            cache.initCatalog(1L, properties);

            MetaCacheEntryStats snapshot = cache.stats(1L).get(PaimonExternalMetaCache.ENTRY_SNAPSHOT);
            Assert.assertFalse(snapshot.isConfigEnabled());
            Assert.assertEquals(17L, snapshot.getTtlSecond());
            Assert.assertEquals(23L, snapshot.getCapacity());
            Assert.assertFalse(snapshot.isWeightBounded());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testLatestSnapshotUsesLatestSchemaForPinnedRead() {
        PaimonLatestSnapshotProjectionLoader loader = new PaimonLatestSnapshotProjectionLoader(
                new PaimonPartitionInfoLoader(),
                (nameMapping, schemaId, tableGeneration, retainedTable) -> new PaimonSchemaCacheValue(
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
                (nameMapping, schemaId, tableGeneration, retainedTable) -> new PaimonSchemaCacheValue(
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
                (nameMapping, schemaId, tableGeneration, retainedTable) -> {
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
                (nameMapping, schemaId, tableGeneration, retainedTable) -> new PaimonSchemaCacheValue(
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
                (nameMapping, schemaId, tableGeneration, retainedTable) -> new PaimonSchemaCacheValue(
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
        return newPartitionedTable(name, options, null);
    }

    private FileStoreTable newPartitionedTable(
            String name, Map<String, String> options, String comment) throws Exception {
        TableSchema schema = new TableSchema(
                0,
                java.util.Arrays.asList(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", new IntType())),
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                options,
                comment);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(),
                new Path(temporaryFolder.newFolder(name).toURI()),
                schema,
                CatalogEnvironment.empty());
    }

    private FileStoreTable newStringPartitionedTable(String name) throws Exception {
        TableSchema schema = new TableSchema(
                0,
                java.util.Arrays.asList(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "part", DataTypes.STRING())),
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(),
                new Path(temporaryFolder.newFolder(name).toURI()),
                schema,
                CatalogEnvironment.empty());
    }

    private FileStoreTable newPartitionedTableWithNestedField(
            String name, String nestedFieldName) throws Exception {
        RowType nestedType = new RowType(Collections.singletonList(
                new DataField(2, nestedFieldName, DataTypes.STRING())));
        TableSchema schema = new TableSchema(
                0,
                java.util.Arrays.asList(
                        new DataField(0, "payload", nestedType),
                        new DataField(1, "part", new IntType())),
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(),
                new Path(temporaryFolder.newFolder(name).toURI()),
                schema,
                CatalogEnvironment.empty());
    }

    private FileStoreTable newTableWithExtraFields(String name, int fieldCount) throws Exception {
        ArrayList<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "part", new IntType()));
        for (int index = 0; index < fieldCount; index++) {
            fields.add(new DataField(index + 1, "field_" + index, new IntType()));
        }
        TableSchema schema = new TableSchema(
                0,
                fields,
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(),
                new Path(temporaryFolder.newFolder(name).toURI()),
                schema,
                CatalogEnvironment.empty());
    }

    private FileStoreTable newTableWithNestedFields(String name, int nestedFieldCount) throws Exception {
        ArrayList<DataField> nestedFields = new ArrayList<>();
        for (int index = 0; index < nestedFieldCount; index++) {
            nestedFields.add(new DataField(index + 2, "nested_" + index, new IntType()));
        }
        RowType nestedType = new RowType(nestedFields);
        TableSchema schema = new TableSchema(
                0,
                java.util.Arrays.asList(
                        new DataField(0, "payload", nestedType),
                        new DataField(1, "part", new IntType())),
                1,
                Collections.singletonList("part"),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(), new Path(temporaryFolder.newFolder(name).toURI()),
                schema, CatalogEnvironment.empty());
    }

    private FileStoreTable newTableWithOptions(String name, int optionCount) throws Exception {
        Map<String, String> options = new HashMap<>();
        for (int index = 0; index < optionCount; index++) {
            options.put("key_" + index, "value_" + index);
        }
        return newPartitionedTable(name, options);
    }

    private FileStoreTable newTableWithPayloadType(String name, DataType type) {
        TableSchema schema = new TableSchema(
                0, Collections.singletonList(new DataField(0, "payload", type)), 1,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null);
        return new AppendOnlyFileStoreTable(
                LocalFileIO.create(), new Path("file:/tmp/paimon-composite-" + name),
                schema, CatalogEnvironment.empty());
    }

    private DataType nestedArrayType(int depth) {
        DataType type = new IntType();
        for (int index = 0; index < depth; index++) {
            type = new ArrayType(type);
        }
        return type;
    }

    private DataType nestedMapType(int depth) {
        DataType type = new IntType();
        for (int index = 0; index < depth; index++) {
            type = new MapType(new IntType(), type);
        }
        return type;
    }

    private DataType nestedMultisetType(int depth) {
        DataType type = new IntType();
        for (int index = 0; index < depth; index++) {
            type = new MultisetType(type);
        }
        return type;
    }

    private DataType nestedRowType(int depth) {
        DataType type = new IntType();
        for (int index = 0; index < depth; index++) {
            type = new RowType(Collections.singletonList(
                    new DataField(index + 1, "nested_" + index, type)));
        }
        return type;
    }

    private void assertTableDeltaAgainstJol(
            String fixture, FileStoreTable smallTable, FileStoreTable populatedTable) {
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey smallKey = new PaimonSnapshotEntryKey(
                mapping, 1L, smallTable.schema().id(), 1L);
        PaimonSnapshotEntryKey populatedKey = new PaimonSnapshotEntryKey(
                mapping, 1L, populatedTable.schema().id(), 1L);
        PaimonSnapshotCacheValue small = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY,
                new PaimonSnapshot(1L, smallTable.schema().id(), smallTable));
        PaimonSnapshotCacheValue populated = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY,
                new PaimonSnapshot(1L, populatedTable.schema().id(), populatedTable));
        long smallEstimate = small.prepareForCachePublication(smallKey).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(populatedKey).getBytes();
        // The estimate reserves the lookup maps every nested RowType can materialize after
        // admission, so the JOL oracle measures the fully grown graph.
        materializeRowTypeIndexes(smallTable.schema());
        materializeRowTypeIndexes(populatedTable.schema());
        EstimatorCalibrationAssertions.assertConservativeDelta(
                fixture, smallEstimate, populatedEstimate, small, populated);
    }

    private static final String[] ROW_TYPE_LAZY_FIELDS = {
            "laziedNameToField", "laziedNameToIndex", "laziedFieldIdToField", "laziedFieldIdToIndex"};

    private void materializeRowTypeIndexes(TableSchema schema) {
        for (DataField field : schema.fields()) {
            materializeRowTypeIndexes(field.type());
        }
    }

    private void materializeRowTypeIndexes(DataType type) {
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            if (!rowType.getFields().isEmpty()) {
                DataField first = rowType.getFields().get(0);
                rowType.getField(first.name());
                rowType.getFieldIndex(first.name());
                rowType.getField(first.id());
                rowType.getFieldIndexByFieldId(first.id());
            }
            for (DataField field : rowType.getFields()) {
                materializeRowTypeIndexes(field.type());
            }
        } else if (type instanceof ArrayType) {
            materializeRowTypeIndexes(((ArrayType) type).getElementType());
        } else if (type instanceof MultisetType) {
            materializeRowTypeIndexes(((MultisetType) type).getElementType());
        } else if (type instanceof MapType) {
            materializeRowTypeIndexes(((MapType) type).getKeyType());
            materializeRowTypeIndexes(((MapType) type).getValueType());
        } else if (type instanceof VectorType) {
            materializeRowTypeIndexes(((VectorType) type).getElementType());
        }
    }

    private RowType wideRowType(int fieldCount) {
        ArrayList<DataField> fields = new ArrayList<>();
        for (int index = 0; index < fieldCount; index++) {
            fields.add(new DataField(1000 + index, "wide_" + index, new IntType()));
        }
        return new RowType(fields);
    }

    private DataType rowOfLeafTypes(int fieldCount, Class<? extends DataType> leafType) {
        ArrayList<DataField> fields = new ArrayList<>();
        for (int index = 0; index < fieldCount; index++) {
            DataType type = leafType == VectorType.class
                    ? new VectorType(4, new FloatType()) : new DecimalType(10, 2);
            fields.add(new DataField(index + 1, "leaf_" + index, type));
        }
        return new RowType(fields);
    }

    private long snapshotWeight(PaimonSnapshotEntryKey key, FileStoreTable table, int partitionCount) {
        PaimonPartitionInfo partitionInfo = Mockito.mock(PaimonPartitionInfo.class);
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = sizeOnlyMap(partitionCount);
        Map<String, org.apache.paimon.partition.Partition> partitions = sizeOnlyMap(partitionCount);
        Mockito.when(partitionInfo.getNameToPartitionItem()).thenReturn(partitionItems);
        Mockito.when(partitionInfo.getNameToPartition()).thenReturn(partitions);
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                partitionInfo, new PaimonSnapshot(1L, table.schema().id(), table));
        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(key);
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    private PaimonSnapshotCacheValue snapshotValueWithRealPartitions(
            FileStoreTable table, int partitionCount, int valueLength, Type partitionType)
            throws AnalysisException {
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = new HashMap<>();
        Map<String, org.apache.paimon.partition.Partition> partitions = new HashMap<>();
        for (int index = 0; index < partitionCount; index++) {
            String value = partitionType == Type.INT
                    ? Integer.toString(index)
                    : "p" + index + repeatedCharacter('x', valueLength);
            String name = "part=" + value;
            partitionItems.put(name, PaimonUtil.toListPartitionItem(
                    Collections.singletonList(value), Collections.singletonList(partitionType)));
            partitions.put(name, new org.apache.paimon.partition.Partition(
                    Collections.singletonMap("part", value),
                    100L, 1024L, 1L, 1L, 1, true));
        }
        PaimonPartitionInfo partitionInfo = new PaimonPartitionInfo(partitionItems, partitions);
        return new PaimonSnapshotCacheValue(
                partitionInfo, new PaimonSnapshot(1L, table.schema().id(), table));
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> sizeOnlyMap(int size) {
        Map<K, V> map = Mockito.mock(Map.class);
        Mockito.when(map.size()).thenReturn(size);
        return map;
    }

    private Object readField(RowType rowType, String fieldName) throws Exception {
        return readField(rowType, RowType.class, fieldName);
    }

    private static String repeatedCharacter(char character, int count) {
        char[] characters = new char[count];
        java.util.Arrays.fill(characters, character);
        return new String(characters);
    }

    private Object readField(Object target, Class<?> owner, String fieldName) throws Exception {
        Field field = owner.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
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
            PaimonSnapshotCacheValue fence = new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(-1L, 0L, null));
            tableEntry.put(t1, new PaimonTableCacheValue(null, fence));
            tableEntry.put(t2, new PaimonTableCacheValue(null, fence));

            PaimonSnapshotEntryKey snapshotKey = new PaimonSnapshotEntryKey(t1, 1L, 2L, 1L);
            org.apache.doris.datasource.metacache.MetaCacheEntry<
                    PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshotEntry = cache.entry(catalogId,
                    PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class);
            snapshotEntry.put(snapshotKey, new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 2L, null)));

            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(tableEntry.getIfPresent(t1));
            Assert.assertNotNull(tableEntry.getIfPresent(t2));
            Assert.assertNull(snapshotEntry.getIfPresent(snapshotKey));
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
            PaimonSnapshotCacheValue fence = new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(-1L, 0L, null));
            tableEntry.put(db1Table, new PaimonTableCacheValue(null, fence));
            tableEntry.put(db2Table, new PaimonTableCacheValue(null, fence));

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
