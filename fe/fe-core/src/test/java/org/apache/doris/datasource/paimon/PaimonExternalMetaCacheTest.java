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
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
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
import java.util.List;
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

        // Every partition column beyond the first adds a literal to each ListPartitionItem key
        // and an entry to the Partition spec; the estimate scales with the loaded width.
        PaimonSnapshotCacheValue wideInts = snapshotValueWithRealPartitions(
                intTable, 32, 0, Type.INT, 3);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon wide snapshot partitions",
                populatedInts.prepareForCachePublication(intKey).getBytes(),
                wideInts.prepareForCachePublication(intKey).getBytes(),
                populatedInts, wideInts);
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
        materializeStoreGraph(emptyTable);
        materializeStoreGraph(populatedTable);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon table fields", emptyEstimate, populatedEstimate, empty, populated);
    }

    @Test
    public void testStoreGraphFormulaAgainstJolOwnedGraph() throws Exception {
        // AppendOnlyFileStore copies every field for its non-null row type; KeyValueFileStore
        // copies the primary-key fields and shares the rest. Both derive RowTypes with lazy
        // lookup maps and copy the table options; the estimate must cover them after
        // newReadBuilder().newScan() runs on the admitted table.
        assertTableDeltaAgainstJol("paimon append store fields",
                newTableWithExtraFields("jol_store_append_narrow", 10, false, 0),
                newTableWithExtraFields("jol_store_append_wide", 300, false, 0));
        assertTableDeltaAgainstJol("paimon primary-key store fields",
                newTableWithExtraFields("jol_store_pk_narrow", 10, true, 0),
                newTableWithExtraFields("jol_store_pk_wide", 300, true, 0));
        assertTableDeltaAgainstJol("paimon store options",
                newTableWithExtraFields("jol_store_options_none", 10, false, 0),
                newTableWithExtraFields("jol_store_options_many", 10, false, 100));
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
    public void testUnknownDataTypeIsChargedGenerically() {
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
        // Unknown logical types receive the generic per-node weight instead of disabling
        // weighted caching for the table.
        FileStoreTable table = newTableWithPayloadType("unknown-type", unknownType);
        Assert.assertTrue(PaimonCacheSizeEstimator.retainedTablePayloadBytes(table) > 0L);

        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotEntryKey key = new PaimonSnapshotEntryKey(mapping, 1L, table.schema().id(), 1L);
        PaimonSnapshotCacheValue value = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, table.schema().id(), table));
        MetaCacheSizeEstimate estimate = value.prepareForCachePublication(key);

        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        Assert.assertTrue(estimate.getBytes() > 0L);
        Assert.assertSame(table, value.getSnapshot().getTable());
    }

    @Test
    public void testTableEntryWeightCoversBaseOnlyScanAndReleasesOnInvalidate() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.singletonMap(
                    "meta.cache.paimon.table.max-weight", "8MB"));
            Assert.assertTrue(cache.stats(catalogId).get(PaimonExternalMetaCache.ENTRY_TABLE).isWeightBounded());

            NameMapping mapping = new NameMapping(catalogId, "db", "tbl", "db", "tbl");
            FileStoreTable table = newTableWithExtraFields("table_entry_weight", 64, true, 8);
            Object lazyStoreBefore = readField(table, table.getClass(), "lazyStore");
            PaimonTableCacheValue value = new PaimonTableCacheValue(table);
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            tables.put(mapping, value);

            Assert.assertSame(value, tables.peekIfPresent(mapping));
            Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                    value.getSizeEstimate().isComplete());
            long estimate = value.getSizeEstimate().getBytes();
            Assert.assertTrue(estimate > 0L);
            MetaCacheEntryStats stats = cache.stats(catalogId).get(PaimonExternalMetaCache.ENTRY_TABLE);
            // The entry adds a fixed per-record overhead on top of the value estimate.
            long reservedWeight = stats.getEstimatedWeight();
            Assert.assertTrue(reservedWeight >= estimate);
            Assert.assertSame("table publication must not materialize FileStoreTable.store()",
                    lazyStoreBefore, readField(table, table.getClass(), "lazyStore"));

            // A base-only scan (fetchRowCount, table-only paths) opens the store and its RowType
            // indexes on the admitted handle; the reserved weight already covers that graph.
            long beforeScan = EstimatorCalibrationAssertions.graphSize(value);
            materializeStoreGraph(table);
            materializeRowTypeIndexes(table.schema());
            long afterScan = EstimatorCalibrationAssertions.graphSize(value);
            Assert.assertTrue("scan must grow the retained graph", afterScan > beforeScan);
            Assert.assertTrue("estimate " + estimate + " must cover the grown graph " + afterScan,
                    estimate >= afterScan);
            Assert.assertEquals(reservedWeight,
                    cache.stats(catalogId).get(PaimonExternalMetaCache.ENTRY_TABLE).getEstimatedWeight());

            tables.invalidateKey(mapping);
            Assert.assertNull(tables.peekIfPresent(mapping));
            Assert.assertEquals(0L,
                    cache.stats(catalogId).get(PaimonExternalMetaCache.ENTRY_TABLE).getEstimatedWeight());

            // Unsupported table implementations fail closed and stay outside the cache.
            PaimonTableCacheValue unsupported = new PaimonTableCacheValue(Mockito.mock(Table.class));
            tables.put(mapping, unsupported);
            Assert.assertNull(tables.peekIfPresent(mapping));
            Assert.assertFalse(unsupported.getSizeEstimate().isComplete());
            Assert.assertTrue(unsupported.getSizeEstimate().getIncompleteReason()
                    .startsWith("unsupported_paimon_table:"));
            MetaCacheEntryStats rejected = cache.stats(catalogId).get(PaimonExternalMetaCache.ENTRY_TABLE);
            Assert.assertEquals(1L, rejected.getWeightAdmissionRejectedCount());
            Assert.assertEquals(0L, rejected.getEstimatedWeight());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testTableEntryFormulaAgainstJolOwnedGraph() throws Exception {
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        FileStoreTable smallTable = newTableWithExtraFields("jol_table_entry_narrow", 10, true, 0);
        FileStoreTable populatedTable = newTableWithExtraFields("jol_table_entry_wide", 300, true, 50);
        PaimonTableCacheValue small = new PaimonTableCacheValue(smallTable);
        PaimonTableCacheValue populated = new PaimonTableCacheValue(populatedTable);
        long smallEstimate = small.prepareForCachePublication(mapping).getBytes();
        long populatedEstimate = populated.prepareForCachePublication(mapping).getBytes();
        materializeRowTypeIndexes(smallTable.schema());
        materializeRowTypeIndexes(populatedTable.schema());
        materializeStoreGraph(smallTable);
        materializeStoreGraph(populatedTable);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                "paimon table entry", smallEstimate, populatedEstimate, small, populated);
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
    public void testDeepAndBroadContainerTreesAreBoundedAtAdmission() {
        // A container-only chain deeper than the structural guard must reject weighted
        // admission (fail closed) without failing the load or overflowing the stack.
        FileStoreTable deepTable = newTableWithPayloadType("deep-chain", nestedArrayType(400));
        NameMapping mapping = new NameMapping(1L, "db", "tbl", "db", "tbl");
        PaimonSnapshotCacheValue deepValue = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, deepTable.schema().id(), deepTable));
        MetaCacheSizeEstimate deepEstimate = deepValue.prepareForCachePublication(
                new PaimonSnapshotEntryKey(mapping, 1L, deepTable.schema().id(), 1L));
        Assert.assertFalse(deepEstimate.isComplete());

        // A broad container tree with few fields but far more nodes than the element budget
        // must also reject admission instead of doing unbounded publication work.
        DataType broad = new IntType();
        for (int level = 0; level < 17; level++) {
            broad = new MapType(broad.copy(true), broad);
        }
        FileStoreTable broadTable = newTableWithPayloadType("broad-tree", broad);
        PaimonSnapshotCacheValue broadValue = new PaimonSnapshotCacheValue(
                PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, broadTable.schema().id(), broadTable));
        MetaCacheSizeEstimate broadEstimate = broadValue.prepareForCachePublication(
                new PaimonSnapshotEntryKey(mapping, 1L, broadTable.schema().id(), 1L));
        Assert.assertFalse(broadEstimate.isComplete());
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
                newTableWithPayloadType("row-unload", wideRowType(200)));
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
    public void testExpiredBaseTableRetiresSnapshotAndSchemaProjectionsBeforeReplacement() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try {
            long catalogId = 1L;
            Map<String, String> properties = new HashMap<>();
            properties.put("meta.cache.paimon.table.ttl-second", "1");
            properties.put("meta.cache.paimon.snapshot.ttl-second", "3600");
            cache.initCatalog(catalogId, properties);
            NameMapping mapping = new NameMapping(catalogId, "db", "tbl", "db", "tbl");
            PaimonTableCacheValue table = new PaimonTableCacheValue(Mockito.mock(Table.class));
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<
                    PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots = cache.entry(
                    catalogId, PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemas =
                    cache.entry(catalogId, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class);
            tables.put(mapping, table);
            PaimonSnapshotEntryKey snapshotKey = new PaimonSnapshotEntryKey(mapping, 1L, 2L, table.getGeneration());
            PaimonSchemaCacheKey schemaKey = new PaimonSchemaCacheKey(mapping, table.getGeneration(), 2L);
            snapshots.put(snapshotKey, new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 2L, table.getPaimonTable())));
            schemas.put(schemaKey, new SchemaCacheValue(Collections.emptyList()));

            Thread.sleep(1_500L);
            com.github.benmanes.caffeine.cache.Cache<?, ?> caffeine =
                    (com.github.benmanes.caffeine.cache.Cache<?, ?>) readField(
                            tables, org.apache.doris.datasource.metacache.MetaCacheEntry.class, "loadingData");
            caffeine.cleanUp();
            Assert.assertNull("idle table handle must expire", tables.peekIfPresent(mapping));

            // Expiry is a plain removal with no successor published: the delayed removal callback
            // alone retires the projections keyed by the expired generation.
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
            while ((snapshots.peekIfPresent(snapshotKey) != null || schemas.peekIfPresent(schemaKey) != null)
                    && System.nanoTime() < deadline) {
                Thread.sleep(20L);
            }
            Assert.assertNull(snapshots.peekIfPresent(snapshotKey));
            Assert.assertNull(schemas.peekIfPresent(schemaKey));
            Assert.assertNull(tables.peekIfPresent(mapping));

            // A successor published afterwards keeps its own projections.
            PaimonTableCacheValue next = new PaimonTableCacheValue(Mockito.mock(Table.class));
            tables.put(mapping, next);
            PaimonSnapshotEntryKey nextSnapshotKey = new PaimonSnapshotEntryKey(mapping, 1L, 2L, next.getGeneration());
            snapshots.put(nextSnapshotKey, new PaimonSnapshotCacheValue(
                    PaimonPartitionInfo.EMPTY, new PaimonSnapshot(1L, 2L, next.getPaimonTable())));
            Assert.assertNotNull(snapshots.peekIfPresent(nextSnapshotKey));
            Assert.assertSame(next, tables.peekIfPresent(mapping));
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    /** Mocked catalog/env/table graph for the base-table + projection flows. */
    private static final class MockedPaimonCatalog {
        private final Env env = Mockito.mock(Env.class);
        private final PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        private final FileStoreTable baseTable = Mockito.mock(FileStoreTable.class);
        private final java.util.concurrent.atomic.AtomicLong latestSnapshotId =
                new java.util.concurrent.atomic.AtomicLong(7L);
        private final NameMapping mapping = new NameMapping(1L, "db", "tbl", "remote_db", "remote_tbl");
        private final AtomicInteger partitionEnumerations = new AtomicInteger();
        private final AtomicInteger schemaLoads = new AtomicInteger();
        // When set, the next partition enumeration signals enumerationEntered and blocks on it.
        private volatile java.util.concurrent.CountDownLatch blockNextEnumeration;
        private final java.util.concurrent.CountDownLatch enumerationEntered =
                new java.util.concurrent.CountDownLatch(1);

        private MockedPaimonCatalog() {
            PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
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
            Mockito.doReturn(database).when(catalog).getDbNullable("db");
            Mockito.when(database.getTableNullable("tbl")).thenReturn(externalTable);
            Mockito.doReturn(Optional.of(database)).when(catalog).getDb("db");
            Mockito.doReturn(Optional.of(externalTable)).when(database).getTable("tbl");
            Column partitionColumn = new Column("part", Type.INT);
            Mockito.doAnswer(invocation -> {
                schemaLoads.incrementAndGet();
                return new PaimonSchemaCacheValue(
                        Collections.singletonList(partitionColumn),
                        Collections.singletonList(partitionColumn), null);
            }).when(externalTable).loadSchemaForCache(Mockito.any(), Mockito.anyLong());

            FileStoreTable latestSchemaTable = Mockito.mock(FileStoreTable.class);
            FileStoreTable fenceTable = Mockito.mock(FileStoreTable.class);
            FileStoreTable snapshotTable = Mockito.mock(FileStoreTable.class);
            Snapshot latestSnapshot = Mockito.mock(Snapshot.class);
            SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
            TableSchema latestSchema = Mockito.mock(TableSchema.class);
            ReadBuilder readBuilder = Mockito.mock(ReadBuilder.class);
            TableScan tableScan = Mockito.mock(TableScan.class);
            Mockito.when(baseTable.copyWithLatestSchema()).thenReturn(latestSchemaTable);
            Mockito.when(latestSchemaTable.latestSnapshot()).thenReturn(Optional.of(latestSnapshot));
            Mockito.when(latestSnapshot.id()).thenAnswer(invocation -> latestSnapshotId.get());
            Mockito.when(latestSchemaTable.schemaManager()).thenReturn(schemaManager);
            Mockito.when(schemaManager.latest()).thenReturn(Optional.of(latestSchema));
            Mockito.when(latestSchema.id()).thenReturn(3L);
            Mockito.when(latestSchemaTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(fenceTable);
            Mockito.when(fenceTable.copyWithoutTimeTravel(Mockito.anyMap())).thenReturn(snapshotTable);
            Mockito.when(snapshotTable.options()).thenReturn(Collections.emptyMap());
            Mockito.when(snapshotTable.newReadBuilder()).thenReturn(readBuilder);
            Mockito.when(readBuilder.newScan()).thenReturn(tableScan);
            Mockito.when(tableScan.listPartitionEntries()).thenAnswer(invocation -> {
                partitionEnumerations.incrementAndGet();
                java.util.concurrent.CountDownLatch block = blockNextEnumeration;
                if (block != null) {
                    blockNextEnumeration = null;
                    enumerationEntered.countDown();
                    Assert.assertTrue(block.await(5L, java.util.concurrent.TimeUnit.SECONDS));
                }
                return Collections.emptyList();
            });
            Mockito.when(catalog.getPaimonTable(mapping)).thenReturn(baseTable);
        }

        private ExternalTable dorisTable() {
            ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);
            return dorisTable;
        }
    }

    @Test
    public void testRejectedBaseTableDoesNotAccumulateSnapshotOrSchemaProjections() {
        // A mocked table has no supported layout, so its weight estimate is incomplete and every
        // load is rejected by the weight-bounded table entry.
        MockedPaimonCatalog mocked = new MockedPaimonCatalog();
        NameMapping mapping = mocked.mapping;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(mocked.env);
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.paimon.table.max-weight", "1MB"));
            ExternalTable dorisTable = mocked.dorisTable();
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<
                    PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots = cache.entry(
                    1L, PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemas =
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class);

            for (int i = 0; i < 5; i++) {
                Assert.assertEquals(7L, cache.getSnapshotCache(dorisTable).getSnapshot().getSnapshotId());
                Assert.assertNotNull(cache.getPaimonSchemaCacheValue(mapping, 3L));
                Assert.assertNull("rejected table handle must not be published", tables.peekIfPresent(mapping));
                Assert.assertEquals("projections of an unpublished generation must be retired",
                        0L, snapshots.stats().getEstimatedSize());
                Assert.assertEquals(0L, schemas.stats().getEstimatedSize());
                Assert.assertEquals("rejected generations must not retain latest-fence owners",
                        0, observedFenceOwnerCount(cache));
            }
            Assert.assertEquals(10L, tables.stats().getWeightAdmissionRejectedCount());
            Mockito.verify(mocked.catalog, Mockito.times(10)).getPaimonTable(mapping);
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testIneffectiveBaseTableServesProjectionsWithoutChildCaching() {
        // table.max-weight=0 (and table.enable=false with snapshot re-enabled) leave the base entry
        // ineffective while the children report enabled: nothing keyed by an unpublished
        // generation is reachable, so the children are bypassed instead of loaded and discarded.
        assertIneffectiveBaseBypassesChildren(Collections.singletonMap("meta.cache.paimon.table.max-weight", "0"));
        Map<String, String> explicitSnapshot = new HashMap<>();
        explicitSnapshot.put("meta.cache.paimon.table.enable", "false");
        explicitSnapshot.put("meta.cache.paimon.snapshot.enable", "true");
        assertIneffectiveBaseBypassesChildren(explicitSnapshot);
    }

    private void assertIneffectiveBaseBypassesChildren(Map<String, String> catalogProperties) {
        MockedPaimonCatalog mocked = new MockedPaimonCatalog();
        NameMapping mapping = mocked.mapping;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(mocked.env);
            cache.initCatalog(1L, catalogProperties);
            ExternalTable dorisTable = mocked.dorisTable();
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<
                    PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots = cache.entry(
                    1L, PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<PaimonSchemaCacheKey, SchemaCacheValue> schemas =
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_SCHEMA,
                            PaimonSchemaCacheKey.class, SchemaCacheValue.class);
            Assert.assertFalse(tables.isEffectivelyEnabled());
            Assert.assertTrue(snapshots.isEffectivelyEnabled());
            Assert.assertTrue(schemas.isEffectivelyEnabled());

            for (int i = 0; i < 3; i++) {
                Assert.assertEquals(7L, cache.getSnapshotCache(dorisTable).getSnapshot().getSnapshotId());
                Assert.assertNotNull(cache.getPaimonSchemaCacheValue(mapping, 3L));
                Assert.assertNull(tables.peekIfPresent(mapping));
                Assert.assertEquals(0L, snapshots.stats().getEstimatedSize());
                Assert.assertEquals(0L, schemas.stats().getEstimatedSize());
            }
            Assert.assertEquals("no projection was ever admitted", 0L, snapshots.stats().getInvalidateCount());
            Assert.assertEquals(0L, schemas.stats().getInvalidateCount());
            Assert.assertTrue("no admission is attempted", tables.stats().getWeightAdmissionRejectedCount() <= 0L);
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testAdvancingLatestFenceKeepsOnlyNewestProjectionOfTableGeneration() throws Exception {
        MockedPaimonCatalog mocked = new MockedPaimonCatalog();
        NameMapping mapping = mocked.mapping;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PaimonExternalMetaCache cache = new PaimonExternalMetaCache(executor);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(mocked.env);
            cache.initCatalog(1L, Collections.emptyMap());
            ExternalTable dorisTable = mocked.dorisTable();
            org.apache.doris.datasource.metacache.MetaCacheEntry<NameMapping, PaimonTableCacheValue> tables =
                    cache.entry(1L, PaimonExternalMetaCache.ENTRY_TABLE,
                            NameMapping.class, PaimonTableCacheValue.class);
            org.apache.doris.datasource.metacache.MetaCacheEntry<
                    PaimonSnapshotEntryKey, PaimonSnapshotCacheValue> snapshots = cache.entry(
                    1L, PaimonExternalMetaCache.ENTRY_SNAPSHOT,
                    PaimonSnapshotEntryKey.class, PaimonSnapshotCacheValue.class);

            PaimonSnapshotCacheValue at7 = cache.getSnapshotCache(dorisTable);
            PaimonTableCacheValue tableValue = tables.peekIfPresent(mapping);
            Assert.assertNotNull(tableValue);
            PaimonSnapshotEntryKey key7 = new PaimonSnapshotEntryKey(mapping, 7L, 3L, tableValue.getGeneration());
            Assert.assertSame(at7, snapshots.peekIfPresent(key7));
            Assert.assertSame(at7, cache.getSnapshotCache(dorisTable));

            // Commits observed before the table handle refreshes advance the fence: only the
            // newest projection of this generation stays reachable.
            mocked.latestSnapshotId.set(8L);
            PaimonSnapshotCacheValue at8 = cache.getSnapshotCache(dorisTable);
            PaimonSnapshotEntryKey key8 = new PaimonSnapshotEntryKey(mapping, 8L, 3L, tableValue.getGeneration());
            Assert.assertEquals(8L, at8.getSnapshot().getSnapshotId());
            Assert.assertNull(snapshots.peekIfPresent(key7));
            Assert.assertSame(at8, snapshots.peekIfPresent(key8));
            Assert.assertEquals(1L, snapshots.stats().getEstimatedSize());
            Assert.assertSame("the table handle itself is not replaced", tableValue, tables.peekIfPresent(mapping));

            // Reversed completion: a call that observed fence 8 is still enumerating partitions
            // while a later call observes fence 9 and finishes first. The most recently observed
            // fence wins; the older load must not survive next to it.
            snapshots.invalidateKey(key8);
            java.util.concurrent.CountDownLatch releaseOlderLoad = new java.util.concurrent.CountDownLatch(1);
            mocked.blockNextEnumeration = releaseOlderLoad;
            ExecutorService olderCall = Executors.newSingleThreadExecutor();
            java.util.concurrent.Future<PaimonSnapshotCacheValue> older;
            PaimonSnapshotEntryKey key9 = new PaimonSnapshotEntryKey(mapping, 9L, 3L, tableValue.getGeneration());
            try {
                older = olderCall.submit(() -> {
                    try (MockedStatic<Env> workerEnv = Mockito.mockStatic(Env.class)) {
                        workerEnv.when(Env::getCurrentEnv).thenReturn(mocked.env);
                        return cache.getSnapshotCache(dorisTable);
                    }
                });
                Assert.assertTrue(mocked.enumerationEntered.await(5L, java.util.concurrent.TimeUnit.SECONDS));
                mocked.latestSnapshotId.set(9L);
                PaimonSnapshotCacheValue at9 = cache.getSnapshotCache(dorisTable);
                Assert.assertSame(at9, snapshots.peekIfPresent(key9));
                releaseOlderLoad.countDown();
                Assert.assertEquals(8L, older.get(5L, java.util.concurrent.TimeUnit.SECONDS)
                        .getSnapshot().getSnapshotId());
                Assert.assertNull(snapshots.peekIfPresent(key8));
                Assert.assertSame(at9, snapshots.peekIfPresent(key9));
                Assert.assertEquals(1L, snapshots.stats().getEstimatedSize());
            } finally {
                releaseOlderLoad.countDown();
                olderCall.shutdownNow();
            }

            // Rollback: the latest snapshot moves backwards; the newly observed fence replaces the
            // projection of the higher snapshot id instead of being retired by it.
            mocked.latestSnapshotId.set(8L);
            PaimonSnapshotCacheValue rolledBack = cache.getSnapshotCache(dorisTable);
            Assert.assertEquals(8L, rolledBack.getSnapshot().getSnapshotId());
            Assert.assertSame(rolledBack, snapshots.peekIfPresent(key8));
            Assert.assertNull(snapshots.peekIfPresent(key9));
            Assert.assertSame(rolledBack, cache.getSnapshotCache(dorisTable));
            Assert.assertEquals(1L, snapshots.stats().getEstimatedSize());
            Assert.assertEquals(5, mocked.partitionEnumerations.get());
            Assert.assertEquals("the published generation keeps exactly one latest-fence owner",
                    1, observedFenceOwnerCount(cache));
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
        return newTableWithExtraFields(name, fieldCount, false, 0);
    }

    private FileStoreTable newTableWithExtraFields(
            String name, int fieldCount, boolean primaryKey, int optionCount) throws Exception {
        ArrayList<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "part", new IntType()));
        for (int index = 0; index < fieldCount; index++) {
            fields.add(new DataField(index + 1, "field_" + index, new IntType()));
        }
        if (primaryKey) {
            fields.add(new DataField(fieldCount + 1, "id", new IntType(false)));
        }
        Map<String, String> options = new HashMap<>();
        for (int index = 0; index < optionCount; index++) {
            options.put("option_" + index, "value_" + index);
        }
        TableSchema schema = new TableSchema(
                0,
                fields,
                fields.size(),
                Collections.singletonList("part"),
                primaryKey ? java.util.Arrays.asList("id", "part") : Collections.emptyList(),
                options,
                null);
        Path location = new Path(temporaryFolder.newFolder(name).toURI());
        return primaryKey
                ? new PrimaryKeyFileStoreTable(LocalFileIO.create(), location, schema, CatalogEnvironment.empty())
                : new AppendOnlyFileStoreTable(LocalFileIO.create(), location, schema, CatalogEnvironment.empty());
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
        // admission and the store graph scan planning creates, so the JOL oracle measures the
        // fully grown graph.
        materializeRowTypeIndexes(smallTable.schema());
        materializeRowTypeIndexes(populatedTable.schema());
        materializeStoreGraph(smallTable);
        materializeStoreGraph(populatedTable);
        EstimatorCalibrationAssertions.assertConservativeDelta(
                fixture, smallEstimate, populatedEstimate, small, populated);
    }

    /** What scan planning materializes after admission: the store and its RowType indexes. */
    private void materializeStoreGraph(FileStoreTable table) {
        table.newReadBuilder().newScan();
        try {
            Object store = readField(table, table.getClass(), "lazyStore");
            for (Class<?> owner = store.getClass(); owner != null && owner != Object.class;
                    owner = owner.getSuperclass()) {
                for (Field field : owner.getDeclaredFields()) {
                    if (RowType.class.isAssignableFrom(field.getType())) {
                        field.setAccessible(true);
                        Object rowType = field.get(store);
                        if (rowType != null) {
                            materializeRowTypeIndexes((RowType) rowType);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
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
        return snapshotValueWithRealPartitions(table, partitionCount, valueLength, partitionType, 1);
    }

    private PaimonSnapshotCacheValue snapshotValueWithRealPartitions(
            FileStoreTable table, int partitionCount, int valueLength, Type partitionType,
            int partitionColumnCount) throws AnalysisException {
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = new HashMap<>();
        Map<String, org.apache.paimon.partition.Partition> partitions = new HashMap<>();
        // Production typed specs key the schema-owned column names: one shared String reference
        // across every partition, so the fixture must share them too.
        String[] columnNames = new String[partitionColumnCount];
        for (int column = 0; column < partitionColumnCount; column++) {
            columnNames[column] = new String(("part" + column).toCharArray());
        }
        for (int index = 0; index < partitionCount; index++) {
            String value = partitionType == Type.INT
                    ? Integer.toString(index)
                    : "p" + index + repeatedCharacter('x', valueLength);
            String name = "part=" + value;
            List<String> values = new ArrayList<>();
            List<Type> types = new ArrayList<>();
            Map<String, String> spec = new java.util.LinkedHashMap<>();
            for (int column = 0; column < partitionColumnCount; column++) {
                // Each loaded column owns its own value String.
                String columnValue = new String(value.toCharArray());
                values.add(columnValue);
                types.add(partitionType);
                spec.put(columnNames[column], columnValue);
            }
            partitionItems.put(name, PaimonUtil.toListPartitionItem(values, types));
            partitions.put(name, new org.apache.paimon.partition.Partition(
                    spec, 100L, 1024L, 1L, 1L, 1, true));
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

    private int observedFenceOwnerCount(PaimonExternalMetaCache cache) {
        try {
            return ((java.util.Map<?, ?>) readField(
                    cache, PaimonExternalMetaCache.class, "latestObservedFences")).size();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
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
