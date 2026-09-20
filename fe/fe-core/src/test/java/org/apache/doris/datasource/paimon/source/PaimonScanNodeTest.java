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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonMvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.datasource.paimon.PaimonReaderOptions;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonUtils;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPaimonReaderType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableMap;
import mockit.MockUp;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class PaimonScanNodeTest {
    private boolean originalEnableVariantV2;

    @Mock
    private SessionVariable sv;

    @Mock
    private PaimonFileExternalCatalog paimonFileExternalCatalog;

    @Before
    public void saveVariantV2Config() {
        originalEnableVariantV2 = Config.enable_variant_v2;
        // Statement-scoped scan task reuse is on by default; the mock's field would otherwise
        // read false and bypass the cache under test.
        sv.enableExternalScanTaskReuse = true;
        // These routing fixtures bypass scan initialization and use an upgraded eligible BE.
        Backend backend = GsonUtils.GSON.fromJson("{\"supportsPaimonRustReader\":true}", Backend.class);
        new MockUp<FederationBackendPolicy>() {
            @mockit.Mock
            public Collection<Backend> getBackends() {
                return Collections.singletonList(backend);
            }
        };
    }

    @After
    public void restoreVariantV2Config() {
        Config.enable_variant_v2 = originalEnableVariantV2;
    }

    @Test
    public void testDoesNotUseHiveParquetInt96TimeZone() {
        PaimonScanNode node = Mockito.mock(PaimonScanNode.class, Mockito.CALLS_REAL_METHODS);
        Assert.assertEquals("", node.getHiveParquetTimeZone());
    }

    @Test
    public void testVariantProjectionRequiresVariantV2Recursively() throws UserException {
        List<Type> variantTypes = Arrays.asList(
                VariantType.COMPUTE_V2_INSTANCE,
                new ArrayType(VariantType.COMPUTE_V2_INSTANCE),
                new MapType(Type.STRING, VariantType.COMPUTE_V2_INSTANCE),
                new StructType(new StructField("payload", VariantType.COMPUTE_V2_INSTANCE)));

        for (Type variantType : variantTypes) {
            assertVariantProjectionRequiresVariantV2(variantType);
        }
    }

    private void assertVariantProjectionRequiresVariantV2(Type variantType) throws UserException {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        SlotDescriptor slot = new SlotDescriptor(new SlotId(0), desc);
        slot.setColumn(new Column("payload", variantType));
        desc.addSlot(slot);

        Config.enable_variant_v2 = false;
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Paimon VARIANT columns require FE config enable_variant_v2=true",
                () -> PaimonScanNode.checkVariantV2Enabled(desc));
        Config.enable_variant_v2 = true;
        PaimonScanNode.checkVariantV2Enabled(desc);
    }

    @Test
    public void testStatementCacheReusesSerializedTasksForEquivalentScans() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
            PaimonExternalTable relationTable = Mockito.mock(PaimonExternalTable.class);
            PaimonExternalTable targetTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(catalog.getId()).thenReturn(7L);
            Mockito.when(relationTable.getId()).thenReturn(11L);
            Mockito.when(targetTable.getId()).thenReturn(13L);
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "value", DataTypes.INT())));
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            List<Predicate> idEqualsOne = Collections.singletonList(predicateBuilder.equal(0, 1));
            List<Predicate> idEqualsTwo = Collections.singletonList(predicateBuilder.equal(0, 2));
            AtomicInteger planCount = new AtomicInteger();

            Table baselineTable = mockPlanningTable(rowType, Collections.emptyMap(), planCount);
            List<org.apache.paimon.table.source.Split> first = assertPlanCount(newPlanningNode(
                    0, relationTable, targetTable, catalog, baselineTable,
                    101L, 3L, Collections.emptyMap(), idEqualsOne, "id"), planCount, 1);
            List<org.apache.paimon.table.source.Split> duplicate = assertPlanCount(newPlanningNode(
                    1, relationTable, targetTable, catalog, baselineTable,
                    101L, 3L, Collections.emptyMap(), idEqualsOne, "id"), planCount, 1);
            Assert.assertNotSame(first.get(0), duplicate.get(0));

            assertPlanCount(newPlanningNode(
                    2, relationTable, targetTable, catalog,
                    mockPlanningTable(rowType, Collections.emptyMap(), planCount),
                    102L, 3L, Collections.emptyMap(), idEqualsOne, "id"), planCount, 2);
            assertPlanCount(newPlanningNode(
                    3, relationTable, targetTable, catalog,
                    mockPlanningTable(rowType, Collections.emptyMap(), planCount),
                    101L, 3L, ImmutableMap.of("scan.mode", "delta"), idEqualsOne, "id"), planCount, 3);
            assertPlanCount(newPlanningNode(
                    4, relationTable, targetTable, catalog,
                    mockPlanningTable(rowType, Collections.emptyMap(), planCount),
                    101L, 3L, Collections.emptyMap(), idEqualsTwo, "id"), planCount, 4);
            assertPlanCount(newPlanningNode(
                    5, relationTable, targetTable, catalog,
                    mockPlanningTable(rowType, Collections.emptyMap(), planCount),
                    101L, 3L, Collections.emptyMap(), idEqualsOne, "value"), planCount, 5);
            assertPlanCount(newPlanningNode(
                    6, relationTable, targetTable, catalog,
                    mockPlanningTable(rowType, ImmutableMap.of("bucket", "2"), planCount),
                    101L, 3L, Collections.emptyMap(), idEqualsOne, "id"), planCount, 6);
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testStatementCacheSeparatesIncrementalSnapshotRanges() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
            PaimonExternalTable relationTable = Mockito.mock(PaimonExternalTable.class);
            PaimonExternalTable targetTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(catalog.getId()).thenReturn(31L);
            Mockito.when(relationTable.getId()).thenReturn(37L);
            Mockito.when(targetTable.getId()).thenReturn(41L);
            RowType rowType = new RowType(Collections.singletonList(
                    new DataField(0, "id", DataTypes.INT())));
            AtomicInteger planCount = new AtomicInteger();
            Table table = mockPlanningTable(rowType, Collections.emptyMap(), planCount);

            Map<String, String> firstRange = new HashMap<>();
            firstRange.put("startSnapshotId", "1");
            firstRange.put("endSnapshotId", "2");
            PaimonScanNode first = newPlanningNode(
                    9, relationTable, targetTable, catalog, table,
                    101L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            first.setScanParams(new TableScanParams(
                    TableScanParams.INCREMENTAL_READ, firstRange, Collections.emptyList()));
            assertPlanCount(first, planCount, 1);

            Map<String, String> sameRange = new HashMap<>();
            sameRange.put("endSnapshotId", "2");
            sameRange.put("startSnapshotId", "1");
            PaimonScanNode equivalent = newPlanningNode(
                    10, relationTable, targetTable, catalog, table,
                    101L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            equivalent.setScanParams(new TableScanParams(
                    TableScanParams.INCREMENTAL_READ, sameRange, Collections.emptyList()));
            assertPlanCount(equivalent, planCount, 1);

            Map<String, String> secondRange = new HashMap<>();
            secondRange.put("startSnapshotId", "3");
            secondRange.put("endSnapshotId", "4");
            PaimonScanNode different = newPlanningNode(
                    11, relationTable, targetTable, catalog, table,
                    101L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            different.setScanParams(new TableScanParams(
                    TableScanParams.INCREMENTAL_READ, secondRange, Collections.emptyList()));
            assertPlanCount(different, planCount, 2);
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testStatementCacheStopsSerializingEntryOverByteBudget() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
            PaimonExternalTable relationTable = Mockito.mock(PaimonExternalTable.class);
            PaimonExternalTable targetTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(catalog.getId()).thenReturn(43L);
            Mockito.when(relationTable.getId()).thenReturn(47L);
            Mockito.when(targetTable.getId()).thenReturn(53L);
            RowType rowType = new RowType(Collections.singletonList(
                    new DataField(0, "id", DataTypes.INT())));
            AtomicInteger planCount = new AtomicInteger();
            AtomicInteger serializationWriteCount = new AtomicInteger();
            List<org.apache.paimon.table.source.Split> plannedSplits = Arrays.asList(
                    new CountingSplit(serializationWriteCount),
                    new CountingSplit(serializationWriteCount),
                    new CountingSplit(serializationWriteCount));
            Table table = mockPlanningTable(
                    rowType, Collections.emptyMap(), planCount, plannedSplits);

            PaimonScanNode first = newPlanningNode(
                    12, relationTable, targetTable, catalog, table,
                    101L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            first.setMaxRetainedSerializedTaskBytes(256);
            Assert.assertSame(plannedSplits, first.getPaimonSplitFromAPI());
            Assert.assertEquals(1, planCount.get());
            int firstWriteCount = serializationWriteCount.get();
            Assert.assertTrue(firstWriteCount > 0);
            Assert.assertTrue(firstWriteCount < CountingSplit.PAYLOAD_SIZE);

            PaimonScanNode second = newPlanningNode(
                    13, relationTable, targetTable, catalog, table,
                    101L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            second.setMaxRetainedSerializedTaskBytes(256);
            Assert.assertSame(plannedSplits, second.getPaimonSplitFromAPI());
            Assert.assertEquals(2, planCount.get());
            int secondWriteCount = serializationWriteCount.get();
            Assert.assertTrue(secondWriteCount > firstWriteCount);
            Assert.assertTrue(secondWriteCount < firstWriteCount + CountingSplit.PAYLOAD_SIZE);
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testStatementCacheUsesRemainingSerializationBudget() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
            PaimonExternalTable relationTable = Mockito.mock(PaimonExternalTable.class);
            PaimonExternalTable targetTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(catalog.getId()).thenReturn(43L);
            Mockito.when(relationTable.getId()).thenReturn(47L);
            Mockito.when(targetTable.getId()).thenReturn(53L);
            RowType rowType = new RowType(Collections.singletonList(
                    new DataField(0, "id", DataTypes.INT())));
            AtomicInteger planCount = new AtomicInteger();
            AtomicInteger serializationWriteCount = new AtomicInteger();
            List<org.apache.paimon.table.source.Split> plannedSplits = Collections.singletonList(
                    new CountingSplit(serializationWriteCount));
            int serializedBytes = PaimonUtil.serializeObject(plannedSplits.get(0)).length;
            serializationWriteCount.set(0);
            long totalBudget = serializedBytes + 10L;
            Table table = mockPlanningTable(
                    rowType, Collections.emptyMap(), planCount, plannedSplits);

            PaimonScanNode first = newPlanningNode(
                    14, relationTable, targetTable, catalog, table,
                    101L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            first.setMaxRetainedSerializedTaskBytes(totalBudget);
            Assert.assertNotSame(plannedSplits, first.getPaimonSplitFromAPI());
            int firstWriteCount = serializationWriteCount.get();
            Assert.assertEquals(CountingSplit.PAYLOAD_SIZE, firstWriteCount);

            PaimonScanNode second = newPlanningNode(
                    15, relationTable, targetTable, catalog, table,
                    102L, 3L, Collections.emptyMap(), Collections.emptyList(), "id");
            second.setMaxRetainedSerializedTaskBytes(totalBudget);
            Assert.assertSame(plannedSplits, second.getPaimonSplitFromAPI());

            Assert.assertEquals(2, planCount.get());
            Assert.assertEquals(firstWriteCount, serializationWriteCount.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testPinnedFileCreationCacheIgnoresUnusedProjection() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
            PaimonExternalTable relationTable = Mockito.mock(PaimonExternalTable.class);
            PaimonExternalTable targetTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(catalog.getId()).thenReturn(17L);
            Mockito.when(relationTable.getId()).thenReturn(19L);
            Mockito.when(targetTable.getId()).thenReturn(23L);
            FileStoreTable table = Mockito.mock(FileStoreTable.class);
            CoreOptions coreOptions = Mockito.mock(CoreOptions.class);
            SnapshotReader reader = Mockito.mock(SnapshotReader.class);
            SnapshotReader.Plan plan = Mockito.mock(SnapshotReader.Plan.class);
            AtomicInteger planCount = new AtomicInteger();
            Mockito.when(table.options()).thenReturn(ImmutableMap.of("scan.snapshot-id", "29"));
            Mockito.when(table.primaryKeys()).thenReturn(Collections.emptyList());
            Mockito.when(table.coreOptions()).thenReturn(coreOptions);
            Mockito.when(coreOptions.bucket()).thenReturn(1);
            Mockito.when(table.newSnapshotReader()).thenReturn(reader);
            Mockito.when(reader.withMode(ScanMode.ALL)).thenReturn(reader);
            Mockito.when(reader.withSnapshot(29L)).thenReturn(reader);
            Mockito.when(reader.withManifestEntryFilter(ArgumentMatchers.any())).thenReturn(reader);
            Mockito.when(reader.read()).thenReturn(plan);
            Mockito.when(plan.splits()).thenAnswer(invocation -> {
                planCount.incrementAndGet();
                return Collections.singletonList(createDataSplit("planned.parquet"));
            });
            Snapshot latestSnapshot = Mockito.mock(Snapshot.class);
            Mockito.when(latestSnapshot.id()).thenReturn(29L);
            Mockito.when(table.latestSnapshot()).thenReturn(Optional.of(latestSnapshot));
            Map<String, String> options = PaimonScanParams.resolveOptions(
                    table, ImmutableMap.of("scan.file-creation-time-millis", "1234"));

            assertPlanCount(newPlanningNode(
                    7, relationTable, targetTable, catalog, table,
                    29L, 4L, options, Collections.emptyList(), "projected_a"), planCount, 1);
            assertPlanCount(newPlanningNode(
                    8, relationTable, targetTable, catalog, table,
                    29L, 4L, options, Collections.emptyList(), "projected_b"), planCount, 1);
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testSerializedTableCacheKeyIsStablePerScanNode() {
        PaimonScanNode first = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonScanNode second = newTestNode(new PlanNodeId(1), new TupleId(1), sv);

        String firstKey = first.getSerializedTableCacheKey().orElse("");
        Assert.assertFalse(firstKey.isEmpty());
        Assert.assertEquals(firstKey, first.getSerializedTableCacheKey().orElse(""));
        Assert.assertNotEquals(firstKey, second.getSerializedTableCacheKey().orElse(""));
    }

    @Test
    public void testRegularScanDoesNotComputeMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.emptyList()));
        DataSplit dataSplit = Mockito.spy(createDataSplit("regular.parquet"));
        Mockito.doReturn(Collections.singletonList(dataSplit)).when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(true);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        List<org.apache.doris.spi.Split> splits = node.getSplits(1);

        Assert.assertEquals(1, splits.size());
        Mockito.verify(dataSplit, Mockito.never()).mergedRowCount();
    }

    @Test
    public void testRustEnabledKeepsNativeAggregateSplits() throws Exception {
        SessionVariable session = new SessionVariable();
        session.setEnablePaimonRustReader(true);
        session.enableFileScannerV2 = true;
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), session));
        PaimonSource source = Mockito.mock(PaimonSource.class);
        DataTable table = Mockito.mock(DataTable.class, Mockito.RETURNS_DEEP_STUBS);
        RowType rowType = DataTypes.ROW(DataTypes.INT());
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(table.rowType()).thenReturn(rowType);
        // Native ORC eligibility now inspects historical types even for file aggregates.
        Mockito.when(table.schemaManager().schema(1L).logicalRowType()).thenReturn(rowType);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonExternalTable.class));
        node.setSource(source);
        setField(FileQueryScanNode.class, node, "fileSplitter",
                new FileSplitter(64L * 1024 * 1024, 64L * 1024 * 1024, 0));
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());
        DataSplit dataSplit = Mockito.spy(createDataSplit(Arrays.asList("first.parquet", "second.orc")));
        Mockito.doReturn(Collections.singletonList(dataSplit)).when(node).getPaimonSplitFromAPI();

        // File aggregates need native file readers; enabling Rust must not replace raw-file splits
        // with logical DataSplits, which require row reads for COUNT(col) and MIN/MAX.
        for (TPushAggOp aggregate : Arrays.asList(TPushAggOp.COUNT, TPushAggOp.MINMAX)) {
            node.setPushDownAggNoGrouping(aggregate);
            node.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));
            List<org.apache.doris.spi.Split> splits = node.getSplits(1);
            Assert.assertEquals(2, splits.size());
            for (org.apache.doris.spi.Split split : splits) {
                Assert.assertNull(((PaimonSplit) split).getSplit());
                Assert.assertFalse(((PaimonSplit) split).getRowCount().isPresent());
            }
        }
        Mockito.verify(dataSplit, Mockito.never()).mergedRowCount();
    }

    @Test
    public void testCountColumnKeepsAllSplitsWhileCountStarUsesMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.<String>emptyList()));
        List<org.apache.paimon.table.source.Split> dataSplits = Arrays.asList(
                mockCountDataSplit("f1.parquet", 4_000),
                mockCountDataSplit("f2.parquet", 5_000),
                mockCountDataSplit("f3.parquet", 6_000));
        Mockito.doReturn(dataSplits).when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(true);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getParallelExecInstanceNum(ArgumentMatchers.nullable(String.class))).thenReturn(1);

        // Before the fix, the raw COUNT opcode made this path keep only parallel representative
        // splits and attach the 15,000 metadata rows. BE rejects that shortcut for COUNT(col), so
        // it would scan only those representatives and silently miss the discarded DataSplits.
        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));
        List<org.apache.doris.spi.Split> countColumnSplits = node.getSplits(1);
        Assert.assertEquals(3, countColumnSplits.size());
        for (org.apache.doris.spi.Split split : countColumnSplits) {
            Assert.assertFalse(((PaimonSplit) split).getRowCount().isPresent());
        }

        // COUNT(*) remains metadata-only. The 15,000 rows exceed the parallel threshold, so one
        // configured execution instance retains one representative split carrying the full sum.
        node.setPushDownCountSlotIds(Collections.emptyList());
        List<org.apache.doris.spi.Split> countStarSplits = node.getSplits(1);
        Assert.assertEquals(1, countStarSplits.size());
        Assert.assertEquals(Optional.of(15_000L), ((PaimonSplit) countStarSplits.get(0)).getRowCount());
    }

    @Test
    public void testNonCountScanDoesNotComputeMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.<String>emptyList()));
        DataSplit dataSplit = mockCountDataSplit("ordinary.parquet", 1_000);
        Mockito.clearInvocations(dataSplit);
        Mockito.doReturn(Collections.singletonList(dataSplit)).when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(true);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        List<org.apache.doris.spi.Split> splits = node.getSplits(1);

        Assert.assertEquals(1, splits.size());
        Mockito.verify(dataSplit, Mockito.never()).mergedRowCount();
    }

    @Test
    public void testIncrementalBinlogCountStarDoesNotUsePhysicalRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        PaimonSource source = mockPaimonSourceWithPartitionKeys(Collections.emptyList());
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(source.getExternalTable()).thenReturn(binlogTable);
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.INCREMENTAL_READ,
                ImmutableMap.of("startSnapshotId", "1", "endSnapshotId", "2"),
                Collections.emptyList()));
        Mockito.doReturn(Arrays.asList(
                mockCountDataSplit("before.parquet", 1),
                mockCountDataSplit("after.parquet", 1)))
                .when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.emptyList());
        List<org.apache.doris.spi.Split> splits = node.getSplits(1);

        Assert.assertEquals(2, splits.size());
        for (org.apache.doris.spi.Split split : splits) {
            Assert.assertFalse(((PaimonSplit) split).getRowCount().isPresent());
        }
    }

    @Test
    public void testSplitWeight() throws UserException {

        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);

        PaimonSource source = Mockito.spy(new PaimonSource());
        Table paimonTable = Mockito.mock(Table.class);
        Mockito.doReturn(paimonTable).when(source).getPaimonTable();
        Mockito.when(paimonTable.partitionKeys()).thenReturn(Collections.emptyList());
        paimonScanNode.setSource(source);

        DataFileMeta dfm1 = DataFileMeta.forAppend("f1.parquet", 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
        BinaryRow binaryRow1 = BinaryRow.singleColumn(1);
        DataSplit ds1 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow1)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm1))
                .build();

        DataFileMeta dfm2 = DataFileMeta.forAppend("f2.parquet", 32L * 1024 * 1024, 2L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
        BinaryRow binaryRow2 = BinaryRow.singleColumn(1);
        DataSplit ds2 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow2)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm2))
                .build();


        // Mock PaimonScanNode to return test data splits
        PaimonScanNode spyPaimonScanNode = Mockito.spy(paimonScanNode);
        Mockito.doReturn(new ArrayList<org.apache.paimon.table.source.Split>() {
            {
                add(ds1);
                add(ds2);
            }
        }).when(spyPaimonScanNode).getPaimonSplitFromAPI();

        long maxInitialSplitSize = 32L * 1024L * 1024L;
        long maxSplitSize = 64L * 1024L * 1024L;
        // Ensure fileSplitter is initialized on the spy as doInitialize() is not called in this unit test
        FileSplitter fileSplitter = new FileSplitter(maxInitialSplitSize, maxSplitSize,
                0);
        try {
            java.lang.reflect.Field field = FileQueryScanNode.class.getDeclaredField("fileSplitter");
            field.setAccessible(true);
            field.set(spyPaimonScanNode, fileSplitter);

            java.lang.reflect.Field storagePropertiesField =
                    PaimonScanNode.class.getDeclaredField("storagePropertiesMap");
            storagePropertiesField.setAccessible(true);
            storagePropertiesField.set(spyPaimonScanNode, Collections.emptyMap());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to inject test fields into PaimonScanNode", e);
        }

        // Note: The original PaimonSource is sufficient for this test
        // No need to mock catalog properties since doInitialize() is not called in this test
        // Mock SessionVariable behavior
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getMaxInitialSplitSize()).thenReturn(maxInitialSplitSize);
        Mockito.when(sv.getMaxSplitSize()).thenReturn(maxSplitSize);

        // native
        mockNativeReader(spyPaimonScanNode);
        List<org.apache.doris.spi.Split> s1 = spyPaimonScanNode.getSplits(1);
        PaimonSplit s11 = (PaimonSplit) s1.get(0);
        PaimonSplit s12 = (PaimonSplit) s1.get(1);
        Assert.assertEquals(2, s1.size());
        Assert.assertEquals(100, s11.getSplitWeight().getRawValue());
        Assert.assertNull(s11.getSplit());
        Assert.assertEquals(50, s12.getSplitWeight().getRawValue());
        Assert.assertNull(s12.getSplit());

        // jni
        mockJniReader(spyPaimonScanNode);
        List<org.apache.doris.spi.Split> s2 = spyPaimonScanNode.getSplits(1);
        PaimonSplit s21 = (PaimonSplit) s2.get(0);
        PaimonSplit s22 = (PaimonSplit) s2.get(1);
        Assert.assertEquals(2, s2.size());
        Assert.assertNotNull(s21.getSplit());
        Assert.assertNotNull(s22.getSplit());
        Assert.assertEquals(100, s21.getSplitWeight().getRawValue());
        Assert.assertEquals(50, s22.getSplitWeight().getRawValue());
    }

    @Test
    public void testValidateIncrementalReadParams() throws UserException {
        // Test valid parameter combinations

        // 1. Only startSnapshotId
        Map<String, String> params1 = new HashMap<>();
        params1.put("startSnapshotId", "5");
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "endSnapshotId is required when using snapshot-based incremental read",
                () -> PaimonScanNode.validateIncrementalReadParams(params1));

        // 2. Both startSnapshotId and endSnapshotId
        Map<String, String> params = new HashMap<>();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "5");
        Map<String, String> result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1,5", result.get("incremental-between"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(16, result.size());

        // 3. startSnapshotId + endSnapshotId + incrementalBetweenScanMode
        params.clear();
        params.put("startSnapshotId", "2");
        params.put("endSnapshotId", "8");
        params.put("incrementalBetweenScanMode", "diff");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("2,8", result.get("incremental-between"));
        Assert.assertEquals("diff", result.get("incremental-between-scan-mode"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(16, result.size());

        // 4. Only startTimestamp
        params.clear();
        params.put("startTimestamp", "1000");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1000," + Long.MAX_VALUE, result.get("incremental-between-timestamp"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertTrue(result.containsKey("scan.snapshot-id") && result.get("scan.snapshot-id") == null);
        Assert.assertEquals(16, result.size());

        // 5. Both startTimestamp and endTimestamp
        params.clear();
        params.put("startTimestamp", "1000");
        params.put("endTimestamp", "2000");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1000,2000", result.get("incremental-between-timestamp"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertTrue(result.containsKey("scan.snapshot-id") && result.get("scan.snapshot-id") == null);
        Assert.assertEquals(16, result.size());

        // Test invalid parameter combinations

        // 6. Test mutual exclusivity - both snapshot and timestamp params
        params.clear();
        params.put("startSnapshotId", "1");
        params.put("startTimestamp", "1000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for mutual exclusivity");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot specify both snapshot-based parameters"));
        }

        // 7. Test snapshot params without required startSnapshotId
        params.clear();
        params.put("endSnapshotId", "5");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startSnapshotId is missing");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId is required"));
        }

        // 8. Test timestamp params without required startTimestamp
        params.clear();
        params.put("endTimestamp", "2000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startTimestamp is missing");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp is required"));
        }

        // 9. Test incrementalBetweenScanMode without endSnapshotId
        params.clear();
        params.put("startSnapshotId", "1");
        params.put("incrementalBetweenScanMode", "auto");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when incrementalBetweenScanMode appears without endSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("incrementalBetweenScanMode can only be specified when both"));
        }

        // 10. Test incrementalBetweenScanMode alone
        params.clear();
        params.put("incrementalBetweenScanMode", "auto");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when incrementalBetweenScanMode appears alone");
        } catch (UserException e) {
            Assert.assertTrue(
                    e.getMessage().contains("startSnapshotId is required when using snapshot-based incremental read"));
        }

        // 11. Test invalid snapshot ID values < 0)
        params.clear();
        params.put("startSnapshotId", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for startSnapshotId < 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be greater than or equal to 0"));
        }

        params.clear();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for endSnapshotId < 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("endSnapshotId must be greater than or equal to 0"));
        }

        // 12. Test start > end for snapshot IDs
        params.clear();
        params.put("startSnapshotId", "6");
        params.put("endSnapshotId", "5");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startSnapshotId > endSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be less than or equal to endSnapshotId"));
        }

        // 12.1. Test startSnapshotId == endSnapshotId (should be allowed, consistent with Spark Paimon behavior)
        params.clear();
        params.put("startSnapshotId", "5");
        params.put("endSnapshotId", "5");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("5,5", result.get("incremental-between"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(16, result.size());

        // 13. Test invalid timestamp values (< 0)
        params.clear();
        params.put("startTimestamp", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for startTimestamp < 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp must be greater than or equal to 0"));
        }

        params.clear();
        params.put("startTimestamp", "1000");
        params.put("endTimestamp", "0");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for endTimestamp ≤ 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("endTimestamp must be greater than 0"));
        }

        // 14. Test start ≥ end for timestamps
        params.clear();
        params.put("startTimestamp", "2000");
        params.put("endTimestamp", "2000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startTimestamp = endTimestamp");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp must be less than endTimestamp"));
        }

        params.clear();
        params.put("startTimestamp", "3000");
        params.put("endTimestamp", "2000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startTimestamp > endTimestamp");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp must be less than endTimestamp"));
        }

        // 15. Test invalid number format
        params.clear();
        params.put("startSnapshotId", "invalid");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for invalid number format");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid startSnapshotId format"));
        }

        params.clear();
        params.put("startTimestamp", "invalid");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for invalid timestamp format");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid startTimestamp format"));
        }

        // 16. Test invalid incrementalBetweenScanMode values
        params.clear();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "5");
        params.put("incrementalBetweenScanMode", "invalid");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for invalid scan mode");
        } catch (UserException e) {
            Assert.assertTrue(
                    e.getMessage().contains("incrementalBetweenScanMode must be one of: auto, diff, delta, changelog"));
        }

        // 17. Test valid incrementalBetweenScanMode values (case insensitive)
        String[] validModes = {"auto", "AUTO", "diff", "DIFF", "delta", "DELTA", "changelog", "CHANGELOG"};
        for (String mode : validModes) {
            params.clear();
            params.put("startSnapshotId", "1");
            params.put("endSnapshotId", "5");
            params.put("incrementalBetweenScanMode", mode);
            result = PaimonScanNode.validateIncrementalReadParams(params);
            Assert.assertEquals("1,5", result.get("incremental-between"));
            Assert.assertEquals(mode, result.get("incremental-between-scan-mode"));
            Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
            Assert.assertEquals(16, result.size());
        }

        // 18. Test no parameters at all
        params.clear();
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when no parameters provided");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("at least one valid parameter group must be specified"));
        }
    }

    @Test
    public void testPaimonDataSystemTableForceJniEvenWhenNativeSupported() throws UserException {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);
        PaimonScanNode spyPaimonScanNode = Mockito.spy(paimonScanNode);

        DataFileMeta dfm = DataFileMeta.forAppend("f1.parquet", 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
        BinaryRow binaryRow = BinaryRow.singleColumn(1);
        DataSplit dataSplit = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm))
                .build();

        Mockito.doReturn(Collections.singletonList(dataSplit)).when(spyPaimonScanNode).getPaimonSplitFromAPI();
        mockNativeReader(spyPaimonScanNode);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(source.getExternalTable()).thenReturn(binlogTable);
        spyPaimonScanNode.setSource(source);

        long maxInitialSplitSize = 32L * 1024L * 1024L;
        long maxSplitSize = 64L * 1024L * 1024L;
        FileSplitter fileSplitter = new FileSplitter(maxInitialSplitSize, maxSplitSize, 0);
        try {
            java.lang.reflect.Field field = FileQueryScanNode.class.getDeclaredField("fileSplitter");
            field.setAccessible(true);
            field.set(spyPaimonScanNode, fileSplitter);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to inject FileSplitter into PaimonScanNode test", e);
        }

        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getMaxSplitSize()).thenReturn(maxSplitSize);

        Assert.assertTrue(spyPaimonScanNode.shouldForceJniForSystemTable());
        List<org.apache.doris.spi.Split> splits = spyPaimonScanNode.getSplits(1);
        Assert.assertEquals(1, splits.size());
        Assert.assertNotNull(((PaimonSplit) splits.get(0)).getSplit());

        PaimonSysExternalTable auditLogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(auditLogTable.getSysTableType()).thenReturn("audit_log");
        Mockito.when(source.getExternalTable()).thenReturn(auditLogTable);

        Assert.assertTrue(spyPaimonScanNode.shouldForceJniForSystemTable());
        List<org.apache.doris.spi.Split> auditLogSplits = spyPaimonScanNode.getSplits(1);
        Assert.assertEquals(1, auditLogSplits.size());
        Assert.assertNotNull(((PaimonSplit) auditLogSplits.get(0)).getSplit());

        PaimonSysExternalTable rowTrackingTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(rowTrackingTable.getSysTableType()).thenReturn("row_tracking");
        Mockito.when(source.getExternalTable()).thenReturn(rowTrackingTable);

        Assert.assertTrue(spyPaimonScanNode.shouldForceJniForSystemTable());
        List<org.apache.doris.spi.Split> rowTrackingSplits = spyPaimonScanNode.getSplits(1);
        Assert.assertEquals(1, rowTrackingSplits.size());
        Assert.assertNotNull(((PaimonSplit) rowTrackingSplits.get(0)).getSplit());
    }

    @Test
    public void testPaimonDataSystemTablesBypassCppReader() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        node.setSource(source);
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        for (String type : Arrays.asList("audit_log", "binlog", "row_tracking")) {
            Mockito.when(systemTable.getSysTableType()).thenReturn(type);
            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit(type + ".parquet")));
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testSchemaSelectingOptionsBypassCppReader() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(table);
        Table baseTable = Mockito.mock(Table.class);
        Mockito.when(baseTable.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.snapshot-id", "1"),
                Collections.emptyList()));
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(createDataSplit("historical.parquet")));

        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
    }

    @Test
    public void testSystemTablePassesIncrementalOptionsToPaimonTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("audit_log");
        Table baseTable = Mockito.mock(Table.class);
        Table copiedTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable((TableScanParams) null)).thenReturn(baseTable);
        node.setSource(source);

        Map<String, String> params = new HashMap<>();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "2");
        node.setScanParams(new TableScanParams(
                TableScanParams.INCREMENTAL_READ, params, Collections.emptyList()));

        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("scan.timestamp", null);
        expectedOptions.put("scan.timestamp-millis", null);
        expectedOptions.put("scan.watermark", null);
        expectedOptions.put("scan.file-creation-time-millis", null);
        expectedOptions.put("scan.creation-time-millis", null);
        expectedOptions.put("scan.snapshot-id", null);
        expectedOptions.put("scan.tag-name", null);
        expectedOptions.put("scan.version", null);
        expectedOptions.put("scan.bounded.watermark", null);
        expectedOptions.put("scan.mode", null);
        expectedOptions.put("log.scan", null);
        expectedOptions.put("log.scan.timestamp-millis", null);
        expectedOptions.put("incremental-between-timestamp", null);
        expectedOptions.put("incremental-between-scan-mode", null);
        expectedOptions.put("incremental-to-auto-tag", null);
        expectedOptions.put("incremental-between", "1,2");
        Mockito.when(baseTable.copy(expectedOptions)).thenReturn(copiedTable);
        Mockito.when(copiedTable.options()).thenReturn(Collections.emptyMap());

        try {
            Assert.assertSame(copiedTable, invokePrivateMethod(node, "getProcessedTable"));
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.fail("Paimon system table should accept incremental options, but got: "
                    + e.getTargetException().getMessage());
        }
        Mockito.verify(baseTable).copy(expectedOptions);
    }

    @Test
    public void testPinnedFileCreationScanPreservesBatchReaderFilters() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        SnapshotReader reader = Mockito.mock(SnapshotReader.class);
        SnapshotReader.Plan plan = Mockito.mock(SnapshotReader.Plan.class);
        CoreOptions coreOptions = Mockito.mock(CoreOptions.class);
        org.apache.paimon.options.Options configuration = new org.apache.paimon.options.Options();
        configuration.set(CoreOptions.BATCH_SCAN_MODE, CoreOptions.BatchScanMode.NONE);

        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(externalTable.getId()).thenReturn(2L);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getTargetTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class))).thenReturn(table);
        Mockito.when(snapshot.id()).thenReturn(23L);
        Mockito.when(table.latestSnapshot()).thenReturn(Optional.of(snapshot));
        Mockito.when(table.options()).thenReturn(ImmutableMap.of("scan.snapshot-id", "23"));
        Mockito.when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        Mockito.when(table.coreOptions()).thenReturn(coreOptions);
        Mockito.when(coreOptions.batchScanSkipLevel0()).thenReturn(true);
        Mockito.when(coreOptions.toConfiguration()).thenReturn(configuration);
        Mockito.when(coreOptions.bucket()).thenReturn(BucketMode.POSTPONE_BUCKET);
        Mockito.when(table.newSnapshotReader()).thenReturn(reader);
        Mockito.when(reader.withMode(ScanMode.ALL)).thenReturn(reader);
        Mockito.when(reader.withSnapshot(23L)).thenReturn(reader);
        Mockito.when(reader.withManifestEntryFilter(ArgumentMatchers.any())).thenReturn(reader);
        Mockito.when(reader.withLevelFilter(ArgumentMatchers.any())).thenReturn(reader);
        Mockito.when(reader.enableValueFilter()).thenReturn(reader);
        Mockito.when(reader.onlyReadRealBuckets()).thenReturn(reader);
        Mockito.when(reader.read()).thenReturn(plan);
        Mockito.when(plan.splits()).thenReturn(Collections.emptyList());
        node.setSource(source);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.file-creation-time-millis", "1234"),
                Collections.emptyList());
        scanParams.getOrResolveMapParams(options -> PaimonScanParams.resolveOptions(table, options));
        node.setScanParams(scanParams);
        // This reader-filter test deliberately has no bound MVCC snapshot; avoid asking the
        // otherwise unstubbed external-table mock to manufacture one from scan options.
        node.setRelationSnapshot(Optional.empty());

        Assert.assertTrue(node.getPaimonSplitFromAPI().isEmpty());

        Mockito.verify(reader).withLevelFilter(ArgumentMatchers.any());
        Mockito.verify(reader).enableValueFilter();
        Mockito.verify(reader).onlyReadRealBuckets();
    }

    @Test
    public void testBoundEmptySnapshotDoesNotReadLaterCommit() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table liveTable = Mockito.mock(Table.class);
        node.setSource(source);
        node.setRelationSnapshot(Optional.of(new PaimonMvccSnapshot(
                new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(PaimonSnapshot.INVALID_SNAPSHOT_ID, 1L, liveTable)))));

        Assert.assertTrue(node.getPaimonSplitFromAPI().isEmpty());
        Mockito.verify(liveTable, Mockito.never()).newReadBuilder();
    }

    @Test
    public void testBoundEmptyDataSnapshotStillPlansMetadataSystemTable() throws Exception {
        // A live statement scope is required: the metadata-system-table path plans through the
        // serialized scan-task cache (the split is a deserialized copy, not the planned instance).
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            assertBoundEmptyDataSnapshotStillPlansMetadataSystemTable();
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private void assertBoundEmptyDataSnapshotStillPlansMetadataSystemTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Table paimonTable = Mockito.mock(Table.class);
        ReadBuilder readBuilder = Mockito.mock(ReadBuilder.class);
        TableScan scan = Mockito.mock(TableScan.class);
        TableScan.Plan plan = Mockito.mock(TableScan.Plan.class);
        org.apache.paimon.table.source.Split schemaSplit = Mockito.mock(
                org.apache.paimon.table.source.Split.class, Mockito.withSettings().serializable());

        Mockito.when(catalog.getId()).thenReturn(3L);
        Mockito.when(systemTable.getId()).thenReturn(4L);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getTargetTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        Mockito.when(systemTable.getSysTableType()).thenReturn("schemas");
        Mockito.when(source.getPaimonTable((TableScanParams) null)).thenReturn(paimonTable);
        Mockito.when(paimonTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(paimonTable.rowType()).thenReturn(RowType.of());
        Mockito.when(paimonTable.newReadBuilder()).thenReturn(readBuilder);
        Mockito.when(readBuilder.withFilter(ArgumentMatchers.anyList())).thenReturn(readBuilder);
        Mockito.when(readBuilder.withProjection(ArgumentMatchers.any(int[].class))).thenReturn(readBuilder);
        Mockito.when(readBuilder.newScan()).thenReturn(scan);
        Mockito.when(scan.plan()).thenReturn(plan);
        Mockito.when(plan.splits()).thenReturn(Collections.singletonList(schemaSplit));
        node.setSource(source);
        setField(PaimonScanNode.class, node, "predicates", Collections.emptyList());
        node.setRelationSnapshot(Optional.of(new PaimonMvccSnapshot(
                new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(PaimonSnapshot.INVALID_SNAPSHOT_ID, 1L, paimonTable)))));

        List<org.apache.paimon.table.source.Split> plannedSplits = node.getPaimonSplitFromAPI();
        Assert.assertEquals(1, plannedSplits.size());
        Assert.assertNotSame(schemaSplit, plannedSplits.get(0));
        Mockito.verify(paimonTable).newReadBuilder();
    }

    @Test
    public void testSystemWrapperIsNotRecappedAfterItsHiddenSource() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Table rawSource = Mockito.mock(Table.class);
        Table safeWrapper = Mockito.mock(Table.class);

        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(rawSource);
        Mockito.when(systemTable.getSysTableType()).thenReturn("partitions");
        Mockito.when(source.getPaimonTable((TableScanParams) null)).thenReturn(safeWrapper);
        Mockito.when(safeWrapper.options()).thenReturn(Collections.emptyMap());
        node.setSource(source);

        // The system-table factory has already normalized each hidden fallback leaf. Copying the
        // outer wrapper with one cap would broadcast it and erase a smaller sibling preference.
        Assert.assertSame(safeWrapper, invokePrivateMethod(node, "getProcessedTable"));
        Mockito.verify(safeWrapper, Mockito.never()).copy(ArgumentMatchers.anyMap());
        Mockito.verify(source).validateEffectiveSystemDataTable(null);
    }

    @Test
    public void testSystemTableRejectsIncrementalReadWhenReaderIgnoresRange() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("snapshots");
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(Mockito.mock(Table.class));
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.INCREMENTAL_READ,
                ImmutableMap.of("startSnapshotId", "1", "endSnapshotId", "2"),
                Collections.emptyList()));

        try {
            invokePrivateMethod(node, "getProcessedTable");
            Assert.fail("snapshots must reject an incremental range it does not consume");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getTargetException().getMessage()
                    .contains("does not support INCR"));
        }
    }

    @Test
    public void testSystemTablePassesDynamicOptionsToPaimonTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("table_indexes");
        Table baseTable = Mockito.mock(Table.class);
        Table copiedTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        baseTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        node.setSource(source);

        Map<String, String> options = ImmutableMap.of(
                "scan.snapshot-id", "12345",
                "scan.mode", "from-snapshot");
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS, options, Collections.emptyList()));
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(copiedTable);
        Mockito.when(copiedTable.options()).thenReturn(options);

        try {
            Assert.assertSame(copiedTable, invokePrivateMethod(node, "getProcessedTable"));
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.fail("Paimon system table should accept dynamic options, but got: "
                    + e.getTargetException().getMessage());
        }
        Mockito.verify(baseTable).copy(ArgumentMatchers.argThat(applied ->
                "12345".equals(applied.get("scan.snapshot-id"))
                        && "from-snapshot".equals(applied.get("scan.mode"))
                        && applied.containsKey("scan.tag-name")
                        && applied.get("scan.tag-name") == null));
    }

    @Test
    public void testDataTableQueryOptionsOverrideDefaultsWithoutMutation() throws Exception {
        Map<String, String> defaultOptions = new HashMap<>();
        defaultOptions.put("scan.mode", "latest");
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                defaultOptions,
                null);
        Table baseTable = new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class),
                new Path("memory://paimon_dynamic_options"),
                schema,
                CatalogEnvironment.empty());

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonExternalTable.class));
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        baseTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        node.setSource(source);

        Map<String, String> queryOptions = ImmutableMap.of(
                "scan.mode", "from-snapshot",
                "scan.snapshot-id", "2");
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS, queryOptions, Collections.emptyList()));

        Table processedTable = (Table) invokePrivateMethod(node, "getProcessedTable");
        Assert.assertEquals("from-snapshot", processedTable.options().get("scan.mode"));
        Assert.assertEquals("2", processedTable.options().get("scan.snapshot-id"));
        Assert.assertEquals("latest", baseTable.options().get("scan.mode"));
        Assert.assertFalse(baseTable.options().containsKey("scan.snapshot-id"));
    }

    @Test
    public void testRejectsUnsafePhysicalOptionsAtFinalPlanningBoundary() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table unsafePhysicalTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(unsafePhysicalTable);
        Mockito.when(unsafePhysicalTable.options()).thenReturn(ImmutableMap.of("read.batch-size", "0"));
        node.setSource(source);

        try {
            invokePrivateMethod(node, "getProcessedTable");
            Assert.fail("The final planning boundary must reject an effective zero batch size");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getTargetException().getMessage().contains("read.batch-size"));
        }
    }

    @Test
    public void testFinalPlanningBoundaryCapsAcceptedManifestParallelism() throws Exception {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        org.junit.Assume.assumeTrue(localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        FileStoreTable rawTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable safeTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(rawTable);
        Mockito.when(rawTable.options()).thenReturn(ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity + 1)));
        Mockito.when(rawTable.copyWithoutTimeTravel(ArgumentMatchers.anyMap())).thenReturn(safeTable);
        Mockito.when(safeTable.options()).thenReturn(ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity)));
        node.setSource(source);

        Assert.assertSame(safeTable, invokePrivateMethod(node, "getProcessedTable"));
        Mockito.verify(rawTable).copyWithoutTimeTravel(ArgumentMatchers.argThat(options ->
                String.valueOf(localCapacity)
                        .equals(options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()))));
    }

    @Test
    public void testDataTableOptionsUseRelationScopedCatalogHandle() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table statementSnapshotTable = Mockito.mock(Table.class);
        Table relationScopedTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(statementSnapshotTable);
        node.setSource(source);

        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.snapshot-id", "1"),
                Collections.emptyList());
        node.setScanParams(scanParams);
        Mockito.when(source.getPaimonTable(scanParams)).thenReturn(relationScopedTable);
        Mockito.when(relationScopedTable.options()).thenReturn(Collections.emptyMap());

        Assert.assertSame(relationScopedTable, invokePrivateMethod(node, "getProcessedTable"));
        Mockito.verify(source).getPaimonTable(scanParams);
        Mockito.verify(statementSnapshotTable, Mockito.never()).copy(ArgumentMatchers.anyMap());
    }

    @Test
    public void testFileColumnPositionsUseProcessedHistoricalSchema() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.snapshot-id", "1"),
                Collections.emptyList()));
        Table historicalTable = Mockito.mock(Table.class);
        Mockito.when(historicalTable.rowType()).thenReturn(new org.apache.paimon.types.RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "old_name", new org.apache.paimon.types.VarCharType()))));
        setField(PaimonScanNode.class, node, "processedTable", historicalTable);

        Assert.assertEquals(Arrays.asList("id", "old_name"), node.getFileColumnNames());
    }

    @Test
    public void testLatestScanUsesRefreshedDescriptorColumnPositions() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        Column latestColumn = Mockito.mock(Column.class);
        Mockito.when(latestColumn.getName()).thenReturn("renamed_name");
        PaimonExternalTable externalTable = (PaimonExternalTable) node.getTupleDesc().getTable();
        // File scan metadata is resolved against the relation snapshot, so mock the snapshot-aware lookup.
        Mockito.when(externalTable.getFullSchema(Mockito.<Optional<MvccSnapshot>>any()))
                .thenReturn(Collections.singletonList(latestColumn));

        Table staleTableHandle = Mockito.mock(Table.class);
        setField(PaimonScanNode.class, node, "processedTable", staleTableHandle);

        Assert.assertEquals(Collections.singletonList("renamed_name"), node.getFileColumnNames());
    }

    @Test
    public void testDataTableQueryOptionsReplaceInheritedSnapshotSelector() throws Exception {
        Map<String, String> defaultOptions = new HashMap<>();
        defaultOptions.put("scan.snapshot-id", "9");
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                defaultOptions,
                null);
        Table pinnedLatestTable = new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class),
                new Path("memory://paimon_dynamic_tag"),
                schema,
                CatalogEnvironment.empty());

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonExternalTable.class));
        Mockito.when(source.getPaimonTable()).thenReturn(pinnedLatestTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        pinnedLatestTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "tag1"),
                Collections.emptyList()));

        Table processedTable = (Table) invokePrivateMethod(node, "getProcessedTable");
        Assert.assertEquals("tag1", processedTable.options().get("scan.tag-name"));
        Assert.assertFalse(processedTable.options().containsKey("scan.snapshot-id"));
    }

    @Test
    public void testBackendSerializationUsesDynamicOptionsTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("table_indexes");
        Table baseTable = Mockito.mock(Table.class);
        Table copiedTable = Mockito.mock(Table.class, Mockito.withSettings().serializable());
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        baseTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        // The invocation happens on the deserialized mock copy, so Mockito cannot record it
        // against this test instance when checking strict stubbings.
        Mockito.lenient().when(copiedTable.name()).thenReturn("files-at-snapshot");
        node.setSource(source);

        Map<String, String> options = ImmutableMap.of("scan.snapshot-id", "1");
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS, options, Collections.emptyList()));
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(copiedTable);
        Mockito.when(copiedTable.options()).thenReturn(options);

        try {
            invokePrivateMethod(node, "serializeProcessedTable");
        } catch (NoSuchMethodException e) {
            Assert.fail("PaimonScanNode must serialize the processed table for backend JNI reads");
        }

        java.lang.reflect.Field field = PaimonScanNode.class.getDeclaredField("serializedTable");
        field.setAccessible(true);
        String encoded = (String) field.get(node);
        Table decoded = InstantiationUtil.deserializeObject(
                Base64.getUrlDecoder().decode(encoded), PaimonUtil.class.getClassLoader());
        Assert.assertEquals("files-at-snapshot", decoded.name());
    }

    @Test
    public void testSystemTableRejectsNonIncrementalScanParams() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonSysExternalTable.class));
        Mockito.when(source.getPaimonTable()).thenReturn(Mockito.mock(Table.class));
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.BRANCH,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "branch1"),
                Collections.emptyList()));

        try {
            invokePrivateMethod(node, "getProcessedTable");
            Assert.fail("Paimon system table should reject non-incremental scan params");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getTargetException().getMessage()
                    .contains("only support INCR or OPTIONS scan params"));
        }
    }

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getFileFormatFromTableProperties()).thenReturn("parquet");
        node.setSource(source);

        RawFile rawFile = Mockito.mock(RawFile.class);
        Mockito.when(rawFile.path()).thenReturn("file.parquet");
        Mockito.when(rawFile.fileSize()).thenReturn(10_000L * 1024L * 1024L);

        DataSplit dataSplit = Mockito.mock(DataSplit.class);
        Mockito.when(dataSplit.convertToRawFiles()).thenReturn(Optional.of(Collections.singletonList(rawFile)));

        Method method = PaimonScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, Collections.singletonList(dataSplit), false);
        Assert.assertEquals(100L * 1024L * 1024L, target);
    }

    @Test
    public void testSelectFeSplitSizeUsesCoarseSizeOnlyForNativeColumnarFile() {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSizeOnFe(512L * 1024L * 1024L);
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);

        Assert.assertEquals(512L * 1024L * 1024L,
                node.selectFeSplitSizeForRawFile("file.parquet", 64L * 1024L * 1024L, true));
        Assert.assertEquals(64L * 1024L * 1024L,
                node.selectFeSplitSizeForRawFile("file.avro", 64L * 1024L * 1024L, true));
        Assert.assertEquals(64L * 1024L * 1024L,
                node.selectFeSplitSizeForRawFile("file-without-suffix", 64L * 1024L * 1024L, false));
    }

    @Test
    public void testLegacyOrcTimestampLtzRequiresSdkReaderRecursively() throws Exception {
        org.apache.paimon.types.DataType ltz = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
        for (org.apache.paimon.types.DataType type : Arrays.asList(
                ltz, DataTypes.ARRAY(ltz), DataTypes.MULTISET(ltz), DataTypes.MAP(ltz, DataTypes.INT()),
                DataTypes.MAP(DataTypes.INT(), ltz), DataTypes.ROW(DataTypes.FIELD(1, "ts", ltz)))) {
            for (Map<String, String> options : Arrays.asList(Collections.<String, String>emptyMap(),
                    Collections.singletonMap("orc.timestamp-ltz.legacy.type", "true"))) {
                PaimonScanNode node = legacyOrcRoutingNode(DataTypes.ROW(type), options, null);
                Assert.assertFalse(node.supportNativeReader(rawFiles("data.orc")));
                Assert.assertFalse(node.supportNativeReader(rawFiles("data.parquet", "data.orc")));
                Assert.assertTrue(node.supportNativeReader(rawFiles("data.parquet")));
            }
        }
    }

    @Test
    public void testModernOrcTimestampAndNtzOnlyFilesKeepNativeReader() throws Exception {
        RowType ltz = DataTypes.ROW(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6));
        PaimonScanNode modern = legacyOrcRoutingNode(ltz,
                Collections.singletonMap("orc.timestamp-ltz.legacy.type", "false"), null);
        Assert.assertTrue(modern.supportNativeReader(rawFiles("data.orc")));

        RowType ntz = DataTypes.ROW(DataTypes.TIMESTAMP(6));
        TableSchema history = Mockito.mock(TableSchema.class);
        Mockito.lenient().when(history.logicalRowType()).thenReturn(ntz);
        PaimonScanNode nativeNtz = legacyOrcRoutingNode(ntz, Collections.emptyMap(), history);
        Assert.assertTrue(nativeNtz.supportNativeReader(rawFiles("data.orc")));
        Assert.assertFalse(nativeNtz.supportNativeReader(Optional.empty()));
        Assert.assertFalse(nativeNtz.supportNativeReader(rawFiles("data.avro")));
    }

    @Test
    public void testLegacyOrcRoutingUsesHistoricalTypesAndEffectiveOptions() throws Exception {
        RowType ltz = DataTypes.ROW(DataTypes.ARRAY(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        TableSchema history = Mockito.mock(TableSchema.class);
        Mockito.lenient().when(history.logicalRowType()).thenReturn(ltz);
        PaimonScanNode historicalLtz = legacyOrcRoutingNode(DataTypes.ROW(DataTypes.TIMESTAMP(6)),
                Collections.emptyMap(), history);
        Assert.assertFalse(historicalLtz.supportNativeReader(rawFiles("data.orc")));

        PaimonScanNode optionsOverride = legacyOrcRoutingNode(ltz, Collections.emptyMap(), null);
        Table effectiveTable = Mockito.mock(Table.class);
        Mockito.lenient().when(effectiveTable.options())
                .thenReturn(Collections.singletonMap("orc.timestamp-ltz.legacy.type", "false"));
        Mockito.lenient().when(effectiveTable.rowType()).thenReturn(ltz);
        setField(PaimonScanNode.class, optionsOverride, "processedTable", effectiveTable);
        Assert.assertTrue(optionsOverride.supportNativeReader(rawFiles("data.orc")));
    }

    @Test
    public void testLegacyOrcHistoryLookupUsesResolvedRelationAndIsCached() throws Exception {
        RowType ntz = DataTypes.ROW(DataTypes.TIMESTAMP(6));
        PaimonScanNode node = legacyOrcRoutingNode(ntz, Collections.emptyMap(), null);
        DataTable effectiveTable = Mockito.mock(DataTable.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(effectiveTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(effectiveTable.rowType()).thenReturn(ntz);
        TableSchema history = Mockito.mock(TableSchema.class);
        Mockito.when(history.logicalRowType()).thenReturn(ntz);
        Mockito.when(effectiveTable.schemaManager().schema(7L)).thenReturn(history);
        setField(PaimonScanNode.class, node, "processedTable", effectiveTable);

        Assert.assertTrue(node.supportNativeReader(rawFiles("first.orc", "second.orc")));
        Assert.assertTrue(node.supportNativeReader(rawFiles("third.orc")));
        Mockito.verify(effectiveTable.schemaManager(), Mockito.times(1)).schema(7L);
    }

    private PaimonScanNode legacyOrcRoutingNode(RowType rowType, Map<String, String> options,
            TableSchema history) {
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, new SessionVariable(), ScanContext.EMPTY);
        DataTable table = Mockito.mock(DataTable.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.lenient().when(table.options()).thenReturn(options);
        Mockito.lenient().when(table.rowType()).thenReturn(rowType);
        Mockito.lenient().when(table.schemaManager().schema(7L)).thenReturn(history);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.lenient().when(source.getPaimonTable()).thenReturn(table);
        Mockito.lenient().when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonExternalTable.class));
        node.setSource(source);
        return node;
    }

    private Optional<List<RawFile>> rawFiles(String... paths) {
        List<RawFile> files = new ArrayList<>();
        for (String path : paths) {
            RawFile file = Mockito.mock(RawFile.class);
            Mockito.when(file.path()).thenReturn(path);
            Mockito.lenient().when(file.schemaId()).thenReturn(7L);
            files.add(file);
        }
        return Optional.of(files);
    }

    @Test
    public void testGetBackendPaimonOptionsForJdbcCatalog() throws Exception {
        String driverUrl = "file:///tmp/postgresql-42.5.0.jar";
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:postgresql://127.0.0.1:5442/postgres");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.driver_url", driverUrl);
        props.put("paimon.jdbc.driver_class", "org.postgresql.Driver");
        PaimonJdbcMetaStoreProperties jdbcMetaStoreProperties =
                (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);

        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(jdbcMetaStoreProperties);

        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);

        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);
        node.setSource(source);

        Map<String, String> backendOptions = node.getBackendPaimonOptions();
        Assert.assertEquals("org.postgresql.Driver", backendOptions.get("jdbc.driver_class"));
        Assert.assertEquals(driverUrl, backendOptions.get("jdbc.driver_url"));
        Assert.assertEquals(2, backendOptions.size());
    }

    @Test
    public void testGetBackendPaimonOptionsForJniIOManager() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.jni.enable_jni_io_manager", "true");
        props.put("paimon.jni.io_manager.tmp_dir", "/tmp/doris-paimon");
        props.put("paimon.jni.io_manager.impl_class", "org.example.CustomIOManager");

        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getProperties()).thenReturn(props);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(Mockito.mock(MetastoreProperties.class));

        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        node.setSource(source);

        Map<String, String> backendOptions = node.getBackendPaimonOptions();
        Assert.assertEquals("true", backendOptions.get("jni.enable_jni_io_manager"));
        Assert.assertEquals("/tmp/doris-paimon", backendOptions.get("jni.io_manager.tmp_dir"));
        Assert.assertEquals("org.example.CustomIOManager",
                backendOptions.get("jni.io_manager.impl_class"));
        Assert.assertEquals(3, backendOptions.size());
    }

    @Test
    public void testApplyBackendPaimonOptionsAtScanNodeLevel() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        node.setSource(source);

        Map<String, String> backendOptions = new HashMap<>();
        backendOptions.put("jdbc.driver_url", "file:///tmp/postgresql-42.5.0.jar");
        backendOptions.put("jdbc.driver_class", "org.postgresql.Driver");
        setField(FileQueryScanNode.class, node, "params", new TFileScanRangeParams());
        setField(PaimonScanNode.class, node, "backendPaimonOptions", backendOptions);
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        invokePrivateMethod(node, "setScanLevelPaimonOptions");

        Assert.assertEquals(backendOptions, node.getFileScanRangeParams().getPaimonOptions());

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        PaimonSplit jniSplit = new PaimonSplit(createDataSplit("scan_level.parquet"));
        Assert.assertNotNull(jniSplit.getPartitionValues());
        Assert.assertTrue(jniSplit.getPartitionValues().isEmpty());
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, jniSplit);
        Assert.assertFalse(rangeDesc.getTableFormatParams().getPaimonParams().isSetPaimonOptions());
    }

    @Test
    public void testSetPartitionValuesBuildsAlignedMetadata() {
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = Mockito.mock(Table.class);
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        Mockito.when(paimonTable.partitionKeys()).thenReturn(Arrays.asList("region", "dt"));
        node.setSource(source);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("dt", null);
        partitionValues.put("region", "cn");
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        node.setPartitionValues(rangeDesc, partitionValues);

        Assert.assertEquals(Arrays.asList("region", "dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("cn", ""), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(false, true), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testGetPathPartitionKeysReturnsTablePartitionKeys() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table table = Mockito.mock(Table.class);
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("Dt", "Region"));
        Mockito.when(sysTable.isDataTable()).thenReturn(true);
        node.setSource(source);

        Assert.assertEquals(Arrays.asList("Dt", "Region"), node.getPathPartitionKeys());
    }

    @Test
    public void testGetPathPartitionKeysReturnsEmptyForMetadataSystemTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(sysTable.isDataTable()).thenReturn(false);
        node.setSource(source);

        Assert.assertEquals(Collections.emptyList(), node.getPathPartitionKeys());
    }

    @Test
    public void testSetPaimonParamsUsesOrderedPartitionKeys() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table table = Mockito.mock(Table.class);
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(sysTable.isDataTable()).thenReturn(true);
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("Pt", "Dt"));
        node.setSource(source);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setColumnsFromPathKeys(Collections.singletonList("stale"));
        rangeDesc.setColumnsFromPath(Collections.singletonList("old"));
        rangeDesc.setColumnsFromPathIsNull(Collections.singletonList(false));
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("Dt", null);
        partitionValues.put("Pt", "p1");
        PaimonSplit split = new PaimonSplit(createDataSplit("ordered.parquet"));
        split.setPaimonPartitionValues(partitionValues);

        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class}, rangeDesc, split);

        Assert.assertEquals(Arrays.asList("Pt", "Dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("p1", ""), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(false, true), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testNativeSplitCarriesPartitionMetadataWithoutRuntimeFilterPruning() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonScanNode spyNode = Mockito.spy(node);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getCatalog()).thenReturn(Mockito.mock(PaimonExternalCatalog.class));
        Table table = Mockito.mock(Table.class);
        PaimonSysExternalTable externalTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(table.partitionKeys()).thenReturn(Collections.singletonList("par"));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(
                DataTypes.FIELD(0, "par", DataTypes.INT())));
        Mockito.when(externalTable.isDataTable()).thenReturn(true);
        spyNode.setSource(source);

        Mockito.doReturn(Collections.singletonList(createDataSplit("partitioned.parquet")))
                .when(spyNode).getPaimonSplitFromAPI();
        mockNativeReader(spyNode);
        setField(FileQueryScanNode.class, spyNode, "fileSplitter",
                new FileSplitter(32L * 1024 * 1024, 64L * 1024 * 1024, 0));
        setField(PaimonScanNode.class, spyNode, "storagePropertiesMap", Collections.emptyMap());
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getMaxInitialSplitSize()).thenReturn(32L * 1024 * 1024);
        Mockito.when(sv.getMaxSplitSize()).thenReturn(64L * 1024 * 1024);
        Mockito.when(sv.getTimeZone()).thenReturn("UTC");

        List<org.apache.doris.spi.Split> splits = spyNode.getSplits(1);

        Assert.assertEquals(1, splits.size());
        PaimonSplit split = (PaimonSplit) splits.get(0);
        Assert.assertEquals(Collections.singletonMap("par", "1"),
                split.getPaimonPartitionValues());
        Assert.assertEquals(Collections.emptyList(), split.getPartitionValues());
    }

    @Test
    public void testSetPaimonParamsUsesJniForLogicalSplit() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), new SessionVariable());
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        node.setSource(source);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(createDataSplit("jni-only.parquet")));

        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        Assert.assertTrue(rangeDesc.getTableFormatParams().getPaimonParams().isSetPaimonSplit());
    }

    @Test
    public void testRustReaderSelectionRequiresFileScannerV2() throws Exception {
        // The V1 FileScanner explicitly rejects PAIMON_RUST, so a rust request may
        // only be encoded when enable_file_scanner_v2 is on; with V2 disabled the
        // split falls back to JNI so the selected scanner can consume it.
        for (boolean scannerV2 : Arrays.asList(true, false)) {
            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = scannerV2;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                    1, Collections.singletonList(new DataField(0, "id", new IntType())),
                    0, Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyMap(), null));
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(source.getExternalTable()).thenReturn(externalTable);
            Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
            setField(PaimonScanNode.class, node, "backendStorageProperties",
                    ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
            Mockito.when(externalTable.getDbName()).thenReturn("db");
            Mockito.when(externalTable.getName()).thenReturn("t");
            node.setSource(source);
            // The rust gate and schema serialization use doInitialize's cached
            // getProcessedTable() result, not the raw source table.
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);
            setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("rust_gate.parquet")));

            Assert.assertEquals(scannerV2 ? TPaimonReaderType.PAIMON_RUST : TPaimonReaderType.PAIMON_JNI,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testRustSchemaJsonShipsProcessedTableRelationOptions() throws Exception {
        // Relation options such as t@options('read.batch-size'='1') are applied by
        // getProcessedTable(); the JNI reader serializes that effective table, and the
        // rust reader rebuilds its table from the shipped schema JSON, deriving its
        // read batch size from the schema options. Only the processed table's schema
        // carries the override here, and source.getPaimonTable() is deliberately left
        // unstubbed (it returns null): if the serialization ever regresses to the raw
        // cached table, the rust gate falls back to JNI and this test fails.
        SessionVariable vars = new SessionVariable();
        vars.setEnablePaimonRustReader(true);
        vars.enableFileScannerV2 = true;

        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);

        // The processed table: the relation override is merged into its schema.
        FileStoreTable processed = Mockito.mock(FileStoreTable.class);
        Mockito.when(processed.schema()).thenReturn(new TableSchema(
                1, Collections.singletonList(new DataField(0, "id", new IntType())),
                0, Collections.emptyList(), Collections.emptyList(),
                ImmutableMap.of("read.batch-size", "1"), null));

        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
        setField(PaimonScanNode.class, node, "backendStorageProperties",
                ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
        Mockito.when(externalTable.getDbName()).thenReturn("db");
        Mockito.when(externalTable.getName()).thenReturn("t");
        node.setSource(source);
        setField(PaimonScanNode.class, node, "processedTable", processed);
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(createDataSplit("relation_options.parquet")));

        org.apache.doris.thrift.TPaimonFileDesc fileDesc =
                rangeDesc.getTableFormatParams().getPaimonParams();
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, fileDesc.getReaderType());
        TableSchema shipped = org.apache.paimon.utils.JsonSerdeUtil.fromJson(
                fileDesc.getPaimonTableSchemaJson(), TableSchema.class);
        Assert.assertEquals("1", shipped.options().get("read.batch-size"));
    }

    @Test
    public void testRustSchemaJsonStripsTimeTravelSelectors() throws Exception {
        // A statement fence pins the data snapshot by merging scan.snapshot-id into the
        // schema options (isolateSnapshotRead) while the fields keep the latest schema —
        // a schema-only ALTER after the last data commit must stay visible. The rust
        // reader pins data via the serialized DataSplit and re-resolves a shipped
        // selector in copy_with_time_travel, swapping the fields back to the pinned
        // snapshot's older schema; the shipped JSON must therefore carry the resolved
        // fields without the planning selectors, keeping every other option.
        // The derived scan.mode is stripped with it in every spelling: paimon accepts
        // enum option values case-insensitively but preserves the original string in
        // the options map, so a table can persist scan.mode=FROM-SNAPSHOT — the rust
        // reader recognizes the value case-insensitively and would reject the now-bare
        // uppercase mode for lacking its selector, while JNI accepts the original
        // option.
        for (String scanMode : new String[] {"from-snapshot", "FROM-SNAPSHOT", " From-Snapshot "}) {
            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);

            // Resolved (latest) schema: the column added after the last data commit is a
            // field, and the fence snapshot id is only planning state in the options.
            FileStoreTable processed = Mockito.mock(FileStoreTable.class);
            Mockito.when(processed.schema()).thenReturn(new TableSchema(
                    1, Arrays.asList(
                            new DataField(0, "id", new IntType()),
                            new DataField(1, "added_after", DataTypes.STRING())),
                    1, Collections.emptyList(), Collections.emptyList(),
                    ImmutableMap.of(
                            "scan.snapshot-id", "3",
                            "scan.tag-name", "stale-tag",
                            // Paimon's copyInternal materializes the derived scan mode for
                            // the merged selector; it dangles once the selector is stripped.
                            "scan.mode", scanMode,
                            "read.batch-size", "1"),
                    null));

            PaimonSource source = Mockito.mock(PaimonSource.class);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(source.getExternalTable()).thenReturn(externalTable);
            Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
            setField(PaimonScanNode.class, node, "backendStorageProperties",
                    ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
            Mockito.when(externalTable.getDbName()).thenReturn("db");
            Mockito.when(externalTable.getName()).thenReturn("t");
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", processed);
            setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("strip_selectors.parquet")));

            org.apache.doris.thrift.TPaimonFileDesc fileDesc =
                    rangeDesc.getTableFormatParams().getPaimonParams();
            Assert.assertEquals("scan.mode " + scanMode, TPaimonReaderType.PAIMON_RUST,
                    fileDesc.getReaderType());
            TableSchema shipped = org.apache.paimon.utils.JsonSerdeUtil.fromJson(
                    fileDesc.getPaimonTableSchemaJson(), TableSchema.class);
            Assert.assertNull(shipped.options().get(CoreOptions.SCAN_SNAPSHOT_ID.key()));
            Assert.assertNull(shipped.options().get(CoreOptions.SCAN_TAG_NAME.key()));
            // The fence-materialized "from-snapshot" would otherwise dangle: the rust
            // ReadBuilder requires a selector for it and rejects the open.
            Assert.assertNull("scan.mode " + scanMode + " must be stripped",
                    shipped.options().get(CoreOptions.SCAN_MODE.key()));
            Assert.assertEquals("1", shipped.options().get("read.batch-size"));
            // The resolved fields are transported untouched, including the column added
            // after the last data commit.
            Assert.assertEquals(
                    Arrays.asList("id", "added_after"),
                    shipped.fields().stream().map(DataField::name).collect(Collectors.toList()));
        }
    }

    @Test
    public void testGetFieldIndexMatchesMixedCaseColumns() {
        List<String> fieldNames = Arrays.asList("data", "mIxEd_COL", "PART");

        Assert.assertEquals(1, PaimonScanNode.getFieldIndex(fieldNames, "mixed_col"));
        Assert.assertEquals(2, PaimonScanNode.getFieldIndex(fieldNames, "part"));
        Assert.assertEquals(-1, PaimonScanNode.getFieldIndex(fieldNames, "missing_col"));
    }

    @Test
    public void testHistorySchemaUsesRelationPaimonTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        DataTable branchTable = Mockito.mock(DataTable.class, Mockito.RETURNS_DEEP_STUBS);
        TableSchema branchSchema = Mockito.mock(TableSchema.class);
        Mockito.when(branchTable.schemaManager().schema(3L)).thenReturn(branchSchema);
        Mockito.when(branchSchema.id()).thenReturn(3L);
        Mockito.when(branchSchema.fields()).thenReturn(Collections.emptyList());
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(branchTable);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        node.setSource(source);
        setField(FileQueryScanNode.class, node, "params", new TFileScanRangeParams());

        try (MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            invokePrivateMethod(node, "putHistorySchemaInfo", new Class<?>[] {Long.class}, 3L);
            paimonUtils.verify(
                    () -> PaimonUtils.getSchemaCacheValue(externalTable, 3L), Mockito.never());
        }

        Mockito.verify(branchTable.schemaManager()).schema(3L);
        Assert.assertEquals(3L, node.getFileScanRangeParams().getHistorySchemaInfo().get(0).getSchemaId());
    }

    @Test
    public void testPaimonHistorySemanticsDoNotInheritHiveInt96Timezone() throws Exception {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(0), new TupleId(0), sv));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        Mockito.lenient().when(table.getHiveParquetTimeZone()).thenReturn("Asia/Shanghai");
        Mockito.lenient().doReturn(table).when(node).getTargetTable();
        Method getter = FileQueryScanNode.class.getDeclaredMethod("getHiveParquetTimeZone");
        getter.setAccessible(true);
        Assert.assertEquals("", getter.invoke(node));
    }

    @Test
    public void testNativeParquetSignalDoesNotIncludeOrcOrJni() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        DataTable table = Mockito.mock(DataTable.class, Mockito.RETURNS_DEEP_STUBS);
        TableSchema schema = Mockito.mock(TableSchema.class);
        Mockito.when(table.schemaManager().schema(3L)).thenReturn(schema);
        Mockito.when(schema.id()).thenReturn(3L);
        Mockito.when(schema.fields()).thenReturn(Collections.emptyList());
        Mockito.when(table.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        node.setSource(source);
        TFileScanRangeParams params = new TFileScanRangeParams();
        setField(FileQueryScanNode.class, node, "params", params);

        node.setScanParams(new TFileRangeDesc(), new PaimonSplit(createDataSplit("jni.parquet")));
        Assert.assertFalse(params.isContainsNativeParquet());
        PaimonSplit nativeSplit = Mockito.mock(PaimonSplit.class);
        Mockito.when(nativeSplit.getTableFormatType()).thenReturn(org.apache.doris.datasource.TableFormatType.PAIMON);
        Mockito.when(nativeSplit.getSchemaId()).thenReturn(3L);
        Mockito.when(nativeSplit.getDeletionFile()).thenReturn(Optional.empty());
        Mockito.when(nativeSplit.getRowCount()).thenReturn(Optional.empty());
        Mockito.when(nativeSplit.getPathString()).thenReturn("/native.orc");
        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, nativeSplit);
        Assert.assertEquals(TFileFormatType.FORMAT_ORC, range.getFormatType());
        Assert.assertFalse(params.getHistorySchemaInfo().isEmpty());
        Assert.assertFalse(params.isContainsNativeParquet());

        Mockito.when(nativeSplit.getPathString()).thenReturn("/native.parquet");
        node.setScanParams(range, nativeSplit);
        Assert.assertEquals(TFileFormatType.FORMAT_PARQUET, range.getFormatType());
        Assert.assertTrue(params.isContainsNativeParquet());
        // A later ORC range must not erase the capability required by an earlier Parquet range.
        Mockito.when(nativeSplit.getPathString()).thenReturn("/later.orc");
        node.setScanParams(range, nativeSplit);
        Assert.assertTrue(params.isContainsNativeParquet());
    }

    private void mockJniReader(PaimonScanNode spyNode) {
        Mockito.doReturn(false).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private Table mockPlanningTable(RowType rowType, Map<String, String> tableOptions,
            AtomicInteger planCount) {
        return mockPlanningTable(rowType, tableOptions, planCount,
                Collections.singletonList(createDataSplit("planned.parquet")));
    }

    private Table mockPlanningTable(RowType rowType, Map<String, String> tableOptions,
            AtomicInteger planCount, List<org.apache.paimon.table.source.Split> plannedSplits) {
        Table table = Mockito.mock(Table.class);
        ReadBuilder readBuilder = Mockito.mock(ReadBuilder.class);
        TableScan scan = Mockito.mock(TableScan.class);
        TableScan.Plan plan = Mockito.mock(TableScan.Plan.class);
        Mockito.when(table.rowType()).thenReturn(rowType);
        Mockito.when(table.options()).thenReturn(tableOptions);
        Mockito.when(table.newReadBuilder()).thenReturn(readBuilder);
        Mockito.when(readBuilder.withFilter(ArgumentMatchers.anyList())).thenReturn(readBuilder);
        Mockito.when(readBuilder.withProjection(ArgumentMatchers.any(int[].class))).thenReturn(readBuilder);
        Mockito.when(readBuilder.newScan()).thenReturn(scan);
        Mockito.when(scan.plan()).thenReturn(plan);
        Mockito.when(plan.splits()).thenAnswer(invocation -> {
            planCount.incrementAndGet();
            return plannedSplits;
        });
        return table;
    }

    private static final class CountingSplit implements org.apache.paimon.table.source.Split {
        private static final int PAYLOAD_SIZE = 1024 * 1024;
        private transient AtomicInteger serializationWriteCount;

        private CountingSplit(AtomicInteger serializationWriteCount) {
            this.serializationWriteCount = serializationWriteCount;
        }

        @Override
        public long rowCount() {
            return 1;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.of(1);
        }

        private void writeObject(ObjectOutputStream output) throws IOException {
            output.defaultWriteObject();
            for (int i = 0; i < PAYLOAD_SIZE; i++) {
                serializationWriteCount.incrementAndGet();
                output.writeByte(i);
            }
        }
    }

    private PaimonScanNode newPlanningNode(int id, PaimonExternalTable relationTable,
            PaimonExternalTable targetTable, PaimonExternalCatalog catalog, Table paimonTable,
            long snapshotId, long schemaId, Map<String, String> resolvedOptions,
            List<Predicate> predicates, String projectedColumn) throws Exception {
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(relationTable.getDatabase()).thenReturn(database);
        Mockito.when(relationTable.getName()).thenReturn("table");
        TupleDescriptor desc = new TupleDescriptor(new TupleId(id));
        desc.setTable(relationTable);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(id), desc);
        slot.setColumn(new Column(projectedColumn, Type.INT));
        desc.addSlot(slot);

        PaimonScanNode node =
                new PaimonScanNode(new PlanNodeId(id), desc, false, sv, ScanContext.EMPTY);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.getExternalTable()).thenReturn(relationTable);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);
        node.setSource(source);
        setField(PaimonScanNode.class, node, "processedTable", paimonTable);
        setField(PaimonScanNode.class, node, "predicates", predicates);
        node.setRelationSnapshot(Optional.of(new PaimonMvccSnapshot(
                new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(snapshotId, schemaId, paimonTable)))));
        if (!resolvedOptions.isEmpty()) {
            TableScanParams scanParams = new TableScanParams(
                    TableScanParams.OPTIONS, resolvedOptions, Collections.emptyList());
            scanParams.reuseResolvedMapParams(resolvedOptions);
            node.setScanParams(scanParams);
        }
        return node;
    }

    private List<org.apache.paimon.table.source.Split> assertPlanCount(
            PaimonScanNode node, AtomicInteger planCount, int expected)
            throws UserException {
        List<org.apache.paimon.table.source.Split> splits = node.getPaimonSplitFromAPI();
        Assert.assertEquals(1, splits.size());
        Assert.assertEquals(expected, planCount.get());
        return splits;
    }

    private PaimonScanNode newTestNode(PlanNodeId planNodeId, TupleId tupleId, SessionVariable sessionVariable) {
        TupleDescriptor desc = new TupleDescriptor(tupleId);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(externalTable.getPaimonTable(ArgumentMatchers.any(Optional.class))).thenReturn(paimonTable);
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(externalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(Mockito.mock(CatalogIf.class));
        desc.setTable(externalTable);
        return new PaimonScanNode(planNodeId, desc, false, sessionVariable, ScanContext.EMPTY);
    }

    private PaimonSource mockPaimonSourceWithPartitionKeys(List<String> partitionKeys) {
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(partitionKeys);
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        return source;
    }

    private Table mockPaimonTableWithPartitionKeys(List<String> partitionKeys) {
        Table paimonTable = Mockito.mock(Table.class);
        Mockito.when(paimonTable.partitionKeys()).thenReturn(partitionKeys);
        return paimonTable;
    }

    private void mockNativeReader(PaimonScanNode spyNode) {
        Mockito.doReturn(true).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private void setField(Class<?> clazz, Object target, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Object invokePrivateMethod(Object target, String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private Object invokePrivateMethod(Object target, String methodName) throws Exception {
        return invokePrivateMethod(target, methodName, new Class<?>[0]);
    }

    private DataSplit createDataSplit(String fileName) {
        return createDataSplit(Collections.singletonList(fileName));
    }

    private DataSplit createDataSplit(List<String> fileNames) {
        List<DataFileMeta> dataFileMetas = fileNames.stream().map(fileName ->
                DataFileMeta.forAppend(fileName, 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                        1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                        Collections.<String>emptyList(), null, null, Collections.<String>emptyList()))
                .collect(Collectors.toList());
        return DataSplit.builder()
                .rawConvertible(true)
                .withPartition(BinaryRow.singleColumn(1))
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(dataFileMetas)
                .build();
    }

    private DataSplit mockCountDataSplit(String fileName, long rowCount) {
        DataFileMeta dataFileMeta = DataFileMeta.forAppend(fileName, 64L * 1024 * 1024, rowCount,
                SimpleStats.EMPTY_STATS, 1L, 1L, 1L, Collections.<String>emptyList(), null,
                FileSource.APPEND, Collections.<String>emptyList(), null, null,
                Collections.<String>emptyList());
        DataSplit dataSplit = Mockito.mock(DataSplit.class);
        Mockito.when(dataSplit.rowCount()).thenReturn(rowCount);
        Mockito.when(dataSplit.mergedRowCount()).thenReturn(OptionalLong.of(rowCount));
        Mockito.when(dataSplit.partition()).thenReturn(BinaryRow.singleColumn(1));
        Mockito.when(dataSplit.dataFiles()).thenReturn(Collections.singletonList(dataFileMeta));
        Mockito.when(dataSplit.convertToRawFiles()).thenReturn(Optional.empty());
        Mockito.when(dataSplit.deletionFiles()).thenReturn(Optional.empty());
        return dataSplit;
    }

    @Test
    public void testRustReaderSelectionRejectsFallbackSplits() throws Exception {
        RustRoutingFixture f = new RustRoutingFixture();
        DataOutputSerializer out = new DataOutputSerializer(1024);
        f.split.serialize(out);
        out.writeBoolean(true);
        DataInputDeserializer in = new DataInputDeserializer();
        in.setBuffer(out.getSharedBuffer(), 0, out.length());
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                f.reader(FallbackReadFileStoreTable.FallbackDataSplit.deserialize(in)));
        setField(PaimonScanNode.class, f.node, "processedTable", Mockito.mock(FallbackReadFileStoreTable.class));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
    }

    @Test
    public void testRustReaderSelectionRejectsQueryAuthTables() throws Exception {
        RustRoutingFixture f = new RustRoutingFixture();
        f.schema(1, new IntType(), ImmutableMap.of("query-auth.enabled", "true"), false);
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
    }

    @Test
    public void testRustReaderSelectionRejectsUnsupportedPkDvModes() throws Exception {
        // The pinned rust read_pk supports partial-update / aggregation tables
        // with deletion vectors only in the fully materialized shape: it rejects
        // deletion-vectors.merge-on-read=true outright, and otherwise requires
        // every split to be compacted (level != 0) and known free of retract rows
        // (DataSplit::is_fully_materialized_pk_dv). All other shapes are valid
        // Java/JNI scans, so they must fall back here instead of failing the
        // BE open. Deduplicate stays rust-eligible: its read_pk routes
        // uncompacted DV splits to the KV reader, which applies the DVs.
        SessionVariable vars = new SessionVariable();
        vars.setEnablePaimonRustReader(true);
        vars.enableFileScannerV2 = true;

        // createDataSplit builds a level-0 file — the ordinary uncompacted shape.
        DataSplit levelZeroSplit = createDataSplit("dv_level0.parquet");
        DataSplit unknownDeleteCountSplit = dvSplitOverFiles(
                dvCompactedFile("dv_unknown.parquet", null));
        DataSplit retractsSplit = dvSplitOverFiles(
                dvCompactedFile("dv_retracts.parquet", 1L));
        DataSplit materializedSplit = dvSplitOverFiles(
                dvCompactedFile("dv_materialized.parquet", 0L));

        for (CoreOptions.MergeEngine engine : Arrays.asList(
                CoreOptions.MergeEngine.PARTIAL_UPDATE, CoreOptions.MergeEngine.AGGREGATE)) {
            // deletion-vectors.merge-on-read=true: rejected table-wide.
            Assert.assertEquals("merge-on-read " + engine, TPaimonReaderType.PAIMON_JNI,
                    dvReaderTypeOf(vars, engine, true,
                            ImmutableMap.of("deletion-vectors.merge-on-read", "true"),
                            materializedSplit));
            // merge-on-read=false but the split still needs per-key merge work:
            // level-0 data, unknown delete count, or known retracts — all fall
            // back per split.
            Assert.assertEquals("level-0 " + engine, TPaimonReaderType.PAIMON_JNI,
                    dvReaderTypeOf(vars, engine, true, Collections.emptyMap(), levelZeroSplit));
            Assert.assertEquals("unknown delete count " + engine, TPaimonReaderType.PAIMON_JNI,
                    dvReaderTypeOf(vars, engine, true, Collections.emptyMap(), unknownDeleteCountSplit));
            Assert.assertEquals("retracts " + engine, TPaimonReaderType.PAIMON_JNI,
                    dvReaderTypeOf(vars, engine, true, Collections.emptyMap(), retractsSplit));
            // A fully materialized compacted split reads raw on rust, and without
            // deletion vectors the KV reader handles any split.
            Assert.assertEquals("materialized " + engine, TPaimonReaderType.PAIMON_RUST,
                    dvReaderTypeOf(vars, engine, true, Collections.emptyMap(), materializedSplit));
            Assert.assertEquals("no dv " + engine, TPaimonReaderType.PAIMON_RUST,
                    dvReaderTypeOf(vars, engine, false, Collections.emptyMap(), levelZeroSplit));
        }

        // Deduplicate keeps rust eligibility for uncompacted DV splits and for
        // merge-on-read=true: the KV reader applies the attached per-file DVs.
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST,
                dvReaderTypeOf(vars, CoreOptions.MergeEngine.DEDUPLICATE, true,
                        ImmutableMap.of("deletion-vectors.merge-on-read", "true"), levelZeroSplit));
    }

    // Builds a node whose processed table is a primary-key table with the given
    // merge engine and deletion-vector options (all other gates open: no
    // query-auth, v2 on, ordinary DataSplit) and returns the reader type chosen
    // for the given split.
    private TPaimonReaderType dvReaderTypeOf(SessionVariable vars, CoreOptions.MergeEngine mergeEngine,
            boolean deletionVectorsEnabled, Map<String, String> schemaOptions, DataSplit split)
            throws Exception {
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
        // lenient: the schema / external-table stubs are only consumed when the
        // split stays rust-eligible.
        Mockito.lenient().when(paimonTable.schema()).thenReturn(new TableSchema(
                1, Collections.singletonList(new DataField(0, "id", new IntType())),
                0, Collections.emptyList(), Collections.emptyList(), schemaOptions, null));
        CoreOptions options = Mockito.mock(CoreOptions.class);
        Mockito.when(paimonTable.coreOptions()).thenReturn(options);
        Mockito.when(options.queryAuthEnabled()).thenReturn(false);
        Mockito.when(options.mergeEngine()).thenReturn(mergeEngine);
        Mockito.when(options.deletionVectorsEnabled()).thenReturn(deletionVectorsEnabled);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Mockito.lenient().when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
        setField(PaimonScanNode.class, node, "backendStorageProperties",
                ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
        Mockito.lenient().when(externalTable.getDbName()).thenReturn("db");
        Mockito.lenient().when(externalTable.getName()).thenReturn("t");
        node.setSource(source);
        setField(PaimonScanNode.class, node, "processedTable", paimonTable);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(split));
        return rangeDesc.getTableFormatParams().getPaimonParams().getReaderType();
    }

    // A compacted (level != 0) data file with an explicit retract-row count:
    // null = unknown, 0 = fully materialized, >0 = rows still to retract.
    private static DataFileMeta dvCompactedFile(String fileName, Long deleteRowCount) {
        return DataFileMeta.create(fileName, 64L * 1024 * 1024, 1L,
                DataFileMeta.EMPTY_MIN_KEY, DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, 5, deleteRowCount, null,
                FileSource.APPEND, Collections.emptyList(), null, Collections.emptyList());
    }

    private static DataSplit dvSplitOverFiles(DataFileMeta... files) {
        return DataSplit.builder()
                .rawConvertible(true)
                .withPartition(BinaryRow.singleColumn(1))
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Arrays.asList(files))
                .build();
    }

    @Test
    public void testRustReaderSelectionRejectsOrcTimestampLtzSchemas() throws Exception {
        RustRoutingFixture f = new RustRoutingFixture();
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, f.reader(createDataSplit("plain.orc")));
        f.schema(1, new org.apache.paimon.types.LocalZonedTimestampType(6), Collections.emptyMap(), false);
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, f.reader(f.split));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(createDataSplit("ltz.orc")));
    }

    @Test
    public void testRustReaderSelectionRejectsMixedFormatOrcLtzSplits() throws Exception {
        // Paimon allows per-level file.format, so one DataSplit can mix
        // Parquet and ORC members. The old gate read only the split path's
        // suffix (the first file), so a first-Parquet/later-ORC split with an
        // LTZ schema passed while rust still applied the shifted ORC decode
        // to the ORC members. The format now comes from every member file.
        SessionVariable vars = new SessionVariable();
        vars.setEnablePaimonRustReader(true);
        vars.enableFileScannerV2 = true;
        List<DataField> ltzFields = Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "ts_ltz", new LocalZonedTimestampType(6)));

        // First file Parquet, later member ORC: the first file's suffix no
        // longer speaks for the split -> JNI.
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                readerTypeOf(vars, ltzFields, "first.parquet", "second.orc"));
        // ORC first, later Parquet member (the shape the suffix check did
        // catch) stays JNI.
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                readerTypeOf(vars, ltzFields, "first.orc", "second.parquet"));
        // All-Parquet members with the same LTZ schema stay rust-eligible.
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST,
                readerTypeOf(vars, ltzFields, "a.parquet", "b.parquet"));
    }

    @Test
    public void testRustReaderSelectionRejectsNestedTimestampLtzSchemas() throws Exception {
        // An LTZ nested under MAP/ARRAY/ROW reaches the same shifted ORC
        // decode through the container's field materialization; the old
        // top-level-only type-root check missed it. The search recurses; the
        // same containers without LTZ stay rust-eligible, and Parquet splits
        // with nested LTZ are unaffected (only the ORC decoder diverges).
        SessionVariable vars = new SessionVariable();
        vars.setEnablePaimonRustReader(true);
        vars.enableFileScannerV2 = true;
        LocalZonedTimestampType ltz = new LocalZonedTimestampType(6);

        // LTZ under each container kind, uniform-ORC split -> JNI.
        for (DataField nested : Arrays.asList(
                new DataField(1, "m", new org.apache.paimon.types.MapType(new IntType(), ltz)),
                new DataField(1, "arr", new org.apache.paimon.types.ArrayType(ltz)),
                new DataField(1, "r",
                        new RowType(Collections.singletonList(new DataField(0, "ts_ltz", ltz)))))) {
            Assert.assertEquals("nested " + nested.name() + " under ORC",
                    TPaimonReaderType.PAIMON_JNI,
                    readerTypeOf(vars, Arrays.asList(
                            new DataField(0, "id", new IntType()), nested),
                            "nested.orc"));
            // The same nested-LTZ schema over Parquet stays rust-eligible.
            Assert.assertEquals("nested " + nested.name() + " over Parquet",
                    TPaimonReaderType.PAIMON_RUST,
                    readerTypeOf(vars, Arrays.asList(
                            new DataField(0, "id", new IntType()), nested),
                            "nested.parquet"));
        }

        // Containers without any LTZ over ORC stay rust-eligible.
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST,
                readerTypeOf(vars, Arrays.asList(
                        new DataField(0, "id", new IntType()),
                        new DataField(1, "m",
                                new org.apache.paimon.types.MapType(new IntType(), new IntType()))),
                        "map_int.orc"));
    }

    @Test
    public void testRustReaderSelectionRejectsExternalDataFileSplits() throws Exception {
        // data-file.external-paths: paimon can store an absolute location in
        // each DataFileMeta, and both Java and the serialized rust split prefer
        // it over the bucket path — but the pinned rust table builds one FileIO
        // from paimon_table, whose storage enum parses every file with the
        // warehouse-selected backend: an hdfs table with an s3:// external file
        // (or an s3 table with an oss:// file) reaches the wrong parser and
        // fails the open, while JNI reads it. Any split carrying an external
        // file therefore routes to JNI, regardless of the external path's own
        // scheme (the shipped options describe only the warehouse).
        SessionVariable vars = new SessionVariable();
        vars.setEnablePaimonRustReader(true);
        vars.enableFileScannerV2 = true;
        List<DataField> fields = Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "ts", new TimestampType(6)));

        // A split whose file carries an external path -> JNI.
        DataFileMeta external = DataFileMeta.forAppend(
                "external.parquet", 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList())
                .newExternalPath("hdfs://other-nn/external/wh/external.parquet");
        DataSplit externalSplit = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(BinaryRow.singleColumn(1))
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(external))
                .build();
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                readerTypeOfSplit(vars, fields, externalSplit));

        // The same split without an external path stays rust-eligible.
        DataSplit plainSplit = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(BinaryRow.singleColumn(1))
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(DataFileMeta.forAppend(
                        "plain.parquet", 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                        1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                        Collections.<String>emptyList(), null, null, Collections.<String>emptyList())))
                .build();
        Assert.assertEquals(TPaimonReaderType.PAIMON_RUST,
                readerTypeOfSplit(vars, fields, plainSplit));
    }

    @Test
    public void testRustReaderSelectionRejectsDeduplicateIgnoreDeleteTables() throws Exception {
        // deduplicate.ignore-delete=true tables stay on JNI: Java's
        // DeduplicateMergeFunction skips retract records when the option is set
        // (including old, uncompacted files that still contain them), but the
        // pinned rust read_pk does not pass table options into its deduplicate
        // merge — it picks the latest row and omits the key when that row is
        // DELETE/UPDATE_BEFORE. An uncompacted insert followed by a delete
        // therefore returns the insert through JNI but silently disappears
        // through rust.
        for (Object[] shape : new Object[][] {
                // DEDUPLICATE engine with the option on -> JNI.
                {CoreOptions.MergeEngine.DEDUPLICATE, true, TPaimonReaderType.PAIMON_JNI},
                // Option off, same engine -> rust.
                {CoreOptions.MergeEngine.DEDUPLICATE, false, TPaimonReaderType.PAIMON_RUST},
                // Option on under a non-deduplicate engine: meaningless there,
                // the gate must not reject -> rust.
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, true, TPaimonReaderType.PAIMON_RUST}}) {
            CoreOptions.MergeEngine mergeEngine = (CoreOptions.MergeEngine) shape[0];
            boolean ignoreDelete = (Boolean) shape[1];
            TPaimonReaderType expected = (TPaimonReaderType) shape[2];

            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            CoreOptions options = Mockito.mock(CoreOptions.class);
            Mockito.when(paimonTable.coreOptions()).thenReturn(options);
            Mockito.when(options.queryAuthEnabled()).thenReturn(false);
            Mockito.when(options.mergeEngine()).thenReturn(mergeEngine);
            Mockito.when(options.ignoreDelete()).thenReturn(ignoreDelete);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(source.getExternalTable()).thenReturn(externalTable);
            Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
            setField(PaimonScanNode.class, node, "backendStorageProperties",
                    ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
            Mockito.when(externalTable.getDbName()).thenReturn("db");
            Mockito.when(externalTable.getName()).thenReturn("t");
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);
            if (expected == TPaimonReaderType.PAIMON_RUST) {
                // Only the rust branch serializes the schema (a JNI-expected
                // stub would trip Mockito's strict unused-stubbing check).
                Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                        1, Collections.singletonList(new DataField(0, "id", new IntType())),
                        0, Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyMap(), null));
            }

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("dedup_ignore_del.parquet")));
            Assert.assertEquals("engine " + mergeEngine + ", ignore-delete " + ignoreDelete,
                    expected, rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testRustReaderSelectionRejectsRestTokenTables() throws Exception {
        // doInitialize snapshots RESTTokenFileIO.validToken().token() into
        // the backend storage properties, discarding expireAtMillis and the
        // REST refresh context, so the shipped credentials look static — but
        // the pinned rust table reuses one option map with no refresh
        // callback, while paimon 1.4.2's JNI RESTTokenFileIO checks expiry
        // before each file operation and obtains a replacement token. A scan
        // crossing the token TTL would start on rust and later fail
        // authentication; only the table's FileIO type reveals the token is
        // renewable.
        for (boolean restToken : new boolean[] {true, false}) {
            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            CoreOptions options = Mockito.mock(CoreOptions.class);
            Mockito.when(paimonTable.coreOptions()).thenReturn(options);
            Mockito.when(options.queryAuthEnabled()).thenReturn(false);
            // S3-location static credentials, an ordinary shape the
            // REST-token snapshot also produces.
            setField(PaimonScanNode.class, node, "backendStorageProperties", ImmutableMap.of(
                    "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT",
                    "AWS_ACCESS_KEY", "ak",
                    "AWS_SECRET_KEY", "sk",
                    "AWS_ENDPOINT", "http://127.0.0.1:19001",
                    "AWS_REGION", "us-east-1"));
            if (restToken) {
                Mockito.when(paimonTable.fileIO())
                        .thenReturn(Mockito.mock(RESTTokenFileIO.class));
                // The JNI branch never serializes the schema.
            } else {
                Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                        1, Collections.singletonList(new DataField(0, "id", new IntType())),
                        0, Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyMap(), null));
                // A plain (or unresolved) FileIO shape stays rust-eligible.
                Mockito.when(paimonTable.fileIO()).thenReturn(null);
            }
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(source.getExternalTable()).thenReturn(externalTable);
            Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
            Mockito.when(externalTable.getDbName()).thenReturn("db");
            Mockito.when(externalTable.getName()).thenReturn("t");
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("rest_token.parquet")));
            Assert.assertEquals("RESTTokenFileIO=" + restToken,
                    restToken ? TPaimonReaderType.PAIMON_JNI : TPaimonReaderType.PAIMON_RUST,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testRustReaderSelectionRejectsRustUnsupportedMergeOptions() throws Exception {
        // Java supports partial-update.remove-record-on-delete /
        // aggregation.remove-record-on-delete and the wider per-field option
        // matrix, but the pinned paimon-rust PartialUpdateConfig /
        // AggregationConfig validations return Unsupported for them — the
        // rust predicates check option-key PRESENCE, so the FE gate mirrors
        // presence exactly (an off value still routes to JNI, because the
        // rust merge construction rejects the bare key). Non-DV tables are
        // the review's case: without this gate an ordinary DataSplit passes
        // the compound gate and fails during the rust merge construction
        // instead of scanning through JNI.
        for (Object[] shape : new Object[][] {
                // Partial-update unsupported keys -> JNI.
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "partial-update.remove-record-on-delete",
                        TPaimonReaderType.PAIMON_JNI},
                {CoreOptions.MergeEngine.PARTIAL_UPDATE,
                        "partial-update.remove-record-on-sequence-group",
                        TPaimonReaderType.PAIMON_JNI},
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "fields.v.ignore-retract",
                        TPaimonReaderType.PAIMON_JNI},
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "fields.v.distinct",
                        TPaimonReaderType.PAIMON_JNI},
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "fields.v.ignore-delete",
                        TPaimonReaderType.PAIMON_JNI},
                // Aggregation unsupported keys -> JNI.
                {CoreOptions.MergeEngine.AGGREGATE, "aggregation.remove-record-on-delete",
                        TPaimonReaderType.PAIMON_JNI},
                {CoreOptions.MergeEngine.AGGREGATE, "fields.v.sequence-group",
                        TPaimonReaderType.PAIMON_JNI},
                {CoreOptions.MergeEngine.AGGREGATE, "ignore-delete",
                        TPaimonReaderType.PAIMON_JNI},
                // Keys the rust read path DOES support: bare /
                // partial-update-prefixed ignore-delete, sequence groups and
                // field aggregation stay rust-eligible.
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "partial-update.ignore-delete",
                        TPaimonReaderType.PAIMON_RUST},
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "fields.v.sequence-group",
                        TPaimonReaderType.PAIMON_RUST},
                {CoreOptions.MergeEngine.PARTIAL_UPDATE, "fields.v.aggregate-function",
                        TPaimonReaderType.PAIMON_RUST},
                {CoreOptions.MergeEngine.AGGREGATE, "fields.v.aggregate-function",
                        TPaimonReaderType.PAIMON_RUST},
                {CoreOptions.MergeEngine.AGGREGATE, "fields.default-aggregate-function",
                        TPaimonReaderType.PAIMON_RUST}}) {
            CoreOptions.MergeEngine mergeEngine = (CoreOptions.MergeEngine) shape[0];
            String optionKey = (String) shape[1];
            TPaimonReaderType expected = (TPaimonReaderType) shape[2];

            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            CoreOptions options = Mockito.mock(CoreOptions.class);
            Mockito.when(paimonTable.coreOptions()).thenReturn(options);
            Mockito.when(options.queryAuthEnabled()).thenReturn(false);
            Mockito.when(options.mergeEngine()).thenReturn(mergeEngine);
            Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                    1, Collections.singletonList(new DataField(0, "id", new IntType())),
                    0, Collections.emptyList(), Collections.emptyList(),
                    ImmutableMap.of(optionKey, "true"), null));
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(source.getExternalTable()).thenReturn(externalTable);
            Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
            setField(PaimonScanNode.class, node, "backendStorageProperties",
                    ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
            Mockito.when(externalTable.getDbName()).thenReturn("db");
            Mockito.when(externalTable.getName()).thenReturn("t");
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("merge_option_gate.parquet")));
            Assert.assertEquals("engine " + mergeEngine + ", option " + optionKey, expected,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    // Builds a node whose processed table carries the given row-type fields
    // (all other gates open: no query-auth, v2 on, ordinary DataSplit) and
    // returns the reader type chosen for a split over the given member files.
    // Multiple file names build a multi-file split (paimon allows per-level
    // file.format, so one DataSplit can mix Parquet and ORC members).
    private TPaimonReaderType readerTypeOf(SessionVariable vars, List<DataField> fields,
            String... fileNames) throws Exception {
        return readerTypeOfSplit(vars, fields, createDataSplit(Arrays.asList(fileNames)));
    }

    // Same harness as readerTypeOf over an externally built DataSplit (e.g. one
    // whose member files carry data-file.external-paths locations).
    private TPaimonReaderType readerTypeOfSplit(SessionVariable vars, List<DataField> fields,
            DataSplit dataSplit) throws Exception {
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                1, fields, 0, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), null));
        CoreOptions options = Mockito.mock(CoreOptions.class);
        Mockito.when(paimonTable.coreOptions()).thenReturn(options);
        Mockito.when(options.queryAuthEnabled()).thenReturn(false);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
        setField(PaimonScanNode.class, node, "backendStorageProperties",
                ImmutableMap.of("AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
        Mockito.when(externalTable.getDbName()).thenReturn("db");
        Mockito.when(externalTable.getName()).thenReturn("t");
        node.setSource(source);
        setField(PaimonScanNode.class, node, "processedTable", paimonTable);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(dataSplit));
        return rangeDesc.getTableFormatParams().getPaimonParams().getReaderType();
    }

    @Test
    public void testRustReaderSelectionRejectsIncrementalScans() throws Exception {
        for (String mode : Arrays.asList("delta", "changelog", "diff")) {
            RustRoutingFixture f = new RustRoutingFixture();
            f.node.setScanParams(new TableScanParams("incr", ImmutableMap.of(
                    "incremental-between", "5,10", "incremental-between-scan-mode", mode), null));
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
        }
    }

    @Test
    public void testRustReaderSelectionRejectsAmbientCredentialProviderModes() throws Exception {
        for (String provider : Arrays.asList("ENV", "SYSTEM_PROPERTIES", "WEB_IDENTITY",
                "CONTAINER", "INSTANCE_PROFILE")) {
            RustRoutingFixture f = new RustRoutingFixture();
            f.credentials(provider);
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
        }
    }

    @Test
    public void testRustReaderSelectionRequiresExplicitS3Credentials() throws Exception {
        // DEFAULT without static keys may use JVM-only credentials or anonymous fallback.
        for (String[] shape : new String[][] {
                {"ANONYMOUS", "s3://warehouse/wh", "PAIMON_RUST", "", ""},
                {"DEFAULT", "s3://warehouse/wh", "PAIMON_JNI", "", ""},
                {"DEFAULT", "s3a://warehouse/wh", "PAIMON_JNI", "example-key", ""},
                {"DEFAULT", "s3://warehouse/wh", "PAIMON_JNI", " ", " "},
                {"DEFAULT", "s3://warehouse/wh", "PAIMON_RUST", "example-key", "example-secret"},
                {"", "s3://warehouse/wh", "PAIMON_JNI", "", ""},
                {"ANONYMOUS", "oss://warehouse/wh", "PAIMON_JNI", "", ""}}) {
            String mode = shape[0];
            String location = shape[1];
            TPaimonReaderType expected =
                    TPaimonReaderType.valueOf(shape[2]);
            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            Mockito.when(source.getTableLocation()).thenReturn(location);
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            // Supply both paths so an incorrect Rust selection fails the
            // reader-type assertion rather than unrelated schema setup.
            Mockito.lenient().when(source.getExternalTable()).thenReturn(externalTable);
            Mockito.lenient().when(paimonTable.schema()).thenReturn(new TableSchema(
                    1, Collections.singletonList(new DataField(0, "id", new IntType())),
                    0, Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyMap(), null));
            Mockito.lenient().when(externalTable.getDbName()).thenReturn("db");
            Mockito.lenient().when(externalTable.getName()).thenReturn("t");
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);
            setField(PaimonScanNode.class, node, "backendStorageProperties",
                    ImmutableMap.of("AWS_CREDENTIALS_PROVIDER_TYPE", mode,
                            "AWS_ACCESS_KEY", shape[3], "AWS_SECRET_KEY", shape[4]));

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("provider_ok.parquet")));
            Assert.assertEquals("mode " + mode + " on " + location, expected,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }
    // --- filterVectorIndexOptions ---------------------------------------------------
    //
    // CoreOptions.primaryKeyVectorIndexOptions seeds its result with a copy of every
    // table option, so what reaches TExternalSearchRequest.paimon_options is the whole table
    // property bag unless it is filtered. These cover the three reasons it must be:
    // a thrift-fatal null value, redundant table options paimon-rust already merges
    // itself, and properties that should not travel to BE at all.

    @Test
    public void testFilterVectorIndexOptionsKeepsFieldAndAlgorithmScopedKeys() {
        Map<String, String> options = new HashMap<>();
        options.put("field.embedding.vector-dim", "2048");
        options.put("ivf-flat.metric", "l2");
        options.put("ivf-flat.nlist", "1024");
        options.put("fields.embedding.some-field-option", "v");

        Map<String, String> filtered = PaimonScanNode.filterVectorIndexOptions(
                options, "embedding", "ivf-flat");

        Assert.assertEquals("l2", filtered.get("ivf-flat.metric"));
        Assert.assertEquals("1024", filtered.get("ivf-flat.nlist"));
        Assert.assertEquals("v", filtered.get("fields.embedding.some-field-option"));
        // "field." (singular) is a different namespace from the "fields." one the filter
        // keeps, so the dimension is not forwarded -- paimon-rust reads it off the index.
        Assert.assertFalse(filtered.containsKey("field.embedding.vector-dim"));
        Assert.assertEquals(3, filtered.size());
    }

    @Test
    public void testFilterVectorIndexOptionsDropsNullValues() {
        // The actual production failure: these catalog audit properties carry a Java null,
        // which HashMap accepts and thrift's writeString does not (NPE in the generated
        // TPaimonVectorSearchOptions writer). They are also not algorithm- or field-scoped,
        // so they would be dropped anyway -- asserted separately so a future widening of
        // the prefix rule cannot silently reintroduce the crash.
        Map<String, String> options = new HashMap<>();
        options.put("owner", null);
        options.put("createdBy", null);
        options.put("updatedBy", null);
        options.put("ivf-flat.metric", "l2");
        options.put("ivf-flat.nprobe", null);

        Map<String, String> filtered = PaimonScanNode.filterVectorIndexOptions(
                options, "embedding", "ivf-flat");

        Assert.assertFalse(filtered.containsValue(null));
        Assert.assertFalse(filtered.containsKey("ivf-flat.nprobe"));
        Assert.assertEquals(Collections.singletonMap("ivf-flat.metric", "l2"), filtered);
    }

    @Test
    public void testFilterVectorIndexOptionsDropsUnrelatedTableProperties() {
        Map<String, String> options = new HashMap<>();
        options.put("path", "viewfs://cluster/db/tbl");
        options.put("security.hadoop.username", "hudi");
        options.put("bucket", "1");
        options.put("primary-key", "id");
        options.put("merge-engine", "partial-update");
        options.put("ivf-flat.metric", "l2");

        Map<String, String> filtered = PaimonScanNode.filterVectorIndexOptions(
                options, "embedding", "ivf-flat");

        Assert.assertEquals(Collections.singletonMap("ivf-flat.metric", "l2"), filtered);
    }

    @Test
    public void testFilterVectorIndexOptionsDropsPkVectorDeclarations() {
        // fields.<col>.pk-vector.* is FE's own index declaration, not a search parameter;
        // primaryKeyVectorAlgorithmOptions excludes it from its collection too.
        Map<String, String> options = new HashMap<>();
        options.put("fields.embedding.pk-vector.index.type", "ivf-flat");
        options.put("fields.embedding.pk-vector.distance.metric", "l2");
        options.put("fields.embedding.pk-vector.index.options", "{\"nlist\":\"1024\"}");
        options.put("fields.embedding.refine-factor", "2");

        Map<String, String> filtered = PaimonScanNode.filterVectorIndexOptions(
                options, "embedding", "ivf-flat");

        Assert.assertEquals(Collections.singletonMap("fields.embedding.refine-factor", "2"), filtered);
    }

    @Test
    public void testFilterVectorIndexOptionsWithNullAlgorithmKeepsFieldScopedOnly() {
        // A null index type must not be turned into a "null." prefix; keep the
        // field-scoped half rather than guessing.
        Map<String, String> options = new HashMap<>();
        options.put("fields.embedding.refine-factor", "2");
        options.put("ivf-flat.metric", "l2");
        options.put("owner", null);

        Map<String, String> filtered = PaimonScanNode.filterVectorIndexOptions(
                options, "embedding", null);

        Assert.assertEquals(Collections.singletonMap("fields.embedding.refine-factor", "2"), filtered);
    }

    @Test
    public void testFilterVectorIndexOptionsIsScopedToTheQueriedColumn() {
        Map<String, String> options = new HashMap<>();
        options.put("fields.embedding.refine-factor", "2");
        options.put("fields.other_vec.refine-factor", "8");

        Map<String, String> filtered = PaimonScanNode.filterVectorIndexOptions(
                options, "embedding", "ivf-flat");

        Assert.assertEquals(Collections.singletonMap("fields.embedding.refine-factor", "2"), filtered);
    }

    @Test
    public void testRustReaderRejectsConflictingStaticCredentials() throws Exception {
        for (Map<String, String> extra : Arrays.asList(
                ImmutableMap.of("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS"),
                ImmutableMap.of("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/example"),
                ImmutableMap.of("AWS_TOKEN", "expired-example-token"),
                ImmutableMap.of("AWS_TOKEN", " "), ImmutableMap.of("AWS_ROLE_ARN", " "))) {
            RustRoutingFixture f = new RustRoutingFixture();
            Map<String, String> properties = new HashMap<>();
            properties.put("AWS_ACCESS_KEY", "example-key");
            properties.put("AWS_SECRET_KEY", "example-secret");
            properties.putAll(extra);
            setField(PaimonScanNode.class, f.node, "backendStorageProperties", properties);
            // Hadoop's Simple provider ignores these settings; Rust must not change identity.
            Assert.assertEquals(extra.toString(), TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
        }
    }

    @Test
    public void testRustReaderRejectsUnsupportedPersistedBranchScanModes() throws Exception {
        for (String mode : Arrays.asList("latest", "latest-full", "full", "compacted-full", "LATEST-FULL", " default ")) {
            RustRoutingFixture f = new RustRoutingFixture();
            f.schema(1, new IntType(), ImmutableMap.of("branch", "audit", "scan.mode", mode), false);
            // A branch can keep its persisted mode without a time-travel selector to strip it.
            Assert.assertEquals(mode, TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
        }
    }

    @Test
    public void testRustReaderSelectionGatesUnverifiedLocationSchemes() throws Exception {
        // The pinned paimon-rust storage dispatcher (io/storage.rs) selects the
        // FileIO parser from the table location's URI scheme, and libpaimon_c.a
        // compiles in separate COS, OBS, GCS and Azdls parsers besides OSS and
        // S3. Doris normalizes those object stores' credentials into the AWS_*
        // aliases, which the BE rust bridge translates only into the s3.* /
        // fs.oss.* key families — a cosn:// / obs:// / gs:// / abfs:// table
        // would reach its scheme's parser without the key family it reads
        // (fs.cosn.userinfo.*, fs.obs.*, gcs.*, azure.*) and fail the open.
        // Those schemes must fall back to JNI before the split is encoded;
        // the verified schemes (s3 / s3a / oss, plus credential-free hdfs and
        // local paths) stay rust-eligible.
        for (String[] shape : new String[][] {
                // Unverified object-store schemes -> JNI (property translation
                // not implemented; Doris sends these as AWS_* aliases).
                {"cosn://bucket/wh/db.db/t", "PAIMON_JNI"},
                {"obs://bucket/wh/db.db/t", "PAIMON_JNI"},
                {"gs://bucket/wh/db.db/t", "PAIMON_JNI"},
                {"abfs://bucket/wh/db.db/t", "PAIMON_JNI"},
                {"abfss://bucket@account/wh/db.db/t", "PAIMON_JNI"},
                {"az://bucket/wh/db.db/t", "PAIMON_JNI"},
                {"azure://bucket/wh/db.db/t", "PAIMON_JNI"},
                // Uppercase URI-scheme variants of otherwise verified schemes:
                // the crate strips only a lowercase prefix, and the DataSplit's
                // file paths carry the original casing too -> JNI.
                {"S3://bucket/wh/db.db/t", "PAIMON_JNI"},
                {"Hdfs://nn/wh/db.db/t", "PAIMON_JNI"},
                // A null location cannot be verified (and cannot ship
                // paimon_table for the rust reader) -> JNI.
                {null, "PAIMON_JNI"},
                // Verified schemes with implemented property translation or
                // credential-free parsers -> rust.
                {"s3://bucket/wh/db.db/t", "PAIMON_RUST"},
                {"s3a://bucket/wh/db.db/t", "PAIMON_RUST"},
                {"oss://bucket/wh/db.db/t", "PAIMON_RUST"},
                {"hdfs://nn/wh/db.db/t", "PAIMON_RUST"},
                {"file:///paimon/wh/db.db/t", "PAIMON_RUST"},
                {"/paimon/wh/db.db/t", "PAIMON_RUST"}}) {
            String location = shape[0];
            TPaimonReaderType expected = TPaimonReaderType.valueOf(shape[1]);
            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            if (location != null) {
                Mockito.when(source.getTableLocation()).thenReturn(location);
            }
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            if (expected == TPaimonReaderType.PAIMON_RUST) {
                // Only the rust path serializes the schema and the table
                // identity; the JNI iterations must not leave unused stubs.
                Mockito.when(source.getExternalTable()).thenReturn(externalTable);
                Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                        1, Collections.singletonList(new DataField(0, "id", new IntType())),
                        0, Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyMap(), null));
                Mockito.when(externalTable.getDbName()).thenReturn("db");
                Mockito.when(externalTable.getName()).thenReturn("t");
            }
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);
            // Production-shaped COS/OBS/GCS/Azure map: static credentials
            // normalized to the AWS_* aliases (plus the S3-compatible
            // connection settings).
            setField(PaimonScanNode.class, node, "backendStorageProperties", ImmutableMap.of(
                    "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT",
                    "AWS_ACCESS_KEY", "ak",
                    "AWS_SECRET_KEY", "sk",
                    "AWS_ENDPOINT", "http://127.0.0.1:19001",
                    "AWS_REGION", "us-east-1"));

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("scheme_gate.parquet")));
            Assert.assertEquals("location " + location, expected,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testIsRustVerifiedLocationSchemeClassification() {
        // A plain path without a URI scheme is a local-filesystem location, which
        // reads through the crate's LocalFs parser and needs no credentials. The
        // scheme must appear in the exact lowercase form the pinned crate
        // consumes: it lowercases only its storage dispatch, while its path
        // extraction strips a lowercase prefix from the original string — a
        // S3:// or Hdfs:// warehouse must route to JNI instead (the Java stack
        // is case-insensitive everywhere).
        Assert.assertTrue(PaimonScanNode.isRustVerifiedLocationScheme("s3://bucket/wh"));
        Assert.assertTrue(PaimonScanNode.isRustVerifiedLocationScheme("/local/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("S3://bucket/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("OSS://bucket/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("Hdfs://nn/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("FILE:///paimon/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("cosn://bucket/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("COSN://bucket/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme("viewfs://cluster/wh"));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedLocationScheme(null));
    }

    @Test
    public void testRustReaderSelectionRejectsAuthenticatedHdfsBackends() throws Exception {
        // The pinned paimon-rust HDFS parser reads only the hdfs.name-node /
        // hdfs.enable-append keys: no kerberos, no proxy user, no hadoop HA
        // resolution. A Kerberized HDFS catalog (or one with hadoop.username /
        // HA nameservice config) passes the location scheme gate, so the scan
        // would open as the BE process's ambient identity and fail the access
        // JNI honors — those shapes must fall back to JNI; the credential-free
        // shape stays rust-eligible.
        for (String[][] shape : new String[][][] {
                // Production-shaped credential-free HdfsProperties output: an
                // fs.defaultFS, the always-written simple-auth markers and
                // pass-through tunables, none of which carry an identity.
                {new String[] {"fs.defaultFS", "hdfs://nn:8020"},
                        new String[] {"hdfs.security.authentication", "simple"},
                        new String[] {"ipc.client.fallback-to-simple-auth-allowed", "true"},
                        new String[] {"PAIMON_RUST"}},
                // A null / empty map is the anonymous default the parser
                // supports (the regression suite's plain hdfs:// table).
                {new String[] {"PAIMON_RUST"}},
                // Kerberos: authentication type plus principal / keytab, as
                // HdfsProperties emits them for a Kerberized catalog.
                {new String[] {"hadoop.security.authentication", "kerberos"},
                        new String[] {"hadoop.kerberos.principal", "hdfs/_HOST@REALM"},
                        new String[] {"hadoop.kerberos.keytab", "/etc/security/hdfs.keytab"},
                        new String[] {"PAIMON_JNI"}},
                {new String[] {"hdfs.security.authentication", "kerberos"}, new String[] {"PAIMON_JNI"}},
                // Proxy user: hadoop.username authenticates as a specific user
                // the rust parser would drop.
                {new String[] {"hadoop.username", "hive"}, new String[] {"PAIMON_JNI"}},
                // HA nameservice resolution: dfs.nameservices / dfs.ha.* config
                // cannot be resolved without the dfs.* channel.
                {new String[] {"dfs.nameservices", "ns1"},
                        new String[] {"dfs.ha.namenodes.ns1", "nn1,nn2"},
                        new String[] {"PAIMON_JNI"}},
                {new String[] {"dfs.ha.namenodes.ns1", "nn1,nn2"}, new String[] {"PAIMON_JNI"}},
                // Simple-auth client options: valid without Kerberos, but the
                // rust storage_hdfs parser drops every dfs./hadoop./fs. key
                // beyond its two, and hdfs-native defaults
                // dfs.client.use.datanode.hostname to false — behind
                // containers/NAT it connects to the advertised IP instead of
                // the hostname and fails while JNI succeeds.
                {new String[] {"dfs.client.use.datanode.hostname", "true"},
                        new String[] {"PAIMON_JNI"}},
                {new String[] {"hadoop.proxyuser.kerberos", "x"},
                        new String[] {"PAIMON_JNI"}}}) {
            Map<String, String> backendProperties = new HashMap<>();
            for (int i = 0; i < shape.length - 1; i++) {
                backendProperties.put(shape[i][0], shape[i][1]);
            }
            TPaimonReaderType expected = TPaimonReaderType.valueOf(
                    shape[shape.length - 1][0]);

            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0),
                    new TupleDescriptor(new TupleId(0)), false, vars, ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            Mockito.when(source.getTableLocation()).thenReturn("hdfs://nn/wh/db.db/t");
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            if (expected == TPaimonReaderType.PAIMON_RUST) {
                Mockito.when(source.getExternalTable()).thenReturn(externalTable);
                Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                        1, Collections.singletonList(new DataField(0, "id", new IntType())),
                        0, Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyMap(), null));
                Mockito.when(externalTable.getDbName()).thenReturn("db");
                Mockito.when(externalTable.getName()).thenReturn("t");
            }
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);
            setField(PaimonScanNode.class, node, "backendStorageProperties", backendProperties);

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("hdfs_auth_gate.parquet")));
            Assert.assertEquals("backend " + backendProperties, expected,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testIsRustVerifiedHdfsBackendClassification() {
        // The anonymous / null map is the open-tested shape; the always-written
        // simple-auth markers and fs.defaultFS carry no identity; every
        // auth-bearing key (type, principal, keytab, proxy user) and the HA
        // nameservice config route to JNI.
        Assert.assertTrue(PaimonScanNode.isRustVerifiedHdfsBackend(null));
        Assert.assertTrue(PaimonScanNode.isRustVerifiedHdfsBackend(Collections.emptyMap()));
        Assert.assertTrue(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "fs.defaultFS", "hdfs://nn:8020",
                "hadoop.security.authentication", "simple")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "hadoop.security.authentication", "kerberos")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "hadoop.security.authentication", "KERBEROS")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "hadoop.kerberos.principal", "hdfs/_HOST@REALM")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "hadoop.kerberos.keytab", "/etc/security/hdfs.keytab")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "hadoop.username", "hive")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "dfs.nameservices", "ns1")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "dfs.ha.namenodes.ns1", "nn1,nn2")));
        // A blank value is the unset case (values are null-filtered upstream,
        // but a blank site-config entry must not gate an otherwise
        // credential-free catalog).
        Assert.assertTrue(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "hadoop.username", " ")));
        // The always-shipped inert keys stay rust-eligible together.
        Assert.assertTrue(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "fs.defaultFS", "hdfs://nn:8020",
                "hadoop.security.authentication", "simple",
                "hdfs.security.authentication", "simple",
                "ipc.client.fallback-to-simple-auth-allowed", "true")));
        // Any other dfs./hadoop./fs. client option is dropped by the rust
        // parser and would diverge from JNI -> JNI (the canonical simple-auth
        // case is dfs.client.use.datanode.hostname).
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "dfs.client.use.datanode.hostname", "true")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "dfs.client.socket-timeout", "60000")));
        Assert.assertFalse(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")));
        // Keys outside the hadoop/dfs/fs namespaces cannot change what the
        // rust HDFS parser does (the BE bridge passes them through untouched
        // and the parser ignores them).
        Assert.assertTrue(PaimonScanNode.isRustVerifiedHdfsBackend(ImmutableMap.of(
                "warehouse", "hdfs://nn/wh", "unrelated.key", "value")));
    }

    @Test
    public void testRustReaderSelectionRejectsProjectedVariantColumns() throws Exception {
        // The rust leaf feeds its Arrow arrays to the slot serdes, and
        // DataTypeVariantV2SerDe::read_column_from_arrow unconditionally returns
        // NOT_IMPLEMENTED_ERROR — a nested Variant (ARRAY / MAP / STRUCT
        // containing one) reaches the same decoder through the container
        // serdes. Projections containing Variant must route to JNI; a table
        // whose VARIANT column is not projected by this query stays on rust.
        Type[][] shapes = new Type[][] {
                // Plain projected columns -> rust.
                new Type[] {Type.INT, Type.STRING},
                // A projected VARIANT -> JNI.
                new Type[] {Type.INT, Type.VARIANT},
                // Nested Variant inside every container kind -> JNI.
                new Type[] {new ArrayType(Type.VARIANT)},
                new Type[] {new MapType(Type.STRING, Type.VARIANT)},
                new Type[] {new StructType(new StructField("v", Type.VARIANT))},
                // The same containers without Variant stay rust-eligible.
                new Type[] {new ArrayType(Type.INT)},
                new Type[] {new MapType(Type.STRING, Type.INT)}};
        boolean[] expectRust = new boolean[] {true, false, false, false, false, true, true};

        for (int i = 0; i < shapes.length; i++) {
            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            for (int j = 0; j < shapes[i].length; j++) {
                SlotDescriptor slot = new SlotDescriptor(new SlotId(j), desc);
                slot.setType(shapes[i][j]);
                desc.addSlot(slot);
            }

            SessionVariable vars = new SessionVariable();
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;

            PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), desc, false, vars,
                    ScanContext.EMPTY);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            Mockito.when(source.getTableLocation()).thenReturn("s3://bucket/wh/db.db/t");
            FileStoreTable paimonTable = Mockito.mock(FileStoreTable.class);
            PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
            if (expectRust[i]) {
                Mockito.when(source.getExternalTable()).thenReturn(externalTable);
                Mockito.when(paimonTable.schema()).thenReturn(new TableSchema(
                        1, Collections.singletonList(new DataField(0, "id", new IntType())),
                        0, Collections.emptyList(), Collections.emptyList(),
                        Collections.emptyMap(), null));
                Mockito.when(externalTable.getDbName()).thenReturn("db");
                Mockito.when(externalTable.getName()).thenReturn("t");
            }
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", paimonTable);
            setField(PaimonScanNode.class, node, "backendStorageProperties", ImmutableMap.of(
                    "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT",
                    "AWS_ACCESS_KEY", "ak",
                    "AWS_SECRET_KEY", "sk",
                    "AWS_ENDPOINT", "http://127.0.0.1:19001",
                    "AWS_REGION", "us-east-1"));

            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit("variant_gate.parquet")));
            Assert.assertEquals("projection " + Arrays.toString(shapes[i]),
                    expectRust[i] ? TPaimonReaderType.PAIMON_RUST : TPaimonReaderType.PAIMON_JNI,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    /**
     * Every negative assertion starts from a complete Rust-eligible fixture, so missing
     * schema or credentials cannot accidentally satisfy a fallback test.
     */
    private class RustRoutingFixture {
        final SessionVariable vars = new SessionVariable();
        final TupleDescriptor tuple = new TupleDescriptor(new TupleId(0));
        final PaimonSource source = Mockito.mock(PaimonSource.class);
        final FileStoreTable table = Mockito.mock(FileStoreTable.class);
        final org.apache.paimon.schema.SchemaManager schemas =
                Mockito.mock(org.apache.paimon.schema.SchemaManager.class);
        final PaimonScanNode node;
        final DataSplit split = createDataSplit("capabilities.parquet");

        RustRoutingFixture() throws Exception {
            vars.setEnablePaimonRustReader(true);
            vars.enableFileScannerV2 = true;
            node = new PaimonScanNode(new PlanNodeId(0), tuple, false, vars, ScanContext.EMPTY);
            PaimonExternalTable external = Mockito.mock(PaimonExternalTable.class);
            Mockito.when(source.getExternalTable()).thenReturn(external);
            Mockito.when(external.getDbName()).thenReturn("db");
            Mockito.when(external.getName()).thenReturn("t");
            Mockito.when(source.getTableLocation()).thenReturn("s3://warehouse/wh/db.db/t");
            Mockito.when(table.schemaManager()).thenReturn(schemas);
            schema(1, new IntType(), Collections.emptyMap(), false);
            node.setSource(source);
            setField(PaimonScanNode.class, node, "processedTable", table);
            credentials("DEFAULT");
            Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, reader(split));
        }

        void credentials(String provider) throws Exception {
            setField(PaimonScanNode.class, node, "backendStorageProperties", ImmutableMap.of(
                    "AWS_CREDENTIALS_PROVIDER_TYPE", provider,
                    "AWS_ACCESS_KEY", "example-key", "AWS_SECRET_KEY", "example-secret"));
        }

        void schema(long id, org.apache.paimon.types.DataType valueType,
                Map<String, String> options, boolean pk) throws Exception {
            TableSchema schema = new TableSchema(id, Arrays.asList(
                    new DataField(0, "id", new IntType(false)), new DataField(1, "v", valueType)),
                    10, Collections.emptyList(), pk ? Collections.singletonList("id") : Collections.emptyList(),
                    options, null);
            setField(PaimonScanNode.class, node, "rustReaderCapabilities", null);
            Mockito.when(table.schema()).thenReturn(schema);
            Mockito.when(table.coreOptions()).thenReturn(new CoreOptions(options));
        }

        TPaimonReaderType reader(DataSplit input) throws Exception {
            TFileRangeDesc range = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class}, range, new PaimonSplit(input));
            return range.getTableFormatParams().getPaimonParams().getReaderType();
        }

        void project(Type full, Type pruned) throws Exception {
            setField(PaimonScanNode.class, node, "rustReaderCapabilities", null);
            SlotDescriptor slot = new SlotDescriptor(new SlotId(0), tuple);
            slot.setColumn(new Column("v", full));
            slot.setType(pruned);
            tuple.addSlot(slot);
        }
    }

    @Test
    public void testRustPrunedNestedProjectionFallback() throws Exception {
        StructType full = new StructType(new StructField("a", Type.INT), new StructField("b", Type.INT));
        StructType pruned = new StructType(new StructField("b", Type.INT));
        Type[][] shapes = {{full, pruned}, {new ArrayType(full), new ArrayType(pruned)},
                {new MapType(Type.INT, full), new MapType(Type.INT, pruned)}};
        for (Type[] shape : shapes) {
            RustRoutingFixture f = new RustRoutingFixture();
            f.project(shape[0], shape[0]);
            Assert.assertEquals(TPaimonReaderType.PAIMON_RUST, f.reader(f.split));
            f.tuple.getSlots().get(0).setType(shape[1]);
            setField(PaimonScanNode.class, f.node, "rustReaderCapabilities", null);
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
        }
    }

    @Test
    public void testRustHistoricalIntegerNarrowingFallback() throws Exception {
        for (org.apache.paimon.types.DataType target : Arrays.asList(
                new org.apache.paimon.types.TinyIntType(), new org.apache.paimon.types.SmallIntType(),
                new IntType())) {
            RustRoutingFixture f = new RustRoutingFixture();
            TableSchema old = new TableSchema(1, Arrays.asList(new DataField(0, "id", new IntType(false)),
                    new DataField(1, "v", new org.apache.paimon.types.BigIntType())), 1,
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null);
            Mockito.when(f.schemas.schema(1)).thenReturn(old);
            f.schema(2, target, Collections.emptyMap(), false);
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
            Mockito.verify(f.schemas, Mockito.times(1)).schema(1);
        }
    }

    @Test
    public void testRustStringHistoryToTemporalFallback() throws Exception {
        for (org.apache.paimon.types.DataType sourceType : Arrays.asList(
                DataTypes.STRING(), DataTypes.VARCHAR(20), DataTypes.CHAR(20))) {
            for (org.apache.paimon.types.DataType target : Arrays.asList(DataTypes.DATE(),
                    DataTypes.TIMESTAMP(0), DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))) {
                assertHistoricalTemporalRouting(sourceType, target, TPaimonReaderType.PAIMON_JNI);
            }
        }
        assertHistoricalTemporalRouting(DataTypes.DATE(), DataTypes.DATE(), TPaimonReaderType.PAIMON_RUST);
    }

    @Test
    public void testRustIntegerHistoryToTimestampFallback() throws Exception {
        for (int precision : new int[] {0, 3, 6}) {
            for (org.apache.paimon.types.DataType target : Arrays.asList(DataTypes.TIMESTAMP(precision),
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision))) {
                for (org.apache.paimon.types.DataType sourceType : Arrays.asList(DataTypes.INT(), DataTypes.BIGINT())) {
                    assertHistoricalTemporalRouting(sourceType, target, TPaimonReaderType.PAIMON_JNI);
                }
                assertHistoricalTemporalRouting(target, target, TPaimonReaderType.PAIMON_RUST);
            }
        }
    }

    private void assertHistoricalTemporalRouting(org.apache.paimon.types.DataType sourceType,
            org.apache.paimon.types.DataType target, TPaimonReaderType expected) throws Exception {
        org.apache.paimon.types.DataType[][] shapes = {
                {sourceType, target},
                {DataTypes.ROW(new DataField(2, "old_name", sourceType)),
                        DataTypes.ROW(new DataField(2, "renamed", target))},
                {DataTypes.ARRAY(sourceType), DataTypes.ARRAY(target)},
                {DataTypes.MAP(sourceType, DataTypes.INT()), DataTypes.MAP(target, DataTypes.INT())},
                {DataTypes.MAP(DataTypes.INT(), sourceType), DataTypes.MAP(DataTypes.INT(), target)}};
        for (org.apache.paimon.types.DataType[] shape : shapes) {
            RustRoutingFixture f = new RustRoutingFixture();
            Mockito.when(f.schemas.schema(1)).thenReturn(new TableSchema(1, Arrays.asList(
                    new DataField(0, "id", new IntType(false)), new DataField(1, "v", shape[0])),
                    10, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null));
            f.schema(2, shape[1], Collections.emptyMap(), false);
            Assert.assertEquals(Arrays.toString(shape), expected, f.reader(f.split));
        }
    }

    @Test
    public void testRustFloatingHistoryToDecimalFallback() throws Exception {
        for (org.apache.paimon.types.DataType oldType : Arrays.asList(
                DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.INT(), DataTypes.DECIMAL(3, 2))) {
            org.apache.paimon.types.DataType decimal = DataTypes.DECIMAL(10, 2);
            org.apache.paimon.types.DataType[][] shapes = {
                    {oldType, decimal},
                    {DataTypes.ROW(new DataField(2, "old_name", oldType)),
                            DataTypes.ROW(new DataField(2, "renamed", decimal))},
                    {DataTypes.ARRAY(oldType), DataTypes.ARRAY(decimal)},
                    {DataTypes.MAP(oldType, DataTypes.INT()), DataTypes.MAP(decimal, DataTypes.INT())},
                    {DataTypes.MAP(DataTypes.INT(), oldType), DataTypes.MAP(DataTypes.INT(), decimal)}};
            for (org.apache.paimon.types.DataType[] shape : shapes) {
                RustRoutingFixture f = new RustRoutingFixture();
                Mockito.when(f.schemas.schema(1)).thenReturn(new TableSchema(1, Arrays.asList(
                        new DataField(0, "id", new IntType(false)), new DataField(1, "v", shape[0])),
                        10, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null));
                f.schema(2, shape[1], Collections.emptyMap(), false);
                boolean floating = oldType.equals(DataTypes.FLOAT()) || oldType.equals(DataTypes.DOUBLE());
                Assert.assertEquals(Arrays.toString(shape), floating
                        ? TPaimonReaderType.PAIMON_JNI : TPaimonReaderType.PAIMON_RUST, f.reader(f.split));
            }
        }
    }

    @Test
    public void testRustUnusedAggregateNamesFallback() throws Exception {
        for (String engine : Arrays.asList("aggregation", "partial-update")) {
            List<String> optionKeys = new ArrayList<>(Arrays.asList(
                    "fields.default-aggregate-function", "fields.id.aggregate-function"));
            if ("partial-update".equals(engine)) {
                optionKeys.add("fields.seq.aggregate-function");
            }
            for (String key : optionKeys) {
                for (String function : Arrays.asList("collect", "sum")) {
                    RustRoutingFixture f = new RustRoutingFixture();
                    Map<String, String> options = new HashMap<>();
                    options.put("merge-engine", engine);
                    options.put("fields.v.aggregate-function", "max");
                    options.put(key, function);
                    f.schema(1, new IntType(), options, true);
                    if ("partial-update".equals(engine)) {
                        options.put("fields.seq.sequence-group", "v");
                        Mockito.when(f.table.schema()).thenReturn(new TableSchema(1, Arrays.asList(
                                new DataField(0, "id", new IntType(false)), new DataField(1, "v", new IntType()),
                                new DataField(2, "seq", new IntType())), 2, Collections.emptyList(),
                                Collections.singletonList("id"), options, null));
                    }
                    DataFileMeta file = Mockito.spy(f.split.dataFiles().get(0));
                    Mockito.doReturn(Optional.of(0L)).when(file).deleteRowCount();
                    DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW)
                            .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                            .withDataFiles(Collections.singletonList(file)).build();
                    Assert.assertEquals(engine + " " + key + "=" + function,
                            TPaimonReaderType.PAIMON_JNI, f.reader(split));
                }
            }
        }
    }

    @Test
    public void testRustNonIntegerHistoryToIntegerFallback() throws Exception {
        for (org.apache.paimon.types.DataType sourceType : Arrays.asList(
                DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DECIMAL(10, 2), DataTypes.STRING())) {
            for (org.apache.paimon.types.DataType targetType : Arrays.asList(
                    DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT())) {
                org.apache.paimon.types.DataType[][] shapes = {
                        {sourceType, targetType},
                        {DataTypes.ROW(new DataField(2, "old_name", sourceType)),
                                DataTypes.ROW(new DataField(2, "renamed", targetType))},
                        {DataTypes.ARRAY(sourceType), DataTypes.ARRAY(targetType)},
                        {DataTypes.MAP(sourceType, DataTypes.INT()), DataTypes.MAP(targetType, DataTypes.INT())},
                        {DataTypes.MAP(DataTypes.INT(), sourceType), DataTypes.MAP(DataTypes.INT(), targetType)}};
                for (org.apache.paimon.types.DataType[] shape : shapes) {
                    RustRoutingFixture f = new RustRoutingFixture();
                    Mockito.when(f.schemas.schema(1)).thenReturn(new TableSchema(1, Arrays.asList(
                            new DataField(0, "id", new IntType(false)), new DataField(1, "v", shape[0])),
                            10, Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null));
                    f.schema(2, shape[1], Collections.emptyMap(), false);
                    Assert.assertEquals(Arrays.toString(shape), TPaimonReaderType.PAIMON_JNI, f.reader(f.split));
                }
            }
        }
    }

    @Test
    public void testRustPartialUpdateDefaultAggregateAndSequenceFields() throws Exception {
        for (org.apache.paimon.types.DataType valueType : Arrays.asList(
                DataTypes.TINYINT(), DataTypes.DECIMAL(2, 0), DataTypes.DOUBLE())) {
            for (boolean override : Arrays.asList(false, true)) {
                RustRoutingFixture f = new RustRoutingFixture();
                Map<String, String> options = new HashMap<>();
                options.put("merge-engine", "partial-update");
                options.put("fields.seq,seq2.sequence-group", "v");
                options.put("fields.default-aggregate-function", "sum");
                if (override) {
                    options.put("fields.v.aggregate-function", "max");
                }
                f.schema(1, valueType, options, true);
                Mockito.when(f.table.schema()).thenReturn(new TableSchema(1, Arrays.asList(
                        new DataField(0, "id", new IntType(false)), new DataField(1, "v", valueType),
                        new DataField(2, "seq", new IntType()), new DataField(3, "seq2", new IntType())),
                        3, Collections.emptyList(), Collections.singletonList("id"), options, null));
                DataFileMeta file = Mockito.spy(f.split.dataFiles().get(0));
                Mockito.doReturn(Optional.of(0L)).when(file).deleteRowCount();
                // Legacy writers can retain an unbounded same-key run even in one file.
                DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                        .withDataFiles(Collections.singletonList(file)).build();
                Assert.assertEquals(valueType + " override=" + override,
                        TPaimonReaderType.PAIMON_JNI,
                        f.reader(split));
            }
        }
    }

    @Test
    public void testRustMultiFilePrimaryKeyMergeFallback() throws Exception {
        for (String engine : Arrays.asList("deduplicate", "aggregation", "partial-update")) {
            for (boolean primaryKey : Arrays.asList(false, true)) {
                for (int fileCount : new int[] {1, 2, 8}) {
                    RustRoutingFixture f = new RustRoutingFixture();
                    f.schema(1, new IntType(), ImmutableMap.of("merge-engine", engine,
                            "read.batch-size", "1"), primaryKey);
                    List<DataFileMeta> files = new ArrayList<>();
                    for (int i = 0; i < fileCount; i++) {
                        DataFileMeta file = Mockito.spy(createDataSplit("run-" + i + ".parquet")
                                .dataFiles().get(0));
                        Mockito.doReturn(Optional.of(0L)).when(file).deleteRowCount();
                        files.add(file);
                    }
                    DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW)
                            .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                            .withDataFiles(files).build();
                    Assert.assertEquals(engine + " pk=" + primaryKey + " files=" + fileCount,
                            primaryKey && (fileCount > 1 || !"deduplicate".equals(engine)) ? TPaimonReaderType.PAIMON_JNI
                                    : TPaimonReaderType.PAIMON_RUST, f.reader(split));
                }
            }
        }
    }

    @Test
    public void testRustNestedEvolutionAndCompatibleHistory() throws Exception {
        org.apache.paimon.types.DataType small = new org.apache.paimon.types.TinyIntType();
        org.apache.paimon.types.DataType large = new org.apache.paimon.types.BigIntType();
        org.apache.paimon.types.RowType oldRow = new org.apache.paimon.types.RowType(Arrays.asList(
                new DataField(2, "a", large), new DataField(3, "b", new IntType())));
        org.apache.paimon.types.RowType newRow = new org.apache.paimon.types.RowType(Arrays.asList(
                new DataField(3, "b", new IntType()), new DataField(2, "renamed", small)));
        org.apache.paimon.types.DataType[][] shapes = {
                {large, small}, {oldRow, newRow},
                {new org.apache.paimon.types.ArrayType(oldRow), new org.apache.paimon.types.ArrayType(newRow)},
                {new org.apache.paimon.types.MapType(large, new IntType()),
                        new org.apache.paimon.types.MapType(small, new IntType())},
                {new org.apache.paimon.types.MapType(new IntType(), oldRow),
                        new org.apache.paimon.types.MapType(new IntType(), newRow)}};
        for (org.apache.paimon.types.DataType[] shape : shapes) {
            for (boolean narrow : Arrays.asList(true, false)) {
                RustRoutingFixture f = new RustRoutingFixture();
                TableSchema old = new TableSchema(1, Arrays.asList(new DataField(0, "id", new IntType(false)),
                        new DataField(1, "v", shape[narrow ? 0 : 1])), 10,
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), null);
                Mockito.when(f.schemas.schema(1)).thenReturn(old);
                f.schema(2, shape[narrow ? 1 : 0], Collections.emptyMap(), false);
                Assert.assertEquals(narrow ? TPaimonReaderType.PAIMON_JNI : TPaimonReaderType.PAIMON_RUST,
                        f.reader(f.split));
            }
        }
        RustRoutingFixture missing = new RustRoutingFixture();
        missing.schema(2, new IntType(), Collections.emptyMap(), false);
        Mockito.when(missing.schemas.schema(1)).thenThrow(new IllegalStateException("missing schema"));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, missing.reader(missing.split));
        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI, missing.reader(missing.split));
        Mockito.verify(missing.schemas, Mockito.times(1)).schema(1);
    }

    @Test
    public void testRustDefaultAggregateAndFieldOverride() throws Exception {
        for (Map<String, String> options : Arrays.asList(
                ImmutableMap.of("merge-engine", "aggregation", "fields.default-aggregate-function", "sum"),
                ImmutableMap.of("merge-engine", "aggregation", "fields.default-aggregate-function", "max"),
                ImmutableMap.of("merge-engine", "aggregation", "fields.default-aggregate-function", "sum",
                        "fields.v.aggregate-function", "max"))) {
            RustRoutingFixture f = new RustRoutingFixture();
            f.schema(1, new IntType(), options, true);
            DataFileMeta file = Mockito.spy(f.split.dataFiles().get(0));
            Mockito.doReturn(Optional.of(0L)).when(file).deleteRowCount();
            DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW)
                    .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                    .withDataFiles(Collections.singletonList(file)).build();
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                    f.reader(split));
        }
    }

    @Test
    public void testRustPrimaryKeyRetractsFallback() throws Exception {
        for (String engine : Arrays.asList("deduplicate", "aggregation", "partial-update")) {
            for (Long deletes : Arrays.asList(0L, 1L, null)) {
                RustRoutingFixture f = new RustRoutingFixture();
                f.schema(1, new IntType(), ImmutableMap.of("merge-engine", engine), true);
                DataFileMeta file = Mockito.spy(f.split.dataFiles().get(0));
                Mockito.doReturn(Optional.ofNullable(deletes)).when(file).deleteRowCount();
                DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                        .withDataFiles(Collections.singletonList(file)).build();
                Assert.assertEquals(engine + " deletes=" + deletes,
                        "deduplicate".equals(engine) && deletes != null && deletes == 0L ? TPaimonReaderType.PAIMON_RUST
                                : TPaimonReaderType.PAIMON_JNI, f.reader(split));
            }
        }
    }

    @Test
    public void testRustAggregateFunctionAndArithmeticFallback() throws Exception {
        Object[][] cases = {
                {"collect", new org.apache.paimon.types.ArrayType(new IntType()), false},
                {"sum", new org.apache.paimon.types.TinyIntType(), false},
                {"sum", new org.apache.paimon.types.SmallIntType(), false},
                {"sum", new IntType(), false},
                {"sum", new org.apache.paimon.types.BigIntType(), false},
                {"product", new IntType(), false},
                {"sum", new org.apache.paimon.types.DecimalType(2, 0), false},
                {"sum", new org.apache.paimon.types.DecimalType(18, 2), false},
                {"sum", new org.apache.paimon.types.DecimalType(19, 2), true},
                {"product", new org.apache.paimon.types.DecimalType(20, 2), false},
                {"sum", new org.apache.paimon.types.DoubleType(), true},
                {"max", new IntType(), true},
                {"max", new org.apache.paimon.types.ArrayType(new IntType()), false},
                {"bool_and", new org.apache.paimon.types.BooleanType(), true},
                {"last_non_null_value", new org.apache.paimon.types.ArrayType(new IntType()), true}
        };
        for (String engine : Arrays.asList("aggregation", "partial-update")) {
            for (Object[] shape : cases) {
                RustRoutingFixture f = new RustRoutingFixture();
                f.schema(1, (org.apache.paimon.types.DataType) shape[1], ImmutableMap.of(
                        "merge-engine", engine, "fields.v.aggregate-function", (String) shape[0]), true);
                DataFileMeta file = Mockito.spy(f.split.dataFiles().get(0));
                Mockito.doReturn(Optional.of(0L)).when(file).deleteRowCount();
                DataSplit split = DataSplit.builder().withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0).withBucketPath("file:///warehouse/db/t/bucket-0")
                        .withDataFiles(Collections.singletonList(file)).build();
                Assert.assertEquals(engine + " " + Arrays.toString(shape),
                        TPaimonReaderType.PAIMON_JNI, f.reader(split));
            }
        }
    }


}
