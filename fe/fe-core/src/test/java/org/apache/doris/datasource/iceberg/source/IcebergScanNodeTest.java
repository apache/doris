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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergSysExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ScanTaskUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @SuppressWarnings("unchecked")
    private static Optional<Map<Integer, List<String>>> extractNameMapping(
            IcebergScanNode node) throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod("extractNameMapping");
        method.setAccessible(true);
        return (Optional<Map<Integer, List<String>>>) method.invoke(node);
    }

    private static Table useFrozenTableGeneration(IcebergScanNode node, Table table) throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod("useFrozenTableGeneration", Table.class);
        method.setAccessible(true);
        return (Table) method.invoke(node, table);
    }

    private static class TestIcebergScanNode extends IcebergScanNode {
        private final boolean enableMappingVarbinary;
        private final boolean batchMode;
        private final boolean enableMappingTimestampTz;
        private TableScan tableScan;

        TestIcebergScanNode(SessionVariable sv) {
            this(sv, false, false, false);
        }

        TestIcebergScanNode(SessionVariable sv, boolean enableMappingVarbinary) {
            this(sv, enableMappingVarbinary, false, false);
        }

        TestIcebergScanNode(SessionVariable sv, boolean enableMappingVarbinary, boolean batchMode) {
            this(sv, enableMappingVarbinary, false, batchMode);
        }

        TestIcebergScanNode(SessionVariable sv, boolean enableMappingVarbinary,
                boolean enableMappingTimestampTz, boolean batchMode) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), sv, ScanContext.EMPTY);
            this.enableMappingVarbinary = enableMappingVarbinary;
            this.enableMappingTimestampTz = enableMappingTimestampTz;
            this.batchMode = batchMode;
        }

        void setTableScan(TableScan tableScan) {
            this.tableScan = tableScan;
        }

        @Override
        public TableScan createTableScan() {
            return tableScan;
        }

        TableScan createRealTableScan() throws UserException {
            return super.createTableScan();
        }

        @Override
        public boolean isBatchMode() {
            return batchMode;
        }

        @Override
        protected boolean getEnableMappingVarbinary() {
            return enableMappingVarbinary;
        }

        @Override
        protected boolean getEnableMappingTimestampTz() {
            return enableMappingTimestampTz;
        }

        @Override
        public List<String> getPathPartitionKeys() {
            return Collections.emptyList();
        }

        SlotDescriptor addSlot(int slotId, Column column) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc);
            slot.setColumn(column);
            desc.addSlot(slot);
            return slot;
        }

        boolean projectsVariant() {
            return IcebergScanNode.projectsVariant(desc);
        }

        @Override
        public TableSnapshot getQueryTableSnapshot() {
            return tableSnapshot;
        }

        @Override
        public TableScanParams getScanParams() {
            return scanParams;
        }

        int enableAndGetIcebergScanSemanticsVersion() {
            params = new TFileScanRangeParams();
            enableCurrentIcebergScanSemantics();
            return params.getIcebergScanSemanticsVersion();
        }

        TFileScanRangeParams initializeAndGetIcebergSchemaInfo() throws UserException {
            params = new TFileScanRangeParams();
            initializeIcebergSchemaInfo(Optional.empty());
            return params;
        }
    }

    @Test
    public void testEmitsCurrentIcebergScanSemanticsCapability() {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());

        Assert.assertEquals(IcebergScanNode.ICEBERG_SCAN_SEMANTICS_VERSION,
                node.enableAndGetIcebergScanSemanticsVersion());
    }

    @Test
    public void testPartitionEvolutionKeepsNonFileSlotInReaderSchema() throws Exception {
        Column evolvedIdentityColumn = new Column("int_col", Type.BIGINT, true);
        evolvedIdentityColumn.setUniqueId(1);
        Column projectedColumn = new Column("payload", Type.STRING, true);
        projectedColumn.setUniqueId(2);

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        // Reader schema resolution is pinned to the relation snapshot, including partition-only columns.
        Mockito.when(targetTable.getFullSchema(Mockito.<Optional<MvccSnapshot>>any())).thenReturn(
                ImmutableList.of(evolvedIdentityColumn, projectedColumn));
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);

        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        node.addSlot(1, projectedColumn);
        setIcebergSource(node, source);
        Mockito.doReturn(Collections.emptyMap()).when(node).getBase64EncodedInitialDefaultsForScan();

        TFileScanRangeParams scanParams = node.initializeAndGetIcebergSchemaInfo();

        Assert.assertEquals(2, scanParams.getHistorySchemaInfo().get(0).getRootField().getFieldsSize());
        Assert.assertEquals("int_col", scanParams.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(0).getFieldPtr().getName());
        Assert.assertEquals("payload", scanParams.getHistorySchemaInfo().get(0).getRootField()
                .getFields().get(1).getFieldPtr().getName());
    }

    @Test
    public void testSetPartitionValuesBuildsStableAlignedMetadata() throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        Schema schema = new Schema(
                Types.NestedField.required(1, "Region", Types.StringType.get()),
                Types.NestedField.required(2, "Dt", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("Region")
                .identity("Dt")
                .build();
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(spec.specId(), spec);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(spec);
        Mockito.when(table.specs()).thenReturn(specs);
        setIcebergTable(node, table);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("Dt", null);
        partitionValues.put("Region", "cn");
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        node.setPartitionValues(rangeDesc, partitionValues);

        Assert.assertEquals(Arrays.asList("Region", "Dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("cn", ""), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(false, true), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testSetPartitionValuesUsesPerSpecMetadataWithFileScannerV2() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableFileScannerV2 = true;
        TestIcebergScanNode node = new TestIcebergScanNode(sessionVariable);
        Schema schema = new Schema(
                Types.NestedField.required(1, "Region", Types.StringType.get()),
                Types.NestedField.required(2, "Dt", Types.StringType.get()),
                Types.NestedField.required(3, "Category", Types.StringType.get()));
        PartitionSpec oldSpec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .identity("Region")
                .identity("Dt")
                .build();
        PartitionSpec currentSpec = PartitionSpec.builderFor(schema)
                .withSpecId(2)
                .identity("Dt")
                .identity("Category")
                .build();
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(oldSpec.specId(), oldSpec);
        specs.put(currentSpec.specId(), currentSpec);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(currentSpec);
        Mockito.when(table.specs()).thenReturn(specs);
        setIcebergTable(node, table);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("Category", "books");
        partitionValues.put("Dt", null);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        node.setPartitionValues(rangeDesc, partitionValues);

        Assert.assertEquals(Arrays.asList("Dt", "Category"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("", "books"), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(true, false), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testSetPartitionValuesKeepsCommonMetadataWithLegacyFileScanner() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableFileScannerV2 = false;
        TestIcebergScanNode node = new TestIcebergScanNode(sessionVariable);
        Schema schema = new Schema(
                Types.NestedField.required(1, "Region", Types.StringType.get()),
                Types.NestedField.required(2, "Dt", Types.StringType.get()),
                Types.NestedField.required(3, "Category", Types.StringType.get()));
        PartitionSpec oldSpec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .identity("Region")
                .identity("Dt")
                .build();
        PartitionSpec currentSpec = PartitionSpec.builderFor(schema)
                .withSpecId(2)
                .identity("Dt")
                .identity("Category")
                .build();
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(oldSpec.specId(), oldSpec);
        specs.put(currentSpec.specId(), currentSpec);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(currentSpec);
        Mockito.when(table.specs()).thenReturn(specs);
        setIcebergTable(node, table);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("Region", "cn");
        partitionValues.put("Dt", "2026-07-31");
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        node.setPartitionValues(rangeDesc, partitionValues);

        Assert.assertEquals(Collections.singletonList("Dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Collections.singletonList("2026-07-31"), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Collections.singletonList(false), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testSetPartitionValuesSkipsValuesUnsupportedByMappedTypes() throws Exception {
        for (boolean enableFileScannerV2 : Arrays.asList(false, true)) {
            SessionVariable sessionVariable = new SessionVariable();
            sessionVariable.enableFileScannerV2 = enableFileScannerV2;
            sessionVariable.setTimeZone("Asia/Shanghai");
            TestIcebergScanNode node = new TestIcebergScanNode(sessionVariable, true, true, false);
            Schema schema = new Schema(
                    Types.NestedField.required(1, "Dt", Types.StringType.get()),
                    Types.NestedField.required(2, "uuid_col", Types.UUIDType.get()),
                    Types.NestedField.required(3, "ts_tz", Types.TimestampType.withZone()));
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .identity("Dt")
                    .identity("uuid_col")
                    .identity("ts_tz")
                    .build();
            Table table = Mockito.mock(Table.class);
            Mockito.when(table.schema()).thenReturn(schema);
            Mockito.when(table.spec()).thenReturn(spec);
            Mockito.when(table.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
            setIcebergTable(node, table);

            Map<String, String> partitionValues = new LinkedHashMap<>();
            partitionValues.put("Dt", "2026-08-03");
            partitionValues.put("uuid_col", "123e4567-e89b-12d3-a456-426614174000");
            partitionValues.put("ts_tz", "2026-08-03T16:00:00");
            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            node.setPartitionValues(rangeDesc, partitionValues);

            Assert.assertEquals(Collections.singletonList("Dt"), rangeDesc.getColumnsFromPathKeys());
            Assert.assertEquals(Collections.singletonList("2026-08-03"), rangeDesc.getColumnsFromPath());
            Assert.assertEquals(Collections.singletonList(false), rangeDesc.getColumnsFromPathIsNull());

            Mockito.clearInvocations(table);
            node.setPartitionValues(new TFileRangeDesc(), partitionValues);
            Mockito.verify(table, Mockito.never()).specs();
            Mockito.verify(table, Mockito.never()).schema();
        }
    }

    @Test
    public void testExtractNameMappingDistinguishesAbsentAndEmpty() throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        Table table = Mockito.mock(Table.class);
        setIcebergTable(node, table);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(Mockito.mock(IcebergExternalTable.class));
        setIcebergSource(node, source);

        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
        Assert.assertFalse(extractNameMapping(node).isPresent());

        Mockito.when(table.properties()).thenReturn(
                Collections.singletonMap(TableProperties.DEFAULT_NAME_MAPPING, "[]"));
        Optional<Map<Integer, List<String>>> emptyMapping = extractNameMapping(node);
        Assert.assertTrue(emptyMapping.isPresent());
        Assert.assertTrue(emptyMapping.get().isEmpty());
    }

    @Test
    public void testSnapshotCacheIgnoresIdlessNameMappingWrapper() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(Collections.singletonMap(
                TableProperties.DEFAULT_NAME_MAPPING,
                "[{\"names\":[\"legacy_wrapper\"],\"fields\":["
                        + "{\"field-id\":7,\"names\":[\"legacy_child\"]}]}]"));

        IcebergSnapshotCacheValue snapshotCacheValue = new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(),
                        Collections.emptyMap()),
                new IcebergSnapshot(1L, 1L),
                IcebergUtils.getNameMapping(table));

        Assert.assertTrue(snapshotCacheValue.getNameMapping().isPresent());
        Assert.assertEquals(Collections.singletonList("legacy_child"),
                snapshotCacheValue.getNameMapping().get().get(7));
        Assert.assertEquals(Collections.singleton(7),
                snapshotCacheValue.getNameMapping().get().keySet());
    }

    @Test
    public void testExtractNameMappingUsesStatementPinnedMetadataAfterPropertyRefresh() throws Exception {
        Table refreshedTable = Mockito.mock(Table.class);
        Mockito.when(refreshedTable.properties()).thenReturn(
                Collections.singletonMap(TableProperties.DEFAULT_NAME_MAPPING, "[]"));

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(targetTable.getName()).thenReturn("tbl");
        Mockito.when(targetTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, refreshedTable);
        setIcebergSource(node, source);

        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        statementContext.setSnapshot(new MvccTableInfo(targetTable), new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(1L, 11L), Optional.empty())));
        try {
            Assert.assertFalse(extractNameMapping(node).isPresent());
        } finally {
            ConnectContext.remove();
        }
    }

    private static class CountPlanningIcebergScanNode extends IcebergScanNode {
        private final TableScan tableScan;
        private final long snapshotCount;
        private int snapshotCountCalls;

        CountPlanningIcebergScanNode(SessionVariable sv, TableScan tableScan, long snapshotCount) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), sv, ScanContext.EMPTY);
            this.tableScan = tableScan;
            this.snapshotCount = snapshotCount;
        }

        @Override
        public TableScan createTableScan() {
            return tableScan;
        }

        @Override
        public long getCountFromSnapshot() {
            ++snapshotCountCalls;
            return snapshotCount;
        }

        void addSlot(int slotId, Column column) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc);
            slot.setColumn(column);
            desc.addSlot(slot);
        }
    }

    @Test
    public void testTableLevelCountSplitPlanningRequiresCountStar() {
        SessionVariable sv = Mockito.mock(SessionVariable.class);
        Mockito.when(sv.getEnableExternalTableBatchMode()).thenReturn(false);
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(Mockito.mock(Snapshot.class));

        // COUNT(required_col) carries a non-empty semantic argument list. Even though its result
        // equals COUNT(*) for valid data, BE intentionally reads the column to enforce schema
        // contracts. FE must therefore leave all real file tasks available to that fallback.
        CountPlanningIcebergScanNode countColumnNode =
                new CountPlanningIcebergScanNode(sv, tableScan, 30_000);
        countColumnNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countColumnNode.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));
        Assert.assertFalse(countColumnNode.isBatchMode());
        Assert.assertEquals(0, countColumnNode.snapshotCountCalls);

        // COUNT(*) has an explicitly empty argument list, so snapshot row count remains eligible
        // and doGetSplits may retain only representative tasks for parallel materialization.
        CountPlanningIcebergScanNode countStarNode =
                new CountPlanningIcebergScanNode(sv, tableScan, 30_000);
        countStarNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countStarNode.setPushDownCountSlotIds(Collections.emptyList());
        Assert.assertFalse(countStarNode.isBatchMode());
        Assert.assertEquals(1, countStarNode.snapshotCountCalls);
    }

    @Test
    public void testCountStarVariantCompatibilityExemptionRequiresSnapshotCount() throws Exception {
        SessionVariable sv = Mockito.mock(SessionVariable.class);
        Mockito.when(sv.getEnableExternalTableBatchMode()).thenReturn(false);
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(Mockito.mock(Snapshot.class));
        Backend oldBackend = Mockito.mock(Backend.class);
        Mockito.when(oldBackend.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(oldBackend.getId()).thenReturn(10004L);

        CountPlanningIcebergScanNode metadataCount =
                new CountPlanningIcebergScanNode(sv, tableScan, 12);
        metadataCount.addSlot(1, new Column("payload", Type.VARIANT));
        metadataCount.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        metadataCount.setPushDownCountSlotIds(Collections.emptyList());
        metadataCount.checkVariantBackendCompatibilityForCurrentScan(
                Collections.singletonList(oldBackend));

        CountPlanningIcebergScanNode scanFallback =
                new CountPlanningIcebergScanNode(sv, tableScan, -1);
        scanFallback.addSlot(1, new Column("payload", Type.VARIANT));
        scanFallback.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        scanFallback.setPushDownCountSlotIds(Collections.emptyList());
        try {
            scanFallback.checkVariantBackendCompatibilityForCurrentScan(
                    Collections.singletonList(oldBackend));
            Assert.fail("COUNT(*) data fallback must retain the Variant backend gate");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10004"));
        }
    }

    @Test
    public void testSameIcebergScanReusesPlannedFileTasksWithinStatement() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode firstNode = new TestIcebergScanNode(new SessionVariable());
            TestIcebergScanNode secondNode = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(firstNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondNode, mockIcebergSource(10L, 20L));
            TableScan firstScan = mockTableScan(30L, 40, Expressions.equal("id", 1));
            TableScan secondScan = mockTableScan(30L, 40, Expressions.equal("id", 1));
            FileScanTask task = Mockito.mock(FileScanTask.class);
            AtomicInteger planCalls = new AtomicInteger();

            List<FileScanTask> firstTasks = firstNode.getOrPlanFileScanTasks(firstScan, () -> {
                planCalls.incrementAndGet();
                return Collections.singletonList(task);
            });
            List<FileScanTask> secondTasks = secondNode.getOrPlanFileScanTasks(secondScan, () -> {
                planCalls.incrementAndGet();
                return Collections.emptyList();
            });

            Assert.assertEquals(1, planCalls.get());
            Assert.assertSame(firstTasks, secondTasks);
            Assert.assertEquals(Collections.singletonList(task), secondTasks);
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testRepeatedActualIcebergPlanningReusesManifestTasks() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        Table table = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(),
                temporaryFolder.newFolder("repeated_scan_table").toURI().toString());
        AppendFiles append = table.newFastAppend();
        int fileCount = 100;
        for (int i = 0; i < fileCount; i++) {
            append.appendFile(DataFiles.builder(table.spec())
                    .withPath("file:/tmp/repeated-scan-" + i + ".parquet")
                    .withFileSizeInBytes(1024)
                    .withRecordCount(1)
                    .withFormat(FileFormat.PARQUET)
                    .build());
        }
        append.commit();

        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(node, mockIcebergSource(10L, 20L));
            AtomicInteger planCalls = new AtomicInteger();

            List<FileScanTask> tasks = Collections.emptyList();
            for (int i = 0; i < 3; i++) {
                TableScan scan = table.newScan().filter(Expressions.equal("id", 1));
                tasks = node.getOrPlanFileScanTasks(scan, () -> {
                    planCalls.incrementAndGet();
                    return materializeTasks(scan);
                });
            }

            Assert.assertEquals(1, planCalls.get());
            Assert.assertEquals(fileCount, tasks.size());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    private static List<FileScanTask> materializeTasks(TableScan scan) {
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> plannedTasks = scan.planFiles()) {
            plannedTasks.forEach(tasks::add);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tasks;
    }

    @Test
    public void testIcebergScanTaskCacheSeparatesSnapshotSchemaAndPredicate() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(node, mockIcebergSource(10L, 20L));
            AtomicInteger planCalls = new AtomicInteger();

            node.getOrPlanFileScanTasks(
                    mockTableScan(30L, 40, Expressions.equal("id", 1)),
                    () -> plannedTask(planCalls));
            node.getOrPlanFileScanTasks(
                    mockTableScan(31L, 40, Expressions.equal("id", 1)),
                    () -> plannedTask(planCalls));
            node.getOrPlanFileScanTasks(
                    mockTableScan(30L, 41, Expressions.equal("id", 1)),
                    () -> plannedTask(planCalls));
            node.getOrPlanFileScanTasks(
                    mockTableScan(30L, 40, Expressions.equal("id", 2)),
                    () -> plannedTask(planCalls));

            Assert.assertEquals(4, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testPreparedExecutionResetClearsIcebergScanTaskCache() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(node, mockIcebergSource(10L, 20L));
            AtomicInteger planCalls = new AtomicInteger();
            TableScan scan = mockTableScan(30L, 40, Expressions.equal("id", 1));

            node.getOrPlanFileScanTasks(scan, () -> plannedTask(planCalls));
            statementContext.resetMvccSnapshots();
            node.getOrPlanFileScanTasks(scan, () -> plannedTask(planCalls));

            Assert.assertEquals(2, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testStatementCloseClearsIcebergScanTaskCache() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(node, mockIcebergSource(10L, 20L));
            AtomicInteger planCalls = new AtomicInteger();
            TableScan scan = mockTableScan(30L, 40, Expressions.equal("id", 1));

            node.getOrPlanFileScanTasks(scan, () -> plannedTask(planCalls));
            statementContext.close();
            node.getOrPlanFileScanTasks(scan, () -> plannedTask(planCalls));

            Assert.assertEquals(2, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    private static IcebergSource mockIcebergSource(long catalogId, long tableId) {
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getId()).thenReturn(catalogId);
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.getTargetTable()).thenReturn(table);
        return source;
    }

    private static TableScan mockTableScan(
            long snapshotId, int schemaId, org.apache.iceberg.expressions.Expression filter) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Schema schema = new Schema(schemaId,
                ImmutableList.of(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(scan.snapshot()).thenReturn(snapshot);
        Mockito.when(scan.schema()).thenReturn(schema);
        Mockito.when(scan.filter()).thenReturn(filter);
        return scan;
    }

    private static List<FileScanTask> plannedTask(AtomicInteger planCalls) {
        planCalls.incrementAndGet();
        return Collections.singletonList(Mockito.mock(FileScanTask.class));
    }

    @Test
    public void testInitialDefaultMetadataUsesCurrentSchemaForOrdinaryScan() throws Exception {
        Schema snapshotSchema = new Schema(Types.NestedField.optional("historical_binary")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build());
        Schema currentSchema = new Schema(Types.NestedField.optional("current_string")
                .withId(7)
                .ofType(Types.StringType.get())
                .withInitialDefault("not-base64")
                .build());
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.schemaId()).thenReturn(11);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(11, snapshotSchema));
        Mockito.when(table.schema()).thenReturn(currentSchema);
        TableScan snapshotScan = Mockito.mock(TableScan.class);
        Mockito.when(snapshotScan.snapshot()).thenReturn(snapshot);
        Mockito.when(snapshotScan.table()).thenReturn(table);
        Mockito.when(snapshotScan.schema()).thenReturn(currentSchema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(snapshotScan);
        setIcebergTable(node, table);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(Mockito.mock(TableIf.class));
        setIcebergSource(node, source);

        Map<Integer, String> defaults = node.getBase64EncodedInitialDefaultsForScan();
        Assert.assertTrue(defaults.isEmpty());
    }

    @Test
    public void testInitialDefaultMetadataUsesStatementPinnedSchemaAfterCacheInvalidation() throws Exception {
        Schema pinnedSchema = new Schema(11, ImmutableList.of(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build()));
        Schema refreshedSchema = new Schema(12,
                ImmutableList.of(Types.NestedField.optional(8, "replacement", Types.IntegerType.get())));
        Table refreshedTable = Mockito.mock(Table.class);
        Mockito.when(refreshedTable.schema()).thenReturn(refreshedSchema);
        Mockito.when(refreshedTable.schemas()).thenReturn(ImmutableMap.of(
                pinnedSchema.schemaId(), pinnedSchema,
                refreshedSchema.schemaId(), refreshedSchema));

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(targetTable.getName()).thenReturn("tbl");
        Mockito.when(targetTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, refreshedTable);
        setIcebergSource(node, source);

        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        statementContext.setSnapshot(new MvccTableInfo(targetTable), new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(1L, pinnedSchema.schemaId()))));
        try {
            Assert.assertEquals(Collections.singletonMap(7, "AAEC/w=="),
                    node.getBase64EncodedInitialDefaultsForScan());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testInitialDefaultMetadataUsesSnapshotSchemaForExplicitSelection() throws Exception {
        Schema snapshotSchema = new Schema(Types.NestedField.optional("historical_binary")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build());
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.schemaId()).thenReturn(11);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(11, snapshotSchema));
        TableScan snapshotScan = Mockito.mock(TableScan.class);
        Mockito.when(snapshotScan.snapshot()).thenReturn(snapshot);
        Mockito.when(snapshotScan.table()).thenReturn(table);

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);
        IcebergTableQueryInfo selectedSnapshot = Mockito.mock(IcebergTableQueryInfo.class);
        Mockito.when(selectedSnapshot.getSchemaId()).thenReturn(11);

        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        node.setTableScan(snapshotScan);
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        Mockito.doReturn(selectedSnapshot).when(node).getSpecifiedSnapshot();

        Map<Integer, String> defaults = node.getBase64EncodedInitialDefaultsForScan();

        Assert.assertEquals(Collections.singletonMap(7, "AAEC/w=="), defaults);
    }

    @Test
    public void testInitialDefaultMetadataUsesStatementPinnedBranchSchema() throws Exception {
        Schema dataSnapshotSchema = new Schema(11, ImmutableList.of(Types.NestedField.optional("string_default")
                .withId(7)
                .ofType(Types.StringType.get())
                .withInitialDefault("not-base64")
                .build()));
        Schema branchSchema = new Schema(12, ImmutableList.of(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build()));
        Snapshot dataSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(dataSnapshot.schemaId()).thenReturn(dataSnapshotSchema.schemaId());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                dataSnapshotSchema.schemaId(), dataSnapshotSchema,
                branchSchema.schemaId(), branchSchema));
        TableScan branchScan = Mockito.mock(TableScan.class);
        Mockito.when(branchScan.snapshot()).thenReturn(dataSnapshot);
        Mockito.when(branchScan.table()).thenReturn(table);

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(targetTable.getName()).thenReturn("tbl");
        Mockito.when(targetTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);
        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        node.setTableScan(branchScan);
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        Mockito.doReturn(new IcebergTableQueryInfo(1L, "branch", branchSchema.schemaId()))
                .when(node).getSpecifiedSnapshot();

        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        statementContext.setSnapshot(new MvccTableInfo(targetTable), new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(1L, branchSchema.schemaId()))));
        try {
            Assert.assertEquals(Collections.singletonMap(7, "AAEC/w=="),
                    node.getBase64EncodedInitialDefaultsForScan());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testInitialDefaultMetadataUsesSystemTableSchemaWithoutTableScan() throws Exception {
        Schema systemTableSchema = new Schema(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build());
        Table systemTable = Mockito.mock(Table.class);
        Mockito.when(systemTable.schema()).thenReturn(systemTableSchema);

        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        Field icebergTableField = IcebergScanNode.class.getDeclaredField("icebergTable");
        icebergTableField.setAccessible(true);
        icebergTableField.set(node, systemTable);
        Field isSystemTableField = IcebergScanNode.class.getDeclaredField("isSystemTable");
        isSystemTableField.setAccessible(true);
        isSystemTableField.setBoolean(node, true);

        Map<Integer, String> defaults = node.getBase64EncodedInitialDefaultsForScan();

        Assert.assertEquals(Collections.singletonMap(7, "AAEC/w=="), defaults);
        Mockito.verify(node, Mockito.never()).createTableScan();
    }

    @Test
    public void testHistoricalPredicateUsesSelectedScanSchema() throws Exception {
        Schema historicalSchema = new Schema(
                Types.NestedField.optional(7, "old_name", Types.IntegerType.get()));
        Schema currentSchema = new Schema(
                Types.NestedField.optional(8, "new_name", Types.IntegerType.get()));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(currentSchema);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(
                historicalSchema.schemaId(), historicalSchema));
        TableScan scan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(scan.schema()).thenReturn(historicalSchema);
        Mockito.when(scan.metricsReporter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.useSnapshot(1L)).thenReturn(scan);
        Mockito.when(scan.project(historicalSchema)).thenReturn(scan);
        Mockito.when(scan.filter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.planWith(Mockito.any())).thenReturn(scan);
        Mockito.when(table.newScan()).thenReturn(scan);

        IcebergSource source = Mockito.mock(IcebergSource.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);

        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        Mockito.doReturn(new IcebergTableQueryInfo(1L, null, historicalSchema.schemaId()))
                .when(node).getSpecifiedSnapshot();
        node.addConjunct(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(new TableName(), "old_name"), new IntLiteral(1, Type.INT)));

        node.createRealTableScan();

        Mockito.verify(scan).filter(Mockito.argThat(expression -> expression.toString().contains("old_name")));
    }

    @Test
    public void testPinnedBranchUsesFrozenSnapshotWithCurrentSchema() throws Exception {
        Schema snapshotSchema = new Schema(11, ImmutableList.of(
                Types.NestedField.optional(1, "old_name", Types.StringType.get())));
        Schema currentSchema = new Schema(12, ImmutableList.of(
                Types.NestedField.optional(1, "new_name", Types.StringType.get()),
                Types.NestedField.optional(2, "new_col", Types.StringType.get())));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(currentSchema);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(11, snapshotSchema, 12, currentSchema));
        Mockito.when(table.refs()).thenReturn(Collections.singletonMap(
                "moving", SnapshotRef.branchBuilder(2L).build()));
        TableScan scan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(scan.schema()).thenReturn(currentSchema);
        Mockito.when(scan.metricsReporter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.useSnapshot(1L)).thenReturn(scan);
        Mockito.when(scan.project(currentSchema)).thenReturn(scan);
        Mockito.when(table.newScan()).thenReturn(scan);

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(targetTable.getName()).thenReturn("tbl");
        Mockito.when(targetTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        IcebergExternalCatalog sourceCatalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);
        Mockito.when(source.getCatalog()).thenReturn(sourceCatalog);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        node.setScanParams(new TableScanParams(TableScanParams.BRANCH,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "moving"), Collections.emptyList()));

        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        statementContext.setSnapshot(new MvccTableInfo(targetTable), new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(1L, currentSchema.schemaId()))));
        try {
            node.createRealTableScan();

            Mockito.verify(scan).useSnapshot(1L);
            Mockito.verify(scan).project(currentSchema);
            Mockito.verify(scan, Mockito.never()).useRef(Mockito.anyString());
            Mockito.verify(table, Mockito.never()).refs();
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testPinnedLatestUsesFrozenSnapshot() throws Exception {
        Schema schema = new Schema(21, ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(schema.schemaId(), schema));
        TableScan scan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(scan.schema()).thenReturn(schema);
        Mockito.when(scan.metricsReporter(Mockito.any())).thenReturn(scan);
        Mockito.when(scan.useSnapshot(7L)).thenReturn(scan);
        Mockito.when(scan.project(schema)).thenReturn(scan);
        Mockito.when(table.newScan()).thenReturn(scan);

        IcebergExternalTable targetTable = Mockito.mock(IcebergExternalTable.class);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);
        Mockito.when(source.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        node.setRelationSnapshot(Optional.of(new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(7L, schema.schemaId())))));

        node.createRealTableScan();

        Mockito.verify(scan).useSnapshot(7L);
        Mockito.verify(scan).project(schema);
    }

    @Test
    public void testPinnedEmptyTableDoesNotUseInvalidSnapshot() throws Exception {
        Schema schema = new Schema(21, ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(schema.schemaId(), schema));
        TableScan scan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(scan.schema()).thenReturn(schema);
        Mockito.when(scan.metricsReporter(Mockito.any())).thenReturn(scan);
        Mockito.when(table.newScan()).thenReturn(scan);

        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(Mockito.mock(IcebergExternalTable.class));
        Mockito.when(source.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        node.setRelationSnapshot(Optional.of(new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(-1L, schema.schemaId())))));

        node.createRealTableScan();

        Mockito.verify(scan, Mockito.never()).useSnapshot(Mockito.anyLong());
    }

    @Test
    public void testPinnedEmptyTableUsesFrozenGenerationAfterRefresh() throws Exception {
        Schema schema = new Schema(21, ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        Table frozenEmptyTable = Mockito.mock(Table.class);
        TableScan frozenEmptyScan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(frozenEmptyScan.schema()).thenReturn(schema);
        Mockito.when(frozenEmptyScan.metricsReporter(Mockito.any())).thenReturn(frozenEmptyScan);
        Mockito.when(frozenEmptyScan.planWith(Mockito.any())).thenReturn(frozenEmptyScan);
        Mockito.when(frozenEmptyTable.newScan()).thenReturn(frozenEmptyScan);

        Table refreshedTable = Mockito.mock(Table.class);
        TableScan refreshedScan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(refreshedScan.schema()).thenReturn(schema);
        Mockito.when(refreshedScan.metricsReporter(Mockito.any())).thenReturn(refreshedScan);
        Mockito.when(refreshedScan.planWith(Mockito.any())).thenReturn(refreshedScan);
        Mockito.when(refreshedTable.newScan()).thenReturn(refreshedScan);

        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(Mockito.mock(IcebergExternalTable.class));
        Mockito.when(source.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, refreshedTable);
        setIcebergSource(node, source);
        node.setRelationSnapshot(Optional.of(new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(-1L, schema.schemaId()), Optional.empty(), frozenEmptyTable))));

        TableScan plannedScan = node.createRealTableScan();

        Assert.assertSame(frozenEmptyScan, plannedScan);
        Mockito.verify(frozenEmptyTable).newScan();
        Mockito.verify(refreshedTable, Mockito.never()).newScan();
    }

    @Test
    public void testPinnedNonEmptyTableUsesFrozenGenerationAfterRefresh() throws Exception {
        Schema schema = new Schema(21, ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        Table frozenTable = Mockito.mock(Table.class);
        TableScan frozenScan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(frozenScan.schema()).thenReturn(schema);
        Mockito.when(frozenScan.metricsReporter(Mockito.any())).thenReturn(frozenScan);
        Mockito.when(frozenScan.useSnapshot(101L)).thenReturn(frozenScan);
        Mockito.when(frozenScan.project(schema)).thenReturn(frozenScan);
        Mockito.when(frozenScan.planWith(Mockito.any())).thenReturn(frozenScan);
        Mockito.when(frozenTable.newScan()).thenReturn(frozenScan);
        Mockito.when(frozenTable.schemas()).thenReturn(Collections.singletonMap(schema.schemaId(), schema));

        Table refreshedTable = Mockito.mock(Table.class);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(Mockito.mock(IcebergExternalTable.class));
        Mockito.when(source.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, refreshedTable);
        setIcebergSource(node, source);
        node.setRelationSnapshot(Optional.of(new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(101L, schema.schemaId()), Optional.empty(), frozenTable))));

        TableScan plannedScan = node.createRealTableScan();

        Assert.assertSame(frozenScan, plannedScan);
        Mockito.verify(frozenScan).useSnapshot(101L);
        Mockito.verify(refreshedTable, Mockito.never()).newScan();
    }

    @Test
    public void testSnapshotSelectableMetadataTableUsesFrozenBaseGeneration() throws Exception {
        Schema schema = new Schema(21, ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "file:/tmp/frozen-metadata-table",
                Collections.emptyMap());
        Table frozenBaseTable = new BaseTable(new StaticTableOperations(
                metadata, Mockito.mock(org.apache.iceberg.io.FileIO.class),
                Mockito.mock(org.apache.iceberg.io.LocationProvider.class)), "table");
        Table currentMetadataTable = Mockito.mock(Table.class);

        IcebergSysExternalTable targetTable = Mockito.mock(IcebergSysExternalTable.class);
        Mockito.when(targetTable.supportsSnapshotSelection()).thenReturn(true);
        Mockito.when(targetTable.getSysTableType()).thenReturn(MetadataTableType.FILES.name());
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergSource(node, source);
        Field isSystemTableField = IcebergScanNode.class.getDeclaredField("isSystemTable");
        isSystemTableField.setAccessible(true);
        isSystemTableField.setBoolean(node, true);
        node.setRelationSnapshot(Optional.of(new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(-1L, schema.schemaId()), Optional.empty(), frozenBaseTable))));

        Table retainedMetadataTable = useFrozenTableGeneration(node, currentMetadataTable);

        Assert.assertNotSame(currentMetadataTable, retainedMetadataTable);
        Assert.assertTrue(retainedMetadataTable instanceof BaseMetadataTable);
        Assert.assertEquals(schema.asStruct(), ((BaseMetadataTable) retainedMetadataTable).table()
                .schema().asStruct());
    }

    @Test
    public void testAllMetadataTableDoesNotUseSnapshot() throws Exception {
        Schema schema = new Schema(21, ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(schema.schemaId(), schema));
        TableScan scan = Mockito.mock(TableScan.class, Mockito.RETURNS_SELF);
        Mockito.when(scan.schema()).thenReturn(schema);
        Mockito.when(scan.metricsReporter(Mockito.any())).thenReturn(scan);
        Mockito.when(table.newScan()).thenReturn(scan);

        IcebergSysExternalTable targetTable = Mockito.mock(IcebergSysExternalTable.class);
        Mockito.when(targetTable.supportsSnapshotSelection()).thenReturn(false);
        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getTargetTable()).thenReturn(targetTable);
        Mockito.when(source.getCatalog()).thenReturn(Mockito.mock(IcebergExternalCatalog.class));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        setIcebergSource(node, source);
        node.setRelationSnapshot(Optional.of(new IcebergMvccSnapshot(
                new IcebergSnapshotCacheValue(new IcebergPartitionInfo(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                        new IcebergSnapshot(7L, schema.schemaId())))));

        node.createRealTableScan();

        Mockito.verify(scan, Mockito.never()).useSnapshot(Mockito.anyLong());
    }

    private static void setIcebergTable(IcebergScanNode node, Table table) throws Exception {
        Field icebergTableField = IcebergScanNode.class.getDeclaredField("icebergTable");
        icebergTableField.setAccessible(true);
        icebergTableField.set(node, table);
        for (String fieldName : Arrays.asList("orderedPathPartitionKeys", "orderedPartitionMetadataKeys")) {
            Field field = IcebergScanNode.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(node, null);
        }
    }

    private static void setIcebergSource(IcebergScanNode node, IcebergSource source) throws Exception {
        Field sourceField = IcebergScanNode.class.getDeclaredField("source");
        sourceField.setAccessible(true);
        sourceField.set(node, source);
    }

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestIcebergScanNode node = new TestIcebergScanNode(sv);

        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(10_000L * MB);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.length()).thenReturn(10_000L * MB);

        try (org.mockito.MockedStatic<ScanTaskUtil> mockedScanTaskUtil =
                Mockito.mockStatic(ScanTaskUtil.class)) {
            mockedScanTaskUtil.when(() -> ScanTaskUtil.contentSizeInBytes(dataFile))
                    .thenReturn(10_000L * MB);

            Method method = IcebergScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", Iterable.class);
            method.setAccessible(true);
            long target = (long) method.invoke(node, Collections.singletonList(task));
            Assert.assertEquals(100 * MB, target);
        }
    }

    @Test
    public void testSetIcebergParamsKeepsDeletionVectorOffsetAsLong() throws Exception {
        SessionVariable sv = new SessionVariable();
        TestIcebergScanNode node = new TestIcebergScanNode(sv);

        Field formatVersionField = IcebergScanNode.class.getDeclaredField("formatVersion");
        formatVersionField.setAccessible(true);
        formatVersionField.set(node, 3);

        String dataPath = "file:///tmp/data-file.parquet";
        String deletePath = "file:///tmp/delete-shared.puffin";
        IcebergSplit split = new IcebergSplit(LocationPath.of(dataPath), 0, 128, 128, new String[0],
                3, Collections.emptyMap(), new ArrayList<>(), dataPath);
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setSplitFileFormat(FileFormat.PARQUET);
        split.setFirstRowId(10L);
        split.setLastUpdatedSequenceNumber(20L);
        split.setDeleteFileFilters(Collections.emptyList(), Collections.singletonList(
                new IcebergDeleteFileFilter.DeletionVector(deletePath, -1L, -1L, 256L,
                        (long) Integer.MAX_VALUE + 5L, (long) Integer.MAX_VALUE + 7L)));

        Method method = IcebergScanNode.class.getDeclaredMethod("setIcebergParams",
                TFileRangeDesc.class, IcebergSplit.class);
        method.setAccessible(true);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, split);

        TIcebergDeleteFileDesc deleteFileDesc = rangeDesc.getTableFormatParams()
                .getIcebergParams()
                .getDeleteFiles()
                .get(0);
        Assert.assertEquals((long) Integer.MAX_VALUE + 5L, deleteFileDesc.getContentOffset());
        Assert.assertEquals((long) Integer.MAX_VALUE + 7L, deleteFileDesc.getContentSizeInBytes());
    }

    @Test
    public void testSetIcebergParamsUsesSplitFileFormat() throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        String dataPath = "file:///tmp/data-file.orc";
        IcebergSplit split = new IcebergSplit(LocationPath.of(dataPath), 0, 128, 128, new String[0],
                2, Collections.emptyMap(), new ArrayList<>(), dataPath);
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setSplitFileFormat(FileFormat.ORC);

        Method method = IcebergScanNode.class.getDeclaredMethod("setIcebergParams",
                TFileRangeDesc.class, IcebergSplit.class);
        method.setAccessible(true);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, split);

        // Iceberg tables may mix file formats, so each range must preserve its split format.
        Assert.assertEquals(TFileFormatType.FORMAT_ORC, rangeDesc.getFormatType());
    }

    @Test
    public void testPositionDeleteSystemTableValidatesDeletionVectorMetadata() throws Exception {
        DeleteFile deleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(deleteFile.path()).thenReturn("file:///tmp/delete-shared.puffin");
        Mockito.when(deleteFile.format()).thenReturn(FileFormat.PUFFIN);
        Mockito.when(deleteFile.fileSizeInBytes()).thenReturn(100L);
        Mockito.when(deleteFile.contentOffset()).thenReturn(null);
        Mockito.when(deleteFile.contentSizeInBytes()).thenReturn(10L);

        PositionDeletesScanTask task = Mockito.mock(PositionDeletesScanTask.class);
        Mockito.when(task.file()).thenReturn(deleteFile);
        Mockito.when(task.start()).thenReturn(0L);
        Mockito.when(task.length()).thenReturn(100L);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        Method method = IcebergScanNode.class.getDeclaredMethod(
                "createIcebergPositionDeleteSysSplit", PositionDeletesScanTask.class);
        method.setAccessible(true);

        try {
            method.invoke(node, task);
            Assert.fail("position_deletes planning should reject invalid deletion vector metadata");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assert.assertTrue(e.getCause().getMessage().contains("delete-shared.puffin"));
        }
    }

    @Test
    public void testSetIcebergParamsPropagatesPositionDeleteFileFormat() throws Exception {
        SessionVariable sv = new SessionVariable();
        TestIcebergScanNode node = new TestIcebergScanNode(sv);

        Field formatVersionField = IcebergScanNode.class.getDeclaredField("formatVersion");
        formatVersionField.setAccessible(true);
        formatVersionField.set(node, 2);

        String dataPath = "file:///tmp/data-file.parquet";
        String deletePath = "file:///tmp/delete-file.orc";
        IcebergSplit split = new IcebergSplit(LocationPath.of(dataPath), 0, 128, 128, new String[0],
                2, Collections.emptyMap(), new ArrayList<>(), dataPath);
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setSplitFileFormat(FileFormat.PARQUET);
        split.setDeleteFileFilters(Collections.emptyList(), Collections.singletonList(
                new IcebergDeleteFileFilter.PositionDelete(deletePath, -1L, -1L, 256L,
                        org.apache.iceberg.FileFormat.ORC)));

        Method method = IcebergScanNode.class.getDeclaredMethod("setIcebergParams",
                TFileRangeDesc.class, IcebergSplit.class);
        method.setAccessible(true);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        method.invoke(node, rangeDesc, split);

        TIcebergDeleteFileDesc deleteFileDesc = rangeDesc.getTableFormatParams()
                .getIcebergParams()
                .getDeleteFiles()
                .get(0);
        Assert.assertEquals(org.apache.doris.thrift.TFileFormatType.FORMAT_ORC, deleteFileDesc.getFileFormat());
    }

    @Test
    public void testPartitionDataJsonMatchesRenamedFieldById() throws Exception {
        SessionVariable sv = new SessionVariable();
        TestIcebergScanNode node = new TestIcebergScanNode(sv);

        Schema schema = new Schema(Types.NestedField.required(1, "p", Types.IntegerType.get()));
        PartitionSpec oldSpec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData partitionData = new PartitionData(oldSpec.partitionType());
        partitionData.set(0, 10);
        int partitionFieldId = oldSpec.fields().get(0).fieldId();
        List<Types.NestedField> outputPartitionFields = Collections.singletonList(
                Types.NestedField.optional(partitionFieldId, "p2", Types.IntegerType.get()));

        Method method = IcebergScanNode.class.getDeclaredMethod("getPartitionDataObjectJson",
                PartitionData.class, PartitionSpec.class, List.class);
        method.setAccessible(true);

        Assert.assertEquals("{\"p2\":10}", method.invoke(node, partitionData, oldSpec, outputPartitionFields));
    }

    @Test
    public void testRejectBinaryPartitionValueWithoutBinarySafeTransport() throws Exception {
        assertUnsupportedPositionDeletesPartitionValue(
                Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {0, (byte) 0xff}), false, "binary");
        assertUnsupportedPositionDeletesPartitionValue(
                Types.FixedType.ofLength(2), ByteBuffer.wrap(new byte[] {0, (byte) 0xff}), false, "fixed[2]");
    }

    @Test
    public void testRejectUuidPartitionValueWhenMappedToVarbinary() throws Exception {
        assertUnsupportedPositionDeletesPartitionValue(
                Types.UUIDType.get(), UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), true, "uuid");
    }

    private void assertUnsupportedPositionDeletesPartitionValue(
            org.apache.iceberg.types.Type type, Object value, boolean enableMappingVarbinary,
            String expectedType) throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), enableMappingVarbinary);
        Schema schema = new Schema(Types.NestedField.required(1, "p", type));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        PartitionData partitionData = new PartitionData(spec.partitionType());
        partitionData.set(0, value);

        Method method = IcebergScanNode.class.getDeclaredMethod("getPartitionDataObjectJson",
                PartitionData.class, PartitionSpec.class, List.class);
        method.setAccessible(true);
        try {
            method.invoke(node, partitionData, spec, spec.partitionType().fields());
            Assert.fail("Binary partition values must not be silently materialized as NULL");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof UserException);
            Assert.assertTrue(e.getCause().getMessage().contains("partition field 'p'"));
            Assert.assertTrue(e.getCause().getMessage().contains(expectedType));
        }
    }

    @Test
    public void testRejectUnsupportedPositionDeleteFileFormat() throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        Method method = IcebergScanNode.class.getDeclaredMethod(
                "getNativePositionDeleteFileFormat", FileFormat.class);
        method.setAccessible(true);

        try {
            method.invoke(node, FileFormat.AVRO);
            Assert.fail("AVRO position delete files should be rejected explicitly");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
            Assert.assertEquals("Unsupported Iceberg position delete file format: AVRO",
                    e.getCause().getMessage());
        }
    }

    @Test
    public void testRejectSmoothUpgradeSourceBackendForPositionDeletes() throws Exception {
        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isSmoothUpgradeSrc()).thenReturn(false);
        IcebergScanNode.checkPositionDeletesBackendCompatibility(Collections.singletonList(currentBackend));

        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10001L);
        List<Backend> backends = new ArrayList<>();
        backends.add(currentBackend);
        backends.add(smoothUpgradeSource);

        try {
            IcebergScanNode.checkPositionDeletesBackendCompatibility(backends);
            Assert.fail("smooth upgrade source backend should reject native position_deletes planning");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10001 is a smooth upgrade source"));
        }
    }

    @Test
    public void testRejectSmoothUpgradeSourceBackendForVariantProjection() throws Exception {
        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isSmoothUpgradeSrc()).thenReturn(false);
        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10002L);
        List<Backend> backends = ImmutableList.of(currentBackend, smoothUpgradeSource);

        IcebergScanNode.checkVariantBackendCompatibility(false, backends);
        try {
            IcebergScanNode.checkVariantBackendCompatibility(true, backends);
            Assert.fail("semantic Variant projection must not be assigned to an old backend");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10002 is a smooth upgrade source"));
            Assert.assertTrue(e.getMessage().contains("Variant"));
        }
    }

    @Test
    public void testBatchVariantProjectionUsesSharedCompatibilityGate() throws Exception {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), false, true);
        node.addSlot(1, new Column("payload", Type.VARIANT));

        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isSmoothUpgradeSrc()).thenReturn(false);
        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10003L);

        Assert.assertTrue(node.isBatchMode());
        try {
            node.checkVariantBackendCompatibilityForCurrentScan(
                    ImmutableList.of(currentBackend, smoothUpgradeSource));
            Assert.fail("batch Variant projection must use the shared backend compatibility gate");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10003 is a smooth upgrade source"));
        }
    }

    @Test
    public void testVariantUpgradeGateUsesEffectiveProjectedSlotType() {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        StructType fullType = new StructType(
                new StructField("label", Type.STRING),
                new StructField("payload", Type.VARIANT));
        SlotDescriptor slot = node.addSlot(1, new Column("info", fullType));

        // Nested-column pruning keeps the original Column for identity but replaces the slot type
        // with the actual payload serialized to BE.
        slot.setType(new StructType(new StructField("label", Type.STRING)));
        Assert.assertFalse(node.projectsVariant());

        slot.setType(fullType);
        Assert.assertTrue(node.projectsVariant());
    }
}
