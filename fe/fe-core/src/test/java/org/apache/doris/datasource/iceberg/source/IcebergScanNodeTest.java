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

import org.apache.doris.analysis.AccessPathInfo;
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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
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
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TMetaAccessPath;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class IcebergScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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

    private static SlotDescriptor slotDescriptor(int slotId) {
        return new SlotDescriptor(new SlotId(slotId), new TupleDescriptor(new TupleId(0)));
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

        org.apache.doris.nereids.trees.expressions.Expression defaultExpression(Column column)
                throws UserException {
            return getDefaultValueExpression(column);
        }

        boolean hasInitialDefault(Column column) throws UserException {
            return hasDefaultValue(column);
        }

        int enableAndGetIcebergScanSemanticsVersion() {
            params = new TFileScanRangeParams();
            enableCurrentIcebergScanSemantics();
            return params.getIcebergScanSemanticsVersion();
        }

        TFileScanRangeParams initializeAndGetIcebergSchemaInfo(Schema scanSchema) throws UserException {
            params = new TFileScanRangeParams();
            initializeIcebergSchemaInfo(Optional.empty(), scanSchema, Collections.emptySet());
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
        Schema scanSchema = new Schema(
                Types.NestedField.optional(1, "int_col", Types.LongType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        Column projectedColumn = new Column("payload", Type.STRING, true);
        projectedColumn.setUniqueId(2);
        node.addSlot(1, projectedColumn);

        TFileScanRangeParams scanParams = node.initializeAndGetIcebergSchemaInfo(scanSchema);

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

    private static class PlanFilesCountingIcebergScanNode extends TestIcebergScanNode {
        private final boolean batchMode;
        private int planFileScanCalls;

        PlanFilesCountingIcebergScanNode(SessionVariable sv, boolean batchMode) {
            super(sv);
            this.batchMode = batchMode;
        }

        @Override
        public boolean isBatchMode() {
            return batchMode;
        }

        @Override
        CloseableIterable<FileScanTask> planFileScanTaskWithoutReuse(TableScan scan) {
            planFileScanCalls++;
            return scan.planFiles();
        }

        int getPlanFileScanCalls() {
            return planFileScanCalls;
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
    public void testMetadataCountSkipsEqualityDeleteFilePreflight() throws Exception {
        SessionVariable sv = Mockito.mock(SessionVariable.class);
        TableScan tableScan = Mockito.mock(TableScan.class);
        CountPlanningIcebergScanNode node =
                new CountPlanningIcebergScanNode(sv, tableScan, 30_000);
        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.emptyList());

        Assert.assertEquals(
                Collections.emptySet(), node.getEqualityDeleteFieldIdsForPlanning());
        Assert.assertEquals(1, node.snapshotCountCalls);
        Mockito.verify(tableScan, Mockito.never()).snapshot();
        Mockito.verify(tableScan, Mockito.never()).planFiles();

        Assert.assertFalse(node.isBatchMode());
        Assert.assertEquals(1, node.snapshotCountCalls);
    }

    @Test
    public void testOrdinaryScanReusesPreplannedFileTasks() throws Exception {
        DeleteFile equalityDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(equalityDelete.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(equalityDelete.recordCount()).thenReturn(1L);
        Mockito.when(equalityDelete.equalityFieldIds()).thenReturn(ImmutableList.of(7));
        FileScanTask fileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(fileScanTask.deletes()).thenReturn(ImmutableList.of(equalityDelete));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(ImmutableList.of(fileScanTask)));
        PlanFilesCountingIcebergScanNode node = new PlanFilesCountingIcebergScanNode(
                new SessionVariable(), false);

        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            Assert.assertEquals(ImmutableSet.of(7), node.loadEqualityDeleteFieldIds(tableScan));
            try (CloseableIterable<FileScanTask> plannedTasks =
                         node.planFileScanTask(tableScan)) {
                List<FileScanTask> actualTasks = new ArrayList<>();
                plannedTasks.forEach(actualTasks::add);
                Assert.assertEquals(ImmutableList.of(fileScanTask), actualTasks);
            }
        } finally {
            ConnectContext.remove();
        }

        Mockito.verify(tableScan, Mockito.times(1)).planFiles();
    }

    @Test
    public void testBatchScanKeepsFilePlanningLazy() throws Exception {
        DeleteFile firstEqualityDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(firstEqualityDelete.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(firstEqualityDelete.recordCount()).thenReturn(1L);
        Mockito.when(firstEqualityDelete.equalityFieldIds()).thenReturn(ImmutableList.of(7));
        DeleteFile secondEqualityDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(secondEqualityDelete.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(secondEqualityDelete.recordCount()).thenReturn(1L);
        Mockito.when(secondEqualityDelete.equalityFieldIds()).thenReturn(ImmutableList.of(9));
        FileScanTask firstFileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(firstFileScanTask.deletes()).thenReturn(ImmutableList.of(firstEqualityDelete));
        FileScanTask secondFileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(secondFileScanTask.deletes()).thenReturn(ImmutableList.of(secondEqualityDelete));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(
                        ImmutableList.of(firstFileScanTask, secondFileScanTask)));
        PlanFilesCountingIcebergScanNode node = new PlanFilesCountingIcebergScanNode(
                new SessionVariable(), true);

        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            Assert.assertEquals(Collections.emptySet(), node.getEqualityDeleteFieldIdsForPlanning());
            Mockito.verify(tableScan, Mockito.never()).planFiles();

            try (CloseableIterable<FileScanTask> plannedTasks =
                         node.planFileScanTask(tableScan)) {
                List<FileScanTask> actualTasks = new ArrayList<>();
                plannedTasks.forEach(actualTasks::add);
                Assert.assertEquals(
                        ImmutableList.of(firstFileScanTask, secondFileScanTask), actualTasks);
            }
        } finally {
            ConnectContext.remove();
        }

        Mockito.verify(tableScan, Mockito.times(1)).planFiles();
    }

    @Test
    public void testRewriteTasksKeepExactNonBatchTaskSource() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableExternalTableBatchMode = true;
        IcebergScanNode node = new IcebergScanNode(
                new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                sessionVariable, ScanContext.EMPTY);
        FileScanTask rewriteTask = Mockito.mock(FileScanTask.class);
        List<FileScanTask> rewriteTasks = ImmutableList.of(rewriteTask);
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        statementContext.setIcebergRewriteFileScanTasks(rewriteTasks);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            Assert.assertFalse(node.isBatchMode());
            Assert.assertSame(rewriteTasks, statementContext.getIcebergRewriteFileScanTasks());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testBatchScanPlansFilteredOldManifestAfterColumnRename() throws Exception {
        Schema oldSchema = new Schema(
                Types.NestedField.required(1, "old_name", Types.IntegerType.get()));
        HadoopTables tables = new HadoopTables(new Configuration());
        String tableLocation = temporaryFolder.getRoot().toPath()
                .resolve("filtered_old_manifest").toUri().toString();
        Table table = tables.create(
                oldSchema, PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"), tableLocation);
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(tableLocation + "/data/old-data.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
        table.newFastAppend().appendFile(dataFile).commit();
        table.updateSchema().renameColumn("old_name", "new_name").commit();
        DeleteFile equalityDelete = FileMetadata.deleteFileBuilder(table.spec())
                .ofEqualityDeletes(1)
                .withPath(tableLocation + "/data/equality-delete.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
        table.newRowDelta().addDeletes(equalityDelete).commit();

        TableScan tableScan =
                table.newScan().filter(Expressions.equal("new_name", 1));
        PlanFilesCountingIcebergScanNode node =
                new PlanFilesCountingIcebergScanNode(new SessionVariable(), true);
        setIcebergTable(node, table);
        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            Assert.assertEquals(ImmutableSet.of(1), node.loadEqualityDeleteFieldIds(tableScan));
            try (CloseableIterable<FileScanTask> plannedTasks =
                         node.planFileScanTask(tableScan)) {
                List<FileScanTask> tasks = new ArrayList<>();
                plannedTasks.forEach(tasks::add);
                Assert.assertEquals(1, tasks.size());
                Assert.assertEquals(ImmutableList.of(1),
                        tasks.get(0).deletes().get(0).equalityFieldIds());
            }
        } finally {
            ConnectContext.remove();
        }
        Assert.assertEquals(1, node.getPlanFileScanCalls());
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
        Mockito.when(table.schema()).thenReturn(currentSchema);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(11, snapshotSchema));
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
    public void testInitialDefaultMetadataUsesCurrentSchemaForOrdinaryRead() throws Exception {
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
        Snapshot currentSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(currentSnapshot.schemaId()).thenReturn(11);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(currentSchema);
        Mockito.when(table.currentSnapshot()).thenReturn(currentSnapshot);
        Mockito.when(table.schemas()).thenReturn(Collections.singletonMap(11, snapshotSchema));

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);

        Assert.assertSame(currentSchema, node.getQuerySchema());
        Assert.assertTrue(node.getBase64EncodedInitialDefaultsForScan().isEmpty());
    }

    @Test
    public void testScanColumnsKeepV3RowLineageMetadata() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.properties()).thenReturn(
                Collections.singletonMap(TableProperties.FORMAT_VERSION, "3"));

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);

        List<Column> scanColumns = node.getScanColumns(node.getQuerySchema());

        Assert.assertEquals(3, scanColumns.size());
        Assert.assertEquals("id", scanColumns.get(0).getName());
        Assert.assertEquals(IcebergUtils.ICEBERG_ROW_ID_COL, scanColumns.get(1).getName());
        Assert.assertEquals(MetadataColumns.ROW_ID.fieldId(), scanColumns.get(1).getUniqueId());
        Assert.assertFalse(scanColumns.get(1).isVisible());
        Assert.assertEquals(IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL,
                scanColumns.get(2).getName());
        Assert.assertEquals(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
                scanColumns.get(2).getUniqueId());
        Assert.assertFalse(scanColumns.get(2).isVisible());
    }

    @Test
    public void testBinaryInitialDefaultBuildsLosslessLiteral() throws Exception {
        byte[] defaultBytes = new byte[] {0, 1, 2, (byte) 0xFF};
        Schema schema = new Schema(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(defaultBytes))
                .build());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), true);
        setIcebergTable(node, table);
        Column column = IcebergUtils.parseSchema(schema, true, false).get(0);

        org.apache.doris.nereids.trees.expressions.Expression expression =
                node.defaultExpression(column);
        Assert.assertTrue(
                expression instanceof org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral);
        org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral literal =
                (org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral) expression;
        Assert.assertArrayEquals(defaultBytes, (byte[]) literal.getValue());
    }

    @Test
    public void testLegacyBinaryInitialDefaultBuildsRawByteExpression() throws Exception {
        byte[] defaultBytes = new byte[] {(byte) 0x80, 0, (byte) 0xFF};
        Schema schema = new Schema(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(defaultBytes))
                .build());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), false);
        setIcebergTable(node, table);
        Column column = IcebergUtils.parseSchema(schema, false, false).get(0);

        org.apache.doris.nereids.trees.expressions.Expression expression =
                node.defaultExpression(column);
        Assert.assertTrue(expression
                instanceof org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex);
        Assert.assertEquals("8000FF",
                ((org.apache.doris.nereids.trees.expressions.literal.StringLiteral)
                        expression.child(0)).getStringValue());
    }

    @Test
    public void testInitialDefaultComesFromQuerySchemaInsteadOfColumnDefault() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional("added_int")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(17)
                .build());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        Column columnWithoutDorisDefault = new Column("added_int", Type.INT, true);
        columnWithoutDorisDefault.setUniqueId(7);

        Assert.assertTrue(node.hasInitialDefault(columnWithoutDorisDefault));
        org.apache.doris.nereids.trees.expressions.literal.StringLiteral literal =
                (org.apache.doris.nereids.trees.expressions.literal.StringLiteral)
                        node.defaultExpression(columnWithoutDorisDefault);
        Assert.assertEquals("17", literal.getValue());
    }

    @Test
    public void testStringInitialDefaultIsNotReparsedAsSql() throws Exception {
        String initialDefault = "O'Reilly\\nIceberg";
        Schema schema = new Schema(Types.NestedField.optional("added_string")
                .withId(8)
                .ofType(Types.StringType.get())
                .withInitialDefault(initialDefault)
                .build());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        Column column = IcebergUtils.parseSchema(schema, false, false).get(0);

        org.apache.doris.nereids.trees.expressions.literal.StringLiteral literal =
                (org.apache.doris.nereids.trees.expressions.literal.StringLiteral)
                        node.defaultExpression(column);
        Assert.assertEquals(initialDefault, literal.getValue());
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
    public void testSchemaCarrierSkipsHistoryWithoutEqualityDeletes() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, Mockito.mock(Table.class));

        Assert.assertEquals(schema.columns(),
                node.getSchemaFieldsForScan(schema, Collections.emptySet()));
    }

    @Test
    public void testSchemaCarrierKeepsDroppedEqualityFieldDefault() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.optional("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Types.NestedField renamedEqualityKey = Types.NestedField.optional("k2")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema schemaWithEqualityKey = new Schema(100, ImmutableList.of(id, equalityKey));
        Schema schemaAfterRename = new Schema(1, ImmutableList.of(id, renamedEqualityKey));
        Schema schemaAfterDrop = new Schema(2, ImmutableList.of(id));
        Snapshot snapshotWithEqualityKey = mockSnapshot(1000L, schemaWithEqualityKey, null);
        Snapshot snapshotAfterRename = mockSnapshot(1001L, schemaAfterRename, 1000L);
        Snapshot snapshotAfterDrop = mockSnapshot(1002L, schemaAfterDrop, 1001L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(
                ImmutableList.of(schemaWithEqualityKey, schemaAfterRename, schemaAfterDrop));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                schemaWithEqualityKey.schemaId(), schemaWithEqualityKey,
                schemaAfterRename.schemaId(), schemaAfterRename,
                schemaAfterDrop.schemaId(), schemaAfterDrop));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(snapshotWithEqualityKey);
        Mockito.when(metadata.snapshot(1001L)).thenReturn(snapshotAfterRename);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(snapshotAfterDrop);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(schemaAfterDrop, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(7, fields.get(1).fieldId());
        Assert.assertEquals("k2", fields.get(1).name());
        Assert.assertTrue(fields.get(1).isOptional());
        Assert.assertEquals("7",
                IcebergUtils.getSerializedInitialDefaults(fields, false).get(7));
    }

    @Test
    public void testSchemaCarrierKeepsDroppedNestedEqualityFieldPath() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField existing = Types.NestedField.optional(
                4, "existing", Types.IntegerType.get());
        Types.NestedField equalityKey = Types.NestedField.optional("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Types.NestedField historicalPayload = Types.NestedField.optional(
                3, "payload", Types.StructType.of(existing, equalityKey));
        Types.NestedField currentPayload = Types.NestedField.optional(
                3, "payload", Types.StructType.of(existing));
        Schema historicalSchema = new Schema(1, ImmutableList.of(id, historicalPayload));
        Schema currentSchema = new Schema(2, ImmutableList.of(id, currentPayload));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(ImmutableList.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                historicalSchema.schemaId(), historicalSchema,
                currentSchema.schemaId(), currentSchema));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(historicalSnapshot);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(currentSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields =
                node.getSchemaFieldsForScan(currentSchema, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Types.NestedField payload = fields.get(1);
        Assert.assertEquals(3, payload.fieldId());
        Assert.assertTrue(payload.isOptional());
        Assert.assertEquals(ImmutableList.of(4, 7), payload.type().asStructType().fields().stream()
                .map(Types.NestedField::fieldId)
                .collect(Collectors.toList()));
        Assert.assertEquals("7",
                IcebergUtils.getSerializedInitialDefaults(fields, false).get(7));
    }

    @Test
    public void testSchemaCarrierSkipsUnreferencedUnsupportedHistoricalField() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.optional(
                7, "equality_key", Types.IntegerType.get());
        Types.NestedField unsupported = Types.NestedField.optional(
                9, "dropped_nanos", Types.TimestampNanoType.withoutZone());
        Schema historicalSchema = new Schema(1, ImmutableList.of(id, equalityKey, unsupported));
        Schema currentSchema = new Schema(2, ImmutableList.of(id));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(ImmutableList.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                historicalSchema.schemaId(), historicalSchema,
                currentSchema.schemaId(), currentSchema));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(historicalSnapshot);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(currentSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(
                currentSchema, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(1, fields.get(0).fieldId());
        Assert.assertEquals(7, fields.get(1).fieldId());
        Assert.assertEquals(2, node.getScanColumns(new Schema(fields)).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSchemaCarrierRejectsReferencedUnsupportedHistoricalField() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField unsupported = Types.NestedField.optional(
                9, "equality_nanos", Types.TimestampNanoType.withoutZone());
        Schema historicalSchema = new Schema(1, ImmutableList.of(id, unsupported));
        Schema currentSchema = new Schema(2, ImmutableList.of(id));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(ImmutableList.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                historicalSchema.schemaId(), historicalSchema,
                currentSchema.schemaId(), currentSchema));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(historicalSnapshot);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(currentSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(
                currentSchema, ImmutableSet.of(9));
        node.getScanColumns(new Schema(fields));
    }

    @Test
    public void testApplicableTaskPreflightIgnoresCrossPartitionEqualityDelete() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField unsupported = Types.NestedField.optional(
                9, "dropped_nanos", Types.TimestampNanoType.withoutZone());
        Schema historicalSchema = new Schema(1, ImmutableList.of(id, unsupported));
        Schema currentSchema = new Schema(2, ImmutableList.of(id));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(ImmutableList.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                historicalSchema.schemaId(), historicalSchema,
                currentSchema.schemaId(), currentSchema));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(historicalSnapshot);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(currentSnapshot);

        DeleteFile unrelatedPartitionDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(unrelatedPartitionDelete.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(unrelatedPartitionDelete.recordCount()).thenReturn(1L);
        Mockito.when(unrelatedPartitionDelete.equalityFieldIds()).thenReturn(ImmutableList.of(9));
        FileScanTask unrelatedPartitionTask = Mockito.mock(FileScanTask.class);
        Mockito.when(unrelatedPartitionTask.deletes()).thenReturn(ImmutableList.of(unrelatedPartitionDelete));
        FileScanTask applicablePartitionTask = Mockito.mock(FileScanTask.class);
        Mockito.when(applicablePartitionTask.deletes()).thenReturn(Collections.emptyList());

        Assert.assertEquals(ImmutableSet.of(9),
                IcebergScanNode.collectEqualityDeleteFieldIdsFromTasks(
                        ImmutableList.of(unrelatedPartitionTask)));
        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        setIcebergTable(node, table);
        node.setTableScan(tableScan);
        Mockito.doReturn(CloseableIterable.withNoopClose(ImmutableList.of(applicablePartitionTask)))
                .when(node).planFileScanTaskWithoutReuse(tableScan);
        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        Set<Integer> applicableFieldIds;
        try {
            applicableFieldIds = node.loadEqualityDeleteFieldIds(tableScan);
        } finally {
            ConnectContext.remove();
        }
        Assert.assertEquals(Collections.emptySet(), applicableFieldIds);
        Mockito.verify(node, Mockito.never()).planFileScanTask(tableScan);
        Mockito.verify(node, Mockito.times(1)).planFileScanTaskWithoutReuse(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(
                currentSchema, applicableFieldIds);
        Assert.assertEquals(1, fields.size());
        Assert.assertEquals(1, node.getScanColumns(new Schema(fields)).size());
    }

    @Test
    public void testSchemaCarrierHandlesReusedSchemaId() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.optional("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema reusedSchema = new Schema(100, ImmutableList.of(id));
        Schema schemaWithEqualityKey = new Schema(1, ImmutableList.of(id, equalityKey));
        Snapshot initialSnapshot = mockSnapshot(1000L, reusedSchema, null);
        Snapshot snapshotWithEqualityKey = mockSnapshot(1001L, schemaWithEqualityKey, 1000L);
        Snapshot reactivatedSnapshot = mockSnapshot(1002L, reusedSchema, 1001L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(ImmutableList.of(reusedSchema, schemaWithEqualityKey));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                reusedSchema.schemaId(), reusedSchema,
                schemaWithEqualityKey.schemaId(), schemaWithEqualityKey));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(initialSnapshot);
        Mockito.when(metadata.snapshot(1001L)).thenReturn(snapshotWithEqualityKey);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(reactivatedSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(reusedSchema, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(7, fields.get(1).fieldId());
        Assert.assertEquals("k", fields.get(1).name());
        Assert.assertEquals("7",
                IcebergUtils.getSerializedInitialDefaults(fields, false).get(7));
    }

    @Test
    public void testSchemaCarrierIgnoresFutureRenameForTimeTravel() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.optional("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Types.NestedField futureRenamedKey = Types.NestedField.optional("k2")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema schemaWithEqualityKey = new Schema(100, ImmutableList.of(id, equalityKey));
        Schema timeTravelSchema = new Schema(1, ImmutableList.of(id));
        Schema futureSchema = new Schema(2, ImmutableList.of(id, futureRenamedKey));
        Snapshot initialSnapshot = mockSnapshot(1000L, schemaWithEqualityKey, null);
        Snapshot timeTravelSnapshot = mockSnapshot(1001L, timeTravelSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(
                ImmutableList.of(schemaWithEqualityKey, timeTravelSchema, futureSchema));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                schemaWithEqualityKey.schemaId(), schemaWithEqualityKey,
                timeTravelSchema.schemaId(), timeTravelSchema,
                futureSchema.schemaId(), futureSchema));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(initialSnapshot);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(timeTravelSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(timeTravelSchema, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(7, fields.get(1).fieldId());
        Assert.assertEquals("k", fields.get(1).name());
    }

    @Test
    public void testSchemaCarrierKeepsDefaultWhenParentExpiredBeforeFutureRename() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.optional("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Types.NestedField futureRenamedKey = Types.NestedField.optional("k2")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema schemaWithEqualityKey = new Schema(100, ImmutableList.of(id, equalityKey));
        Schema timeTravelSchema = new Schema(1, ImmutableList.of(id));
        Schema futureSchema = new Schema(2, ImmutableList.of(id, futureRenamedKey));
        Snapshot timeTravelSnapshot = mockSnapshot(1001L, timeTravelSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(
                ImmutableList.of(schemaWithEqualityKey, timeTravelSchema, futureSchema));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                schemaWithEqualityKey.schemaId(), schemaWithEqualityKey,
                timeTravelSchema.schemaId(), timeTravelSchema,
                futureSchema.schemaId(), futureSchema));
        Mockito.when(metadata.snapshot(1000L)).thenReturn(null);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(timeTravelSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(timeTravelSchema, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(7, fields.get(1).fieldId());
        Assert.assertEquals("7",
                IcebergUtils.getSerializedInitialDefaults(fields, false).get(7));
    }

    @Test
    public void testSchemaCarrierHandlesReusedSchemaIdWhenParentExpired() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.optional("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema reusedSchema = new Schema(100, ImmutableList.of(id));
        Schema schemaWithEqualityKey = new Schema(1, ImmutableList.of(id, equalityKey));
        Snapshot reactivatedSnapshot = mockSnapshot(1002L, reusedSchema, 1001L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(ImmutableList.of(reusedSchema, schemaWithEqualityKey));
        Mockito.when(metadata.schemasById()).thenReturn(ImmutableMap.of(
                reusedSchema.schemaId(), reusedSchema,
                schemaWithEqualityKey.schemaId(), schemaWithEqualityKey));
        Mockito.when(metadata.snapshot(1001L)).thenReturn(null);
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        BaseTable table = new BaseTable(operations, "test");
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(reactivatedSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        node.setTableScan(tableScan);

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(reusedSchema, ImmutableSet.of(7));

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(7, fields.get(1).fieldId());
        Assert.assertEquals("k", fields.get(1).name());
        Assert.assertEquals("7",
                IcebergUtils.getSerializedInitialDefaults(fields, false).get(7));
    }

    @Test
    public void testRecursiveInitialDefaultsRequireUpgradedBackends() throws Exception {
        Types.NestedField existing = Types.NestedField.optional(3, "existing", Types.IntegerType.get());
        Types.NestedField nestedDefault = Types.NestedField.optional("added")
                .withId(4)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema schema = new Schema(
                Types.NestedField.optional("scalar")
                        .withId(1)
                        .ofType(Types.IntegerType.get())
                        .withInitialDefault(5)
                        .build(),
                Types.NestedField.optional(2, "payload", Types.StructType.of(existing, nestedDefault)));
        List<Column> columns = IcebergUtils.parseSchema(schema, false, false);
        SlotDescriptor scalarSlot = slotDescriptor(1);
        scalarSlot.setColumn(columns.get(0));
        SlotDescriptor payloadSlot = slotDescriptor(2);
        payloadSlot.setColumn(columns.get(1));

        Assert.assertFalse(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(scalarSlot)));
        Assert.assertTrue(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setType(new StructType(new StructField("existing", Type.INT)));
        payloadSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("2", "3"))));
        Assert.assertFalse(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("2", "4"))));
        Assert.assertTrue(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                metaAccessPath(ImmutableList.of("2", AccessPathInfo.ACCESS_NULL))));
        Assert.assertFalse(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                metaAccessPath(ImmutableList.of("2", "4", AccessPathInfo.ACCESS_NULL))));
        Assert.assertTrue(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isSmoothUpgradeSrc()).thenReturn(false);
        IcebergScanNode.checkCurrentIcebergScanSemanticsBackendCompatibility(
                Collections.singletonList(currentBackend));

        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10002L);
        try {
            IcebergScanNode.checkCurrentIcebergScanSemanticsBackendCompatibility(
                    Collections.singletonList(smoothUpgradeSource));
            Assert.fail("current Iceberg scan semantics must reject a smooth upgrade source backend");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10002 is a smooth upgrade source"));
        }
    }

    @Test
    public void testReusedNestedNameRejectsSmoothUpgradeSourceBackend() throws Exception {
        Types.NestedField unrelated = Types.NestedField.optional(
                8, "id", Types.IntegerType.get());
        Types.NestedField renamedPayload = Types.NestedField.optional(
                3, "renamed_payload",
                Types.ListType.ofOptional(4, Types.IntegerType.get()));
        Types.NestedField replacementPayload = Types.NestedField.optional(
                5, "payload",
                Types.MapType.ofOptional(
                        6, 7, Types.StringType.get(), Types.IntegerType.get()));
        Types.NestedField safe = Types.NestedField.optional(
                9, "safe", Types.IntegerType.get());
        Schema schema = new Schema(unrelated, Types.NestedField.optional(
                1, "root", Types.StructType.of(renamedPayload, replacementPayload, safe)));
        Optional<Map<Integer, List<String>>> mapping = Optional.of(ImmutableMap.of(
                3, ImmutableList.of("payload", "renamed_payload"),
                5, Collections.singletonList("payload")));
        List<Column> columns = IcebergUtils.parseSchema(schema, false, false);
        SlotDescriptor unrelatedSlot = slotDescriptor(8);
        unrelatedSlot.setColumn(columns.get(0));
        SlotDescriptor rootSlot = slotDescriptor(1);
        rootSlot.setColumn(columns.get(1));

        Assert.assertTrue(IcebergScanNode.hasCurrentNameAliasCollision(schema, mapping));
        Assert.assertFalse(IcebergScanNode.hasCurrentNameAliasCollision(
                schema, Optional.of(ImmutableMap.of(
                        3, ImmutableList.of("legacy_payload", "renamed_payload"),
                        5, Collections.singletonList("payload")))));

        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isSmoothUpgradeSrc()).thenReturn(false);
        IcebergScanNode.checkNameMappingBackendCompatibility(
                schema, Collections.singletonList(rootSlot), Collections.emptySet(),
                mapping, Collections.singletonList(currentBackend));

        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10004L);
        IcebergScanNode.checkNameMappingBackendCompatibility(
                schema, Collections.singletonList(unrelatedSlot), Collections.emptySet(),
                mapping, Collections.singletonList(smoothUpgradeSource));

        rootSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("1", "9"))));
        IcebergScanNode.checkNameMappingBackendCompatibility(
                schema, Collections.singletonList(rootSlot), Collections.emptySet(),
                mapping, Collections.singletonList(smoothUpgradeSource));

        rootSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("1", "3"))));
        UserException exception = Assert.assertThrows(UserException.class,
                () -> IcebergScanNode.checkNameMappingBackendCompatibility(
                        schema, Collections.singletonList(rootSlot), Collections.emptySet(),
                        mapping, Collections.singletonList(smoothUpgradeSource)));
        Assert.assertTrue(exception.getMessage().contains(
                "backend 10004 is a smooth upgrade source"));

        UserException equalityException = Assert.assertThrows(UserException.class,
                () -> IcebergScanNode.checkNameMappingBackendCompatibility(
                        schema, Collections.singletonList(unrelatedSlot), ImmutableSet.of(7),
                        mapping, Collections.singletonList(smoothUpgradeSource)));
        Assert.assertTrue(equalityException.getMessage().contains(
                "backend 10004 is a smooth upgrade source"));
    }

    @Test
    public void testCurrentBackendsSkipNameMappingCollisionScan() throws Exception {
        Schema schema = Mockito.mock(Schema.class);
        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isSmoothUpgradeSrc()).thenReturn(false);

        IcebergScanNode.checkNameMappingBackendCompatibility(
                schema, Collections.emptyList(), Collections.emptySet(),
                Optional.empty(), Collections.singletonList(currentBackend));

        Mockito.verify(schema, Mockito.never()).asStruct();
    }

    @Test
    public void testRecursiveInitialDefaultsFollowCollectionAccessPaths() {
        Types.NestedField arrayExisting =
                Types.NestedField.optional(12, "existing", Types.IntegerType.get());
        Types.NestedField arrayDefault = Types.NestedField.optional("added")
                .withId(13)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(13)
                .build();
        Types.NestedField mapExisting =
                Types.NestedField.optional(23, "existing", Types.IntegerType.get());
        Types.NestedField mapDefault = Types.NestedField.optional("added")
                .withId(24)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(24)
                .build();
        Schema schema = new Schema(
                Types.NestedField.optional(10, "items", Types.ListType.ofOptional(
                        11, Types.StructType.of(arrayExisting, arrayDefault))),
                Types.NestedField.optional(20, "entries", Types.MapType.ofOptional(
                        21, 22, Types.StringType.get(),
                        Types.StructType.of(mapExisting, mapDefault))));
        List<Column> columns = IcebergUtils.parseSchema(schema, false, false);
        SlotDescriptor arraySlot = slotDescriptor(10);
        arraySlot.setColumn(columns.get(0));
        SlotDescriptor mapSlot = slotDescriptor(20);
        mapSlot.setColumn(columns.get(1));

        assertRequiresRecursiveInitialDefault(schema, arraySlot, false,
                "10", AccessPathInfo.ACCESS_ALL, "12");
        assertRequiresRecursiveInitialDefault(schema, arraySlot, true,
                "10", AccessPathInfo.ACCESS_ALL, "13");
        assertRequiresRecursiveInitialDefault(schema, arraySlot, false,
                "10", AccessPathInfo.ACCESS_OFFSET);
        assertRequiresRecursiveInitialDefault(schema, arraySlot, false,
                "10", AccessPathInfo.ACCESS_NULL);

        // '*' is emitted for element_at(map, key). The remaining path belongs to the value,
        // rather than the primitive key that is also read for lookup.
        assertRequiresRecursiveInitialDefault(schema, mapSlot, false,
                "20", AccessPathInfo.ACCESS_ALL, "23");
        assertRequiresRecursiveInitialDefault(schema, mapSlot, true,
                "20", AccessPathInfo.ACCESS_ALL, "24");
        assertRequiresRecursiveInitialDefault(schema, mapSlot, false,
                "20", AccessPathInfo.ACCESS_MAP_KEYS);
        assertRequiresRecursiveInitialDefault(schema, mapSlot, false,
                "20", AccessPathInfo.ACCESS_MAP_VALUES, "23");
        assertRequiresRecursiveInitialDefault(schema, mapSlot, true,
                "20", AccessPathInfo.ACCESS_MAP_VALUES, "24");
        assertRequiresRecursiveInitialDefault(schema, mapSlot, false,
                "20", AccessPathInfo.ACCESS_OFFSET);
        assertRequiresRecursiveInitialDefault(schema, mapSlot, false,
                "20", AccessPathInfo.ACCESS_NULL);
    }

    @Test
    public void testPotentiallyMissingRequiredFieldsFollowProjection() {
        Types.NestedField existing = Types.NestedField.optional(
                3, "existing", Types.IntegerType.get());
        Types.NestedField requiredAdded = Types.NestedField.required(
                4, "required_added", Types.IntegerType.get());
        Types.NestedField requiredNested = Types.NestedField.required(
                5, "required_nested", Types.IntegerType.get());
        Schema historicalSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "payload", Types.StructType.of(existing)));
        Schema scanSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                requiredAdded,
                Types.NestedField.optional(
                        2, "payload", Types.StructType.of(existing, requiredNested)));
        List<Column> columns = IcebergUtils.parseSchema(scanSchema, false, false);
        SlotDescriptor idSlot = slotDescriptor(1);
        idSlot.setColumn(columns.get(0));
        SlotDescriptor requiredSlot = slotDescriptor(4);
        requiredSlot.setColumn(columns.get(1));
        SlotDescriptor payloadSlot = slotDescriptor(2);
        payloadSlot.setColumn(columns.get(2));

        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(idSlot), ImmutableList.of(historicalSchema)));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(requiredSlot), ImmutableList.of(historicalSchema)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("2", "3"))));
        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(payloadSlot), ImmutableList.of(historicalSchema)));
        payloadSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("2", "5"))));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(payloadSlot), ImmutableList.of(historicalSchema)));
    }

    @Test
    public void testRequiredFieldFenceExcludesLaterAndOffLineageSchemas() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField required = Types.NestedField.required(
                2, "required_value", Types.IntegerType.get());
        Schema ancestorSchema = new Schema(40, ImmutableList.of(id, required));
        Schema targetSchema = new Schema(41, ImmutableList.of(id, required));
        Schema laterDropSchema = new Schema(42, ImmutableList.of(id));
        Schema offLineageSchema = new Schema(43, ImmutableList.of(id));

        Snapshot ancestorSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(ancestorSnapshot.snapshotId()).thenReturn(100L);
        Mockito.when(ancestorSnapshot.schemaId()).thenReturn(ancestorSchema.schemaId());
        Mockito.when(ancestorSnapshot.parentId()).thenReturn(null);
        Mockito.when(ancestorSnapshot.summary()).thenReturn(ImmutableMap.of());
        Snapshot targetSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(targetSnapshot.snapshotId()).thenReturn(101L);
        Mockito.when(targetSnapshot.schemaId()).thenReturn(targetSchema.schemaId());
        Mockito.when(targetSnapshot.parentId()).thenReturn(100L);
        Mockito.when(targetSnapshot.summary()).thenReturn(ImmutableMap.of());
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(targetSnapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                ancestorSchema.schemaId(), ancestorSchema,
                targetSchema.schemaId(), targetSchema,
                laterDropSchema.schemaId(), laterDropSchema,
                offLineageSchema.schemaId(), offLineageSchema));
        Mockito.when(table.snapshot(100L)).thenReturn(ancestorSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        List<Schema> targetHistory = node.getRequiredFieldSchemaHistory(targetSchema).get();
        SlotDescriptor requiredSlot = slotDescriptor(2);
        requiredSlot.setColumn(IcebergUtils.parseSchema(targetSchema, false, false).get(1));

        Assert.assertEquals(
                ImmutableList.of(targetSchema.schemaId(), ancestorSchema.schemaId()),
                targetHistory.stream().map(Schema::schemaId).collect(Collectors.toList()));
        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                targetSchema, Collections.singletonList(requiredSlot), targetHistory));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                targetSchema, Collections.singletonList(requiredSlot),
                ImmutableList.of(ancestorSchema, targetSchema, laterDropSchema, offLineageSchema)));
    }

    @Test
    public void testRequiredFieldFenceIncludesSchemaOnlyTarget() throws Exception {
        Schema snapshotSchema = new Schema(44,
                Types.NestedField.required(1, "id", Types.LongType.get()));
        Schema schemaOnlyTarget = new Schema(45,
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "required_value", Types.IntegerType.get()));
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(102L);
        Mockito.when(snapshot.schemaId()).thenReturn(snapshotSchema.schemaId());
        Mockito.when(snapshot.parentId()).thenReturn(null);
        Mockito.when(snapshot.summary()).thenReturn(ImmutableMap.of());
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(snapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                snapshotSchema.schemaId(), snapshotSchema,
                schemaOnlyTarget.schemaId(), schemaOnlyTarget));

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        List<Schema> targetHistory = node.getRequiredFieldSchemaHistory(schemaOnlyTarget).get();
        SlotDescriptor requiredSlot = slotDescriptor(2);
        requiredSlot.setColumn(IcebergUtils.parseSchema(schemaOnlyTarget, false, false).get(1));

        Assert.assertEquals(
                ImmutableList.of(schemaOnlyTarget.schemaId(), snapshotSchema.schemaId()),
                targetHistory.stream().map(Schema::schemaId).collect(Collectors.toList()));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                schemaOnlyTarget, Collections.singletonList(requiredSlot), targetHistory));
    }

    @Test
    public void testRequiredFieldFenceRejectsTruncatedSnapshotLineage() throws Exception {
        Schema targetSchema = new Schema(46,
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "required_value", Types.IntegerType.get()));
        Snapshot targetSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(targetSnapshot.snapshotId()).thenReturn(103L);
        Mockito.when(targetSnapshot.schemaId()).thenReturn(targetSchema.schemaId());
        Mockito.when(targetSnapshot.parentId()).thenReturn(101L);
        Mockito.when(targetSnapshot.summary()).thenReturn(ImmutableMap.of());
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(targetSnapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                targetSchema.schemaId(), targetSchema));
        Mockito.when(table.snapshot(101L)).thenReturn(null);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        Optional<List<Schema>> targetHistory =
                node.getRequiredFieldSchemaHistory(targetSchema);
        SlotDescriptor requiredSlot = slotDescriptor(2);
        requiredSlot.setColumn(IcebergUtils.parseSchema(
                targetSchema, false, false).get(1));

        Assert.assertFalse(targetHistory.isPresent());
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                targetSchema, Collections.singletonList(requiredSlot), targetHistory));
    }

    @Test
    public void testRequiredFieldFenceIncludesCherryPickedSourceAncestry() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField required = Types.NestedField.required(
                2, "required_value", Types.IntegerType.get());
        Schema sourceAncestorSchema = new Schema(47, ImmutableList.of(id));
        Schema sourceSchema = new Schema(48, ImmutableList.of(id, required));
        Schema targetSchema = new Schema(49, ImmutableList.of(id, required));

        Snapshot sourceAncestorSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(sourceAncestorSnapshot.snapshotId()).thenReturn(200L);
        Mockito.when(sourceAncestorSnapshot.schemaId()).thenReturn(sourceAncestorSchema.schemaId());
        Mockito.when(sourceAncestorSnapshot.parentId()).thenReturn(null);
        Mockito.when(sourceAncestorSnapshot.summary()).thenReturn(ImmutableMap.of());
        Snapshot sourceSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(sourceSnapshot.snapshotId()).thenReturn(201L);
        Mockito.when(sourceSnapshot.schemaId()).thenReturn(sourceSchema.schemaId());
        Mockito.when(sourceSnapshot.parentId()).thenReturn(200L);
        Mockito.when(sourceSnapshot.summary()).thenReturn(ImmutableMap.of());
        Snapshot targetSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(targetSnapshot.snapshotId()).thenReturn(202L);
        Mockito.when(targetSnapshot.schemaId()).thenReturn(targetSchema.schemaId());
        Mockito.when(targetSnapshot.parentId()).thenReturn(null);
        Mockito.when(targetSnapshot.summary()).thenReturn(ImmutableMap.of(
                SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP,
                "201"));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(targetSnapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(ImmutableMap.of(
                sourceAncestorSchema.schemaId(), sourceAncestorSchema,
                sourceSchema.schemaId(), sourceSchema,
                targetSchema.schemaId(), targetSchema));
        Mockito.when(table.snapshot(200L)).thenReturn(sourceAncestorSnapshot);
        Mockito.when(table.snapshot(201L)).thenReturn(sourceSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        List<Schema> targetHistory = node.getRequiredFieldSchemaHistory(targetSchema).get();
        SlotDescriptor requiredSlot = slotDescriptor(2);
        requiredSlot.setColumn(IcebergUtils.parseSchema(targetSchema, false, false).get(1));

        Assert.assertEquals(
                ImmutableList.of(targetSchema.schemaId(), sourceSchema.schemaId(),
                        sourceAncestorSchema.schemaId()),
                targetHistory.stream().map(Schema::schemaId).collect(Collectors.toList()));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                targetSchema, Collections.singletonList(requiredSlot), targetHistory));
    }

    @Test
    public void testRequiredCollectionWrappersDoNotTriggerUpgradeGate() {
        Types.NestedField existing = Types.NestedField.optional(
                32, "existing", Types.IntegerType.get());
        Types.NestedField requiredNested = Types.NestedField.required(
                33, "required_nested", Types.IntegerType.get());
        Schema historicalSchema = new Schema(
                Types.NestedField.optional(10, "items", Types.ListType.ofOptional(
                        90, Types.IntegerType.get())),
                Types.NestedField.optional(20, "entries", Types.MapType.ofOptional(
                        91, 92, Types.StringType.get(), Types.IntegerType.get())),
                Types.NestedField.optional(30, "struct_items", Types.ListType.ofOptional(
                        31, Types.StructType.of(existing))));
        Schema scanSchema = new Schema(
                Types.NestedField.optional(10, "items", Types.ListType.ofRequired(
                        11, Types.IntegerType.get())),
                Types.NestedField.optional(20, "entries", Types.MapType.ofRequired(
                        21, 22, Types.StringType.get(), Types.IntegerType.get())),
                Types.NestedField.optional(30, "struct_items", Types.ListType.ofOptional(
                        31, Types.StructType.of(existing, requiredNested))));
        List<Column> columns = IcebergUtils.parseSchema(scanSchema, false, false);
        SlotDescriptor itemsSlot = slotDescriptor(10);
        itemsSlot.setColumn(columns.get(0));
        SlotDescriptor entriesSlot = slotDescriptor(20);
        entriesSlot.setColumn(columns.get(1));
        SlotDescriptor structItemsSlot = slotDescriptor(30);
        structItemsSlot.setColumn(columns.get(2));

        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, ImmutableList.of(itemsSlot, entriesSlot), ImmutableList.of(historicalSchema)));
        structItemsSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("30", AccessPathInfo.ACCESS_ALL, "32"))));
        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(structItemsSlot),
                ImmutableList.of(historicalSchema)));
        structItemsSlot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.of("30", AccessPathInfo.ACCESS_ALL, "33"))));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(structItemsSlot),
                ImmutableList.of(historicalSchema)));
    }

    private static void assertRequiresRecursiveInitialDefault(
            Schema schema, SlotDescriptor slot, boolean expected, String... path) {
        slot.setAllAccessPaths(Collections.singletonList(
                dataAccessPath(ImmutableList.copyOf(path))));
        Assert.assertEquals(expected,
                IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                        schema, Collections.singletonList(slot)));
    }

    private static TColumnAccessPath dataAccessPath(List<String> path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
        accessPath.setDataAccessPath(new TDataAccessPath(path));
        return accessPath;
    }

    private static TColumnAccessPath metaAccessPath(List<String> path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.META);
        accessPath.setMetaAccessPath(new TMetaAccessPath(path));
        return accessPath;
    }

    @Test
    public void testEqualityDeleteFieldIdPreflightDistinguishesDeleteContent() throws Exception {
        DeleteFile positionDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(positionDelete.content()).thenReturn(FileContent.POSITION_DELETES);
        DeleteFile emptyEqualityDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(emptyEqualityDelete.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(emptyEqualityDelete.recordCount()).thenReturn(0L);
        Mockito.when(emptyEqualityDelete.equalityFieldIds()).thenReturn(ImmutableList.of(7));

        Assert.assertEquals(Collections.emptySet(),
                IcebergScanNode.collectEqualityDeleteFieldIds(ImmutableList.of(positionDelete)));
        Assert.assertEquals(Collections.emptySet(),
                IcebergScanNode.collectEqualityDeleteFieldIds(ImmutableList.of(emptyEqualityDelete)));

        FileScanTask applicableTask = Mockito.mock(FileScanTask.class);
        Mockito.when(applicableTask.deletes()).thenReturn(ImmutableList.of(emptyEqualityDelete));
        FileScanTask taskWithoutEqualityDeletes = Mockito.mock(FileScanTask.class);
        Mockito.when(taskWithoutEqualityDeletes.deletes()).thenReturn(ImmutableList.of(positionDelete));
        Assert.assertEquals(Collections.emptySet(),
                IcebergScanNode.collectEqualityDeleteFieldIdsFromTasks(
                        ImmutableList.of(applicableTask, taskWithoutEqualityDeletes)));
        Assert.assertEquals(ImmutableList.of(positionDelete),
                IcebergScanNode.getApplicableDeleteFiles(
                        ImmutableList.of(positionDelete, emptyEqualityDelete)));

        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10003L);
        try {
            IcebergScanNode.checkCurrentIcebergScanSemanticsBackendCompatibility(
                    Collections.singletonList(smoothUpgradeSource));
            Assert.fail("equality-delete identity semantics must reject a smooth upgrade source backend");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10003 is a smooth upgrade source"));
        }
    }

    @Test
    public void testMixedVersionBatchUsesExactTaskPlanningWhenEqualityDeletesArePossible() {
        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Backend currentBackend = Mockito.mock(Backend.class);

        Assert.assertTrue(IcebergScanNode.shouldPlanExactTasksForCompatibility(
                true, true, ImmutableList.of(currentBackend, smoothUpgradeSource)));
        Assert.assertFalse(IcebergScanNode.shouldPlanExactTasksForCompatibility(
                true, false, ImmutableList.of(currentBackend, smoothUpgradeSource)));
        Assert.assertFalse(IcebergScanNode.shouldPlanExactTasksForCompatibility(
                true, true, ImmutableList.of(currentBackend)));
        Assert.assertFalse(IcebergScanNode.shouldPlanExactTasksForCompatibility(
                false, true, ImmutableList.of(currentBackend, smoothUpgradeSource)));
    }

    @Test
    public void testForcedFileScannerV1RejectsSmoothUpgradeSourceBackend() throws Exception {
        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10005L);
        Backend currentBackend = Mockito.mock(Backend.class);

        IcebergScanNode.checkFileScannerV1BackendCompatibility(
                true, ImmutableList.of(currentBackend, smoothUpgradeSource));
        IcebergScanNode.checkFileScannerV1BackendCompatibility(
                false, Collections.singletonList(currentBackend));

        UserException exception = Assert.assertThrows(UserException.class,
                () -> IcebergScanNode.checkFileScannerV1BackendCompatibility(
                        false, ImmutableList.of(currentBackend, smoothUpgradeSource)));
        Assert.assertTrue(exception.getMessage().contains(
                "backend 10005 is a smooth upgrade source"));
    }

    @Test
    public void testEqualityDeleteFieldIdPreflightRunsInsideAuthenticator()
            throws Exception {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(snapshot);
        TestIcebergScanNode node = Mockito.spy(
                new TestIcebergScanNode(new SessionVariable()));
        node.setTableScan(tableScan);
        AtomicBoolean authenticated = new AtomicBoolean(false);
        AtomicBoolean loaderObservedAuthentication = new AtomicBoolean(false);
        setPreExecutionAuthenticator(node, new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                authenticated.set(true);
                try {
                    return task.call();
                } finally {
                    authenticated.set(false);
                }
            }
        });
        Mockito.doAnswer(invocation -> {
            loaderObservedAuthentication.set(authenticated.get());
            return ImmutableSet.of(7);
        }).when(node).loadEqualityDeleteFieldIds(tableScan);

        Assert.assertEquals(ImmutableSet.of(7), node.getEqualityDeleteFieldIdsForScan());
        Assert.assertTrue(loaderObservedAuthentication.get());
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

    private static Snapshot mockSnapshot(long snapshotId, Schema schema, Long parentId) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Mockito.when(snapshot.schemaId()).thenReturn(schema.schemaId());
        Mockito.when(snapshot.parentId()).thenReturn(parentId);
        return snapshot;
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

        Field sourceField = IcebergScanNode.class.getDeclaredField("source");
        sourceField.setAccessible(true);
        if (sourceField.get(node) == null) {
            IcebergSource source = Mockito.mock(IcebergSource.class);
            Mockito.when(source.getTargetTable()).thenReturn(Mockito.mock(TableIf.class));
            sourceField.set(node, source);
        }
    }

    private static void setIcebergSource(IcebergScanNode node, IcebergSource source) throws Exception {
        Field sourceField = IcebergScanNode.class.getDeclaredField("source");
        sourceField.setAccessible(true);
        sourceField.set(node, source);
    }

    private static void setPreExecutionAuthenticator(
            IcebergScanNode node, ExecutionAuthenticator authenticator) throws Exception {
        Field authenticatorField = IcebergScanNode.class.getDeclaredField(
                "preExecutionAuthenticator");
        authenticatorField.setAccessible(true);
        authenticatorField.set(node, authenticator);
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
