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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergUtils;
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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ScanTaskUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class IcebergScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @SuppressWarnings("unchecked")
    private static Optional<Map<Integer, List<String>>> extractNameMapping(
            IcebergScanNode node) throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod("extractNameMapping");
        method.setAccessible(true);
        return (Optional<Map<Integer, List<String>>>) method.invoke(node);
    }

    private static class TestIcebergScanNode extends IcebergScanNode {
        private final boolean enableMappingVarbinary;
        private TableScan tableScan;

        TestIcebergScanNode(SessionVariable sv) {
            this(sv, false);
        }

        TestIcebergScanNode(SessionVariable sv, boolean enableMappingVarbinary) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), sv, ScanContext.EMPTY);
            this.enableMappingVarbinary = enableMappingVarbinary;
        }

        void setTableScan(TableScan tableScan) {
            this.tableScan = tableScan;
        }

        @Override
        public TableScan createTableScan() {
            return tableScan;
        }

        @Override
        public boolean isBatchMode() {
            return false;
        }

        @Override
        protected boolean getEnableMappingVarbinary() {
            return enableMappingVarbinary;
        }

        @Override
        public List<String> getPathPartitionKeys() {
            return Collections.emptyList();
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

        void addSlot(int slotId, Column column) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc);
            slot.setColumn(column);
            desc.addSlot(slot);
        }
    }

    @Test
    public void testSystemTableProjectionMatchesFileSlotOrder() throws Exception {
        Schema systemTableSchema = new Schema(
                Types.NestedField.required(1, "file_path", Types.StringType.get()),
                Types.NestedField.required(2, "record_count", Types.LongType.get()),
                Types.NestedField.optional(3, "readable_metrics", Types.StructType.of(
                        Types.NestedField.optional(4, "id", Types.StructType.of(
                                Types.NestedField.optional(5, "lower_bound", Types.IntegerType.get()))))));
        Table systemTable = Mockito.mock(Table.class);
        Mockito.when(systemTable.schema()).thenReturn(systemTableSchema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, systemTable);
        node.addSlot(1, new Column("RECORD_COUNT", Type.BIGINT));
        node.addSlot(2, new Column(Column.GLOBAL_ROWID_COL + "system_table", Type.BIGINT));
        node.addSlot(3, new Column("FILE_PATH", Type.STRING));

        List<Expression> filters = Collections.singletonList(Expressions.greaterThan("record_count", 0L));
        Schema projectedSchema = node.getSystemTableProjectedSchema(filters, false);

        Assert.assertEquals(2, projectedSchema.columns().size());
        Assert.assertEquals("record_count", projectedSchema.columns().get(0).name());
        Assert.assertEquals("file_path", projectedSchema.columns().get(1).name());
        Assert.assertNull(projectedSchema.findField("readable_metrics"));
    }

    @Test
    public void testSystemTableProjectionRejectsUnmaterializedFilterColumn() throws Exception {
        Schema systemTableSchema = new Schema(
                Types.NestedField.required(1, "file_path", Types.StringType.get()),
                Types.NestedField.required(2, "record_count", Types.LongType.get()));
        Table systemTable = Mockito.mock(Table.class);
        Mockito.when(systemTable.schema()).thenReturn(systemTableSchema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, systemTable);
        node.addSlot(1, new Column("record_count", Type.BIGINT));

        try {
            node.getSystemTableProjectedSchema(
                    Collections.singletonList(Expressions.equal("file_path", "data.parquet")), true);
            Assert.fail("Filter columns must be materialized by the planner");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("filter column file_path is not materialized"));
        }
    }

    @Test
    public void testEmitsCurrentIcebergScanSemanticsCapability() {
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());

        Assert.assertEquals(IcebergScanNode.ICEBERG_SCAN_SEMANTICS_VERSION,
                node.enableAndGetIcebergScanSemanticsVersion());
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

    private static void setIcebergTable(IcebergScanNode node, Table table) throws Exception {
        Field icebergTableField = IcebergScanNode.class.getDeclaredField("icebergTable");
        icebergTableField.setAccessible(true);
        icebergTableField.set(node, table);
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
}
