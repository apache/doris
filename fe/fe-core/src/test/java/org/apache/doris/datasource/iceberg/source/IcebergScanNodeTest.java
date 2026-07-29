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

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.rewrite.AccessPathInfo;
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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
        private IcebergTableQueryInfo queryInfo;

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

        void setQueryInfo(IcebergTableQueryInfo queryInfo) {
            this.queryInfo = queryInfo;
        }

        @Override
        public TableScan createTableScan() {
            return tableScan;
        }

        @Override
        public IcebergTableQueryInfo getSpecifiedSnapshot() {
            return queryInfo;
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

        void addSlot(int slotId, Column column) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc.getId());
            slot.setColumn(column);
            desc.addSlot(slot);
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

    private static class PlanFilesCountingIcebergScanNode extends TestIcebergScanNode {
        private final boolean batchMode;
        private final Set<Integer> manifestFieldIds;

        PlanFilesCountingIcebergScanNode(
                SessionVariable sv, boolean batchMode, Set<Integer> manifestFieldIds) {
            super(sv);
            this.batchMode = batchMode;
            this.manifestFieldIds = manifestFieldIds;
        }

        @Override
        public boolean isBatchMode() {
            return batchMode;
        }

        @Override
        CloseableIterable<FileScanTask> planFileScanTaskWithoutReuse(TableScan scan) {
            return scan.planFiles();
        }

        @Override
        Set<Integer> loadEqualityDeleteFieldIdsFromDeleteManifests(TableScan scan) {
            return manifestFieldIds;
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
        Mockito.when(equalityDelete.equalityFieldIds()).thenReturn(List.of(7));
        FileScanTask fileScanTask = Mockito.mock(FileScanTask.class);
        Mockito.when(fileScanTask.deletes()).thenReturn(List.of(equalityDelete));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(List.of(fileScanTask)));
        PlanFilesCountingIcebergScanNode node = new PlanFilesCountingIcebergScanNode(
                new SessionVariable(), false, Collections.emptySet());

        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            Assert.assertEquals(Set.of(7), node.loadEqualityDeleteFieldIds(tableScan));
            try (CloseableIterable<FileScanTask> plannedTasks =
                         node.planFileScanTask(tableScan)) {
                List<FileScanTask> actualTasks = new ArrayList<>();
                plannedTasks.forEach(actualTasks::add);
                Assert.assertEquals(List.of(fileScanTask), actualTasks);
            }
        } finally {
            ConnectContext.remove();
        }

        Mockito.verify(tableScan, Mockito.times(1)).planFiles();
    }

    @Test
    public void testBatchScanDefersFilePlanningUntilSplitDispatch() throws Exception {
        FileScanTask fileScanTask = Mockito.mock(FileScanTask.class);
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(List.of(fileScanTask)));
        PlanFilesCountingIcebergScanNode node = new PlanFilesCountingIcebergScanNode(
                new SessionVariable(), true, Set.of(7));

        ConnectContext context = new ConnectContext();
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            Assert.assertEquals(Set.of(7), node.loadEqualityDeleteFieldIds(tableScan));
            Mockito.verify(tableScan, Mockito.never()).planFiles();

            try (CloseableIterable<FileScanTask> plannedTasks =
                         node.planFileScanTask(tableScan)) {
                List<FileScanTask> actualTasks = new ArrayList<>();
                plannedTasks.forEach(actualTasks::add);
                Assert.assertEquals(List.of(fileScanTask), actualTasks);
            }
        } finally {
            ConnectContext.remove();
        }

        Mockito.verify(tableScan, Mockito.times(1)).planFiles();
    }

    @Test
    public void testBatchScanSkipsDeleteManifestsWhenSnapshotHasNoEqualityDeletes() {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.summary()).thenReturn(
                Map.of(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0"));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(snapshot);
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());

        Assert.assertEquals(
                Collections.emptySet(),
                node.loadEqualityDeleteFieldIdsFromDeleteManifests(tableScan));
        Mockito.verify(snapshot, Mockito.never()).deleteManifests(Mockito.any());
        Mockito.verify(tableScan, Mockito.never()).planFiles();
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
        Schema pinnedSchema = new Schema(11, List.of(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build()));
        Schema refreshedSchema = new Schema(12,
                List.of(Types.NestedField.optional(8, "replacement", Types.IntegerType.get())));
        Table refreshedTable = Mockito.mock(Table.class);
        Mockito.when(refreshedTable.schema()).thenReturn(refreshedSchema);
        Mockito.when(refreshedTable.schemas()).thenReturn(Map.of(
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
        Schema dataSnapshotSchema = new Schema(11, List.of(Types.NestedField.optional("string_default")
                .withId(7)
                .ofType(Types.StringType.get())
                .withInitialDefault("not-base64")
                .build()));
        Schema branchSchema = new Schema(12, List.of(Types.NestedField.optional("binary_default")
                .withId(7)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build()));
        Snapshot dataSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(dataSnapshot.schemaId()).thenReturn(dataSnapshotSchema.schemaId());
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Map.of(
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
        Schema schemaWithEqualityKey = new Schema(100, List.of(id, equalityKey));
        Schema schemaAfterRename = new Schema(1, List.of(id, renamedEqualityKey));
        Schema schemaAfterDrop = new Schema(2, List.of(id));
        Snapshot snapshotWithEqualityKey = mockSnapshot(1000L, schemaWithEqualityKey, null);
        Snapshot snapshotAfterRename = mockSnapshot(1001L, schemaAfterRename, 1000L);
        Snapshot snapshotAfterDrop = mockSnapshot(1002L, schemaAfterDrop, 1001L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(
                List.of(schemaWithEqualityKey, schemaAfterRename, schemaAfterDrop));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(schemaAfterDrop, Set.of(7));

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
        Schema historicalSchema = new Schema(1, List.of(id, historicalPayload));
        Schema currentSchema = new Schema(2, List.of(id, currentPayload));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(List.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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
                node.getSchemaFieldsForScan(currentSchema, Set.of(7));

        Assert.assertEquals(2, fields.size());
        Types.NestedField payload = fields.get(1);
        Assert.assertEquals(3, payload.fieldId());
        Assert.assertTrue(payload.isOptional());
        Assert.assertEquals(List.of(4, 7), payload.type().asStructType().fields().stream()
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
        Schema historicalSchema = new Schema(1, List.of(id, equalityKey, unsupported));
        Schema currentSchema = new Schema(2, List.of(id));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(List.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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
                currentSchema, Set.of(7));

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
        Schema historicalSchema = new Schema(1, List.of(id, unsupported));
        Schema currentSchema = new Schema(2, List.of(id));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(List.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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
                currentSchema, Set.of(9));
        node.getScanColumns(new Schema(fields));
    }

    @Test
    public void testApplicableTaskPreflightIgnoresCrossPartitionEqualityDelete() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField unsupported = Types.NestedField.optional(
                9, "dropped_nanos", Types.TimestampNanoType.withoutZone());
        Schema historicalSchema = new Schema(1, List.of(id, unsupported));
        Schema currentSchema = new Schema(2, List.of(id));
        Snapshot historicalSnapshot = mockSnapshot(1000L, historicalSchema, null);
        Snapshot currentSnapshot = mockSnapshot(1001L, currentSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(List.of(historicalSchema, currentSchema));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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
        Mockito.when(unrelatedPartitionDelete.equalityFieldIds()).thenReturn(List.of(9));
        FileScanTask unrelatedPartitionTask = Mockito.mock(FileScanTask.class);
        Mockito.when(unrelatedPartitionTask.deletes()).thenReturn(List.of(unrelatedPartitionDelete));
        FileScanTask applicablePartitionTask = Mockito.mock(FileScanTask.class);
        Mockito.when(applicablePartitionTask.deletes()).thenReturn(Collections.emptyList());

        Assert.assertEquals(Set.of(9),
                IcebergScanNode.collectEqualityDeleteFieldIdsFromTasks(
                        List.of(unrelatedPartitionTask)));
        TestIcebergScanNode node = Mockito.spy(new TestIcebergScanNode(new SessionVariable()));
        setIcebergTable(node, table);
        node.setTableScan(tableScan);
        Mockito.doReturn(CloseableIterable.withNoopClose(List.of(applicablePartitionTask)))
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
        Schema reusedSchema = new Schema(100, List.of(id));
        Schema schemaWithEqualityKey = new Schema(1, List.of(id, equalityKey));
        Snapshot initialSnapshot = mockSnapshot(1000L, reusedSchema, null);
        Snapshot snapshotWithEqualityKey = mockSnapshot(1001L, schemaWithEqualityKey, 1000L);
        Snapshot reactivatedSnapshot = mockSnapshot(1002L, reusedSchema, 1001L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(List.of(reusedSchema, schemaWithEqualityKey));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(reusedSchema, Set.of(7));

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
        Schema schemaWithEqualityKey = new Schema(100, List.of(id, equalityKey));
        Schema timeTravelSchema = new Schema(1, List.of(id));
        Schema futureSchema = new Schema(2, List.of(id, futureRenamedKey));
        Snapshot initialSnapshot = mockSnapshot(1000L, schemaWithEqualityKey, null);
        Snapshot timeTravelSnapshot = mockSnapshot(1001L, timeTravelSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(
                List.of(schemaWithEqualityKey, timeTravelSchema, futureSchema));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(timeTravelSchema, Set.of(7));

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
        Schema schemaWithEqualityKey = new Schema(100, List.of(id, equalityKey));
        Schema timeTravelSchema = new Schema(1, List.of(id));
        Schema futureSchema = new Schema(2, List.of(id, futureRenamedKey));
        Snapshot timeTravelSnapshot = mockSnapshot(1001L, timeTravelSchema, 1000L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(
                List.of(schemaWithEqualityKey, timeTravelSchema, futureSchema));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(timeTravelSchema, Set.of(7));

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
        Schema reusedSchema = new Schema(100, List.of(id));
        Schema schemaWithEqualityKey = new Schema(1, List.of(id, equalityKey));
        Snapshot reactivatedSnapshot = mockSnapshot(1002L, reusedSchema, 1001L);
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(List.of(reusedSchema, schemaWithEqualityKey));
        Mockito.when(metadata.schemasById()).thenReturn(Map.of(
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

        List<Types.NestedField> fields = node.getSchemaFieldsForScan(reusedSchema, Set.of(7));

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
        SlotDescriptor scalarSlot = new SlotDescriptor(new SlotId(1), new TupleId(0));
        scalarSlot.setColumn(columns.get(0));
        SlotDescriptor payloadSlot = new SlotDescriptor(new SlotId(2), new TupleId(0));
        payloadSlot.setColumn(columns.get(1));

        Assert.assertFalse(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(scalarSlot)));
        Assert.assertTrue(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setType(new StructType(new StructField("existing", Type.INT)));
        payloadSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("2", "3"))));
        Assert.assertFalse(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("2", "4"))));
        Assert.assertTrue(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.meta(List.of("2", AccessPathInfo.ACCESS_NULL))));
        Assert.assertFalse(IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                schema, Collections.singletonList(payloadSlot)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.meta(List.of("2", "4", AccessPathInfo.ACCESS_NULL))));
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
        Optional<Map<Integer, List<String>>> mapping = Optional.of(Map.of(
                3, List.of("payload", "renamed_payload"),
                5, Collections.singletonList("payload")));
        List<Column> columns = IcebergUtils.parseSchema(schema, false, false);
        SlotDescriptor unrelatedSlot = new SlotDescriptor(new SlotId(8), new TupleId(0));
        unrelatedSlot.setColumn(columns.get(0));
        SlotDescriptor rootSlot = new SlotDescriptor(new SlotId(1), new TupleId(0));
        rootSlot.setColumn(columns.get(1));

        Assert.assertTrue(IcebergScanNode.hasCurrentNameAliasCollision(schema, mapping));
        Assert.assertFalse(IcebergScanNode.hasCurrentNameAliasCollision(
                schema, Optional.of(Map.of(
                        3, List.of("legacy_payload", "renamed_payload"),
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
                ColumnAccessPath.data(List.of("1", "9"))));
        IcebergScanNode.checkNameMappingBackendCompatibility(
                schema, Collections.singletonList(rootSlot), Collections.emptySet(),
                mapping, Collections.singletonList(smoothUpgradeSource));

        rootSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("1", "3"))));
        UserException exception = Assert.assertThrows(UserException.class,
                () -> IcebergScanNode.checkNameMappingBackendCompatibility(
                        schema, Collections.singletonList(rootSlot), Collections.emptySet(),
                        mapping, Collections.singletonList(smoothUpgradeSource)));
        Assert.assertTrue(exception.getMessage().contains(
                "backend 10004 is a smooth upgrade source"));

        UserException equalityException = Assert.assertThrows(UserException.class,
                () -> IcebergScanNode.checkNameMappingBackendCompatibility(
                        schema, Collections.singletonList(unrelatedSlot), Set.of(7),
                        mapping, Collections.singletonList(smoothUpgradeSource)));
        Assert.assertTrue(equalityException.getMessage().contains(
                "backend 10004 is a smooth upgrade source"));
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
        SlotDescriptor arraySlot = new SlotDescriptor(new SlotId(10), new TupleId(0));
        arraySlot.setColumn(columns.get(0));
        SlotDescriptor mapSlot = new SlotDescriptor(new SlotId(20), new TupleId(0));
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
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), new TupleId(0));
        idSlot.setColumn(columns.get(0));
        SlotDescriptor requiredSlot = new SlotDescriptor(new SlotId(4), new TupleId(0));
        requiredSlot.setColumn(columns.get(1));
        SlotDescriptor payloadSlot = new SlotDescriptor(new SlotId(2), new TupleId(0));
        payloadSlot.setColumn(columns.get(2));

        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(idSlot), List.of(historicalSchema)));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(requiredSlot), List.of(historicalSchema)));

        payloadSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("2", "3"))));
        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(payloadSlot), List.of(historicalSchema)));
        payloadSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("2", "5"))));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(payloadSlot), List.of(historicalSchema)));
    }

    @Test
    public void testRequiredFieldFenceExcludesLaterAndOffLineageSchemas() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField required = Types.NestedField.required(
                2, "required_value", Types.IntegerType.get());
        Schema ancestorSchema = new Schema(40, List.of(id, required));
        Schema targetSchema = new Schema(41, List.of(id, required));
        Schema laterDropSchema = new Schema(42, List.of(id));
        Schema offLineageSchema = new Schema(43, List.of(id));

        Snapshot ancestorSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(ancestorSnapshot.snapshotId()).thenReturn(100L);
        Mockito.when(ancestorSnapshot.schemaId()).thenReturn(ancestorSchema.schemaId());
        Mockito.when(ancestorSnapshot.parentId()).thenReturn(null);
        Mockito.when(ancestorSnapshot.summary()).thenReturn(Map.of());
        Snapshot targetSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(targetSnapshot.snapshotId()).thenReturn(101L);
        Mockito.when(targetSnapshot.schemaId()).thenReturn(targetSchema.schemaId());
        Mockito.when(targetSnapshot.parentId()).thenReturn(100L);
        Mockito.when(targetSnapshot.summary()).thenReturn(Map.of());
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(targetSnapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Map.of(
                ancestorSchema.schemaId(), ancestorSchema,
                targetSchema.schemaId(), targetSchema,
                laterDropSchema.schemaId(), laterDropSchema,
                offLineageSchema.schemaId(), offLineageSchema));
        Mockito.when(table.snapshot(100L)).thenReturn(ancestorSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        List<Schema> targetHistory = node.getRequiredFieldSchemaHistory(targetSchema).get();
        SlotDescriptor requiredSlot = new SlotDescriptor(new SlotId(2), new TupleId(0));
        requiredSlot.setColumn(IcebergUtils.parseSchema(targetSchema, false, false).get(1));

        Assert.assertEquals(
                List.of(targetSchema.schemaId(), ancestorSchema.schemaId()),
                targetHistory.stream().map(Schema::schemaId).collect(Collectors.toList()));
        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                targetSchema, Collections.singletonList(requiredSlot), targetHistory));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                targetSchema, Collections.singletonList(requiredSlot),
                List.of(ancestorSchema, targetSchema, laterDropSchema, offLineageSchema)));
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
        Mockito.when(snapshot.summary()).thenReturn(Map.of());
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(snapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Map.of(
                snapshotSchema.schemaId(), snapshotSchema,
                schemaOnlyTarget.schemaId(), schemaOnlyTarget));

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        List<Schema> targetHistory = node.getRequiredFieldSchemaHistory(schemaOnlyTarget).get();
        SlotDescriptor requiredSlot = new SlotDescriptor(new SlotId(2), new TupleId(0));
        requiredSlot.setColumn(IcebergUtils.parseSchema(schemaOnlyTarget, false, false).get(1));

        Assert.assertEquals(
                List.of(schemaOnlyTarget.schemaId(), snapshotSchema.schemaId()),
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
        Mockito.when(targetSnapshot.summary()).thenReturn(Map.of());
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(targetSnapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Map.of(
                targetSchema.schemaId(), targetSchema));
        Mockito.when(table.snapshot(101L)).thenReturn(null);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        Optional<List<Schema>> targetHistory =
                node.getRequiredFieldSchemaHistory(targetSchema);
        SlotDescriptor requiredSlot = new SlotDescriptor(new SlotId(2), new TupleId(0));
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
        Schema sourceAncestorSchema = new Schema(47, List.of(id));
        Schema sourceSchema = new Schema(48, List.of(id, required));
        Schema targetSchema = new Schema(49, List.of(id, required));

        Snapshot sourceAncestorSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(sourceAncestorSnapshot.snapshotId()).thenReturn(200L);
        Mockito.when(sourceAncestorSnapshot.schemaId()).thenReturn(sourceAncestorSchema.schemaId());
        Mockito.when(sourceAncestorSnapshot.parentId()).thenReturn(null);
        Mockito.when(sourceAncestorSnapshot.summary()).thenReturn(Map.of());
        Snapshot sourceSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(sourceSnapshot.snapshotId()).thenReturn(201L);
        Mockito.when(sourceSnapshot.schemaId()).thenReturn(sourceSchema.schemaId());
        Mockito.when(sourceSnapshot.parentId()).thenReturn(200L);
        Mockito.when(sourceSnapshot.summary()).thenReturn(Map.of());
        Snapshot targetSnapshot = Mockito.mock(Snapshot.class);
        Mockito.when(targetSnapshot.snapshotId()).thenReturn(202L);
        Mockito.when(targetSnapshot.schemaId()).thenReturn(targetSchema.schemaId());
        Mockito.when(targetSnapshot.parentId()).thenReturn(null);
        Mockito.when(targetSnapshot.summary()).thenReturn(Map.of(
                SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP,
                "201"));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(targetSnapshot);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schemas()).thenReturn(Map.of(
                sourceAncestorSchema.schemaId(), sourceAncestorSchema,
                sourceSchema.schemaId(), sourceSchema,
                targetSchema.schemaId(), targetSchema));
        Mockito.when(table.snapshot(200L)).thenReturn(sourceAncestorSnapshot);
        Mockito.when(table.snapshot(201L)).thenReturn(sourceSnapshot);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(tableScan);
        setIcebergTable(node, table);
        List<Schema> targetHistory = node.getRequiredFieldSchemaHistory(targetSchema).get();
        SlotDescriptor requiredSlot = new SlotDescriptor(new SlotId(2), new TupleId(0));
        requiredSlot.setColumn(IcebergUtils.parseSchema(targetSchema, false, false).get(1));

        Assert.assertEquals(
                List.of(targetSchema.schemaId(), sourceSchema.schemaId(),
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
        SlotDescriptor itemsSlot = new SlotDescriptor(new SlotId(10), new TupleId(0));
        itemsSlot.setColumn(columns.get(0));
        SlotDescriptor entriesSlot = new SlotDescriptor(new SlotId(20), new TupleId(0));
        entriesSlot.setColumn(columns.get(1));
        SlotDescriptor structItemsSlot = new SlotDescriptor(new SlotId(30), new TupleId(0));
        structItemsSlot.setColumn(columns.get(2));

        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, List.of(itemsSlot, entriesSlot), List.of(historicalSchema)));
        structItemsSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("30", AccessPathInfo.ACCESS_ALL, "32"))));
        Assert.assertFalse(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(structItemsSlot),
                List.of(historicalSchema)));
        structItemsSlot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of("30", AccessPathInfo.ACCESS_ALL, "33"))));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(structItemsSlot),
                List.of(historicalSchema)));
    }

    private static void assertRequiresRecursiveInitialDefault(
            Schema schema, SlotDescriptor slot, boolean expected, String... path) {
        slot.setAllAccessPaths(Collections.singletonList(
                ColumnAccessPath.data(List.of(path))));
        Assert.assertEquals(expected,
                IcebergScanNode.requiresRecursiveInitialDefaultMaterialization(
                        schema, Collections.singletonList(slot)));
    }

    @Test
    public void testEqualityDeleteFieldIdPreflightDistinguishesDeleteContent() throws Exception {
        DeleteFile positionDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(positionDelete.content()).thenReturn(FileContent.POSITION_DELETES);
        DeleteFile emptyEqualityDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(emptyEqualityDelete.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(emptyEqualityDelete.recordCount()).thenReturn(0L);
        Mockito.when(emptyEqualityDelete.equalityFieldIds()).thenReturn(List.of(7));

        Assert.assertEquals(Collections.emptySet(),
                IcebergScanNode.collectEqualityDeleteFieldIds(List.of(positionDelete)));
        Assert.assertEquals(Set.of(7),
                IcebergScanNode.collectEqualityDeleteFieldIds(List.of(emptyEqualityDelete)));

        FileScanTask applicableTask = Mockito.mock(FileScanTask.class);
        Mockito.when(applicableTask.deletes()).thenReturn(List.of(emptyEqualityDelete));
        FileScanTask taskWithoutEqualityDeletes = Mockito.mock(FileScanTask.class);
        Mockito.when(taskWithoutEqualityDeletes.deletes()).thenReturn(List.of(positionDelete));
        Assert.assertEquals(Set.of(7),
                IcebergScanNode.collectEqualityDeleteFieldIdsFromTasks(
                        List.of(applicableTask, taskWithoutEqualityDeletes)));

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
            return Set.of(7);
        }).when(node).loadEqualityDeleteFieldIds(tableScan);

        Assert.assertEquals(Set.of(7), node.getEqualityDeleteFieldIdsForScan());
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
