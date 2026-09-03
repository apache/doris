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
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.FederationBackendPolicy;
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
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
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
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    @SuppressWarnings("unchecked")
    private static CloseableIterable<FileScanTask> planFileScanTaskWithManifestCache(
            IcebergScanNode node, TableScan scan) throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod(
                "planFileScanTaskWithManifestCache", TableScan.class);
        method.setAccessible(true);
        return (CloseableIterable<FileScanTask>) method.invoke(node, scan);
    }

    @SuppressWarnings("unchecked")
    private static CloseableIterable<FileScanTask> splitFiles(
            IcebergScanNode node, TableScan scan) throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod("splitFiles", TableScan.class);
        method.setAccessible(true);
        return (CloseableIterable<FileScanTask>) method.invoke(node, scan);
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

    private static class ManifestPlanningIcebergScanNode extends TestIcebergScanNode {
        private final AtomicInteger manifestLoadCount;

        ManifestPlanningIcebergScanNode(SessionVariable sv, AtomicInteger manifestLoadCount) {
            this(sv, manifestLoadCount, false);
        }

        ManifestPlanningIcebergScanNode(
                SessionVariable sv, AtomicInteger manifestLoadCount, boolean batchMode) {
            super(sv, false, batchMode);
            this.manifestLoadCount = manifestLoadCount;
        }

        @Override
        protected List<FileScanTask> loadFileScanTasksWithManifestCache(
                TableScan scan, Snapshot snapshot) {
            manifestLoadCount.incrementAndGet();
            return Collections.emptyList();
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

    private static class ExactFallbackIcebergScanNode extends IcebergScanNode {
        private final TableScan tableScan;
        private int exactPlanFileScanCalls;

        ExactFallbackIcebergScanNode(SessionVariable sessionVariable, TableScan tableScan) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                    sessionVariable, ScanContext.EMPTY);
            this.tableScan = tableScan;
        }

        @Override
        public TableScan createTableScan() {
            return tableScan;
        }

        @Override
        CloseableIterable<FileScanTask> planFileScanTaskWithoutReuse(TableScan scan) {
            exactPlanFileScanCalls++;
            return scan.planFiles();
        }

        @Override
        public List<String> getPathPartitionKeys() {
            return Collections.emptyList();
        }

        void addSlot(int slotId, Column column) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc);
            slot.setColumn(column);
            desc.addSlot(slot);
        }

        int getExactPlanFileScanCalls() {
            return exactPlanFileScanCalls;
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
    public void testTableLevelCountManifestPlanningKeepsFileTasksLazy() throws Exception {
        SessionVariable sv = Mockito.mock(SessionVariable.class);
        Mockito.when(sv.getFileSplitSize()).thenReturn(0L);
        Mockito.when(sv.getMaxSplitSize()).thenReturn(MB);
        Mockito.when(sv.getEnableExternalTableBatchMode()).thenReturn(false);
        AtomicInteger planCalls = new AtomicInteger();
        AtomicInteger consumedTasks = new AtomicInteger();
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(Mockito.mock(Snapshot.class));
        Mockito.when(tableScan.planFiles()).thenAnswer(invocation -> {
            planCalls.incrementAndGet();
            Iterable<FileScanTask> tasks = () -> new Iterator<FileScanTask>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < 100;
                }

                @Override
                public FileScanTask next() {
                    index++;
                    consumedTasks.incrementAndGet();
                    FileScanTask task = Mockito.mock(FileScanTask.class);
                    Mockito.when(task.split(MB)).thenReturn(Collections.singletonList(task));
                    return task;
                }
            };
            return CloseableIterable.withNoopClose(tasks);
        });
        CountPlanningIcebergScanNode node =
                new CountPlanningIcebergScanNode(sv, tableScan, 30_000);
        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.emptyList());

        try (CloseableIterable<FileScanTask> plannedTasks =
                planFileScanTaskWithManifestCache(node, tableScan)) {
            Iterator<FileScanTask> iterator = plannedTasks.iterator();
            Assert.assertTrue(iterator.hasNext());
            iterator.next();
        }

        Assert.assertEquals(1, planCalls.get());
        Assert.assertEquals(1, consumedTasks.get());
        Assert.assertEquals(1, node.snapshotCountCalls);
    }

    @Test
    public void testSystemTableTasksAreConsumedWhilePlanningRemainsStreaming() throws Exception {
        AtomicInteger outstandingTasks = new AtomicInteger();
        AtomicInteger consumedTasks = new AtomicInteger();
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.planFiles()).thenAnswer(invocation -> {
            Iterable<FileScanTask> tasks = () -> new Iterator<FileScanTask>() {
                private int index;

                @Override
                public boolean hasNext() {
                    return index < 100;
                }

                @Override
                public FileScanTask next() {
                    Assert.assertEquals(0, outstandingTasks.get());
                    index++;
                    outstandingTasks.incrementAndGet();
                    return Mockito.mock(FileScanTask.class);
                }
            };
            return CloseableIterable.withNoopClose(tasks);
        });

        IcebergScanNode.consumeSystemTableTasks(tableScan, task -> {
            Assert.assertEquals(1, outstandingTasks.getAndDecrement());
            consumedTasks.incrementAndGet();
        });

        Assert.assertEquals(100, consumedTasks.get());
        Assert.assertEquals(0, outstandingTasks.get());
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
            FileScanTask task = Mockito.mock(FileScanTask.class, Mockito.withSettings().serializable());
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
            Assert.assertNotSame(firstTasks, secondTasks);
            Assert.assertEquals(1, secondTasks.size());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testOversizedIcebergPlanIsNotRetained() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode firstNode = new TestIcebergScanNode(new SessionVariable());
            TestIcebergScanNode secondNode = new TestIcebergScanNode(new SessionVariable());
            firstNode.setMaxRetainedSerializedTaskBytes(1);
            secondNode.setMaxRetainedSerializedTaskBytes(1);
            setIcebergSource(firstNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondNode, mockIcebergSource(10L, 20L));
            TableScan scan = mockTableScan(30L, 40, Expressions.equal("id", 1));
            AtomicInteger planCalls = new AtomicInteger();
            List<FileScanTask> tasks = Arrays.asList(
                    Mockito.mock(FileScanTask.class), Mockito.mock(FileScanTask.class));

            Assert.assertEquals(tasks, firstNode.getOrPlanFileScanTasks(scan, () -> {
                planCalls.incrementAndGet();
                return tasks;
            }));
            Assert.assertEquals(tasks, secondNode.getOrPlanFileScanTasks(scan, () -> {
                planCalls.incrementAndGet();
                return tasks;
            }));

            Assert.assertEquals(2, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testIcebergTaskSerializationStopsAtByteLimit() {
        byte[] largePayload = new byte[Math.toIntExact(MB)];

        Assert.assertFalse(IcebergScanNode.serializeIcebergTaskWithinLimit(largePayload, 1024).isPresent());
        Assert.assertTrue(IcebergScanNode.serializeIcebergTaskWithinLimit(largePayload, 2 * MB).isPresent());
    }

    @Test
    public void testOversizedPositionDeletePlanIsNotRetained() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            TestIcebergScanNode firstNode = new TestIcebergScanNode(new SessionVariable());
            TestIcebergScanNode secondNode = new TestIcebergScanNode(new SessionVariable());
            firstNode.setMaxRetainedSerializedTaskBytes(1);
            secondNode.setMaxRetainedSerializedTaskBytes(1);
            setIcebergSource(firstNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondNode, mockIcebergSource(10L, 20L));
            BatchScan scan = Mockito.mock(BatchScan.class);
            Snapshot snapshot = Mockito.mock(Snapshot.class);
            Mockito.when(snapshot.snapshotId()).thenReturn(30L);
            Mockito.when(scan.snapshot()).thenReturn(snapshot);
            Mockito.when(scan.schema()).thenReturn(new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get())));
            Mockito.when(scan.filter()).thenReturn(Expressions.alwaysTrue());
            Mockito.when(scan.isCaseSensitive()).thenReturn(true);
            AtomicInteger planCalls = new AtomicInteger();
            List<PositionDeletesScanTask> tasks = Arrays.asList(
                    Mockito.mock(PositionDeletesScanTask.class),
                    Mockito.mock(PositionDeletesScanTask.class));

            firstNode.getOrPlanPositionDeleteTasks(scan, () -> {
                planCalls.incrementAndGet();
                return tasks;
            });
            secondNode.getOrPlanPositionDeleteTasks(scan, () -> {
                planCalls.incrementAndGet();
                return tasks;
            });

            Assert.assertEquals(2, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testRepeatedActualIcebergPlanFilesUsesStatementCache() throws Exception {
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

    @Test
    public void testManifestPlanningPathUsesStatementCache() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        AtomicInteger manifestLoadCount = new AtomicInteger();
        try {
            ManifestPlanningIcebergScanNode firstNode =
                    new ManifestPlanningIcebergScanNode(new SessionVariable(), manifestLoadCount);
            ManifestPlanningIcebergScanNode secondNode =
                    new ManifestPlanningIcebergScanNode(new SessionVariable(), manifestLoadCount);
            setIcebergSource(firstNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondNode, mockIcebergSource(10L, 20L));

            try (CloseableIterable<FileScanTask> ignored = planFileScanTaskWithManifestCache(
                    firstNode, mockTableScan(30L, 40, Expressions.equal("id", 1)))) {
                // The test only verifies the production manifest-planning cache wrapper.
            }
            try (CloseableIterable<FileScanTask> ignored = planFileScanTaskWithManifestCache(
                    secondNode, mockTableScan(30L, 40, Expressions.equal("id", 1)))) {
                // The second equivalent relation must reuse the first relation's task list.
            }

            Assert.assertEquals(1, manifestLoadCount.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testManifestPlanningBypassesStatementCacheForStreamingModes() throws Exception {
        AtomicInteger manifestLoadCount = new AtomicInteger();
        AtomicInteger batchPlanCalls = new AtomicInteger();
        AtomicInteger explicitSizePlanCalls = new AtomicInteger();

        ManifestPlanningIcebergScanNode batchNode =
                new ManifestPlanningIcebergScanNode(new SessionVariable(), manifestLoadCount, true);
        setIcebergSource(batchNode, mockIcebergSource(10L, 20L));
        try (CloseableIterable<FileScanTask> ignored = planFileScanTaskWithManifestCache(
                batchNode, mockTableScanWithPlanCounter(batchPlanCalls))) {
            // Batch mode keeps the SDK iterable lazy instead of materializing manifest tasks.
        }

        SessionVariable explicitSizeVariable = new SessionVariable();
        explicitSizeVariable.setFileSplitSize(MB);
        ManifestPlanningIcebergScanNode explicitSizeNode =
                new ManifestPlanningIcebergScanNode(explicitSizeVariable, manifestLoadCount);
        setIcebergSource(explicitSizeNode, mockIcebergSource(10L, 20L));
        try (CloseableIterable<FileScanTask> ignored = planFileScanTaskWithManifestCache(
                explicitSizeNode, mockTableScanWithPlanCounter(explicitSizePlanCalls))) {
            // Explicit split size also keeps the SDK iterable lazy.
        }

        Assert.assertEquals(0, manifestLoadCount.get());
        Assert.assertEquals(1, batchPlanCalls.get());
        Assert.assertEquals(1, explicitSizePlanCalls.get());
    }

    @Test
    public void testSplitFilesUsesStatementCacheOutsideStreamingModes() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        AtomicInteger planCalls = new AtomicInteger();
        try {
            TestIcebergScanNode firstNode = new TestIcebergScanNode(new SessionVariable());
            TestIcebergScanNode secondNode = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(firstNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondNode, mockIcebergSource(10L, 20L));
            TableScan firstScan = mockTableScanWithPlanCounter(planCalls);
            TableScan secondScan = mockTableScanWithPlanCounter(planCalls);

            try (CloseableIterable<FileScanTask> ignored = splitFiles(firstNode, firstScan)) {
                // Materialization happens before splitFiles returns.
            }
            try (CloseableIterable<FileScanTask> ignored = splitFiles(secondNode, secondScan)) {
                // The second equivalent relation must reuse the materialized native tasks.
            }

            Assert.assertEquals(1, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testSplitFilesBypassesStatementCacheForStreamingModes() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext context = new ConnectContext();
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        AtomicInteger batchPlanCalls = new AtomicInteger();
        AtomicInteger explicitSizePlanCalls = new AtomicInteger();
        try {
            TestIcebergScanNode firstBatchNode =
                    new TestIcebergScanNode(new SessionVariable(), false, true);
            TestIcebergScanNode secondBatchNode =
                    new TestIcebergScanNode(new SessionVariable(), false, true);
            setIcebergSource(firstBatchNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondBatchNode, mockIcebergSource(10L, 20L));
            try (CloseableIterable<FileScanTask> ignored = splitFiles(
                    firstBatchNode, mockTableScanWithPlanCounter(batchPlanCalls))) {
                // Batch mode preserves lazy Iceberg planning.
            }
            try (CloseableIterable<FileScanTask> ignored = splitFiles(
                    secondBatchNode, mockTableScanWithPlanCounter(batchPlanCalls))) {
                // Each batch relation must own its streaming iterable.
            }

            SessionVariable explicitSizeVariable = new SessionVariable();
            explicitSizeVariable.setFileSplitSize(MB);
            TestIcebergScanNode firstExplicitSizeNode = new TestIcebergScanNode(explicitSizeVariable);
            TestIcebergScanNode secondExplicitSizeNode = new TestIcebergScanNode(explicitSizeVariable);
            setIcebergSource(firstExplicitSizeNode, mockIcebergSource(10L, 20L));
            setIcebergSource(secondExplicitSizeNode, mockIcebergSource(10L, 20L));
            try (CloseableIterable<FileScanTask> ignored = splitFiles(
                    firstExplicitSizeNode, mockTableScanWithPlanCounter(explicitSizePlanCalls))) {
                // Explicit split size also preserves lazy Iceberg planning.
            }
            try (CloseableIterable<FileScanTask> ignored = splitFiles(
                    secondExplicitSizeNode, mockTableScanWithPlanCounter(explicitSizePlanCalls))) {
                // Each explicitly sized relation must own its streaming iterable.
            }

            Assert.assertEquals(2, batchPlanCalls.get());
            Assert.assertEquals(2, explicitSizePlanCalls.get());
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
            TestIcebergScanNode otherCatalogNode = new TestIcebergScanNode(new SessionVariable());
            TestIcebergScanNode otherTableNode = new TestIcebergScanNode(new SessionVariable());
            setIcebergSource(node, mockIcebergSource(10L, 20L));
            setIcebergSource(otherCatalogNode, mockIcebergSource(11L, 20L));
            setIcebergSource(otherTableNode, mockIcebergSource(10L, 21L));
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
            otherCatalogNode.getOrPlanFileScanTasks(
                    mockTableScan(30L, 40, Expressions.equal("id", 1)),
                    () -> plannedTask(planCalls));
            otherTableNode.getOrPlanFileScanTasks(
                    mockTableScan(30L, 40, Expressions.equal("id", 1)),
                    () -> plannedTask(planCalls));
            node.getOrPlanFileScanTasks(
                    mockTableScan(30L, 40, Expressions.equal("id", 1), true),
                    () -> plannedTask(planCalls));

            Assert.assertEquals(7, planCalls.get());
        } finally {
            statementContext.close();
            ConnectContext.remove();
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
        return mockTableScan(snapshotId, schemaId, filter, false);
    }

    private static TableScan mockTableScan(
            long snapshotId, int schemaId, org.apache.iceberg.expressions.Expression filter,
            boolean caseSensitive) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Schema schema = new Schema(schemaId,
                ImmutableList.of(Types.NestedField.optional(1, "id", Types.IntegerType.get())));
        TableScan scan = Mockito.mock(TableScan.class);
        Mockito.when(scan.snapshot()).thenReturn(snapshot);
        Mockito.when(scan.schema()).thenReturn(schema);
        Mockito.when(scan.filter()).thenReturn(filter);
        Mockito.when(scan.isCaseSensitive()).thenReturn(caseSensitive);
        return scan;
    }

    private static TableScan mockTableScanWithPlanCounter(AtomicInteger planCalls) {
        TableScan scan = mockTableScan(30L, 40, Expressions.equal("id", 1));
        Mockito.when(scan.planFiles()).thenAnswer(invocation -> {
            planCalls.incrementAndGet();
            return CloseableIterable.withNoopClose(Collections.emptyList());
        });
        return scan;
    }

    private static List<FileScanTask> plannedTask(AtomicInteger planCalls) {
        planCalls.incrementAndGet();
        return Collections.singletonList(
                Mockito.mock(FileScanTask.class, Mockito.withSettings().serializable()));
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
    public void testBatchSplitCarriesDroppedEqualitySchemaThroughThrift() throws Exception {
        Types.NestedField id = Types.NestedField.required(1, "id", Types.LongType.get());
        Types.NestedField equalityKey = Types.NestedField.required("k")
                .withId(7)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema historicalSchema = new Schema(1, ImmutableList.of(id, equalityKey));
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

        TestIcebergScanNode node = new TestIcebergScanNode(
                new SessionVariable(), false, false, true);
        setIcebergTable(node, table);
        node.setTableScan(tableScan);
        setPrivateField(node, "plannedScanSchema", currentSchema);
        setPrivateField(node, "plannedNameMapping",
                Optional.of(ImmutableMap.of(7, ImmutableList.of("old_k"))));
        setPrivateField(node, "storagePropertiesMap", Collections.emptyMap());
        setPrivateField(node, "formatVersion", 3);
        setPrivateField(node, "orderedPathPartitionKeys", Collections.emptyList());
        setPrivateField(node, "orderedPartitionMetadataKeys", Collections.emptyList());

        DeleteFile droppedKeyDelete = equalityDeleteFile(7, "file:///tmp/delete.parquet");
        FileScanTask droppedKeyTask = fileScanTask(
                "file:///tmp/data.parquet", droppedKeyDelete);
        IcebergSplit droppedKeySplit = createIcebergSplit(node, droppedKeyTask);
        Assert.assertNotNull(droppedKeySplit.getEqualityDeleteSchema());

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        setIcebergParams(node, rangeDesc, droppedKeySplit);
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] serialized = serializer.serialize(rangeDesc);
        TFileRangeDesc restored = new TFileRangeDesc();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored, serialized);

        Assert.assertTrue(restored.getTableFormatParams().getIcebergParams()
                .isSetEqualityDeleteSchema());
        TSchema splitSchema = restored.getTableFormatParams().getIcebergParams()
                .getEqualityDeleteSchema();
        Assert.assertEquals(1, splitSchema.getRootField().getFieldsSize());
        TField field = splitSchema.getRootField().getFields().get(0).getFieldPtr();
        Assert.assertEquals(7, field.getId());
        Assert.assertEquals("7", field.getInitialDefaultValue());
        Assert.assertFalse(field.isIsOptional());
        Assert.assertEquals(ImmutableList.of("old_k"), field.getNameMapping());
        Assert.assertTrue(field.isNameMappingIsAuthoritative());

        DeleteFile currentKeyDelete = equalityDeleteFile(1, "file:///tmp/current-delete.parquet");
        IcebergSplit currentKeySplit = createIcebergSplit(
                node, fileScanTask("file:///tmp/current-data.parquet", currentKeyDelete));
        Assert.assertNull(currentKeySplit.getEqualityDeleteSchema());
        TFileRangeDesc currentRangeDesc = new TFileRangeDesc();
        setIcebergParams(node, currentRangeDesc, currentKeySplit);
        Assert.assertFalse(currentRangeDesc.getTableFormatParams().getIcebergParams()
                .isSetEqualityDeleteSchema());
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
    public void testCreateScanRangeLocationsRejectsSmoothUpgradeSourceForRecursiveDefault()
            throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableFileScannerV2 = true;
        TestIcebergScanNode node = new TestIcebergScanNode(sessionVariable);

        Types.NestedField existing = Types.NestedField.optional(
                3, "existing", Types.IntegerType.get());
        Types.NestedField nestedDefault = Types.NestedField.optional("added")
                .withId(4)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(5)
                .build();
        Schema schema = new Schema(Types.NestedField.optional(
                2, "payload", Types.StructType.of(existing, nestedDefault)));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
        setIcebergTable(node, table);
        node.addSlot(1, IcebergUtils.parseSchema(schema, false, false).get(0));

        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(null);
        node.setTableScan(tableScan);

        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10006L);
        setBackendPolicy(node, Collections.singletonList(smoothUpgradeSource));

        ConnectContext context = new ConnectContext();
        context.setSessionVariable(sessionVariable);
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            UserException exception = Assert.assertThrows(
                    UserException.class, node::createScanRangeLocations);
            Assert.assertEquals(
                    "Current Iceberg scan semantics are unavailable while backend 10006"
                            + " is a smooth upgrade source",
                    exception.getDetailMessage());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testCreateScanRangeLocationsFallsBackToExactTasksForSmoothUpgradeSource()
            throws Exception {
        Schema schema = new Schema(
                Types.NestedField.optional(7, "delete_key", Types.IntegerType.get()));
        DeleteFile equalityDelete = equalityDeleteFile(
                7, "file:///tmp/exact-fallback-delete.parquet");
        FileScanTask task = fileScanTask(
                "file:///tmp/exact-fallback-data.parquet", equalityDelete);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.summary()).thenReturn(
                ImmutableMap.of(IcebergUtils.TOTAL_EQUALITY_DELETES, "1"));
        TableScan tableScan = Mockito.mock(TableScan.class);
        Mockito.when(tableScan.snapshot()).thenReturn(snapshot);
        Mockito.when(tableScan.planFiles()).thenAnswer(ignored ->
                CloseableIterable.withNoopClose(Collections.singletonList(task)));

        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableFileScannerV2 = true;
        ExactFallbackIcebergScanNode node = new ExactFallbackIcebergScanNode(
                sessionVariable, tableScan);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
        setIcebergTable(node, table);
        setPreExecutionAuthenticator(node, new ExecutionAuthenticator() {
        });
        node.addSlot(1, IcebergUtils.parseSchema(schema, false, false).get(0));
        setPrivateField(node, "isBatchMode", true);

        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10007L);
        setBackendPolicy(node, Collections.singletonList(smoothUpgradeSource));

        ConnectContext context = new ConnectContext();
        context.setSessionVariable(sessionVariable);
        context.setStatementContext(new StatementContext());
        context.setThreadLocalInfo();
        try {
            UserException exception = Assert.assertThrows(
                    UserException.class, node::createScanRangeLocations);
            Assert.assertEquals(
                    "Current Iceberg scan semantics are unavailable while backend 10007"
                            + " is a smooth upgrade source",
                    exception.getDetailMessage());
            Assert.assertEquals(1, node.getExactPlanFileScanCalls());
            Assert.assertFalse(node.isBatchMode());
        } finally {
            ConnectContext.remove();
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

    @Test
    public void testSameIdOptionalToRequiredTransitionsTriggerUpgradeGate() {
        Types.NestedField optionalWithDefault = Types.NestedField.optional("value")
                .withId(40)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Types.NestedField requiredWithDefault = Types.NestedField.required("value")
                .withId(40)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(7)
                .build();
        Schema historicalSchema = new Schema(
                Types.NestedField.optional(10, "items", Types.ListType.ofOptional(
                        11, Types.IntegerType.get())),
                Types.NestedField.optional(20, "entries", Types.MapType.ofOptional(
                        21, 22, Types.StringType.get(), Types.IntegerType.get())),
                optionalWithDefault);
        Schema scanSchema = new Schema(
                Types.NestedField.optional(10, "items", Types.ListType.ofRequired(
                        11, Types.IntegerType.get())),
                Types.NestedField.optional(20, "entries", Types.MapType.ofRequired(
                        21, 22, Types.StringType.get(), Types.IntegerType.get())),
                requiredWithDefault);
        List<Column> columns = IcebergUtils.parseSchema(scanSchema, false, false);
        SlotDescriptor itemsSlot = slotDescriptor(10);
        itemsSlot.setColumn(columns.get(0));
        SlotDescriptor entriesSlot = slotDescriptor(20);
        entriesSlot.setColumn(columns.get(1));
        SlotDescriptor valueSlot = slotDescriptor(40);
        valueSlot.setColumn(columns.get(2));

        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(itemsSlot),
                ImmutableList.of(historicalSchema)));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(entriesSlot),
                ImmutableList.of(historicalSchema)));
        Assert.assertTrue(IcebergScanNode.requiresMissingRequiredFieldRejection(
                scanSchema, Collections.singletonList(valueSlot),
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
        Assert.assertTrue(IcebergScanNode.shouldInspectBatchEqualityDeletes(
                true, ImmutableList.of(currentBackend, smoothUpgradeSource)));
        Assert.assertFalse(IcebergScanNode.shouldInspectBatchEqualityDeletes(
                true, ImmutableList.of(currentBackend)));
        Assert.assertFalse(IcebergScanNode.shouldInspectBatchEqualityDeletes(
                false, ImmutableList.of(currentBackend, smoothUpgradeSource)));
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
    public void testHistoricalPredicateSkipsPushdownAfterColumnChange() throws Exception {
        Schema historicalSchema = new Schema(
                Types.NestedField.optional(7, "old_name", Types.IntegerType.get()),
                Types.NestedField.optional(9, "stable_name", Types.IntegerType.get()));
        Schema currentSchema = new Schema(
                Types.NestedField.optional(7, "new_name", Types.IntegerType.get()),
                Types.NestedField.optional(8, "old_name", Types.IntegerType.get()),
                Types.NestedField.optional(9, "stable_name", Types.IntegerType.get()));
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
        node.addConjunct(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(new TableName(), "stable_name"), new IntLiteral(2, Type.INT)));

        node.createRealTableScan();

        Mockito.verify(scan).filter(Mockito.argThat(expression -> expression.toString().contains("stable_name")));
        Mockito.verify(scan, Mockito.never())
                .filter(Mockito.argThat(expression -> expression.toString().contains("old_name")));
    }

    @Test
    public void testHistoricalPredicatePlansAfterColumnRename() throws Exception {
        assertHistoricalPredicatePlansAfterSchemaEvolution(false);
    }

    @Test
    public void testHistoricalPredicatePlansAfterColumnDrop() throws Exception {
        assertHistoricalPredicatePlansAfterSchemaEvolution(true);
    }

    private void assertHistoricalPredicatePlansAfterSchemaEvolution(boolean dropColumn) throws Exception {
        Schema historicalSchema = new Schema(
                Types.NestedField.optional(1, "x", Types.IntegerType.get()),
                Types.NestedField.optional(2, "y", Types.IntegerType.get()),
                Types.NestedField.optional(3, "part", Types.IntegerType.get()));
        HadoopTables tables = new HadoopTables(new Configuration());
        String tableLocation = temporaryFolder.getRoot().toPath()
                .resolve("historical_predicate_after_" + (dropColumn ? "drop" : "rename")).toUri().toString();
        Table table = tables.create(
                historicalSchema, PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"), tableLocation);
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(tableLocation + "/data/data.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build();
        table.newFastAppend().appendFile(dataFile).commit();
        long historicalSnapshotId = table.currentSnapshot().snapshotId();
        int historicalSchemaId = table.currentSnapshot().schemaId();
        if (dropColumn) {
            table.updateSchema().deleteColumn("x").commit();
        } else {
            table.updateSchema().renameColumn("x", "renamed_x").commit();
        }
        DataFile currentDataFile = DataFiles.builder(table.spec())
                .withPath(tableLocation + "/data/current.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
        // A newer snapshot makes Iceberg resolve historical partition specs during time travel.
        table.newFastAppend().appendFile(currentDataFile).commit();

        TableScan scan = table.newScan()
                .useSnapshot(historicalSnapshotId)
                .project(table.schemas().get(historicalSchemaId));
        BinaryPredicate conjunct = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(new TableName(), "x"), new IntLiteral(1, Type.INT));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        setIcebergTable(node, table);
        org.apache.iceberg.expressions.Expression predicate =
                node.convertToIcebergPruningExpression(conjunct, scan.schema());
        Assert.assertNull(predicate);

        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            Iterator<FileScanTask> iterator = tasks.iterator();
            Assert.assertTrue(iterator.hasNext());
            iterator.next();
            Assert.assertFalse(iterator.hasNext());
        }
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

    private static void setPrivateField(IcebergScanNode node, String fieldName, Object value)
            throws Exception {
        Field field = IcebergScanNode.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(node, value);
    }

    private static DeleteFile equalityDeleteFile(int fieldId, String path) {
        DeleteFile deleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(deleteFile.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(deleteFile.recordCount()).thenReturn(1L);
        Mockito.when(deleteFile.equalityFieldIds()).thenReturn(ImmutableList.of(fieldId));
        Mockito.when(deleteFile.path()).thenReturn(path);
        Mockito.when(deleteFile.fileSizeInBytes()).thenReturn(64L);
        Mockito.when(deleteFile.format()).thenReturn(FileFormat.PARQUET);
        return deleteFile;
    }

    private static FileScanTask fileScanTask(String path, DeleteFile deleteFile) {
        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.path()).thenReturn(path);
        Mockito.when(dataFile.fileSizeInBytes()).thenReturn(128L);
        Mockito.when(dataFile.format()).thenReturn(FileFormat.PARQUET);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.start()).thenReturn(0L);
        Mockito.when(task.length()).thenReturn(128L);
        Mockito.when(task.deletes()).thenReturn(ImmutableList.of(deleteFile));
        return task;
    }

    private static IcebergSplit createIcebergSplit(IcebergScanNode node, FileScanTask task)
            throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod("createIcebergSplit", FileScanTask.class);
        method.setAccessible(true);
        return (IcebergSplit) method.invoke(node, task);
    }

    private static void setIcebergParams(
            IcebergScanNode node, TFileRangeDesc rangeDesc, IcebergSplit split) throws Exception {
        Method method = IcebergScanNode.class.getDeclaredMethod(
                "setIcebergParams", TFileRangeDesc.class, IcebergSplit.class);
        method.setAccessible(true);
        method.invoke(node, rangeDesc, split);
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

    private static void setBackendPolicy(IcebergScanNode node, List<Backend> backends)
            throws Exception {
        FederationBackendPolicy backendPolicy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(backendPolicy.getBackends()).thenReturn(backends);
        Mockito.when(backendPolicy.numBackends()).thenReturn(backends.size());
        Field backendPolicyField = ExternalScanNode.class.getDeclaredField("backendPolicy");
        backendPolicyField.setAccessible(true);
        backendPolicyField.set(node, backendPolicy);
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
    public void testSelectFeSplitSizeUsesCoarseSizeOnlyWithoutDeletes() {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSizeOnFe(512 * MB);
        TestIcebergScanNode node = new TestIcebergScanNode(sv);

        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(dataFile.format()).thenReturn(FileFormat.PARQUET);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.deletes()).thenReturn(Collections.emptyList());
        Assert.assertEquals(512 * MB, node.selectFeSplitSize(task, 64 * MB));

        DeleteFile deleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(task.deletes()).thenReturn(Collections.singletonList(deleteFile));
        Assert.assertEquals(64 * MB, node.selectFeSplitSize(task, 64 * MB));

        Mockito.when(task.deletes()).thenReturn(Collections.emptyList());
        Mockito.when(dataFile.format()).thenReturn(FileFormat.AVRO);
        Assert.assertEquals(64 * MB, node.selectFeSplitSize(task, 64 * MB));
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
