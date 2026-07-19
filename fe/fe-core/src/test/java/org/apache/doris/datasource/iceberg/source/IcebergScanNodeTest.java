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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
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
import java.util.UUID;

public class IcebergScanNodeTest {
    private static final long MB = 1024L * 1024L;

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

        void addSlot(int slotId, Column column) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), desc.getId());
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
    public void testInitialDefaultMetadataUsesSnapshotSchema() throws Exception {
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
        TableScan snapshotScan = Mockito.mock(TableScan.class);
        Mockito.when(snapshotScan.snapshot()).thenReturn(snapshot);
        Mockito.when(snapshotScan.table()).thenReturn(table);
        Mockito.when(snapshotScan.schema()).thenReturn(currentSchema);

        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable());
        node.setTableScan(snapshotScan);

        Map<Integer, String> defaults = node.getBase64EncodedInitialDefaultsForScan();
        Assert.assertEquals(Collections.singletonMap(7, "AAEC/w=="), defaults);
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
