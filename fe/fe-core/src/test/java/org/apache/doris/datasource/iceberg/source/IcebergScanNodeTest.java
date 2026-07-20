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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
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
import org.apache.doris.thrift.TPushAggOp;

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
import org.apache.iceberg.TableScan;
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
