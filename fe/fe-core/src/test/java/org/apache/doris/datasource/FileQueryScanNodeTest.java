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

package org.apache.doris.datasource;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TColumnCategory;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FileQueryScanNodeTest {
    private static final long MB = 1024L * 1024L;
    private static final Method UPDATE_REQUIRED_SLOTS_METHOD;
    private static final Method SET_COLUMN_POSITION_MAPPING_METHOD;

    static {
        try {
            UPDATE_REQUIRED_SLOTS_METHOD = FileQueryScanNode.class.getDeclaredMethod("updateRequiredSlots");
            UPDATE_REQUIRED_SLOTS_METHOD.setAccessible(true);
            SET_COLUMN_POSITION_MAPPING_METHOD = FileQueryScanNode.class.getDeclaredMethod("setColumnPositionMapping");
            SET_COLUMN_POSITION_MAPPING_METHOD.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private TableIf table;

    private static class TestFileQueryScanNode extends FileQueryScanNode {
        private TableIf targetTable;

        TestFileQueryScanNode(SessionVariable sv) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), "test", ScanContext.EMPTY, false, sv);
        }

        TupleDescriptor getTupleDescriptor() {
            return desc;
        }

        void setTargetTable(TableIf targetTable) {
            this.targetTable = targetTable;
        }

        long selectFeSplitSize(long fallbackSize, TFileFormatType format, boolean supportsBeSplit) {
            return selectFeSplitSizeForBe(fallbackSize, format, supportsBeSplit);
        }

        @Override
        protected TFileFormatType getFileFormatType() throws UserException {
            return TFileFormatType.FORMAT_ORC;
        }

        @Override
        protected List<String> getPathPartitionKeys() throws UserException {
            return Collections.emptyList();
        }

        @Override
        protected TableIf getTargetTable() throws UserException {
            return targetTable;
        }

        @Override
        protected Map<String, String> getLocationProperties() throws UserException {
            return Collections.emptyMap();
        }
    }

    @Before
    public void setUp() {
        table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn("test_table");
    }

    @Test
    public void testApplyMaxFileSplitNumLimitRaisesTargetSize() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitKeepsTargetSizeWhenSmall() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 500L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitDisabled() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(0);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testEligibleFileRangeCarriesBeSplitSize() {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSize(16 * MB);
        sv.setFileSplitSizeOnBe(96 * MB);

        TFileRangeDesc range = new TFileRangeDesc();
        range.setFormatType(TFileFormatType.FORMAT_PARQUET);
        FileQueryScanNode.setTargetSplitSize(range, sv);
        Assert.assertTrue(range.isSetTargetSplitSize());
        Assert.assertEquals(96 * MB, range.getTargetSplitSize());
    }

    @Test
    public void testIneligibleFileRangeOmitsBeSplitSize() {
        SessionVariable sv = new SessionVariable();

        TFileRangeDesc range = new TFileRangeDesc();
        range.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        FileQueryScanNode.setTargetSplitSize(range, sv);
        Assert.assertFalse(range.isSetTargetSplitSize());
    }

    @Test
    public void testIcebergDeleteRangeOmitsBeSplitSize() {
        SessionVariable sv = new SessionVariable();
        TFileRangeDesc range = new TFileRangeDesc();
        range.setFormatType(TFileFormatType.FORMAT_PARQUET);
        TIcebergFileDesc iceberg = new TIcebergFileDesc();
        iceberg.setDeleteFiles(Collections.singletonList(new TIcebergDeleteFileDesc()));
        TTableFormatFileDesc tableFormat = new TTableFormatFileDesc();
        tableFormat.setTableFormatType(TableFormatType.ICEBERG.value());
        tableFormat.setIcebergParams(iceberg);
        range.setTableFormatParams(tableFormat);

        FileQueryScanNode.setTargetSplitSize(range, sv);
        Assert.assertFalse(range.isSetTargetSplitSize());
    }

    @Test
    public void testTransactionalHiveAndMetadataCountRangesOmitBeSplitSize() {
        SessionVariable sv = new SessionVariable();
        TFileRangeDesc transactionalRange = new TFileRangeDesc();
        transactionalRange.setFormatType(TFileFormatType.FORMAT_ORC);
        TTableFormatFileDesc transactional = new TTableFormatFileDesc();
        transactional.setTableFormatType(TableFormatType.TRANSACTIONAL_HIVE.value());
        transactionalRange.setTableFormatParams(transactional);
        FileQueryScanNode.setTargetSplitSize(transactionalRange, sv);
        Assert.assertFalse(transactionalRange.isSetTargetSplitSize());

        TFileRangeDesc countRange = new TFileRangeDesc();
        countRange.setFormatType(TFileFormatType.FORMAT_PARQUET);
        TTableFormatFileDesc metadataCount = new TTableFormatFileDesc();
        metadataCount.setTableLevelRowCount(10);
        countRange.setTableFormatParams(metadataCount);
        FileQueryScanNode.setTargetSplitSize(countRange, sv);
        Assert.assertFalse(countRange.isSetTargetSplitSize());
    }

    @Test
    public void testEligibleFileUsesDedicatedFeCoarseSplitSize() {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSize(16 * MB);
        sv.setFileSplitSizeOnFe(512 * MB);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);

        Assert.assertEquals(512 * MB,
                node.selectFeSplitSize(16 * MB, TFileFormatType.FORMAT_ORC, true));
    }

    @Test
    public void testIneligibleFileKeepsLegacyFeSplitSize() {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSizeOnFe(512 * MB);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);

        Assert.assertEquals(16 * MB,
                node.selectFeSplitSize(16 * MB, TFileFormatType.FORMAT_CSV_PLAIN, true));
        Assert.assertEquals(16 * MB,
                node.selectFeSplitSize(16 * MB, TFileFormatType.FORMAT_PARQUET, false));

        sv.enableFileScannerV2 = false;
        Assert.assertEquals(16 * MB,
                node.selectFeSplitSize(16 * MB, TFileFormatType.FORMAT_PARQUET, true));

        sv.enableFileScannerV2 = true;
        sv.maxFileScannersConcurrency = 1;
        Assert.assertEquals(16 * MB,
                node.selectFeSplitSize(16 * MB, TFileFormatType.FORMAT_PARQUET, true));

        TFileRangeDesc range = new TFileRangeDesc();
        range.setFormatType(TFileFormatType.FORMAT_PARQUET);
        FileQueryScanNode.setTargetSplitSize(range, sv);
        Assert.assertFalse(range.isSetTargetSplitSize());

        sv.maxFileScannersConcurrency = 16;
        node.setLimit(100);
        Assert.assertEquals(16 * MB,
                node.selectFeSplitSize(16 * MB, TFileFormatType.FORMAT_PARQUET, true));
    }

    @Test
    public void testDedicatedSplitSizesHaveCoarseAndFineDefaults() {
        SessionVariable sv = new SessionVariable();
        Assert.assertEquals(512 * MB, sv.getFileSplitSizeOnFe());
        Assert.assertEquals(64 * MB, sv.getFileSplitSizeOnBe());
    }

    @Test
    public void testUpdateRequiredSlotsPreservesInlineDefaultValueExpr() throws Exception {
        SessionVariable sv = new SessionVariable();
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        node.setTargetTable(table);

        TupleDescriptor desc = node.getTupleDescriptor();
        desc.setTable(table);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), desc);
        slot.setColumn(new Column("c1", Type.INT));
        desc.addSlot(slot);
        Mockito.when(table.getFullSchema()).thenReturn(Arrays.asList(slot.getColumn()));

        TExpr defaultExpr = new TExpr();
        defaultExpr.setNodes(Collections.emptyList());
        TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
        slotInfo.setSlotId(slot.getId().asInt());
        slotInfo.setCategory(TColumnCategory.REGULAR);
        slotInfo.setIsFileSlot(true);
        slotInfo.setDefaultValueExpr(defaultExpr);

        TFileScanRangeParams params = new TFileScanRangeParams();
        params.setRequiredSlots(Arrays.asList(slotInfo));
        node.params = params;

        UPDATE_REQUIRED_SLOTS_METHOD.invoke(node);

        TFileScanSlotInfo updatedSlotInfo = node.params.getRequiredSlots().get(0);
        Assert.assertSame(slotInfo, updatedSlotInfo);
        Assert.assertTrue(updatedSlotInfo.isSetDefaultValueExpr());
        Assert.assertSame(defaultExpr, updatedSlotInfo.getDefaultValueExpr());
    }

    @Test
    public void testColumnPositionMappingUsesRelationSnapshotSchema() throws Exception {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        IcebergExternalTable externalTable = Mockito.mock(IcebergExternalTable.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        Column oldColumn = new Column("old_name", Type.INT);
        node.setTargetTable(externalTable);
        node.getTupleDescriptor().setTable(externalTable);
        node.setRelationSnapshot(Optional.of(relationSnapshot));
        Mockito.when(externalTable.getFullSchema(Optional.of(relationSnapshot)))
                .thenReturn(Collections.singletonList(oldColumn));

        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), node.getTupleDescriptor());
        slot.setColumn(oldColumn);
        node.getTupleDescriptor().addSlot(slot);
        TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
        slotInfo.setSlotId(slot.getId().asInt());
        slotInfo.setCategory(TColumnCategory.REGULAR);
        slotInfo.setIsFileSlot(true);
        node.params = new TFileScanRangeParams();
        node.params.setRequiredSlots(Collections.singletonList(slotInfo));

        SET_COLUMN_POSITION_MAPPING_METHOD.invoke(node);

        Assert.assertEquals(Collections.singletonList(0), node.params.getColumnIdxs());
        Mockito.verify(externalTable).getFullSchema(Optional.of(relationSnapshot));
        Mockito.verify(externalTable, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
        Mockito.verify(externalTable, Mockito.never()).getFullSchema();
    }

    @Test
    public void testDefaultValueUsesRelationSnapshotSchema() throws Exception {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        IcebergExternalTable externalTable = Mockito.mock(IcebergExternalTable.class);
        MvccSnapshot relationSnapshot = Mockito.mock(MvccSnapshot.class);
        Column historicalColumn = new Column("x", Type.INT, true);
        Column latestSameNamedColumn = new Column(
                "x", Type.INT, false, AggregateType.NONE, true, "42", "");
        node.setTargetTable(externalTable);
        node.getTupleDescriptor().setTable(externalTable);
        node.setRelationSnapshot(Optional.of(relationSnapshot));
        Mockito.when(externalTable.getBaseSchema(Optional.of(relationSnapshot), false))
                .thenReturn(Collections.singletonList(historicalColumn));
        Mockito.when(externalTable.getFullSchema(Optional.of(relationSnapshot)))
                .thenReturn(Collections.singletonList(historicalColumn));
        Mockito.when(externalTable.getFullSchema())
                .thenReturn(Collections.singletonList(latestSameNamedColumn));

        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), node.getTupleDescriptor());
        slot.setColumn(historicalColumn);
        node.getTupleDescriptor().addSlot(slot);

        node.initSchemaParams();

        TExpr defaultExpr = node.params.getDefaultValueOfSrcSlot().get(slot.getId().asInt());
        Assert.assertEquals(TExprNodeType.NULL_LITERAL, defaultExpr.getNodes().get(0).getNodeType());
        Mockito.verify(externalTable, Mockito.never()).getFullSchema();
    }

}
