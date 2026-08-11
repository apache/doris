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
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TColumnCategory;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TSplitSource;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Test
    public void testFileAffinityRangesStayInOneNereidsInstance() {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        List<TScanRangeParams> params = Arrays.asList(
                scanRangeParams(100), scanRangeParams(100), scanRangeParams(100),
                scanRangeParams(50), scanRangeParams(50), scanRangeParams(200));
        node.recordScanRangeFileAffinity(params.get(0).getScanRange(), "s3://bucket/a.parquet");
        node.recordScanRangeFileAffinity(params.get(1).getScanRange(), "s3://bucket/a.parquet");
        node.recordScanRangeFileAffinity(params.get(2).getScanRange(), "s3://bucket/a.parquet");
        node.recordScanRangeFileAffinity(params.get(3).getScanRange(), "s3://bucket/b.parquet");
        node.recordScanRangeFileAffinity(params.get(4).getScanRange(), "s3://bucket/b.parquet");

        ScanRanges scanRanges = new ScanRanges(params, Arrays.asList(100L, 100L, 100L, 50L, 50L, 200L));
        DefaultScanSource source = new DefaultScanSource(ImmutableMap.of(node, scanRanges));
        List<ScanSource> instances = source.parallelize(Collections.singletonList(node), 3);

        Assert.assertEquals(3, instances.size());
        List<List<TScanRangeParams>> rangesPerInstance = instances.stream()
                .map(instance -> ((DefaultScanSource) instance).scanNodeToScanRanges.get(node).params)
                .collect(Collectors.toList());
        assertSameInstance(rangesPerInstance, params.subList(0, 3));
        assertSameInstance(rangesPerInstance, params.subList(3, 5));
        List<Long> instanceWeights = rangesPerInstance.stream()
                .map(instance -> instance.stream()
                        .mapToLong(param -> param.getScanRange().getExtScanRange()
                                .getFileScanRange().getRanges().get(0).getSize())
                        .sum())
                .sorted()
                .collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(100L, 200L, 300L), instanceWeights);
        Set<TScanRangeParams> assigned = Collections.newSetFromMap(new IdentityHashMap<>());
        rangesPerInstance.forEach(assigned::addAll);
        Assert.assertEquals(params.size(), assigned.size());
        Assert.assertTrue(assigned.containsAll(params));
    }

    @Test
    public void testInstanceSplitRemainsRoundRobinWithoutFileAffinity() {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        List<TScanRangeParams> params = Arrays.asList(
                scanRangeParams(100), scanRangeParams(100), scanRangeParams(100),
                scanRangeParams(100), scanRangeParams(100));

        List<List<Integer>> instances = node.splitScanRangeParamsByInstance(params, 2);

        Assert.assertEquals(Arrays.asList(0, 2, 4), instances.get(0));
        Assert.assertEquals(Arrays.asList(1, 3), instances.get(1));
    }

    @Test
    public void testSplitSourceOnlyRangesRemainRoundRobinForLegacyAndNereids() {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        List<TScanRangeParams> params = Arrays.asList(
                splitSourceScanRangeParams(1), splitSourceScanRangeParams(2), splitSourceScanRangeParams(3));

        List<List<TScanRangeParams>> legacyInstances = node.materializeScanRangeParamsByInstance(params, 2);

        Assert.assertEquals(2, legacyInstances.size());
        Assert.assertSame(params.get(0), legacyInstances.get(0).get(0));
        Assert.assertSame(params.get(2), legacyInstances.get(0).get(1));
        Assert.assertSame(params.get(1), legacyInstances.get(1).get(0));

        ScanRanges scanRanges = new ScanRanges(params, Arrays.asList(0L, 0L, 0L));
        DefaultScanSource source = new DefaultScanSource(ImmutableMap.of(node, scanRanges));
        List<ScanSource> nereidsInstances = source.parallelize(Collections.singletonList(node), 2);
        List<List<TScanRangeParams>> nereidsRanges = nereidsInstances.stream()
                .map(instance -> ((DefaultScanSource) instance).scanNodeToScanRanges.get(node).params)
                .collect(Collectors.toList());

        Assert.assertEquals(legacyInstances, nereidsRanges);
    }

    @Test
    public void testUnknownSizeAffinityGroupsRemainDistributed() {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        List<TScanRangeParams> params = Arrays.asList(scanRangeParams(-1), scanRangeParams(-1), scanRangeParams(-1));
        node.recordScanRangeFileAffinity(params.get(0).getScanRange(), "s3://bucket/a.parquet");
        node.recordScanRangeFileAffinity(params.get(1).getScanRange(), "s3://bucket/b.parquet");
        node.recordScanRangeFileAffinity(params.get(2).getScanRange(), "s3://bucket/c.parquet");

        List<List<Integer>> instances = node.splitScanRangeParamsByInstance(params, 3);

        Assert.assertEquals(3, instances.size());
        Assert.assertTrue(instances.stream().allMatch(instance -> instance.size() == 1));
    }

    @Test
    public void testProductionRangeCreationRecordsInstanceAffinity() throws Exception {
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        node.params = new TFileScanRangeParams();
        node.params.setFormatType(TFileFormatType.FORMAT_PARQUET);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(1L);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        Mockito.when(backend.getBePort()).thenReturn(9060);
        FileSplit first = new FileSplit(LocationPath.of("s3://bucket/a.parquet"),
                0, 100, 200, 0, null, Collections.emptyList());
        FileSplit second = new FileSplit(LocationPath.of("s3://bucket/a.parquet"),
                100, 100, 200, 0, null, Collections.emptyList());
        first.setFileAffinitySupported(true);
        second.setFileAffinitySupported(true);

        TScanRangeLocations firstRange = node.splitToScanRange(
                backend, Collections.emptyMap(), first, Collections.emptyList(), true);
        TScanRangeLocations secondRange = node.splitToScanRange(
                backend, Collections.emptyMap(), second, Collections.emptyList(), true);
        List<TScanRangeParams> params = Arrays.asList(
                new TScanRangeParams().setScanRange(firstRange.getScanRange()),
                new TScanRangeParams().setScanRange(secondRange.getScanRange()));

        List<List<TScanRangeParams>> instances = node.materializeScanRangeParamsByInstance(params, 2);

        Assert.assertEquals(1, instances.size());
        Assert.assertSame(params.get(0), instances.get(0).get(0));
        Assert.assertSame(params.get(1), instances.get(0).get(1));
    }

    private static TScanRangeParams scanRangeParams(long size) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setSize(size);
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.addToRanges(rangeDesc);
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);
        TScanRangeParams params = new TScanRangeParams();
        params.setScanRange(scanRange);
        return params;
    }

    private static TScanRangeParams splitSourceScanRangeParams(long splitSourceId) {
        TFileScanRange fileScanRange = new TFileScanRange();
        fileScanRange.setSplitSource(new TSplitSource().setSplitSourceId(splitSourceId));
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);
        return new TScanRangeParams().setScanRange(scanRange);
    }

    private static void assertSameInstance(
            List<List<TScanRangeParams>> rangesPerInstance, List<TScanRangeParams> expectedRanges) {
        List<Integer> matchingInstances = new ArrayList<>();
        for (int i = 0; i < rangesPerInstance.size(); i++) {
            if (containsIdentity(rangesPerInstance.get(i), expectedRanges.get(0))) {
                matchingInstances.add(i);
            }
        }
        Assert.assertEquals(1, matchingInstances.size());
        List<TScanRangeParams> matchingInstance = rangesPerInstance.get(matchingInstances.get(0));
        for (TScanRangeParams expectedRange : expectedRanges) {
            Assert.assertTrue(containsIdentity(matchingInstance, expectedRange));
        }
    }

    private static boolean containsIdentity(List<TScanRangeParams> ranges, TScanRangeParams expected) {
        return ranges.stream().anyMatch(range -> range == expected);
    }

}
