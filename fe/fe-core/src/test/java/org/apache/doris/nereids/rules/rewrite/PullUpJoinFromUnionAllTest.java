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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTestScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

class PullUpJoinFromUnionAllTest {

    @Test
    void comparatorRejectsDifferentProjectOutputSizes() {
        LogicalOlapScan smallScan = newScan(1, "common_small");
        LogicalOlapScan largeScan = newScan(1, "common_large");
        LogicalProject<LogicalOlapScan> smallProject = project(selectSlots(smallScan.getOutput(), 0), smallScan);
        LogicalProject<LogicalOlapScan> largeProject = project(selectSlots(largeScan.getOutput(), 0, 1), largeScan);

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(largeProject, smallProject));
    }

    @Test
    void comparatorRejectsDifferentFilterChildOutputSizes() {
        LogicalFilter<LogicalOlapScan> smallFilter = filter(newCachedOutputScan(1, "common_filter", 0));
        LogicalFilter<LogicalOlapScan> largeFilter = filter(newCachedOutputScan(1, "common_filter", 0, 1));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(largeFilter, smallFilter));
    }

    @Test
    void comparatorRejectsDifferentMaterializedIndexSelections() {
        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        LogicalOlapScan baseScan = newScanWithExtraIndex(11, "common_index", 101);
        LogicalOlapScan indexScan = baseScan.withMaterializedIndexSelected(101);

        Assertions.assertFalse(comparator.isLogicalEqual(baseScan, indexScan));
    }

    @Test
    void comparatorRejectsDifferentManuallySpecifiedPartitions() {
        LogicalOlapScan firstPartitionScan = PlanConstructor.newLogicalOlapScanWithSameId(
                12, "common_partition", 0, ImmutableList.of(1L));
        LogicalOlapScan secondPartitionScan = PlanConstructor.newLogicalOlapScanWithSameId(
                12, "common_partition", 0, ImmutableList.of(2L));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(firstPartitionScan, secondPartitionScan));
    }

    @Test
    void comparatorRejectsDifferentTableSamples() {
        LogicalOlapScan sampleOneScan = newScanWithTableSample(43, "common_sample",
                new org.apache.doris.nereids.trees.TableSample(10, true, 0));
        LogicalOlapScan sampleTwoScan = newScanWithTableSample(43, "common_sample",
                new org.apache.doris.nereids.trees.TableSample(20, true, 0));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(sampleOneScan, sampleTwoScan));
    }

    @Test
    void comparatorRejectsDifferentOlapScanParams() {
        LogicalOlapScan incrementalScan = newScanWithScanParams(44, "common_params",
                new TableScanParams(TableScanParams.INCREMENTAL_READ,
                        ImmutableMap.of("end_ts", "10"), ImmutableList.of()));
        LogicalOlapScan differentIncrementalScan = newScanWithScanParams(44, "common_params",
                new TableScanParams(TableScanParams.INCREMENTAL_READ,
                        ImmutableMap.of("end_ts", "20"), ImmutableList.of()));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(incrementalScan, differentIncrementalScan));
    }

    @Test
    void comparatorRejectsDifferentStreamReadModes() {
        LogicalOlapTableStreamScan snapshotScan = newStreamScanWithOffsets(15, "common_stream", 1000L, 2000L,
                KeysType.UNIQUE_KEYS, ImmutableMap.of(1L, Pair.of(10L, 20L)))
                .withReadMode(StreamReadMode.SNAPSHOT);
        LogicalOlapTableStreamScan resetScan = snapshotScan.withReadMode(StreamReadMode.RESET);

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(snapshotScan, resetScan));
    }

    @Test
    void comparatorRejectsDifferentStreamOffsets() {
        LogicalOlapTableStreamScan left = newStreamScanWithOffsets(45, "common_stream_offset", 1000L, 2000L,
                KeysType.UNIQUE_KEYS, ImmutableMap.of(1L, Pair.of(10L, 20L)));
        LogicalOlapTableStreamScan right = newStreamScanWithOffsets(45, "common_stream_offset", 1000L, 2000L,
                KeysType.UNIQUE_KEYS, ImmutableMap.of(1L, Pair.of(30L, 40L)));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(left, right));
    }

    @Test
    void comparatorRejectsDifferentStreamKeysTypes() {
        LogicalOlapTableStreamScan uniqueKeyScan = newStreamScanWithOffsets(46, "common_stream_keys", 1000L, 2000L,
                KeysType.UNIQUE_KEYS, ImmutableMap.of(1L, Pair.of(10L, 20L)));
        LogicalOlapTableStreamScan dupKeyScan = newStreamScanWithOffsets(46, "common_stream_keys", 1000L, 2000L,
                KeysType.DUP_KEYS, ImmutableMap.of(1L, Pair.of(10L, 20L)));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(uniqueKeyScan, dupKeyScan));
    }

    @Test
    void comparatorRejectsDifferentStreamIdentities() {
        LogicalOlapTableStreamScan left = newStreamScanWithOffsets(47, "common_stream_identity", 1000L, 2000L,
                KeysType.UNIQUE_KEYS, ImmutableMap.of(1L, Pair.of(10L, 20L)));
        LogicalOlapTableStreamScan right = newStreamScanWithOffsets(47, "common_stream_identity", 1000L, 3000L,
                KeysType.UNIQUE_KEYS, ImmutableMap.of(1L, Pair.of(10L, 20L)));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(left, right));
    }

    @Test
    void comparatorRejectsDifferentFileScanSnapshots() {
        LogicalFileScan versionOneScan = newFileScan(17L, TableSnapshot.versionOf("1"));
        LogicalFileScan versionTwoScan = newFileScan(17L, TableSnapshot.versionOf("2"));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(versionOneScan, versionTwoScan));
    }

    @Test
    void comparatorRejectsDifferentSchemaScanConjuncts() {
        LogicalSchemaScan baseScan = newSchemaScan(16, "schema_common");
        LogicalSchemaScan filteredScan = baseScan.withFrontendConjuncts(Optional.of("ctl"),
                Optional.of("db"), Optional.of("tbl"),
                ImmutableList.of(new EqualTo(baseScan.getOutput().get(0), baseScan.getOutput().get(1))));

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertFalse(comparator.isLogicalEqual(baseScan, filteredScan));
    }

    @Test
    void comparatorAcceptsSameOdbcScanWithDifferentRelationIds() {
        LogicalOdbcScan left = newOdbcScan(18L, "odbc_common");
        LogicalOdbcScan right = left.withRelationId(PlanConstructor.getNextRelationId());

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertTrue(comparator.isLogicalEqual(left, right));
    }

    @Test
    void comparatorAcceptsSameTestScan() {
        LogicalTestScan left = newTestScan(19L, "test_common");
        LogicalTestScan right = newTestScan(19L, "test_common");

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertTrue(comparator.isLogicalEqual(left, right));
    }

    @Test
    void comparatorAcceptsSameScanWithDifferentRelationIds() {
        LogicalOlapScan left = newScan(13, "common_relation_id");
        LogicalOlapScan right = left.withRelationId(PlanConstructor.getNextRelationId());

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertTrue(comparator.isLogicalEqual(left, right));
    }

    @Test
    void comparatorAcceptsSameScanWithDifferentTableAliases() {
        LogicalOlapScan left = newScan(14, "common_alias").withTableAlias("left_alias");
        LogicalOlapScan right = newScan(14, "common_alias").withTableAlias("right_alias");

        PullUpJoinFromUnionAll.LogicalPlanComparator comparator =
                new PullUpJoinFromUnionAll().new LogicalPlanComparator();
        Assertions.assertTrue(comparator.isLogicalEqual(left, right));
    }

    @Test
    void ruleSkipsJoinChildrenWithDifferentFilteredCommonSideOutputs() {
        LogicalOlapScan commonSmallScan = newScan(10, "common_small");
        LogicalOlapScan commonLargeScan = newScan(10, "common_large");
        LogicalFilter<LogicalOlapScan> commonSmall = filter(commonSmallScan.withCachedOutput(
                selectSlotsAsSlots(commonSmallScan.getOutput(), 0)));
        LogicalFilter<LogicalOlapScan> commonLarge = filter(commonLargeScan.withCachedOutput(
                selectSlotsAsSlots(commonLargeScan.getOutput(), 0, 1)));
        LogicalOlapScan otherLeft = newScan(20, "other_left");
        LogicalOlapScan otherRight = newScan(30, "other_right");

        LogicalProject<Plan> unionChild1 = outputProject(
                join(commonSmall, otherLeft),
                "x", commonSmall.getOutput().get(0),
                "y", otherLeft.getOutput().get(1));
        LogicalProject<Plan> unionChild2 = outputProject(
                join(commonLarge, otherRight),
                "x", commonLarge.getOutput().get(0),
                "y", otherRight.getOutput().get(1));

        LogicalUnion union = union(unionChild1, unionChild2);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), union)
                .applyTopDown(new PullUpJoinFromUnionAll())
                .getPlan();

        Assertions.assertInstanceOf(LogicalUnion.class, rewritten);
        LogicalUnion rewrittenUnion = (LogicalUnion) rewritten;
        Assertions.assertEquals(2, rewrittenUnion.children().size());
        Assertions.assertInstanceOf(LogicalProject.class, rewrittenUnion.child(0));
        Assertions.assertInstanceOf(LogicalProject.class, rewrittenUnion.child(1));
    }

    @Test
    void ruleSkipsJoinChildrenWithDifferentCommonSideTabletScopes() {
        LogicalOlapScan commonLeft = newScan(40, "common_tablet").withSelectedTabletIds(ImmutableList.of(1L));
        LogicalOlapScan commonRight = newScan(40, "common_tablet").withSelectedTabletIds(ImmutableList.of(2L));
        LogicalOlapScan otherLeft = newScan(41, "other_left_tablet");
        LogicalOlapScan otherRight = newScan(42, "other_right_tablet");

        LogicalProject<Plan> unionChild1 = outputProject(
                join(commonLeft, otherLeft),
                "x", commonLeft.getOutput().get(0),
                "y", otherLeft.getOutput().get(1));
        LogicalProject<Plan> unionChild2 = outputProject(
                join(commonRight, otherRight),
                "x", commonRight.getOutput().get(0),
                "y", otherRight.getOutput().get(1));

        LogicalUnion union = union(unionChild1, unionChild2);

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), union)
                .applyTopDown(new PullUpJoinFromUnionAll())
                .getPlan();

        Assertions.assertInstanceOf(LogicalUnion.class, rewritten);
    }

    private static LogicalOlapScan newScan(long tableId, String tableName) {
        return PlanConstructor.newLogicalOlapScan(tableId, tableName, 0);
    }

    private static LogicalOlapScan newScanWithTableSample(long tableId, String tableName,
            org.apache.doris.nereids.trees.TableSample tableSample) {
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.newOlapTable(tableId, tableName, 0), ImmutableList.of("db"),
                ImmutableList.of(), ImmutableList.of(), Optional.of(tableSample), ImmutableList.of());
    }

    private static LogicalOlapScan newScanWithScanParams(long tableId, String tableName, TableScanParams scanParams) {
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.newOlapTable(tableId, tableName, 0), ImmutableList.of("db"),
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of(),
                Optional.of(scanParams));
    }

    private static LogicalOlapTableStreamScan newStreamScan(long tableId, String tableName) {
        OlapTableStreamWrapper table = Mockito.mock(OlapTableStreamWrapper.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getDatabase()).thenReturn(null);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        Mockito.when(table.getBaseSchema(false)).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        Mockito.when(table.getBaseSchema()).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        return new LogicalOlapTableStreamScan(PlanConstructor.getNextRelationId(),
                table, ImmutableList.of("db"),
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of());
    }

    private static LogicalOlapTableStreamScan newStreamScanWithOffsets(long tableId, String tableName,
            long streamDbId, long streamId, KeysType keysType, ImmutableMap<Long, Pair<Long, Long>> offsets) {
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getId()).thenReturn(tableId);
        Mockito.when(baseTable.getDatabase()).thenReturn(null);
        Mockito.when(baseTable.getName()).thenReturn(tableName);

        OlapTableStreamWrapper table = Mockito.mock(OlapTableStreamWrapper.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getDatabase()).thenReturn(null);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getBaseTable()).thenReturn(baseTable);
        Mockito.when(table.getStreamDbId()).thenReturn(streamDbId);
        Mockito.when(table.getStreamId()).thenReturn(streamId);
        Mockito.when(table.getStreamKeysType()).thenReturn(keysType);
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        Mockito.when(table.getBaseSchema(false)).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        Mockito.when(table.getBaseSchema()).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        Mockito.when(table.getOutputUpdateMap()).thenReturn(offsets);
        return new LogicalOlapTableStreamScan(PlanConstructor.getNextRelationId(),
                table, ImmutableList.of("db"), ImmutableList.of(1L),
                ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of());
    }

    private static LogicalFileScan newFileScan(long tableId, TableSnapshot snapshot) {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getDatabase()).thenReturn(null);
        Mockito.when(table.getName()).thenReturn("ext_common");
        Mockito.when(table.initSelectedPartitions(Mockito.any()))
                .thenReturn(LogicalFileScan.SelectedPartitions.NOT_PRUNED);
        Mockito.when(table.getBaseSchema()).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        return new LogicalFileScan(new RelationId(1), table, Collections.singletonList("db"),
                Collections.emptyList(), Optional.empty(), Optional.of(snapshot), Optional.empty(), Optional.empty());
    }

    private static LogicalSchemaScan newSchemaScan(long tableId, String tableName) {
        return new LogicalSchemaScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.newOlapTable(tableId, tableName, 0), ImmutableList.of("db"));
    }

    private static LogicalOdbcScan newOdbcScan(long tableId, String tableName) {
        OdbcTable table = Mockito.mock(OdbcTable.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        Mockito.when(table.getDatabase()).thenReturn(null);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getBaseSchema()).thenReturn(ImmutableList.of(new Column("id", Type.INT, true)));
        return new LogicalOdbcScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("db"));
    }

    private static LogicalTestScan newTestScan(long tableId, String tableName) {
        return new LogicalTestScan(PlanConstructor.getNextRelationId(),
                PlanConstructor.newOlapTable(tableId, tableName, 0), ImmutableList.of("db"));
    }

    private static LogicalOlapScan newScanWithExtraIndex(long tableId, String tableName, long indexId) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, tableName, 0, KeysType.DUP_KEYS);
        table.setIndexMeta(indexId, tableName + "_mv", table.getFullSchema(),
                0, 0, (short) 0, org.apache.doris.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table, ImmutableList.of("db"));
    }

    private static LogicalOlapScan newCachedOutputScan(long tableId, String tableName, int... indexes) {
        LogicalOlapScan scan = newScan(tableId, tableName);
        return scan.withCachedOutput(selectSlotsAsSlots(scan.getOutput(), indexes));
    }

    private static LogicalFilter<LogicalOlapScan> filter(LogicalOlapScan child) {
        return new LogicalFilter<>(ImmutableSet.of(new EqualTo(child.getOutput().get(0), child.getOutput().get(0))),
                child);
    }

    private static LogicalJoin<Plan, LogicalOlapScan> join(Plan commonSide,
            LogicalOlapScan otherSide) {
        Expression joinCondition = new EqualTo(commonSide.getOutput().get(0), otherSide.getOutput().get(0));
        return new LogicalJoin<>(JoinType.INNER_JOIN, ImmutableList.of(joinCondition), ImmutableList.of(),
                commonSide, otherSide, null);
    }

    private static LogicalProject<Plan> outputProject(Plan child, String leftAlias, Slot leftSlot,
            String rightAlias, Slot rightSlot) {
        return new LogicalProject<>(ImmutableList.of(
                new Alias(leftSlot, leftAlias),
                new Alias(rightSlot, rightAlias)), child);
    }

    private static LogicalUnion union(LogicalProject<Plan> left, LogicalProject<Plan> right) {
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (Slot slot : left.getOutput()) {
            outputs.add(new SlotReference(slot.getName(), slot.getDataType(), slot.nullable()));
        }
        return new LogicalUnion(Qualifier.ALL, outputs.build(), ImmutableList.of(
                toSlotReferences(left.getOutput()),
                toSlotReferences(right.getOutput())), ImmutableList.of(), false, ImmutableList.of(left, right));
    }

    private static LogicalProject<LogicalOlapScan> project(List<NamedExpression> outputs, LogicalOlapScan child) {
        return new LogicalProject<>(outputs, child);
    }

    private static List<NamedExpression> selectSlots(List<Slot> output, int... indexes) {
        ImmutableList.Builder<NamedExpression> selected = ImmutableList.builder();
        for (int index : indexes) {
            selected.add(output.get(index));
        }
        return selected.build();
    }

    private static List<Slot> selectSlotsAsSlots(List<Slot> output, int... indexes) {
        ImmutableList.Builder<Slot> selected = ImmutableList.builder();
        for (int index : indexes) {
            selected.add(output.get(index));
        }
        return selected.build();
    }

    private static List<SlotReference> toSlotReferences(List<Slot> slots) {
        ImmutableList.Builder<SlotReference> references = ImmutableList.builder();
        for (Slot slot : slots) {
            references.add((SlotReference) slot);
        }
        return references.build();
    }
}
