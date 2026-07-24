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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TTargetExprMonotonicity;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.function.Function;

class RuntimeFilterPartitionPruneClassifierTest {
    @Test
    void testRejectInputSlotOutsideMonotonicChild() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc dateTrunc = new DateTrunc(slot, slot);
        Monotonic monotonic = dateTrunc;

        Assertions.assertEquals(1, dateTrunc.getInputSlots().size());
        Assertions.assertFalse(RuntimeFilterPartitionPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                dateTrunc, monotonic.getMonotonicFunctionChildIndex()));
    }

    @Test
    void testAcceptInputSlotOnlyInMonotonicChild() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc dateTrunc = new DateTrunc(slot, new VarcharLiteral("day"));
        Monotonic monotonic = dateTrunc;

        Assertions.assertTrue(RuntimeFilterPartitionPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                dateTrunc, monotonic.getMonotonicFunctionChildIndex()));
    }

    @Test
    void testBloomRangePartitionUnsupported() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.BLOOM, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("BLOOM"));
        Assertions.assertTrue(classification.getPartitionMonotonicity().isEmpty());
    }

    @Test
    void testBloomListPartitionSupported() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.BLOOM, PartitionType.LIST, ListPartitionItem.DUMMY_ITEM);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testInOrBloomRangePartitionStillSupported() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN_OR_BLOOM, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testSyncMvAliasOfPartitionColumnSupported() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column mvColumn = createSyncMvColumn("mv_part_col", 0, partitionColumn);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testSyncMvAliasMatchesCopiedBaseColumnByUniqueId() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column copiedBaseColumn = new Column("part_col", PrimitiveType.INT);
        copiedBaseColumn.setUniqueId(1);
        copiedBaseColumn.setComment("copied metadata");
        Column mvColumn = createSyncMvColumn("mv_part_col", 0, copiedBaseColumn);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testSyncMvAliasMatchesLegacyCopiedBaseColumnByEquality() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        Column copiedBaseColumn = new Column("part_col", PrimitiveType.INT);
        Column mvColumn = createSyncMvColumn("mv_part_col", 0, copiedBaseColumn);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testSyncMvAliasUsesLineageInsteadOfIndexLocalUniqueId() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column nonPartitionColumn = new Column("other_col", PrimitiveType.INT);
        Column mvColumn = createSyncMvColumn("mv_other_col", 1, nonPartitionColumn);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("not a partition column"));
    }

    @Test
    void testSyncMvAliasRejectsSameNamedBaseColumnWithDifferentIdentity() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column differentBaseColumn = new Column("part_col", PrimitiveType.INT);
        differentBaseColumn.setUniqueId(2);
        Column mvColumn = createSyncMvColumn("mv_part_col", 0, differentBaseColumn);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("not a partition column"));
    }

    @Test
    void testSyncMvExpressionRejectsIndexLocalUniqueIdCollision() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column mvColumn = new Column("mv_expr", PrimitiveType.INT);
        mvColumn.setUniqueId(1);
        mvColumn.setDefineExpr(new IntLiteral(1));

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("not a partition column"));
    }

    @Test
    void testAggregateSyncMvColumnIsNotValuePreservingAlias() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column mvColumn = createSyncMvColumn("sum_part_col", 1, partitionColumn);
        mvColumn.setAggregationType(AggregateType.SUM, false);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, mvColumn);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("not a partition column"));
    }

    @Test
    void testOrdinaryRollupRejectsCrossIndexUniqueIdCollision() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column rollupValueColumn = new Column("value_col", PrimitiveType.INT);
        rollupValueColumn.setUniqueId(1);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, rollupValueColumn, 2L);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("not a partition column"));
    }

    @Test
    void testOrdinaryRollupPartitionColumnMatchesByBaseName() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(1);
        Column rollupPartitionColumn = new Column("part_col", PrimitiveType.INT);
        rollupPartitionColumn.setUniqueId(0);

        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.MIN_MAX, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                partitionColumn, rollupPartitionColumn, 2L);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testRejectNoneMovableListTargetExpression() {
        RuntimeFilterPartitionPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN, PartitionType.LIST, ListPartitionItem.DUMMY_ITEM,
                targetSlot -> new FunctionCallExpr("assert_true", ImmutableList.of(
                        new BinaryPredicate(BinaryPredicate.Operator.NE, targetSlot, new IntLiteral(0)),
                        new StringLiteral("rfpp_expr_in_only_error")), false),
                targetSlot -> new AssertTrue(
                        new GreaterThan(targetSlot, new IntegerLiteral(0)),
                        new VarcharLiteral("rfpp_expr_in_only_error")));

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("non-movable"));
        Assertions.assertTrue(classification.getPartitionMonotonicity().isEmpty());
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem) {
        return classify(filterType, partitionType, partitionItem, targetSlot -> targetSlot,
                targetSlot -> targetSlot);
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem,
            Function<SlotRef, Expr> legacyTargetFactory,
            Function<SlotReference, Expression> nereidsTargetFactory) {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        return classify(filterType, partitionType, partitionItem, partitionColumn, partitionColumn,
                legacyTargetFactory, nereidsTargetFactory);
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem,
            Column partitionColumn, Column targetColumn) {
        long selectedIndexId = targetColumn.isMaterializedViewColumn() ? 2L : 1L;
        return classify(filterType, partitionType, partitionItem, partitionColumn, targetColumn,
                selectedIndexId);
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem,
            Column partitionColumn, Column targetColumn, long selectedIndexId) {
        return classify(filterType, partitionType, partitionItem, partitionColumn, targetColumn,
                selectedIndexId, targetSlot -> targetSlot, targetSlot -> targetSlot);
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem,
            Column partitionColumn, Column targetColumn,
            Function<SlotRef, Expr> legacyTargetFactory,
            Function<SlotReference, Expression> nereidsTargetFactory) {
        return classify(filterType, partitionType, partitionItem, partitionColumn, targetColumn,
                1L, legacyTargetFactory, nereidsTargetFactory);
    }

    private RuntimeFilterPartitionPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem,
            Column partitionColumn, Column targetColumn, long selectedIndexId,
            Function<SlotRef, Expr> legacyTargetFactory,
            Function<SlotReference, Expression> nereidsTargetFactory) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), new TupleId(1));
        slotDescriptor.setColumn(targetColumn);
        slotDescriptor.setType(targetColumn.getType());
        SlotRef targetSlot = new SlotRef(slotDescriptor);
        SlotReference nereidsTarget = new SlotReference(targetColumn.getName(), IntegerType.INSTANCE);

        OlapTable table = Mockito.mock(OlapTable.class);
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getSelectedIndexId()).thenReturn(selectedIndexId);
        Mockito.when(scanNode.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L, 2L));
        Mockito.when(table.getBaseIndexId()).thenReturn(1L);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getType()).thenReturn(partitionType);
        Mockito.when(partitionInfo.getPartitionColumns()).thenReturn(ImmutableList.of(partitionColumn));
        Mockito.when(partitionInfo.getItem(1L)).thenReturn(partitionItem);
        Mockito.when(partitionInfo.getItem(2L)).thenReturn(partitionItem);

        return RuntimeFilterPartitionPruneClassifier.classify(filterType,
                legacyTargetFactory.apply(targetSlot), nereidsTargetFactory.apply(nereidsTarget), scanNode);
    }

    private Column createSyncMvColumn(String name, int uniqueId, Column baseColumn) {
        SlotDescriptor baseSlotDescriptor = new SlotDescriptor(new SlotId(2), new TupleId(2));
        baseSlotDescriptor.setColumn(baseColumn);
        baseSlotDescriptor.setType(baseColumn.getType());
        Column mvColumn = new Column(name, PrimitiveType.INT);
        mvColumn.setUniqueId(uniqueId);
        mvColumn.setDefineExpr(new SlotRef(baseSlotDescriptor));
        return mvColumn;
    }

    private void assertSupportedIncreasingPartitions(
            RuntimeFilterPartitionPruneClassifier.Classification classification) {
        Assertions.assertTrue(classification.canPrunePartitions());
        Assertions.assertEquals(1, classification.getPartitionSlot().getSlotId().asInt());
        Map<Long, TTargetExprMonotonicity> monotonicity = classification.getPartitionMonotonicity();
        Assertions.assertEquals(2, monotonicity.size());
        Assertions.assertEquals(TTargetExprMonotonicity.MONOTONIC_INCREASING, monotonicity.get(1L));
        Assertions.assertEquals(TTargetExprMonotonicity.MONOTONIC_INCREASING, monotonicity.get(2L));
    }
}
