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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TTargetExprMonotonicity;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.function.Function;

class RuntimeFilterPruneClassifierTest {
    @Test
    void testSingleColumnHashInSupported() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertTrue(classification.canPruneBuckets());
    }

    @Test
    void testInOrBloomSupportedAtPlanTime() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN_OR_BLOOM, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertTrue(classification.canPruneBuckets());
    }

    @Test
    void testBloomAndNonHashDistributionsRejected() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterPruneClassifier.Classification bloomClassification = classifyBucket(
                TRuntimeFilterType.BLOOM, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));
        RuntimeFilterPruneClassifier.Classification randomClassification = classifyBucket(
                TRuntimeFilterType.IN, distributionColumn, new RandomDistributionInfo(8));

        Assertions.assertFalse(bloomClassification.canPruneBuckets());
        Assertions.assertTrue(bloomClassification.getBucketUnsupportedReason().contains("IN"));
        Assertions.assertFalse(randomClassification.canPruneBuckets());
        Assertions.assertTrue(randomClassification.getBucketUnsupportedReason().contains("not HASH"));
    }

    @Test
    void testCompositeHashAndNonDistributionTargetsRejected() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        Column valueColumn = new Column("value_col", PrimitiveType.INT);
        RuntimeFilterPruneClassifier.Classification compositeClassification = classifyBucket(
                TRuntimeFilterType.IN, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn, valueColumn)));
        RuntimeFilterPruneClassifier.Classification nonDistributionClassification = classifyBucket(
                TRuntimeFilterType.IN, valueColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertFalse(compositeClassification.canPruneBuckets());
        Assertions.assertTrue(compositeClassification.getBucketUnsupportedReason().contains("single-column"));
        Assertions.assertFalse(nonDistributionClassification.canPruneBuckets());
        Assertions.assertTrue(nonDistributionClassification.getBucketUnsupportedReason()
                .contains("distribution column"));
    }

    @Test
    void testDirectMvAliasSupportedButComputedAliasRejected() {
        Column baseColumn = new Column("base_col", PrimitiveType.INT);
        SlotDescriptor baseSlotDescriptor = new SlotDescriptor(new SlotId(2), new TupleId(2));
        baseSlotDescriptor.setColumn(baseColumn);
        baseSlotDescriptor.setType(baseColumn.getType());

        Column directMvColumn = new Column("mv_base_col", PrimitiveType.INT);
        directMvColumn.setDefineExpr(new SlotRef(baseSlotDescriptor));
        RuntimeFilterPruneClassifier.Classification directClassification = classifyBucket(
                TRuntimeFilterType.IN, directMvColumn,
                new HashDistributionInfo(8, ImmutableList.of(baseColumn)), 1L, 2L);

        Column computedMvColumn = new Column("base_col", PrimitiveType.INT);
        computedMvColumn.setDefineExpr(new FunctionCallExpr("abs",
                ImmutableList.of(new SlotRef(baseSlotDescriptor)), true));
        RuntimeFilterPruneClassifier.Classification computedClassification = classifyBucket(
                TRuntimeFilterType.IN, computedMvColumn,
                new HashDistributionInfo(8, ImmutableList.of(baseColumn)), 1L, 2L);

        Assertions.assertTrue(directClassification.canPruneBuckets());
        Assertions.assertFalse(computedClassification.canPruneBuckets());
    }

    @Test
    void testNonBaseIndexWithoutDirectDefinitionRejected() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)), 1L, 2L);

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getBucketUnsupportedReason().contains("no direct base-column"));
    }

    @Test
    void testBaseColumnNamesComparedSymmetrically() {
        Column baseColumn = new Column("base_col", PrimitiveType.INT);
        SlotDescriptor baseSlotDescriptor = new SlotDescriptor(new SlotId(2), new TupleId(2));
        baseSlotDescriptor.setColumn(baseColumn);
        baseSlotDescriptor.setType(baseColumn.getType());

        Column distributionColumn = new Column("mv_dist_col", PrimitiveType.INT);
        distributionColumn.setDefineExpr(new SlotRef(baseSlotDescriptor));
        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN, baseColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertTrue(classification.canPruneBuckets());
    }

    @Test
    void testDifferentUniqueIdsAllowedForSameBaseColumn() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        distributionColumn.setUniqueId(1);
        Column targetColumn = new Column("dist_col", PrimitiveType.INT);
        targetColumn.setUniqueId(2);

        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN, targetColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertTrue(classification.canPruneBuckets());
    }

    @Test
    void testRollupUniqueIdCollisionRejectedForDifferentBaseColumns() {
        Column baseDistributionColumn = new Column("k2", PrimitiveType.INT);
        baseDistributionColumn.setUniqueId(1);

        Column baseTargetColumn = new Column("k1", PrimitiveType.INT);
        SlotDescriptor baseSlotDescriptor = new SlotDescriptor(new SlotId(2), new TupleId(2));
        baseSlotDescriptor.setColumn(baseTargetColumn);
        baseSlotDescriptor.setType(baseTargetColumn.getType());
        Column rollupTargetColumn = new Column("mv_k1", PrimitiveType.INT);
        rollupTargetColumn.setUniqueId(1);
        rollupTargetColumn.setDefineExpr(new SlotRef(baseSlotDescriptor));

        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN, rollupTargetColumn,
                new HashDistributionInfo(8, ImmutableList.of(baseDistributionColumn)));

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getBucketUnsupportedReason().contains("distribution column"));
    }

    @Test
    void testRollupWithDifferentTypeRejected() {
        Column baseDistributionColumn = new Column("dist_col", PrimitiveType.INT);
        SlotDescriptor baseSlotDescriptor = new SlotDescriptor(new SlotId(2), new TupleId(2));
        baseSlotDescriptor.setColumn(baseDistributionColumn);
        baseSlotDescriptor.setType(baseDistributionColumn.getType());
        Column rollupTargetColumn = new Column("mv_dist_col", PrimitiveType.BIGINT);
        rollupTargetColumn.setDefineExpr(new SlotRef(baseSlotDescriptor));

        RuntimeFilterPruneClassifier.Classification classification = classifyBucket(
                TRuntimeFilterType.IN, rollupTargetColumn,
                new HashDistributionInfo(8, ImmutableList.of(baseDistributionColumn)));

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getBucketUnsupportedReason().contains("distribution column"));
    }

    @Test
    void testFeatureGatesAvoidCatalogClassification() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableRuntimeFilterBucketPrune(false);
        sessionVariable.setEnableRuntimeFilterPartitionPrune(false);
        Column targetColumn = new Column("dist_col", PrimitiveType.INT);
        OlapTable table = Mockito.mock(OlapTable.class);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        SlotReference target = slot(targetColumn, table, 1);
        RuntimeFilter filter = newFilter(
                TRuntimeFilterType.IN, target, target, target.getDataType(), scan);

        RuntimeFilterPruneClassifier.Classification classification =
                RuntimeFilterPruneClassifier.classify(filter, sessionVariable);

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertFalse(classification.canPrunePartitions());
        Mockito.verifyNoInteractions(scan);
    }

    @Test
    void testDirectRangeAndListPartitionTargetsSupported() {
        RuntimeFilterPruneClassifier.Classification rangeClassification = classifyPartition(
                TRuntimeFilterType.IN_OR_BLOOM, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                target -> target);
        RuntimeFilterPruneClassifier.Classification listClassification = classifyPartition(
                TRuntimeFilterType.BLOOM, PartitionType.LIST, ListPartitionItem.DUMMY_ITEM,
                target -> target);

        assertSupportedIncreasingPartitions(rangeClassification);
        assertSupportedIncreasingPartitions(listClassification);
    }

    @Test
    void testPartitionUniqueIdCollisionRejected() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        partitionColumn.setUniqueId(0);
        Column baseTargetColumn = new Column("value_col", PrimitiveType.INT);
        baseTargetColumn.setUniqueId(1);
        Column rollupTargetColumn = directRollupColumn("part_col", baseTargetColumn);
        rollupTargetColumn.setUniqueId(0);

        RuntimeFilterPruneClassifier.Classification classification = classifyDirectPartition(
                rollupTargetColumn, partitionColumn, 1L, 2L);

        Assertions.assertFalse(classification.canPrunePartitions());
    }

    @Test
    void testDirectRollupPartitionColumnSupported() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        Column rollupTargetColumn = directRollupColumn("part_col", partitionColumn);

        RuntimeFilterPruneClassifier.Classification classification = classifyDirectPartition(
                rollupTargetColumn, partitionColumn, 1L, 2L);

        assertSupportedIncreasingPartitions(classification);
    }

    @Test
    void testRenamedRollupPartitionColumnRejected() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        Column rollupTargetColumn = directRollupColumn("mv_part_col", partitionColumn);

        RuntimeFilterPruneClassifier.Classification classification = classifyDirectPartition(
                rollupTargetColumn, partitionColumn, 1L, 2L);

        Assertions.assertFalse(classification.canPrunePartitions());
    }

    @Test
    void testNonBasePartitionTargetWithoutDefinitionRejected() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        Column rollupTargetColumn = new Column("part_col", PrimitiveType.INT);

        RuntimeFilterPruneClassifier.Classification classification = classifyDirectPartition(
                rollupTargetColumn, partitionColumn, 1L, 2L);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getPartitionUnsupportedReason()
                .contains("no direct base-column"));
    }

    @Test
    void testComputedRollupPartitionTargetRejected() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        Column rollupTargetColumn = new Column("part_col", PrimitiveType.INT);
        rollupTargetColumn.setDefineExpr(new FunctionCallExpr("abs",
                ImmutableList.of(directSlotRef(partitionColumn)), true));

        RuntimeFilterPruneClassifier.Classification classification = classifyDirectPartition(
                rollupTargetColumn, partitionColumn, 1L, 2L);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getPartitionUnsupportedReason()
                .contains("no direct base-column"));
    }

    @Test
    void testBloomRangePartitionRejected() {
        RuntimeFilterPruneClassifier.Classification classification = classifyPartition(
                TRuntimeFilterType.BLOOM, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM,
                target -> target);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getPartitionUnsupportedReason().contains("BLOOM"));
    }

    @Test
    void testNoneMovableListExpressionRejected() {
        RuntimeFilterPruneClassifier.Classification classification = classifyPartition(
                TRuntimeFilterType.IN, PartitionType.LIST, ListPartitionItem.DUMMY_ITEM,
                target -> new AssertTrue(
                        new GreaterThan(target, new IntegerLiteral(0)),
                        new VarcharLiteral("rfpp_expr_in_only_error")));

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getPartitionUnsupportedReason().contains("non-movable"));
    }

    @Test
    void testTypeAdjustedNonIdentityTargetRejectedBeforeTranslation() {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        OlapTable table = Mockito.mock(OlapTable.class);
        PartitionInfo partitionInfo = partitionInfo(
                partitionColumn, PartitionType.LIST, ListPartitionItem.DUMMY_ITEM);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        PhysicalOlapScan scan = scan(table, ImmutableList.of(1L, 2L));
        SlotReference target = slot(partitionColumn, table, 1);
        RuntimeFilter filter = newFilter(TRuntimeFilterType.IN, target,
                new Add(target, new IntegerLiteral(1)), DateTimeV2Type.SYSTEM_DEFAULT, scan);
        SessionVariable sessionVariable = partitionOnlySession();

        RuntimeFilterPruneClassifier.Classification classification =
                RuntimeFilterPruneClassifier.classify(filter, sessionVariable);

        Assertions.assertFalse(classification.canPrunePartitions());
        Assertions.assertTrue(classification.getPartitionUnsupportedReason().contains("type adjustment"));
    }

    @Test
    void testMonotonicChildMustOwnAllInputSlots() {
        SlotReference slot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        DateTrunc rejected = new DateTrunc(slot, slot);
        DateTrunc accepted = new DateTrunc(slot, new VarcharLiteral("day"));

        Assertions.assertFalse(RuntimeFilterPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                rejected, ((Monotonic) rejected).getMonotonicFunctionChildIndex()));
        Assertions.assertTrue(RuntimeFilterPruneClassifier.hasInputSlotOnlyInMonotonicChild(
                accepted, ((Monotonic) accepted).getMonotonicFunctionChildIndex()));
    }

    private RuntimeFilterPruneClassifier.Classification classifyBucket(
            TRuntimeFilterType filterType, Column targetColumn, DistributionInfo distributionInfo) {
        return classifyBucket(filterType, targetColumn, distributionInfo, 1L, 1L);
    }

    private RuntimeFilterPruneClassifier.Classification classifyBucket(
            TRuntimeFilterType filterType, Column targetColumn, DistributionInfo distributionInfo,
            long baseIndexId, long selectedIndexId) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(table.getBaseIndexId()).thenReturn(baseIndexId);
        Mockito.when(table.getPartition(1L)).thenReturn(partition);
        Mockito.when(partition.getDistributionInfo()).thenReturn(distributionInfo);
        PhysicalOlapScan scan = scan(table, ImmutableList.of(1L));
        Mockito.when(scan.getSelectedIndexId()).thenReturn(selectedIndexId);
        SlotReference target = slot(targetColumn, table, 1);
        RuntimeFilter filter = newFilter(
                filterType, target, target, target.getDataType(), scan);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableRuntimeFilterBucketPrune(true);
        sessionVariable.setEnableRuntimeFilterPartitionPrune(false);
        return RuntimeFilterPruneClassifier.classify(filter, sessionVariable);
    }

    private RuntimeFilterPruneClassifier.Classification classifyPartition(
            TRuntimeFilterType filterType, PartitionType partitionType, PartitionItem partitionItem,
            Function<SlotReference, Expression> targetFactory) {
        Column partitionColumn = new Column("part_col", PrimitiveType.INT);
        OlapTable table = Mockito.mock(OlapTable.class);
        PartitionInfo partitionInfo = partitionInfo(partitionColumn, partitionType, partitionItem);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        PhysicalOlapScan scan = scan(table, ImmutableList.of(1L, 2L));
        SlotReference target = slot(partitionColumn, table, 1);
        Expression targetExpression = targetFactory.apply(target);
        RuntimeFilter filter = newFilter(filterType, target,
                targetExpression, targetExpression.getDataType(), scan);
        return RuntimeFilterPruneClassifier.classify(filter, partitionOnlySession());
    }

    private RuntimeFilterPruneClassifier.Classification classifyDirectPartition(
            Column targetColumn, Column partitionColumn, long baseIndexId, long selectedIndexId) {
        OlapTable table = Mockito.mock(OlapTable.class);
        PartitionInfo partitionInfo = partitionInfo(
                partitionColumn, PartitionType.RANGE, RangePartitionItem.DUMMY_ITEM);
        Mockito.when(table.getBaseIndexId()).thenReturn(baseIndexId);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        PhysicalOlapScan scan = scan(table, ImmutableList.of(1L, 2L));
        Mockito.when(scan.getSelectedIndexId()).thenReturn(selectedIndexId);
        SlotReference target = slot(targetColumn, table, 1);
        RuntimeFilter filter = newFilter(
                TRuntimeFilterType.IN, target, target, target.getDataType(), scan);
        return RuntimeFilterPruneClassifier.classify(filter, partitionOnlySession());
    }

    private Column directRollupColumn(String name, Column baseColumn) {
        Column rollupColumn = new Column(name, baseColumn.getType());
        rollupColumn.setDefineExpr(directSlotRef(baseColumn));
        return rollupColumn;
    }

    private SlotRef directSlotRef(Column column) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(2), new TupleId(2));
        slotDescriptor.setColumn(column);
        slotDescriptor.setType(column.getType());
        return new SlotRef(slotDescriptor);
    }

    private PartitionInfo partitionInfo(
            Column partitionColumn, PartitionType partitionType, PartitionItem partitionItem) {
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(partitionInfo.getType()).thenReturn(partitionType);
        Mockito.when(partitionInfo.getPartitionColumns()).thenReturn(ImmutableList.of(partitionColumn));
        Mockito.when(partitionInfo.getItem(1L)).thenReturn(partitionItem);
        Mockito.when(partitionInfo.getItem(2L)).thenReturn(partitionItem);
        return partitionInfo;
    }

    private PhysicalOlapScan scan(OlapTable table, ImmutableList<Long> selectedPartitionIds) {
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(selectedPartitionIds);
        return scan;
    }

    private SlotReference slot(Column column, OlapTable table, int exprId) {
        return new SlotReference(new ExprId(exprId), column.getName(),
                DataType.fromCatalogType(column.getType()), column.isAllowNull(), ImmutableList.of("t"),
                table, column, table, column);
    }

    private RuntimeFilter newFilter(TRuntimeFilterType filterType, SlotReference target,
            Expression targetExpression, DataType sourceType, PhysicalOlapScan scan) {
        SlotReference source = new SlotReference("src", sourceType);
        AbstractPhysicalPlan builder = Mockito.mock(AbstractPhysicalPlan.class);
        return new RuntimeFilter(RuntimeFilterId.createGenerator().getNextId(),
                source, target, targetExpression, filterType, 0, builder, 10, false,
                TMinMaxRuntimeFilterType.MIN_MAX, scan);
    }

    private SessionVariable partitionOnlySession() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableRuntimeFilterBucketPrune(false);
        sessionVariable.setEnableRuntimeFilterPartitionPrune(true);
        return sessionVariable;
    }

    private void assertSupportedIncreasingPartitions(
            RuntimeFilterPruneClassifier.Classification classification) {
        Assertions.assertTrue(classification.canPrunePartitions());
        Map<Long, TTargetExprMonotonicity> monotonicity = classification.getPartitionMonotonicity();
        Assertions.assertEquals(2, monotonicity.size());
        Assertions.assertEquals(TTargetExprMonotonicity.MONOTONIC_INCREASING, monotonicity.get(1L));
        Assertions.assertEquals(TTargetExprMonotonicity.MONOTONIC_INCREASING, monotonicity.get(2L));
    }
}
