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

package org.apache.doris.nereids.trees.copier;

import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.JoinSkewInfo;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalPlanDeepCopierTest {

    @Test
    public void testDeepCopyOlapScan() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        relationPlan = (LogicalOlapScan) relationPlan.withOperativeSlots(relationPlan.getOutput());
        LogicalOlapScan aCopy =
                (LogicalOlapScan) relationPlan.accept(LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
        for (Slot opSlot : aCopy.getOperativeSlots()) {
            Assertions.assertTrue(aCopy.getOutputSet().contains(opSlot));
        }
    }

    @Test
    public void testDeepCopyOlapScanPreservesPartitionPruningState() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        relationPlan = relationPlan.withSelectedPartitionIds(relationPlan.getSelectedPartitionIds(), true);

        LogicalOlapScan copiedPlan =
                (LogicalOlapScan) relationPlan.accept(LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());

        Assertions.assertTrue(relationPlan.isPartitionPruned());
        Assertions.assertTrue(relationPlan.hasPartitionPredicate());
        Assertions.assertTrue(copiedPlan.isPartitionPruned());
        Assertions.assertTrue(copiedPlan.hasPartitionPredicate());
    }

    @Test
    public void testDeepCopyOlapScanInvalidatesPartitionPruning() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        relationPlan = relationPlan.withSelectedPartitionIds(relationPlan.getSelectedPartitionIds(), true);
        DeepCopierContext context = new DeepCopierContext();
        context.setInvalidatePartitionPruning(true);

        LogicalOlapScan copiedPlan =
                (LogicalOlapScan) relationPlan.accept(LogicalPlanDeepCopier.INSTANCE, context);

        Assertions.assertTrue(relationPlan.isPartitionPruned());
        Assertions.assertTrue(relationPlan.hasPartitionPredicate());
        Assertions.assertFalse(copiedPlan.isPartitionPruned());
        Assertions.assertTrue(copiedPlan.hasPartitionPredicate());
    }

    @Test
    public void testDeepCopyOlapScanWithNonFirstOperativeSlot() {
        LogicalOlapScan relationPlan = PlanConstructor.newLogicalOlapScan(0, "a", 0);
        relationPlan = (LogicalOlapScan) relationPlan.withOperativeSlots(
                ImmutableList.of(relationPlan.getOutput().get(1)));
        LogicalOlapScan aCopy =
                (LogicalOlapScan) relationPlan.accept(LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());

        Assertions.assertEquals(ImmutableList.of(aCopy.getOutput().get(1)), aCopy.getOperativeSlots());
    }

    @Test
    public void testDeepCopySchemaScanCopiesFrontendConjunctSlots() {
        LogicalSchemaScan scan = new LogicalSchemaScan(PlanConstructor.getNextRelationId(),
                SchemaTable.TABLE_MAP.get("table_stream_consumption"), ImmutableList.of("information_schema"));
        SlotReference dbName = (SlotReference) scan.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase("DB_NAME"))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
        scan = scan.withFrontendConjuncts(Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableList.of(new EqualTo(dbName, new VarcharLiteral("db1"))));

        LogicalSchemaScan copied = (LogicalSchemaScan) scan.accept(
                LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());
        Slot copiedConjunctSlot = copied.getFrontendConjuncts().get(0).getInputSlots().iterator().next();
        Slot copiedDbName = copied.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase("DB_NAME"))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        Assertions.assertNotEquals(dbName.getExprId(), copiedConjunctSlot.getExprId());
        Assertions.assertEquals(copiedDbName.getExprId(), copiedConjunctSlot.getExprId());
    }

    @Test
    public void testDeepCopyAggregateWithSourceRepeat() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        List<? extends NamedExpression> groupingKeys = scan.getOutput().subList(0, 1);
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(groupingKeys.get(0)),
                ImmutableList.of()
        );
        SlotReference groupingId = new SlotReference("grouping_id", BigIntType.INSTANCE, false);
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                scan.getOutput().stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                groupingId,
                RepeatType.GROUPING_SETS,
                scan
        );
        List<? extends NamedExpression> groupByExprs = repeat.getOutput().subList(0, 1).stream()
                .map(e -> (NamedExpression) e)
                .collect(ImmutableList.toImmutableList());
        List<? extends NamedExpression> outputExprs = repeat.getOutput();
        LogicalAggregate aggregate = new LogicalAggregate(
                groupByExprs,
                outputExprs,
                repeat
        );
        aggregate = aggregate.withSourceRepeat(repeat);
        DeepCopierContext context = new DeepCopierContext();
        LogicalAggregate<? extends Plan> copiedAggregate = (LogicalAggregate<? extends Plan>) aggregate.accept(
                LogicalPlanDeepCopier.INSTANCE,
                context
        );
        Assertions.assertTrue(copiedAggregate.getSourceRepeat().isPresent());

        Optional<LogicalRepeat<? extends Plan>> copiedRepeat =
                copiedAggregate.collectFirst(LogicalRepeat.class::isInstance);
        Assertions.assertTrue(copiedRepeat.isPresent());
        Assertions.assertSame(copiedAggregate.getSourceRepeat().get(), copiedRepeat.get());

        Assertions.assertNotSame(aggregate, copiedAggregate);
        Assertions.assertNotSame(repeat, copiedRepeat.get());
    }

    @Test
    public void testDeepCopyAggregateWithoutSourceRepeat() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        List<Expression> groupByExprs = scan.getOutput().subList(0, 1).stream()
                .map(e -> (Expression) e)
                .collect(ImmutableList.toImmutableList());
        List<? extends NamedExpression> outputExprs = scan.getOutput();

        LogicalAggregate aggregate = new LogicalAggregate(
                groupByExprs,
                outputExprs,
                scan
        );
        DeepCopierContext context = new DeepCopierContext();
        LogicalAggregate<? extends Plan> copiedAggregate = (LogicalAggregate<? extends Plan>) aggregate.accept(
                LogicalPlanDeepCopier.INSTANCE,
                context
        );
        Assertions.assertFalse(copiedAggregate.getSourceRepeat().isPresent());
        Assertions.assertNotSame(aggregate, copiedAggregate);
        Assertions.assertEquals(aggregate.getGroupByExpressions().size(),
                copiedAggregate.getGroupByExpressions().size());
    }

    @Test
    public void testDeepCopyJoinClonesTheDistributeHintOfTheCopy() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        Slot skew = left.getOutput().get(0);
        DistributeHint hint = new DistributeHint(DistributeType.NONE,
                new JoinSkewInfo(skew, ImmutableList.of(new BigIntLiteral(0)), false));
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))),
                ImmutableList.of(), hint, Optional.empty(), left, right, null);

        LogicalJoin<?, ?> copy = (LogicalJoin<?, ?>) join.accept(
                LogicalPlanDeepCopier.INSTANCE, new DeepCopierContext());

        // the copy reads its own slots, and SaltJoin records its status on the hint of the branch it
        // salted: neither the hint nor the skew expression may be shared between the two branches
        Assertions.assertNotSame(join.getDistributeHint(), copy.getDistributeHint());
        Assertions.assertNotSame(join.getDistributeHint().getSkewExpr(),
                copy.getDistributeHint().getSkewExpr());
        assertSkewExprReadsTheSlotsOfItsOwnBranch(join);
        assertSkewExprReadsTheSlotsOfItsOwnBranch(copy);
    }

    private static void assertSkewExprReadsTheSlotsOfItsOwnBranch(LogicalJoin<?, ?> join) {
        Set<ExprId> output = join.getOutput().stream().map(Slot::getExprId).collect(ImmutableSet.toImmutableSet());
        for (Slot slot : join.getDistributeHint().getSkewExpr().getInputSlots()) {
            Assertions.assertTrue(output.contains(slot.getExprId()),
                    "the hint of a join has to read the slots of its own branch: " + slot);
        }
    }
}
