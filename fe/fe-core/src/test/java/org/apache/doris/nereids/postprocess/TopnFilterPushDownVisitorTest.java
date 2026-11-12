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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.processor.post.TopnFilterContext;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor;
import org.apache.doris.nereids.processor.post.TopnFilterPushDownVisitor.PushDownContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DistributionSpecGather;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate.PushDownAggOp;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TopnFilterPushDownVisitorTest {
    private final SlotReference key = new SlotReference("k", IntegerType.INSTANCE);

    private PhysicalOlapScan scan(SlotReference slot) {
        OlapTable table = PlanConstructor.newOlapTable(10001, "strict_scan", 0);
        LogicalProperties properties = new LogicalProperties(() -> List.of(slot), () -> DataTrait.EMPTY_TRAIT);
        return new PhysicalOlapScan(PlanConstructor.getNextRelationId(), table,
                List.of("test"), -1L, List.of(), List.of(), null, PreAggStatus.on(), List.of(), Optional.empty(),
                properties, Optional.empty(), List.of(), List.of(), List.of(), Optional.empty(), Optional.empty(),
                List.of(), Optional.empty());
    }

    private PhysicalTopN<Plan> source(Slot slot) {
        PhysicalTopN<Plan> topn = Mockito.mock(PhysicalTopN.class);
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(slot, true, false)));
        Mockito.when(topn.getSortPhase()).thenReturn(SortPhase.LOCAL_SORT);
        Mockito.when(topn.getLimit()).thenReturn(3L);
        Mockito.when(topn.getOffset()).thenReturn(2L);
        return topn;
    }

    private TopnFilterContext push(Plan plan, PhysicalTopN<?> source) {
        TopnFilterContext context = new TopnFilterContext();
        plan.accept(new TopnFilterPushDownVisitor(context),
                new PushDownContext(source, source.getOrderKeys().get(0).getExpr(), false));
        return context;
    }

    private void assertEligibility(Plan plan, PhysicalTopN<?> source, boolean strict) {
        TopnFilterContext context = push(plan, source);
        Assertions.assertEquals(1, context.getTopnFilters().size());
        Assertions.assertEquals(strict, context.getTopnFilters().get(0).isStrictEligible());
    }

    @Test
    public void testSourceCandidateCountAndOrderKeys() {
        PhysicalOlapScan scan = scan(key);
        PhysicalTopN<?> topn = source(key);
        assertEligibility(scan, topn, true);
        for (long[] counts : List.of(new long[] {0, 0}, new long[] {1, -1},
                new long[] {Long.MAX_VALUE, 1}, new long[] {3, 2}, new long[] {Long.MAX_VALUE, 0})) {
            Mockito.when(topn.getLimit()).thenReturn(counts[0]);
            Mockito.when(topn.getOffset()).thenReturn(counts[1]);
            assertEligibility(scan, topn, counts[0] > 0 && counts[1] >= 0
                    && counts[0] <= Long.MAX_VALUE - counts[1]);
        }
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, true, false),
                new OrderKey(new IntegerLiteral(1), true, false)));
        assertEligibility(scan, topn, false);
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(new Add(key, new IntegerLiteral(1)),
                true, false)));
        assertEligibility(scan, topn, false);
        Mockito.when(topn.getOrderKeys()).thenReturn(List.of(new OrderKey(key, true, false)));
        Mockito.when(topn.getSortPhase()).thenReturn(SortPhase.MERGE_SORT);
        assertEligibility(scan, topn, false);
    }

    @Test
    public void testSafeUnaryNodesAndExactAliases() {
        PhysicalOlapScan scan = scan(key);
        Alias alias = new Alias(key, "alias");
        Plan plan = new PhysicalProject<>(List.of(alias), null,
                new PhysicalFilter<>(Set.of(new EqualTo(key, new IntegerLiteral(1))), null,
                        new PhysicalDistribute<>(DistributionSpecGather.INSTANCE,
                                new PhysicalQuickSort<>(List.of(new OrderKey(key, true, false)),
                                        SortPhase.LOCAL_SORT, null, scan))));
        assertEligibility(plan, source(alias.toSlot()), true);
        Alias computed = new Alias(new Add(key, new IntegerLiteral(1)), "computed");
        assertEligibility(new PhysicalProject<>(List.of(computed), null, scan), source(computed.toSlot()), false);
    }

    @Test
    public void testUnknownLimitAndHiddenStorageAggregateKeepClosedFilters() {
        PhysicalOlapScan scan = scan(key);
        PhysicalTopN<?> topn = source(key);
        assertEligibility(new PhysicalLimit<>(10, 0, LimitPhase.LOCAL, null, scan), topn, false);
        assertEligibility(new PhysicalLimit<>(10, 1, LimitPhase.LOCAL, null, scan), topn, false);
        // No child is exposed by this wrapper: PlanVisitor unwraps its relation directly.
        assertEligibility(new PhysicalStorageLayerAggregate(scan, PushDownAggOp.MIN_MAX), topn, false);
        Plan unknown = Mockito.mock(Plan.class);
        Mockito.when(unknown.children()).thenReturn(List.of(scan));
        TopnFilterContext context = new TopnFilterContext();
        new TopnFilterPushDownVisitor(context).visit(unknown, new PushDownContext(topn, key, false));
        Assertions.assertFalse(context.getTopnFilters().get(0).isStrictEligible());
        // Rebinding an alias after a downgrade must not re-enable strict bounds.
        Alias alias = new Alias(key, "alias");
        PhysicalProject<?> project = new PhysicalProject<>(List.of(alias), null, scan);
        context = new TopnFilterContext();
        project.accept(new TopnFilterPushDownVisitor(context),
                new PushDownContext(source(alias.toSlot()), alias.toSlot(), false).disableStrict());
        Assertions.assertFalse(context.getTopnFilters().get(0).isStrictEligible());
    }

    @Test
    public void testRepeatOnlyForCommonGroupingKeys() {
        PhysicalOlapScan scan = scan(key);
        SlotReference groupingId = new SlotReference("grouping_id", IntegerType.INSTANCE);
        PhysicalRepeat<?> common = new PhysicalRepeat<>(List.of(List.of(key), List.of(key, groupingId)),
                List.of(key, groupingId), groupingId, null, scan);
        assertEligibility(common, source(key), true);
        PhysicalRepeat<?> erased = new PhysicalRepeat<>(List.of(List.of(key), List.of()),
                List.of(key, groupingId), groupingId, null, scan);
        Assertions.assertTrue(push(erased, source(key)).getTopnFilters().isEmpty());
    }

    @Test
    public void testExistingBarriersStayClosedToPushdown() {
        for (Class<? extends Plan> type : List.of(PhysicalTopN.class, PhysicalWindow.class,
                PhysicalAssertNumRows.class, PhysicalCTEAnchor.class, PhysicalCTEProducer.class)) {
            Plan barrier = Mockito.mock(type);
            Mockito.doCallRealMethod().when(barrier).accept(Mockito.any(), Mockito.any());
            Assertions.assertTrue(push(barrier, source(key)).getTopnFilters().isEmpty(), type.getSimpleName());
        }
    }

    @Test
    public void testUnionActualTargetsAndChildOrder() {
        SlotReference other = new SlotReference("other", IntegerType.INSTANCE);
        SlotReference output = new SlotReference("output", IntegerType.INSTANCE);
        PhysicalOlapScan first = scan(key);
        PhysicalOlapScan second = scan(other);
        for (boolean reverse : List.of(false, true)) {
            for (boolean unsafe : List.of(false, true)) {
                Plan firstBranch = unsafe ? new PhysicalStorageLayerAggregate(first, PushDownAggOp.MIN_MAX) : first;
                PhysicalUnion union = new PhysicalUnion(Qualifier.ALL, List.of(output),
                        reverse ? List.of(List.of(other), List.of(key)) : List.of(List.of(key), List.of(other)),
                        List.of(), null, reverse ? List.of(second, firstBranch) : List.of(firstBranch, second));
                TopnFilterContext context = push(union, source(output));
                Assertions.assertEquals(2, context.getTopnFilters().get(0).targets.size());
                Assertions.assertEquals(!unsafe, context.getTopnFilters().get(0).isStrictEligible());
            }
        }
        PhysicalWindow<?> window = Mockito.mock(PhysicalWindow.class);
        Mockito.doCallRealMethod().when(window).accept(Mockito.any(), Mockito.any());
        Mockito.when(window.getAllChildrenTypes()).thenReturn(new BitSet());
        PhysicalUnion union = new PhysicalUnion(Qualifier.ALL, List.of(output),
                List.of(List.of(key), List.of(other)), List.of(), null, List.of(first, window));
        TopnFilterContext context = push(union, source(output));
        Assertions.assertEquals(1, context.getTopnFilters().get(0).targets.size());
        Assertions.assertTrue(context.getTopnFilters().get(0).isStrictEligible());
    }

    @Test
    public void testJoinTypesAndEqualitySubstitution() {
        SlotReference rightKey = new SlotReference("right_k", IntegerType.INSTANCE);
        PhysicalOlapScan left = scan(key);
        PhysicalOlapScan right = scan(rightKey);
        Set<JoinType> strictJoins = Set.of(JoinType.INNER_JOIN, JoinType.CROSS_JOIN, JoinType.LEFT_OUTER_JOIN,
                JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_ANTI_JOIN);
        for (JoinType type : JoinType.values()) {
            for (boolean mark : List.of(false, true)) {
                PhysicalHashJoin<Plan, Plan> hash = Mockito.mock(PhysicalHashJoin.class);
                Mockito.when(hash.getJoinType()).thenReturn(type);
                Mockito.when(hash.isMarkJoin()).thenReturn(mark);
                Mockito.when(hash.left()).thenReturn(left);
                Mockito.when(hash.right()).thenReturn(right);
                Mockito.when(hash.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(key, rightKey)));
                Mockito.doCallRealMethod().when(hash).accept(Mockito.any(), Mockito.any());
                PhysicalNestedLoopJoin<Plan, Plan> nested = Mockito.mock(PhysicalNestedLoopJoin.class);
                Mockito.when(nested.getJoinType()).thenReturn(type);
                Mockito.when(nested.isMarkJoin()).thenReturn(mark);
                Mockito.when(nested.left()).thenReturn(left);
                Mockito.doCallRealMethod().when(nested).accept(Mockito.any(), Mockito.any());
                for (Plan join : List.of(hash, nested)) {
                    TopnFilterContext context = push(join, source(key));
                    if (!context.getTopnFilters().isEmpty()) {
                        Assertions.assertEquals(!mark && strictJoins.contains(type),
                                context.getTopnFilters().get(0).isStrictEligible(), type + " mark=" + mark);
                    }
                }
                // Retain existing equality-substituted filters, but never grant them strict bounds.
                TopnFilterContext substituted = push(hash, source(rightKey));
                Assertions.assertTrue(substituted.getTopnFilters().stream().noneMatch(f -> f.isStrictEligible()));
            }
        }
    }
}
