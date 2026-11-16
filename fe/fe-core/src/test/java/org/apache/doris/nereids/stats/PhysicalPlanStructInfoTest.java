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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.DistributionSpecAny;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlanShallowCopier;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

class PhysicalPlanStructInfoTest {
    private final LogicalOlapScan a = PlanConstructor.newLogicalOlapScan(0, "a", 0);
    private final LogicalOlapScan b = PlanConstructor.newLogicalOlapScan(1, "b", 0);
    private final LogicalOlapScan c = PlanConstructor.newLogicalOlapScan(2, "c", 0);

    @Test
    void scanAndFilterMatchBothLiteralModes() {
        equivalent(a, scan(a));
        Set<Expression> conditions = Set.of(new EqualTo(a.getOutput().get(0), new IntegerLiteral(10)));
        LogicalFilter<?> logical = new LogicalFilter<>(conditions, a);
        equivalent(logical, new PhysicalFilter<>(conditions, logical.getLogicalProperties(), scan(a)));
    }

    @Test
    void computedProjectDefinitionsReachFilter() {
        Alias alias = new Alias(new Add(
                a.getOutput().get(0), new IntegerLiteral(1)), "computed");
        LogicalProject<?> project = new LogicalProject<>(List.of(alias), a);
        PhysicalProject<?> physicalProject = new PhysicalProject<>(List.of(alias),
                project.getLogicalProperties(), scan(a));
        Set<Expression> conditions = Set.of(new EqualTo(alias.toSlot(), new IntegerLiteral(10)));
        LogicalFilter<?> logical = new LogicalFilter<>(conditions, project);
        equivalent(logical, new PhysicalFilter<>(conditions, logical.getLogicalProperties(), physicalProject));
    }

    @Test
    void joinAlgorithmsAndExchangesDoNotChangeIdentity() {
        for (JoinType type : List.of(JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN,
                JoinType.LEFT_SEMI_JOIN, JoinType.LEFT_ANTI_JOIN, JoinType.NULL_AWARE_LEFT_ANTI_JOIN)) {
            Expression condition = new EqualTo(a.getOutput().get(0), b.getOutput().get(0));
            LogicalJoin<?, ?> logical = new LogicalJoin<>(type, List.of(condition), List.of(),
                    a, b, null);
            PhysicalHashJoin<?, ?> physical = new PhysicalHashJoin<>(type, List.of(condition), List.of(),
                    new DistributeHint(DistributeType.NONE), Optional.empty(), logical.getLogicalProperties(),
                    exchange(scan(a)), exchange(scan(b)));
            equivalent(logical, physical);
            equivalent(logical, new PhysicalNestedLoopJoin<>(type, List.of(condition), List.of(),
                    Optional.empty(), Optional.empty(), logical.getLogicalProperties(), scan(a), scan(b)));
        }
    }

    @Test
    void reorderedJoinChainMatchesMemo() {
        LogicalJoin<?, ?> ab = new LogicalJoin<>(JoinType.CROSS_JOIN, a, b, null);
        LogicalJoin<?, ?> logical = new LogicalJoin<>(JoinType.CROSS_JOIN, ab, c, null);
        PhysicalPlan bc = cross(scan(c), scan(b));
        equivalent(logical, cross(exchange(bc), scan(a)));
    }

    @Test
    void oneAndTwoStageAggregatesMatchLogicalIdentityIncludingHaving() {
        List<Expression> groups = List.of(a.getOutput().get(0));
        Sum sum = new Sum(a.getOutput().get(0));
        Alias logicalSum = new Alias(sum, "s");
        List<NamedExpression> outputs = List.of(a.getOutput().get(0), logicalSum);
        LogicalAggregate<?> logical = new LogicalAggregate<>(groups, outputs, true, a);
        AggregateParam direct = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        PhysicalHashAggregate<?> single = new PhysicalHashAggregate<>(groups,
                List.of(a.getOutput().get(0), new Alias(logicalSum.getExprId(),
                        new AggregateExpression(sum, direct), "s")),
                direct, false, logical.getLogicalProperties(), false, scan(a));
        equivalent(logical, single);
        AggregateParam partial = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Alias buffer = new Alias(new AggregateExpression(sum, partial), "partial_s");
        PhysicalHashAggregate<?> local = new PhysicalHashAggregate<>(groups,
                List.of(a.getOutput().get(0), buffer), partial, false,
                logical.getLogicalProperties(), false, scan(a));
        AggregateParam merge = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        PhysicalHashAggregate<?> global = new PhysicalHashAggregate<>(groups,
                List.of(a.getOutput().get(0), new Alias(logicalSum.getExprId(),
                        new AggregateExpression(sum, merge, buffer.toSlot()), "s")),
                merge, false, logical.getLogicalProperties(), false, exchange(local));
        equivalent(logical, global);
        Set<Expression> conditions = Set.of(new EqualTo(logicalSum.toSlot(), new IntegerLiteral(10)));
        LogicalFilter<?> having = new LogicalFilter<>(conditions, logical);
        equivalent(having, new PhysicalFilter<>(conditions, having.getLogicalProperties(), global));
        GroupStructInfo partialInfo = cloned(local);
        Assertions.assertFalse(partialInfo.isValid());
        Assertions.assertTrue(partialInfo.getSkipReason().contains("partial"), partialInfo.getSkipReason());
    }

    @Test
    void independentNestedAggregatesStaySeparate() {
        List<Expression> groups = List.of(a.getOutput().get(0));
        LogicalAggregate<?> inner = new LogicalAggregate<>(groups, List.of(a.getOutput().get(0)), true, a);
        LogicalAggregate<?> outer = new LogicalAggregate<>(List.of(), List.of(), true, inner);
        AggregateParam direct = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        PhysicalHashAggregate<?> physicalInner = new PhysicalHashAggregate<>(groups,
                List.of(a.getOutput().get(0)), direct, false, inner.getLogicalProperties(), false, scan(a));
        equivalent(outer, new PhysicalHashAggregate<>(List.of(), List.of(), direct, false,
                outer.getLogicalProperties(), false, physicalInner));
    }

    @Test
    void distributedBoundsAreNormalizedButPartialRootsAreNotLearnable() {
        LogicalLimit<?> logicalLocal = new LogicalLimit<>(7, 0, LimitPhase.LOCAL, a);
        LogicalLimit<?> logicalLimit = new LogicalLimit<>(5, 2, LimitPhase.GLOBAL, logicalLocal);
        PhysicalLimit<?> localLimit = new PhysicalLimit<>(7, 0, LimitPhase.LOCAL,
                logicalLimit.getLogicalProperties(), scan(a));
        equivalent(logicalLimit, new PhysicalLimit<>(5, 2, LimitPhase.GLOBAL,
                logicalLimit.getLogicalProperties(), exchange(localLimit)));
        Assertions.assertFalse(cloned(localLimit).isValid());
        List<OrderKey> order = List.of(new OrderKey(a.getOutput().get(0), true, true));
        LogicalTopN<?> logicalTopN = new LogicalTopN<>(order, 5, 2, a);
        PhysicalTopN<?> localTopN = new PhysicalTopN<>(order, 7, 0, SortPhase.LOCAL_SORT,
                logicalTopN.getLogicalProperties(), scan(a));
        equivalent(logicalTopN, new PhysicalTopN<>(order, 5, 2, SortPhase.MERGE_SORT,
                logicalTopN.getLogicalProperties(), exchange(localTopN)));
        Assertions.assertFalse(cloned(localTopN).isValid());
    }

    @Test
    void unionKeepsBranchColumnMappings() {
        for (SetOperation.Qualifier qualifier : SetOperation.Qualifier.values()) {
            List<NamedExpression> outputs = List.of(new SlotReference("union_k", a.getOutput().get(0).getDataType()));
            List<List<SlotReference>> mappings = List.of(
                    List.of((SlotReference) a.getOutput().get(0)),
                    List.of((SlotReference) b.getOutput().get(0)));
            LogicalUnion logical = new LogicalUnion(qualifier, outputs, mappings, List.of(), false, List.of(a, b));
            equivalent(logical, new PhysicalUnion(qualifier, outputs, mappings, List.of(),
                    logical.getLogicalProperties(), List.of(scan(a), scan(b))));
        }
    }

    @Test
    void unsupportedDistinctStagesAndSharedInputsAreExplicit() {
        AggregateParam distinct = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT);
        PhysicalHashAggregate<?> aggregate = new PhysicalHashAggregate<>(List.of(), List.of(), distinct,
                false, a.getLogicalProperties(), false, scan(a));
        GroupStructInfo unsupported = cloned(aggregate);
        Assertions.assertFalse(unsupported.isValid());
        Assertions.assertTrue(unsupported.getSkipReason().contains("distinct"));
        PhysicalPlan shared = scan(a);
        GroupStructInfo sharing = cloned(cross(shared, shared));
        Assertions.assertFalse(sharing.isValid());
        Assertions.assertTrue(sharing.getSkipReason().contains("sharing"));
    }

    private static PhysicalOlapScan scan(LogicalOlapScan scan) {
        return new PhysicalOlapScan(scan.getRelationId(), scan.getTable(), scan.getQualifier(),
                scan.getSelectedIndexId(), scan.getSelectedTabletIds(), scan.getSelectedPartitionIds(),
                scan.hasPartitionPredicate(), DistributionSpecAny.INSTANCE, scan.getPreAggStatus(), scan.getOutput(),
                Optional.empty(), scan.getLogicalProperties(), null, null, scan.getTableSample(),
                scan.getOperativeSlots(), scan.getVirtualColumns(), scan.getScoreOrderKeys(), scan.getScoreLimit(),
                scan.getScoreRangeInfo(), scan.getAnnOrderKeys(), scan.getAnnLimit(), scan.getTableAlias());
    }

    private static PhysicalPlan exchange(PhysicalPlan child) {
        return new PhysicalDistribute<>(DistributionSpecAny.INSTANCE, child);
    }

    private static PhysicalPlan cross(PhysicalPlan left, PhysicalPlan right) {
        return new PhysicalHashJoin<>(JoinType.CROSS_JOIN, List.of(), List.of(),
                new DistributeHint(DistributeType.NONE), Optional.empty(), left.getLogicalProperties(), left, right);
    }

    private static GroupStructInfo cloned(PhysicalPlan physical) {
        PhysicalPlan copy = PhysicalPlanShallowCopier.copy(physical);
        Assertions.assertTrue(copy.getGroupExpression().isEmpty());
        return GroupStructInfo.fromPhysicalPlan(copy);
    }

    private static void equivalent(Plan logical, PhysicalPlan physical) {
        GroupStructInfo expected = GroupStructInfo.of(new Memo(null, logical).getRoot());
        GroupStructInfo actual = cloned(physical);
        Assertions.assertTrue(expected.isValid(), expected.getSkipReason());
        Assertions.assertTrue(actual.isValid(), actual.getSkipReason());
        for (GroupStructInfo.LiteralMode mode : GroupStructInfo.LiteralMode.values()) {
            Assertions.assertEquals(expected.getCanonical(mode), actual.getCanonical(mode));
            Assertions.assertEquals(expected.getShape(mode), actual.getShape(mode));
            Assertions.assertEquals(expected.getFingerprint(mode), actual.getFingerprint(mode));
        }
    }
}
