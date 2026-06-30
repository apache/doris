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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

/**
 * Tests for bucketed hash aggregation. Since the bucketed fusion now happens
 * at the translator level (PhysicalPlanTranslator fuses one-phase GLOBAL
 * PhysicalHashAggregate + PhysicalDistribute into BucketedAggregationNode),
 * these tests verify that the optimizer generates the correct one-phase
 * candidates that the translator will later fuse.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BucketedAggregateTest implements MemoPatternMatchSupported {
    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
    }

    private Rule splitAggWithoutDistinctRule() {
        return SplitAggWithoutDistinct.INSTANCE.buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.SPLIT_AGG_WITHOUT_DISTINCT)
                .findFirst()
                .get();
    }

    private Plan buildAggregateWithGroupBy() {
        Slot age = rStudent.getOutput().get(3).toSlot();
        Slot id = rStudent.getOutput().get(0).toSlot();
        List<Expression> groupByExpressions = Lists.newArrayList(age);
        List<NamedExpression> outputExpressions = Lists.newArrayList(
                age,
                new Alias(new Sum(id), "sum_id"));
        return new LogicalAggregate<>(groupByExpressions, outputExpressions,
                true, Optional.empty(), rStudent);
    }

    private Plan buildAggregateWithoutGroupBy() {
        Slot id = rStudent.getOutput().get(0).toSlot();
        List<Expression> groupByExpressions = Lists.newArrayList();
        List<NamedExpression> outputExpressions = Lists.newArrayList(
                new Alias(new Sum(id), "sum_id"));
        return new LogicalAggregate<>(groupByExpressions, outputExpressions,
                true, Optional.empty(), rStudent);
    }

    @Test
    public void testBucketedAggDisabled() {
        // When bucketed is disabled and beNumber=1, the one-phase GLOBAL candidate
        // (which the translator would fuse) is still generated. The cost model
        // decides whether to pick it or two-phase.
        Plan root = buildAggregateWithGroupBy();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableBucketedHashAgg = false;
        ctx.getSessionVariable().setBeNumberForTest(1);

        PlanChecker.from(ctx, root)
                .applyImplementation(splitAggWithoutDistinctRule())
                .matches(physicalHashAggregate()
                        .when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL)));
    }

    @Test
    public void testBucketedAggMultiBE() {
        // On multi-BE, two-phase aggregation should be generated (local+global).
        // The cost model will prefer two-phase because the bucketed discount
        // only applies when beNumber==1.
        Plan root = buildAggregateWithGroupBy();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableBucketedHashAgg = true;
        ctx.getSessionVariable().setBeNumberForTest(3);

        PlanChecker.from(ctx, root)
                .applyImplementation(splitAggWithoutDistinctRule())
                .matches(physicalHashAggregate());
    }

    @Test
    public void testBucketedAggNoGroupBy() {
        // Without GROUP BY, scalar aggregation is generated, not grouped agg.
        Plan root = buildAggregateWithoutGroupBy();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableBucketedHashAgg = true;
        ctx.getSessionVariable().setBeNumberForTest(1);

        PlanChecker.from(ctx, root)
                .applyImplementation(splitAggWithoutDistinctRule())
                .matches(physicalHashAggregate());
    }

    @Test
    public void testBucketedAggEnabled() {
        // With all bucketed conditions met, the one-phase GLOBAL candidate
        // (with GLOBAL_RESULT aggregate expressions) should be generated.
        // The translator fuses this with the distribute into BucketedAggregationNode.
        Plan root = buildAggregateWithGroupBy();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableBucketedHashAgg = true;
        ctx.getSessionVariable().setBeNumberForTest(1);
        ctx.getSessionVariable().parallelPipelineTaskNum = 2;
        ctx.getSessionVariable().bucketedAggMinInputRows = 0;
        ctx.getSessionVariable().bucketedAggMaxGroupKeys = 0;
        ctx.getSessionVariable().bucketedAggHighCardThreshold = 1.0;

        PlanChecker.from(ctx, root)
                .deriveStats()
                .applyImplementation(splitAggWithoutDistinctRule())
                .matches(physicalHashAggregate()
                        .when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL)));
    }

    @Test
    public void testSingleExecutionInstanceOnlyGeneratesOnePhaseAgg() {
        // When parallelPipelineTaskNum=1, only one-phase GLOBAL is generated
        // (no two-phase candidates because there's nowhere to distribute to).
        Plan root = buildAggregateWithGroupBy();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableBucketedHashAgg = true;
        ctx.getSessionVariable().setBeNumberForTest(1);
        ctx.getSessionVariable().parallelPipelineTaskNum = 1;
        ctx.getSessionVariable().bucketedAggMinInputRows = 0;
        ctx.getSessionVariable().bucketedAggMaxGroupKeys = 0;
        ctx.getSessionVariable().bucketedAggHighCardThreshold = 1.0;

        PlanChecker.from(ctx, root)
                .deriveStats()
                .applyImplementation(splitAggWithoutDistinctRule())
                .matches(physicalHashAggregate()
                        .when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL)))
                .nonMatch(physicalHashAggregate(physicalHashAggregate()));
    }

    @Test
    public void testBucketedAggForcedAggPhase() {
        // When user forces agg_phase = 2, only two-phase is generated.
        // The one-phase GLOBAL candidate (which the translator would fuse) is not generated.
        Plan root = buildAggregateWithGroupBy();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().enableBucketedHashAgg = true;
        ctx.getSessionVariable().setBeNumberForTest(1);
        ctx.getSessionVariable().bucketedAggMinInputRows = 0;
        ctx.getSessionVariable().bucketedAggMaxGroupKeys = 0;
        ctx.getSessionVariable().bucketedAggHighCardThreshold = 1.0;
        ctx.getSessionVariable().aggPhase = 2;

        PlanChecker.from(ctx, root)
                .deriveStats()
                .applyImplementation(splitAggWithoutDistinctRule())
                .matches(physicalHashAggregate(physicalHashAggregate()))
                .nonMatch(physicalHashAggregate()
                        .when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL)
                                && agg.child(0) instanceof LogicalOlapScan));
    }
}
