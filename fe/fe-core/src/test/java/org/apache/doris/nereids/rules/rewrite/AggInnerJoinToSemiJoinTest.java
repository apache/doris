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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test for AggInnerJoinToSemiJoin rule.
 */
class AggInnerJoinToSemiJoinTest implements MemoPatternMatchSupported {
    private LogicalOlapScan scan1;
    private LogicalOlapScan scan2;

    @BeforeEach
    void setUp() throws Exception {
        // clear id so that slot id keep consistent every running
        ConnectContext.remove();
        StatementScopeIdGenerator.clear();
        scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    }

    @Test
    void testRewriteToLeftSemiJoin() {
        // Aggregate only references left side columns, should rewrite to LEFT_SEMI_JOIN
        List<NamedExpression> groupByExprs = scan1.getOutput().stream()
                .collect(Collectors.toList());

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .aggGroupUsingIndex(ImmutableList.of(0, 1), groupByExprs)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new AggInnerJoinToSemiJoin())
                .printlnTree()
                .matches(logicalAggregate(
                        logicalProject(
                                logicalJoin().when(join -> join.getJoinType().isLeftSemiJoin()))));
    }

    @Test
    void testRewriteToRightSemiJoin() {
        // Aggregate only references right side columns, should rewrite to
        // RIGHT_SEMI_JOIN
        List<NamedExpression> groupByExprs = scan2.getOutput().stream()
                .collect(Collectors.toList());

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .projectExprs(ImmutableList.copyOf(scan2.getOutput()))
                .aggGroupUsingIndex(ImmutableList.of(0, 1), groupByExprs)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new AggInnerJoinToSemiJoin())
                .printlnTree()
                .matches(logicalAggregate(
                        logicalProject(
                                logicalJoin().when(join -> join.getJoinType().isRightSemiJoin()))));
    }

    @Test
    void testNoRewriteWithAggFunction() {
        // Aggregate has aggregate function (count), should NOT rewrite
        List<NamedExpression> outputExprs = ImmutableList.<NamedExpression>builder()
                .add(scan1.getOutput().get(0))
                .add(new Alias(new Count(), "cnt"))
                .build();

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .aggGroupUsingIndex(ImmutableList.of(0), outputExprs)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new AggInnerJoinToSemiJoin())
                .printlnTree()
                .matches(logicalAggregate(
                        logicalProject(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin()))));
    }

    @Test
    void testNoRewriteWithBothSideColumns() {
        // Aggregate references columns from both sides, should NOT rewrite
        List<NamedExpression> projectExprs = ImmutableList.<NamedExpression>builder()
                .add(scan1.getOutput().get(0))
                .add(scan2.getOutput().get(0))
                .build();
        List<NamedExpression> groupByExprs = ImmutableList.<NamedExpression>builder()
                .addAll(projectExprs)
                .build();

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .projectExprs(projectExprs)
                .aggGroupUsingIndex(ImmutableList.of(0, 1), groupByExprs)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new AggInnerJoinToSemiJoin())
                .printlnTree()
                .matches(logicalAggregate(
                        logicalProject(
                                logicalJoin().when(join -> join.getJoinType().isInnerJoin()))));
    }

    @Test
    void testNoRewriteWithLeftOuterJoin() {
        // Left outer join, should NOT rewrite
        List<NamedExpression> groupByExprs = scan1.getOutput().stream()
                .collect(Collectors.toList());

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_OUTER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .projectExprs(ImmutableList.copyOf(scan1.getOutput()))
                .aggGroupUsingIndex(ImmutableList.of(0, 1), groupByExprs)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new AggInnerJoinToSemiJoin())
                .printlnTree()
                .matches(logicalAggregate(
                        logicalProject(
                                logicalJoin().when(join -> join.getJoinType().isLeftOuterJoin()))));
    }

    @Test
    void testRewriteWithSimpleProjection() {
        // Simple case: project left columns then aggregate
        List<NamedExpression> projectExprs = ImmutableList.<NamedExpression>of(scan1.getOutput().get(0));
        List<NamedExpression> groupByExprs = ImmutableList.<NamedExpression>copyOf(projectExprs);

        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0)) // t1.id = t2.id
                .projectExprs(projectExprs)
                .aggGroupUsingIndex(ImmutableList.of(0), groupByExprs)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new AggInnerJoinToSemiJoin())
                .printlnTree()
                .matches(logicalAggregate(
                        logicalProject(
                                logicalJoin().when(join -> join.getJoinType().isLeftSemiJoin()))));
    }
}
