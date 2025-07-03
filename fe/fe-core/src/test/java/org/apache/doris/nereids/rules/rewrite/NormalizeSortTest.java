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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * NormalizeSortTest ut
 */
class NormalizeSortTest implements MemoPatternMatchSupported {
    LogicalOlapScan score = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.score);

    @Test
    void testOrderByExpr() {
        Expression add = new Add(score.getOutput().get(0), new IntegerLiteral(1));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(add, true, true));
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .sort(orderKeys)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new NormalizeSort())
                .matches(logicalProject(logicalSort(logicalProject().when(p -> p.getProjects().stream().anyMatch(e -> e.toSql().contains(
                        add.toSql())))))
                        .when(p -> p.getOutput().equals(plan.getOutput())));
    }

    @Test
    void testOrderByAlias() {
        Expression add = new Add(score.getOutput().get(0), new IntegerLiteral(1));
        Expression alias = new Alias(add);
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(alias, true, true));
        LogicalPlan plan = new LogicalPlanBuilder(score)
                .sort(orderKeys)
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new NormalizeSort())
                .matches(logicalProject(logicalSort(logicalProject().when(p -> p.getProjects().stream().anyMatch(e -> e.toSql().contains(
                        alias.toSql())))))
                        .when(p -> p.getOutput().equals(plan.getOutput())));
    }

    @Test
    void testOrderByOneSlotOneNot() {
        Expression add = new Add(score.getOutput().get(0), new IntegerLiteral(1));
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(add, true, true),
                new OrderKey(score.getOutput().get(2), true, true));
        LogicalPlan plan = new LogicalPlanBuilder(score).sort(orderKeys).build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new NormalizeSort())
                .matches(logicalProject(logicalSort(logicalProject().when(p -> p.getProjects().stream().map(Expression::toSql).collect(Collectors.toList()).equals(
                        ImmutableList.<NamedExpression>builder().addAll(score.getOutput()).add(new Alias(add)).build().stream().map(Expression::toSql).collect(Collectors.toList())))))
                .when(p -> p.getOutput().equals(plan.getOutput())));
    }

    @Test
    void testOrderByAllKeySlot() {
        List<OrderKey> orderKeys = Lists.newArrayList(new OrderKey(score.getOutput().get(2), true, true),
                new OrderKey(score.getOutput().get(0), true, true));
        LogicalPlan plan = new LogicalPlanBuilder(score).sort(orderKeys).build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new NormalizeSort())
                .matchesFromRoot(logicalSort(logicalOlapScan().when(scan -> scan.equals(score))));
    }
}
