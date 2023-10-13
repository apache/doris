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

package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement.Assertion;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalPlanBuilder {
    private final LogicalPlan plan;

    public LogicalPlanBuilder(LogicalPlan plan) {
        this.plan = plan;
    }

    public LogicalPlan build() {
        return plan;
    }

    public LogicalPlanBuilder from(LogicalPlan plan) {
        return new LogicalPlanBuilder(plan);
    }

    public LogicalPlanBuilder scan(long tableId, String tableName, int hashColumn) {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(tableId, tableName, hashColumn);
        return from(scan);
    }

    public LogicalPlanBuilder project(List<Integer> slotsIndex) {
        List<NamedExpression> projectExprs = Lists.newArrayList();
        for (Integer index : slotsIndex) {
            projectExprs.add(this.plan.getOutput().get(index));
        }
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projectExprs, this.plan);
        return from(project);
    }

    public LogicalPlanBuilder projectExprs(List<NamedExpression> projectExprs) {
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projectExprs, this.plan);
        return from(project);
    }

    public LogicalPlanBuilder alias(List<Integer> slotsIndex, List<String> alias) {
        Preconditions.checkArgument(slotsIndex.size() == alias.size());

        List<NamedExpression> projectExprs = Lists.newArrayList();
        for (int i = 0; i < slotsIndex.size(); i++) {
            projectExprs.add(new Alias(this.plan.getOutput().get(slotsIndex.get(i)), alias.get(i)));
        }
        LogicalProject<LogicalPlan> project = new LogicalProject<>(projectExprs, this.plan);
        return from(project);
    }

    public LogicalPlanBuilder join(LogicalPlan right, JoinType joinType, Pair<Integer, Integer> hashOnSlots) {
        ImmutableList<EqualTo> hashConjuncts = ImmutableList.of(
                new EqualTo(this.plan.getOutput().get(hashOnSlots.first), right.getOutput().get(hashOnSlots.second)));

        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(joinType, new ArrayList<>(hashConjuncts),
                this.plan, right);
        return from(join);
    }

    public LogicalPlanBuilder join(LogicalPlan right, JoinType joinType,
            List<Pair<Integer, Integer>> hashOnSlots) {
        List<EqualTo> hashConjuncts = hashOnSlots.stream()
                .map(pair -> new EqualTo(this.plan.getOutput().get(pair.first), right.getOutput().get(pair.second)))
                .collect(Collectors.toList());

        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(joinType, new ArrayList<>(hashConjuncts),
                this.plan, right);
        return from(join);
    }

    public LogicalPlanBuilder join(LogicalPlan right, JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjucts) {
        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjucts,
                JoinHint.NONE, Optional.empty(), this.plan, right);
        return from(join);
    }

    public LogicalPlanBuilder joinEmptyOn(LogicalPlan right, JoinType joinType) {
        LogicalJoin<LogicalPlan, LogicalPlan> join = new LogicalJoin<>(joinType, this.plan, right);
        return from(join);
    }

    public LogicalPlanBuilder sort(List<OrderKey> orderKeys) {
        LogicalSort<LogicalPlan> sortPlan = new LogicalSort<>(orderKeys, this.plan);
        return from(sortPlan);
    }

    public LogicalPlanBuilder limit(long limit, long offset) {
        LogicalLimit<LogicalPlan> limitPlan = new LogicalLimit<>(limit, offset, LimitPhase.ORIGIN, this.plan);
        return from(limitPlan);
    }

    public LogicalPlanBuilder limit(long limit) {
        return limit(limit, 0);
    }

    public LogicalPlanBuilder topN(long limit, long offset, List<Integer> orderKeySlotsIndex) {
        List<OrderKey> orderKeys = orderKeySlotsIndex.stream()
                .map(i -> new OrderKey(this.plan.getOutput().get(i), false, false))
                .collect(Collectors.toList());
        LogicalTopN<Plan> topNPlan = new LogicalTopN<>(orderKeys, limit, offset, this.plan);
        return from(topNPlan);
    }

    public LogicalPlanBuilder filter(Expression conjunct) {
        return filter(ImmutableSet.copyOf(ExpressionUtils.extractConjunction(conjunct)));
    }

    public LogicalPlanBuilder filter(Set<Expression> conjuncts) {
        LogicalFilter<LogicalPlan> filter = new LogicalFilter<>(conjuncts, this.plan);
        return from(filter);
    }

    public LogicalPlanBuilder aggAllUsingIndex(List<Integer> groupByKeysIndex, List<Integer> outputExprsIndex) {
        Builder<Expression> groupByBuilder = ImmutableList.builder();
        for (Integer index : groupByKeysIndex) {
            groupByBuilder.add(this.plan.getOutput().get(index));
        }
        ImmutableList<Expression> groupByKeys = groupByBuilder.build();

        Builder<NamedExpression> outputBuilder = ImmutableList.builder();
        for (Integer index : outputExprsIndex) {
            outputBuilder.add(this.plan.getOutput().get(index));
        }
        ImmutableList<NamedExpression> outputExprsList = outputBuilder.build();

        LogicalAggregate<Plan> agg = new LogicalAggregate<>(groupByKeys, outputExprsList, this.plan);
        return from(agg);
    }

    public LogicalPlanBuilder aggGroupUsingIndex(List<Integer> groupByKeysIndex,
            List<NamedExpression> outputExprsList) {
        return aggGroupUsingIndexAndSourceRepeat(groupByKeysIndex, outputExprsList, Optional.empty());
    }

    public LogicalPlanBuilder aggGroupUsingIndexAndSourceRepeat(List<Integer> groupByKeysIndex,
                                                                List<NamedExpression> outputExprsList,
                                                                Optional<LogicalRepeat<?>> sourceRepeat) {
        Builder<Expression> groupByBuilder = ImmutableList.builder();
        for (Integer index : groupByKeysIndex) {
            groupByBuilder.add(this.plan.getOutput().get(index));
        }
        List<Expression> groupByKeys = groupByBuilder.build();

        LogicalAggregate<Plan> agg = new LogicalAggregate<>(groupByKeys, outputExprsList, sourceRepeat, this.plan);
        return from(agg);
    }

    public LogicalPlanBuilder distinct(List<Integer> groupByKeysIndex) {
        Builder<NamedExpression> groupByBuilder = ImmutableList.builder();
        for (Integer index : groupByKeysIndex) {
            groupByBuilder.add(this.plan.getOutput().get(index));
        }
        List<NamedExpression> groupByKeys = groupByBuilder.build();

        LogicalAggregate<Plan> agg = new LogicalAggregate<>(groupByKeys, false, this.plan);
        return from(agg);
    }

    public LogicalPlanBuilder agg(List<Expression> groupByKeys, List<NamedExpression> outputExprsList) {
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(groupByKeys, outputExprsList, this.plan);
        return from(agg);
    }

    public LogicalPlanBuilder assertNumRows(Assertion assertion, long numRows) {
        LogicalAssertNumRows<LogicalPlan> assertNumRows = new LogicalAssertNumRows<>(
                new AssertNumRowsElement(numRows, "", assertion), this.plan);
        return from(assertNumRows);
    }
}
