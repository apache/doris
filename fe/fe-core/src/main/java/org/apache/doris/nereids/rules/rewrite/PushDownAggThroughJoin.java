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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TODO: distinct | just push one level
 * Support Pushdown Count(*)/Count(col).
 * Count(col) -> Sum( cnt * cntStar )
 * Count(*) -> Sum( leftCntStar * rightCntStar )
 * <p>
 * Related paper "Eager aggregation and lazy aggregation".
 * <pre>
 *  aggregate: count(x)
 *  |
 *  join
 *  |   \
 *  |    *
 *  (x)
 *  ->
 *  aggregate: Sum( cnt * cntStar )
 *  |
 *  join
 *  |   \
 *  |    aggregate: count(*) as cntStar
 *  aggregate: count(x) as cnt
 *  </pre>
 * Notice: rule can't optimize condition that groupby is empty when Count(*) exists.
 */
public class PushDownAggThroughJoin implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(innerLogicalJoin())
                        .when(agg -> agg.child().getOtherJoinConjuncts().isEmpty())
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(e -> e instanceof Slot))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> !f.isDistinct()
                                            && (f instanceof Count && (((Count) f).isCountStar() || f.child(
                                            0) instanceof Slot)
                                            || (f instanceof Sum && f.child(0) instanceof Slot))
                                    );
                        })
                        .thenApply(ctx -> {
                            Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                                    .getSessionVariable().getEnableNereidsRules();
                            if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN.type())) {
                                return null;
                            }
                            LogicalAggregate<LogicalJoin<Plan, Plan>> agg = ctx.root;
                            return pushAgg(agg, agg.child(), ImmutableList.of());
                        })
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN),
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().isEmpty())
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(e -> e instanceof Slot))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> !f.isDistinct()
                                            && (f instanceof Count && (((Count) f).isCountStar() || f.child(
                                            0) instanceof Slot)
                                            || (f instanceof Sum && f.child(0) instanceof Slot))
                                    );
                        })
                        .thenApply(ctx -> {
                            Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                                    .getSessionVariable().getEnableNereidsRules();
                            if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN.type())) {
                                return null;
                            }
                            LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg = ctx.root;
                            return pushAgg(agg, agg.child().child(), agg.child().getProjects());
                        })
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN)
        );
    }

    private static LogicalAggregate<Plan> pushAgg(LogicalAggregate<? extends Plan> agg,
            LogicalJoin<Plan, Plan> join, List<NamedExpression> projects) {
        List<Slot> leftOutput = join.left().getOutput();
        List<Slot> rightOutput = join.right().getOutput();

        List<AggregateFunction> leftAggs = new ArrayList<>();
        List<AggregateFunction> rightAggs = new ArrayList<>();
        List<Count> countStars = new ArrayList<>();
        for (AggregateFunction f : agg.getAggregateFunctions()) {
            if (f instanceof Count && ((Count) f).isCountStar()) {
                countStars.add((Count) f);
            } else {
                Slot slot = (Slot) f.child(0);
                if (leftOutput.contains(slot)) {
                    leftAggs.add(f);
                } else if (rightOutput.contains(slot)) {
                    rightAggs.add(f);
                } else {
                    throw new IllegalStateException("Slot " + slot + " not found in join output");
                }
            }
        }

        Set<Slot> leftGroupBy = new HashSet<>();
        Set<Slot> rightGroupBy = new HashSet<>();
        for (Expression e : agg.getGroupByExpressions()) {
            Slot slot = (Slot) e;
            if (leftOutput.contains(slot)) {
                leftGroupBy.add(slot);
            } else if (rightOutput.contains(slot)) {
                rightGroupBy.add(slot);
            } else {
                return null;
            }
        }

        if (!countStars.isEmpty() && leftGroupBy.isEmpty() && rightGroupBy.isEmpty()) {
            return null;
        }

        join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
            if (leftOutput.contains(slot)) {
                leftGroupBy.add(slot);
            } else if (rightOutput.contains(slot)) {
                rightGroupBy.add(slot);
            } else {
                throw new IllegalStateException("Slot " + slot + " not found in join output");
            }
        }));

        Alias leftCnt = null;
        Alias rightCnt = null;
        // left agg
        Map<Slot, NamedExpression> leftSlotToOutput = new HashMap<>();
        Builder<NamedExpression> leftAggOutputBuilder = ImmutableList.<NamedExpression>builder().addAll(leftGroupBy);
        leftAggs.forEach(func -> {
            Alias alias = func.alias(func.getName());
            leftSlotToOutput.put((Slot) func.child(0), alias);
            leftAggOutputBuilder.add(alias);
        });
        if (!rightAggs.isEmpty() || !countStars.isEmpty()) {
            leftCnt = new Count().alias("leftCntStar");
            leftAggOutputBuilder.add(leftCnt);
        }
        LogicalAggregate<Plan> leftAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(leftGroupBy), leftAggOutputBuilder.build(), join.left());
        // right agg
        Map<Slot, NamedExpression> rightSlotToOutput = new HashMap<>();
        Builder<NamedExpression> rightAggOutputBuilder = ImmutableList.<NamedExpression>builder().addAll(rightGroupBy);
        rightAggs.forEach(func -> {
            Alias alias = func.alias(func.getName());
            rightSlotToOutput.put((Slot) func.child(0), alias);
            rightAggOutputBuilder.add(alias);
        });
        if (!leftAggs.isEmpty() || !countStars.isEmpty()) {
            rightCnt = new Count().alias("rightCntStar");
            rightAggOutputBuilder.add(rightCnt);
        }
        LogicalAggregate<Plan> rightAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(rightGroupBy), rightAggOutputBuilder.build(), join.right());

        Plan newJoin = join.withChildren(leftAgg, rightAgg);

        // top Sum agg
        // count(slot) -> sum( count(slot) * cntStar )
        // count(*) -> sum( leftCntStar * leftCntStar )
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof AggregateFunction) {
                AggregateFunction func = (AggregateFunction) ((Alias) ne).child();
                if (func instanceof Count && ((Count) func).isCountStar()) {
                    Preconditions.checkState(rightCnt != null && leftCnt != null);
                    Expression expr = new Sum(new Multiply(leftCnt.toSlot(), rightCnt.toSlot()));
                    newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                } else {
                    Slot slot = (Slot) func.child(0);
                    if (leftSlotToOutput.containsKey(slot)) {
                        Preconditions.checkState(rightCnt != null);
                        Expression expr = new Sum(
                                new Multiply(leftSlotToOutput.get(slot).toSlot(), rightCnt.toSlot()));
                        newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                    } else if (rightSlotToOutput.containsKey(slot)) {
                        Preconditions.checkState(leftCnt != null);
                        Expression expr = new Sum(
                                new Multiply(rightSlotToOutput.get(slot).toSlot(), leftCnt.toSlot()));
                        newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                    } else {
                        throw new IllegalStateException("Slot " + slot + " not found in join output");
                    }
                }
            } else {
                newOutputExprs.add(ne);
            }
        }
        return agg.withAggOutputChild(newOutputExprs, newJoin);
    }
}
