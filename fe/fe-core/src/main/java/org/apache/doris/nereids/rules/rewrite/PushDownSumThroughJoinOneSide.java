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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
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
 * TODO: distinct
 * Related paper "Eager aggregation and lazy aggregation".
 * <pre>
 * aggregate: Sum(x)
 * |
 * join
 * |   \
 * |    *
 * (x)
 * ->
 * aggregate: Sum(sum1)
 * |
 * join
 * |   \
 * |    *
 * aggregate: Sum(x) as sum1
 * </pre>
 */
public class PushDownSumThroughJoinOneSide implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(innerLogicalJoin())
                        .when(agg -> agg.child().getOtherJoinConjuncts().isEmpty())
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> f instanceof Sum && !f.isDistinct() && f.child(0) instanceof Slot);
                        })
                        .thenApply(ctx -> {
                            Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                                    .getSessionVariable().getEnableNereidsRules();
                            if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_SUM_THROUGH_JOIN_ONE_SIDE.type())) {
                                return null;
                            }
                            LogicalAggregate<LogicalJoin<Plan, Plan>> agg = ctx.root;
                            return pushSum(agg, agg.child(), ImmutableList.of());
                        })
                        .toRule(RuleType.PUSH_DOWN_SUM_THROUGH_JOIN),
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().isEmpty())
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> f instanceof Sum && !f.isDistinct() && f.child(0) instanceof Slot);
                        })
                        .thenApply(ctx -> {
                            Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                                    .getSessionVariable().getEnableNereidsRules();
                            if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_SUM_THROUGH_JOIN_ONE_SIDE.type())) {
                                return null;
                            }
                            LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg = ctx.root;
                            return pushSum(agg, agg.child().child(), agg.child().getProjects());
                        })
                        .toRule(RuleType.PUSH_DOWN_SUM_THROUGH_JOIN)
        );
    }

    private LogicalAggregate<Plan> pushSum(LogicalAggregate<? extends Plan> agg,
            LogicalJoin<Plan, Plan> join, List<NamedExpression> projects) {
        List<Slot> leftOutput = join.left().getOutput();
        List<Slot> rightOutput = join.right().getOutput();

        List<Sum> leftSums = new ArrayList<>();
        List<Sum> rightSums = new ArrayList<>();
        for (AggregateFunction f : agg.getAggregateFunctions()) {
            Sum sum = (Sum) f;
            Slot slot = (Slot) sum.child();
            if (leftOutput.contains(slot)) {
                leftSums.add(sum);
            } else if (rightOutput.contains(slot)) {
                rightSums.add(sum);
            } else {
                throw new IllegalStateException("Slot " + slot + " not found in join output");
            }
        }
        if (leftSums.isEmpty() && rightSums.isEmpty()) {
            return null;
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
        join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
            if (leftOutput.contains(slot)) {
                leftGroupBy.add(slot);
            } else if (rightOutput.contains(slot)) {
                rightGroupBy.add(slot);
            } else {
                throw new IllegalStateException("Slot " + slot + " not found in join output");
            }
        }));

        Plan left = join.left();
        Plan right = join.right();

        Map<Slot, NamedExpression> leftSumSlotToOutput = new HashMap<>();
        Map<Slot, NamedExpression> rightSumSlotToOutput = new HashMap<>();

        // left Sum agg
        if (!leftSums.isEmpty()) {
            Builder<NamedExpression> leftSumAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                    .addAll(leftGroupBy);
            leftSums.forEach(func -> {
                Alias alias = func.alias(func.getName());
                leftSumSlotToOutput.put((Slot) func.child(0), alias);
                leftSumAggOutputBuilder.add(alias);
            });
            left = new LogicalAggregate<>(ImmutableList.copyOf(leftGroupBy), leftSumAggOutputBuilder.build(),
                    join.left());
        }

        // right Sum agg
        if (!rightSums.isEmpty()) {
            Builder<NamedExpression> rightSumAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                    .addAll(rightGroupBy);
            rightSums.forEach(func -> {
                Alias alias = func.alias(func.getName());
                rightSumSlotToOutput.put((Slot) func.child(0), alias);
                rightSumAggOutputBuilder.add(alias);
            });
            right = new LogicalAggregate<>(ImmutableList.copyOf(rightGroupBy), rightSumAggOutputBuilder.build(),
                    join.right());
        }

        Preconditions.checkState(left != join.left() || right != join.right());
        Plan newJoin = join.withChildren(left, right);

        // top Sum agg
        // replace sum(x) -> sum(sum#)
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof Sum) {
                Sum oldTopSum = (Sum) ((Alias) ne).child();

                Slot slot = (Slot) oldTopSum.child(0);
                if (leftSumSlotToOutput.containsKey(slot)) {
                    Expression expr = new Sum(leftSumSlotToOutput.get(slot).toSlot());
                    newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                } else if (rightSumSlotToOutput.containsKey(slot)) {
                    Expression expr = new Sum(rightSumSlotToOutput.get(slot).toSlot());
                    newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                } else {
                    throw new IllegalStateException("Slot " + slot + " not found in join output");
                }
            } else {
                newOutputExprs.add(ne);
            }
        }
        return agg.withAggOutputChild(newOutputExprs, newJoin);
    }
}
