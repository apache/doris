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
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
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
 * aggregate: Min/Max/Sum(x)
 * |
 * join
 * |   \
 * |    *
 * (x)
 * ->
 * aggregate: Min/Max/Sum(min1)
 * |
 * join
 * |   \
 * |    *
 * aggregate: Min/Max/Sum(x) as min1
 * </pre>
 */
public class PushDownAggThroughJoinOneSide implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(innerLogicalJoin())
                        .when(agg -> agg.child().getOtherJoinConjuncts().isEmpty())
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> (f instanceof Min || f instanceof Max || f instanceof Sum
                                            || (f instanceof Count && !((Count) f).isCountStar())) && !f.isDistinct()
                                            && f.child(0) instanceof Slot);
                        })
                        .thenApply(ctx -> {
                            Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                                    .getSessionVariable().getEnableNereidsRules();
                            if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE.type())) {
                                return null;
                            }
                            LogicalAggregate<LogicalJoin<Plan, Plan>> agg = ctx.root;
                            return pushMinMaxSumCount(agg, agg.child(), ImmutableList.of());
                        })
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE),
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().isEmpty())
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> (f instanceof Min || f instanceof Max || f instanceof Sum
                                            || (f instanceof Count && (!((Count) f).isCountStar()))) && !f.isDistinct()
                                            && f.child(0) instanceof Slot);
                        })
                        .thenApply(ctx -> {
                            Set<Integer> enableNereidsRules = ctx.cascadesContext.getConnectContext()
                                    .getSessionVariable().getEnableNereidsRules();
                            if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE.type())) {
                                return null;
                            }
                            LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg = ctx.root;
                            return pushMinMaxSumCount(agg, agg.child().child(), agg.child().getProjects());
                        })
                        .toRule(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ONE_SIDE)
        );
    }

    /**
     * Push down Min/Max/Sum through join.
     */
    public static LogicalAggregate<Plan> pushMinMaxSumCount(LogicalAggregate<? extends Plan> agg,
            LogicalJoin<Plan, Plan> join, List<NamedExpression> projects) {
        List<Slot> leftOutput = join.left().getOutput();
        List<Slot> rightOutput = join.right().getOutput();

        List<AggregateFunction> leftFuncs = new ArrayList<>();
        List<AggregateFunction> rightFuncs = new ArrayList<>();
        for (AggregateFunction func : agg.getAggregateFunctions()) {
            Slot slot = (Slot) func.child(0);
            if (leftOutput.contains(slot)) {
                leftFuncs.add(func);
            } else if (rightOutput.contains(slot)) {
                rightFuncs.add(func);
            } else {
                throw new IllegalStateException("Slot " + slot + " not found in join output");
            }
        }
        if (leftFuncs.isEmpty() && rightFuncs.isEmpty()) {
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
        Map<Slot, NamedExpression> leftSlotToOutput = new HashMap<>();
        Map<Slot, NamedExpression> rightSlotToOutput = new HashMap<>();
        if (!leftFuncs.isEmpty()) {
            Builder<NamedExpression> leftAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                    .addAll(leftGroupBy);
            leftFuncs.forEach(func -> {
                Alias alias = func.alias(func.getName());
                leftSlotToOutput.put((Slot) func.child(0), alias);
                leftAggOutputBuilder.add(alias);
            });
            left = new LogicalAggregate<>(ImmutableList.copyOf(leftGroupBy), leftAggOutputBuilder.build(), join.left());
        }
        if (!rightFuncs.isEmpty()) {
            Builder<NamedExpression> rightAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                    .addAll(rightGroupBy);
            rightFuncs.forEach(func -> {
                Alias alias = func.alias(func.getName());
                rightSlotToOutput.put((Slot) func.child(0), alias);
                rightAggOutputBuilder.add(alias);
            });
            right = new LogicalAggregate<>(ImmutableList.copyOf(rightGroupBy), rightAggOutputBuilder.build(),
                    join.right());
        }

        Preconditions.checkState(left != join.left() || right != join.right());
        Plan newJoin = join.withChildren(left, right);

        // top agg TODO: AVG
        // replace
        // min(x) -> min(min#)
        // max(x) -> max(max#)
        // sum(x) -> sum(sum#)
        // count(x) -> sum(count#)
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof AggregateFunction) {
                AggregateFunction func = (AggregateFunction) ((Alias) ne).child();
                Slot slot = (Slot) func.child(0);
                if (leftSlotToOutput.containsKey(slot)) {
                    Expression newFunc = replaceAggFunc(func, leftSlotToOutput.get(slot).toSlot());
                    newOutputExprs.add((NamedExpression) ne.withChildren(newFunc));
                } else if (rightSlotToOutput.containsKey(slot)) {
                    Expression newFunc = replaceAggFunc(func, rightSlotToOutput.get(slot).toSlot());
                    newOutputExprs.add((NamedExpression) ne.withChildren(newFunc));
                } else {
                    throw new IllegalStateException("Slot " + slot + " not found in join output");
                }
            } else {
                newOutputExprs.add(ne);
            }
        }

        // TODO: column prune project
        return agg.withAggOutputChild(newOutputExprs, newJoin);
    }

    private static Expression replaceAggFunc(AggregateFunction func, Slot inputSlot) {
        if (func instanceof Count) {
            return new Sum(inputSlot);
        } else {
            return func.withChildren(inputSlot);
        }
    }
}
