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
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Push down agg function with distinct through join on only one side.
 */
public class PushDownAggWithDistinctThroughJoinOneSide implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().isEmpty())
                        .when(agg -> !agg.isGenerated())
                        .whenNot(agg -> agg.getAggregateFunctions().isEmpty())
                        .whenNot(agg -> agg.child()
                                .child(0).children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            if (funcs.size() > 1) {
                                return false;
                            } else {
                                return funcs.stream()
                                        .allMatch(f -> (f instanceof Min || f instanceof Max || f instanceof Sum
                                                || f instanceof Count) && f.isDistinct()
                                                && f.child(0) instanceof Slot);
                            }
                        })
                        .thenApply(ctx -> {
                            LogicalAggregate<LogicalProject<LogicalJoin<Plan, Plan>>> agg = ctx.root;
                            return pushDownAggWithDistinct(agg, agg.child().child(), agg.child().getProjects());
                        })
                        .toRule(RuleType.PUSH_DOWN_AGG_WITH_DISTINCT_THROUGH_JOIN_ONE_SIDE)
        );
    }

    private static LogicalAggregate<Plan> pushDownAggWithDistinct(LogicalAggregate<? extends Plan> agg,
            LogicalJoin<Plan, Plan> join, List<NamedExpression> projects) {
        Plan leftJoin = join.left();
        Plan rightJoin = join.right();
        List<Slot> leftJoinOutput = leftJoin.getOutput();
        List<Slot> rightJoinOutput = rightJoin.getOutput();

        List<AggregateFunction> leftFuncs = new ArrayList<>();
        List<AggregateFunction> rightFuncs = new ArrayList<>();
        Set<Slot> leftFuncSlotSet = new HashSet<>();
        Set<Slot> rightFuncSlotSet = new HashSet<>();
        Set<Slot> newAggOverJoinGroupByKeys = new HashSet<>();
        for (AggregateFunction func : agg.getAggregateFunctions()) {
            Slot slot = (Slot) func.child(0);
            newAggOverJoinGroupByKeys.add(slot);
            if (leftJoinOutput.contains(slot)) {
                leftFuncs.add(func);
                leftFuncSlotSet.add(slot);
            } else if (rightJoinOutput.contains(slot)) {
                rightFuncs.add(func);
                rightFuncSlotSet.add(slot);
            } else {
                throw new IllegalStateException("Slot " + slot + " not found in join output");
            }
        }
        boolean isLeftSideAggDistinct = !leftFuncs.isEmpty() && rightFuncs.isEmpty();
        boolean isRightSideAggDistinct = leftFuncs.isEmpty() && !rightFuncs.isEmpty();
        if (!isLeftSideAggDistinct && !isRightSideAggDistinct) {
            return null;
        }

        Set<Slot> leftPushDownGroupBy = new HashSet<>();
        Set<Slot> rightPushDownGroupBy = new HashSet<>();
        for (Expression e : agg.getGroupByExpressions()) {
            Slot slot = (Slot) e;
            newAggOverJoinGroupByKeys.add(slot);
            if (leftJoinOutput.contains(slot)) {
                leftPushDownGroupBy.add(slot);
            } else if (rightJoinOutput.contains(slot)) {
                rightPushDownGroupBy.add(slot);
            } else {
                return null;
            }
        }
        join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
            if (leftJoinOutput.contains(slot)) {
                leftPushDownGroupBy.add(slot);
            } else if (rightJoinOutput.contains(slot)) {
                rightPushDownGroupBy.add(slot);
            } else {
                throw new IllegalStateException("Slot " + slot + " not found in join output");
            }
        }));

        if (isLeftSideAggDistinct) {
            leftPushDownGroupBy.add((Slot) leftFuncs.get(0).child(0));
            Builder<NamedExpression> leftAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                    .addAll(leftPushDownGroupBy);
            leftJoin = new LogicalAggregate<>(ImmutableList.copyOf(leftPushDownGroupBy),
                    leftAggOutputBuilder.build(), join.left());
        } else {
            rightPushDownGroupBy.add((Slot) rightFuncs.get(0).child(0));
            Builder<NamedExpression> rightAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                    .addAll(rightPushDownGroupBy);
            rightJoin = new LogicalAggregate<>(ImmutableList.copyOf(rightPushDownGroupBy),
                    rightAggOutputBuilder.build(), join.right());
        }

        Preconditions.checkState(leftJoin != join.left() || rightJoin != join.right(),
                "not pushing down aggr with distinct through join on single side successfully");
        Plan newJoin = join.withChildren(leftJoin, rightJoin);
        LogicalAggregate<? extends Plan> newAggOverJoin = agg.withChildGroupByAndOutput(
                ImmutableList.copyOf(newAggOverJoinGroupByKeys), projects, newJoin);

        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof AggregateFunction) {
                AggregateFunction func = (AggregateFunction) ((Alias) ne).child();
                Slot slot = (Slot) func.child(0);
                if (leftFuncSlotSet.contains(slot) || rightFuncSlotSet.contains(slot)) {
                    Expression newFunc = discardDistinct(func);
                    newOutputExprs.add((NamedExpression) ne.withChildren(newFunc));
                } else {
                    throw new IllegalStateException("Slot " + slot + " not found in join output");
                }
            } else {
                newOutputExprs.add(ne);
            }
        }
        return agg.withAggOutputChild(newOutputExprs, newAggOverJoin);
    }

    private static Expression discardDistinct(AggregateFunction func) {
        Preconditions.checkState(func.isDistinct(), "current aggregation function is not distinct");
        Set<Expression> aggChild = Sets.newLinkedHashSet(func.children());
        return func.withDistinctAndChildren(false, ImmutableList.copyOf(aggChild));
    }
}
