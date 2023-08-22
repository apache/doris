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
public class PushdownCountThroughJoin implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(innerLogicalJoin())
                        .when(agg -> agg.child().getOtherJoinConjuncts().size() == 0)
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(e -> e instanceof Slot))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> f instanceof Count && !f.isDistinct()
                                            && (((Count) f).isCountStar() || f.child(0) instanceof Slot));
                        })
                        .then(agg -> pushCount(agg, agg.child(), ImmutableList.of()))
                        .toRule(RuleType.PUSHDOWN_COUNT_THROUGH_JOIN),
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().size() == 0)
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(e -> e instanceof Slot))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> f instanceof Count && !f.isDistinct()
                                            && (((Count) f).isCountStar() || f.child(0) instanceof Slot));
                        })
                        .then(agg -> pushCount(agg, agg.child().child(), agg.child().getProjects()))
                        .toRule(RuleType.PUSHDOWN_COUNT_THROUGH_JOIN)
        );
    }

    private LogicalAggregate<Plan> pushCount(LogicalAggregate<? extends Plan> agg,
            LogicalJoin<Plan, Plan> join, List<NamedExpression> projects) {
        List<Slot> leftOutput = join.left().getOutput();
        List<Slot> rightOutput = join.right().getOutput();

        List<Count> leftCounts = new ArrayList<>();
        List<Count> rightCounts = new ArrayList<>();
        List<Count> countStars = new ArrayList<>();
        for (AggregateFunction f : agg.getAggregateFunctions()) {
            Count count = (Count) f;
            if (count.isCountStar()) {
                countStars.add(count);
            } else {
                Slot slot = (Slot) count.child(0);
                if (leftOutput.contains(slot)) {
                    leftCounts.add(count);
                } else if (rightOutput.contains(slot)) {
                    rightCounts.add(count);
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
        // left Count agg
        Map<Slot, NamedExpression> leftCntSlotToOutput = new HashMap<>();
        Builder<NamedExpression> leftCntAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                .addAll(leftGroupBy);
        leftCounts.forEach(func -> {
            Alias alias = func.alias(func.getName());
            leftCntSlotToOutput.put((Slot) func.child(0), alias);
            leftCntAggOutputBuilder.add(alias);
        });
        if (!rightCounts.isEmpty() || !countStars.isEmpty()) {
            leftCnt = new Count().alias("leftCntStar");
            leftCntAggOutputBuilder.add(leftCnt);
        }
        LogicalAggregate<Plan> leftCntAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(leftGroupBy), leftCntAggOutputBuilder.build(), join.left());

        // right Count agg
        Map<Slot, NamedExpression> rightCntSlotToOutput = new HashMap<>();
        Builder<NamedExpression> rightCntAggOutputBuilder = ImmutableList.<NamedExpression>builder()
                .addAll(rightGroupBy);
        rightCounts.forEach(func -> {
            Alias alias = func.alias(func.getName());
            rightCntSlotToOutput.put((Slot) func.child(0), alias);
            rightCntAggOutputBuilder.add(alias);
        });

        if (!leftCounts.isEmpty() || !countStars.isEmpty()) {
            rightCnt = new Count().alias("rightCntStar");
            rightCntAggOutputBuilder.add(rightCnt);
        }
        LogicalAggregate<Plan> rightCntAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(rightGroupBy), rightCntAggOutputBuilder.build(), join.right());

        Plan newJoin = join.withChildren(leftCntAgg, rightCntAgg);

        // top Sum agg
        // count(slot) -> sum( count(slot) * cntStar )
        // count(*) -> sum( leftCntStar * leftCntStar )
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof Count) {
                Count oldTopCnt = (Count) ((Alias) ne).child();
                if (oldTopCnt.isCountStar()) {
                    Preconditions.checkState(rightCnt != null && leftCnt != null);
                    Expression expr = new Sum(new Multiply(leftCnt.toSlot(), rightCnt.toSlot()));
                    newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                } else {
                    Slot slot = (Slot) oldTopCnt.child(0);
                    if (leftCntSlotToOutput.containsKey(slot)) {
                        Preconditions.checkState(rightCnt != null);
                        Expression expr = new Sum(
                                new Multiply(leftCntSlotToOutput.get(slot).toSlot(), rightCnt.toSlot()));
                        newOutputExprs.add((NamedExpression) ne.withChildren(expr));
                    } else if (rightCntSlotToOutput.containsKey(slot)) {
                        Preconditions.checkState(leftCnt != null);
                        Expression expr = new Sum(
                                new Multiply(rightCntSlotToOutput.get(slot).toSlot(), leftCnt.toSlot()));
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
