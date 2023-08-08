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
 * aggregate: Sum(min1)
 * |
 * join
 * |   \
 * |    *
 * aggregate: Sum(x) as min1
 * </pre>
 */
public class PushdownSumThroughJoin implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(innerLogicalJoin())
                        .when(agg -> agg.child().getOtherJoinConjuncts().size() == 0)
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> f instanceof Sum && !f.isDistinct() && f.child(0) instanceof Slot);
                        })
                        .then(agg -> pushSum(agg, agg.child(), ImmutableList.of()))
                        .toRule(RuleType.PUSHDOWN_SUM_THROUGH_JOIN),
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().size() == 0)
                        .whenNot(agg -> agg.child().children().stream().anyMatch(p -> p instanceof LogicalAggregate))
                        .when(agg -> {
                            Set<AggregateFunction> funcs = agg.getAggregateFunctions();
                            return !funcs.isEmpty() && funcs.stream()
                                    .allMatch(f -> f instanceof Sum && !f.isDistinct() && f.child(0) instanceof Slot);
                        })
                        .then(agg -> pushSum(agg, agg.child().child(), agg.child().getProjects()))
                        .toRule(RuleType.PUSHDOWN_SUM_THROUGH_JOIN)
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
        if (leftSums.isEmpty() && rightSums.isEmpty()
                || (!leftSums.isEmpty() && !rightSums.isEmpty())) {
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

        List<Sum> sums;
        Set<Slot> sumGroupBy;
        Set<Slot> cntGroupBy;
        Plan sumChild;
        Plan cntChild;
        if (!leftSums.isEmpty()) {
            sums = leftSums;
            sumGroupBy = leftGroupBy;
            cntGroupBy = rightGroupBy;
            sumChild = join.left();
            cntChild = join.right();
        } else {
            sums = rightSums;
            sumGroupBy = rightGroupBy;
            cntGroupBy = leftGroupBy;
            sumChild = join.right();
            cntChild = join.left();
        }

        // Sum agg
        Map<Slot, NamedExpression> sumSlotToOutput = new HashMap<>();
        Builder<NamedExpression> sumAggOutputBuilder = ImmutableList.<NamedExpression>builder().addAll(sumGroupBy);
        sums.forEach(func -> {
            Alias alias = func.alias(func.getName());
            sumSlotToOutput.put((Slot) func.child(0), alias);
            sumAggOutputBuilder.add(alias);
        });
        LogicalAggregate<Plan> sumAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(sumGroupBy), sumAggOutputBuilder.build(), sumChild);

        // Count agg
        Alias cnt = new Count().alias("cnt");
        List<NamedExpression> cntAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(cntGroupBy).add(cnt)
                .build();
        LogicalAggregate<Plan> cntAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(cntGroupBy), cntAggOutput, cntChild);

        Plan newJoin = !leftSums.isEmpty() ? join.withChildren(sumAgg, cntAgg) : join.withChildren(cntAgg, sumAgg);

        // top Sum agg
        // replace sum(x) -> sum(sum# * cnt)
        List<NamedExpression> newOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof AggregateFunction) {
                AggregateFunction func = (AggregateFunction) ((Alias) ne).child();
                Slot slot = (Slot) func.child(0);
                if (sumSlotToOutput.containsKey(slot)) {
                    Expression expr = func.withChildren(new Multiply(sumSlotToOutput.get(slot).toSlot(), cnt.toSlot()));
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
