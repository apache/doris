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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Related paper "Eager aggregation and lazy aggregation".
 * <pre>
 * aggregate: SUM(x), SUM(y)
 * |
 * join
 * |   \
 * |   (y)
 * (x)
 * ->
 * aggregate: SUM(sum1), SUM(y  * cnt)
 * |
 * join
 * |   \
 * |   (y)
 * aggregate: SUM(x) as sum1 , COUNT as cnt
 * </pre>
 */
public class EagerGroupByCount extends OneExplorationRuleFactory {
    public static final EagerGroupByCount INSTANCE = new EagerGroupByCount();

    @Override
    public Rule build() {
        return logicalAggregate(innerLogicalJoin())
                .when(agg -> agg.child().getOtherJoinConjuncts().size() == 0)
                .when(agg -> agg.getAggregateFunctions().stream()
                        .allMatch(f -> f instanceof Sum && ((Sum) f).child() instanceof Slot))
                .then(agg -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = agg.child();
                    List<Slot> leftOutput = join.left().getOutput();
                    List<Sum> leftSums = new ArrayList<>();
                    List<Sum> rightSums = new ArrayList<>();
                    for (AggregateFunction f : agg.getAggregateFunctions()) {
                        Sum sum = (Sum) f;
                        if (leftOutput.contains((Slot) sum.child())) {
                            leftSums.add(sum);
                        } else {
                            rightSums.add(sum);
                        }
                    }
                    if (leftSums.size() == 0 && rightSums.size() == 0) {
                        return null;
                    }

                    // left bottom agg
                    Set<Slot> bottomAggGroupBy = new HashSet<>();
                    agg.getGroupByExpressions().stream().map(e -> (Slot) e).filter(leftOutput::contains)
                            .forEach(bottomAggGroupBy::add);
                    join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
                        if (leftOutput.contains(slot)) {
                            bottomAggGroupBy.add(slot);
                        }
                    }));
                    List<NamedExpression> bottomSums = new ArrayList<>();
                    for (int i = 0; i < leftSums.size(); i++) {
                        bottomSums.add(new Alias(new Sum(leftSums.get(i).child()), "sum" + i));
                    }
                    Alias cnt = new Alias(new Count(), "cnt");
                    List<NamedExpression> bottomAggOutput = ImmutableList.<NamedExpression>builder()
                            .addAll(bottomAggGroupBy).addAll(bottomSums).add(cnt).build();
                    LogicalAggregate<GroupPlan> bottomAgg = new LogicalAggregate<>(
                            ImmutableList.copyOf(bottomAggGroupBy), bottomAggOutput, join.left());
                    Plan newJoin = join.withChildren(bottomAgg, join.right());

                    // top agg
                    List<NamedExpression> newOutputExprs = new ArrayList<>();
                    List<Alias> leftSumOutputExprs = new ArrayList<>();
                    List<Alias> rightSumOutputExprs = new ArrayList<>();
                    for (NamedExpression ne : agg.getOutputExpressions()) {
                        if (ne instanceof Alias && ((Alias) ne).child() instanceof Sum) {
                            Alias sumOutput = (Alias) ne;
                            Slot child = (Slot) ((Sum) (sumOutput).child()).child();
                            if (leftOutput.contains(child)) {
                                leftSumOutputExprs.add(sumOutput);
                            } else {
                                rightSumOutputExprs.add(sumOutput);
                            }
                        } else {
                            newOutputExprs.add(ne);
                        }
                    }
                    for (int i = 0; i < leftSumOutputExprs.size(); i++) {
                        Alias oldSum = leftSumOutputExprs.get(i);
                        // sum in bottom Agg
                        Slot bottomSum = bottomSums.get(i).toSlot();
                        Alias newSum = new Alias(oldSum.getExprId(), new Sum(bottomSum), oldSum.getName());
                        newOutputExprs.add(newSum);
                    }
                    for (Alias oldSum : rightSumOutputExprs) {
                        Sum oldSumFunc = (Sum) oldSum.child();
                        Slot slot = (Slot) oldSumFunc.child();
                        newOutputExprs.add(new Alias(oldSum.getExprId(), new Sum(new Multiply(slot, cnt.toSlot())),
                                oldSum.getName()));
                    }
                    return agg.withAggOutput(newOutputExprs).withChildren(newJoin);
                }).toRule(RuleType.EAGER_GROUP_BY_COUNT);
    }
}
