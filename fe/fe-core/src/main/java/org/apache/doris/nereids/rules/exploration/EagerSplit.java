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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;
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
 * aggregate: SUM(sum1 * cnt2), SUM(sum2 * cnt1)
 * |
 * join
 * |   \
 * |   aggregate: SUM(y) as sum2, COUNT: cnt2
 * aggregate: SUM(x) as sum1, COUNT: cnt1
 * </pre>
 */
public class EagerSplit extends OneExplorationRuleFactory {
    public static final EagerSplit INSTANCE = new EagerSplit();

    @Override
    public Rule build() {
        return logicalAggregate(innerLogicalJoin())
                .when(agg -> agg.getAggregateFunctions().stream()
                        .allMatch(f -> f instanceof Sum && ((Sum) f).child() instanceof SlotReference))
                .then(agg -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = agg.child();
                    List<Slot> leftOutput = join.left().getOutput();
                    List<Slot> rightOutput = join.right().getOutput();
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
                    Set<Slot> leftBottomAggGroupBy = new HashSet<>();
                    agg.getGroupByExpressions().stream().map(e -> (Slot) e).filter(leftOutput::contains)
                            .forEach(leftBottomAggGroupBy::add);
                    join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
                        if (leftOutput.contains(slot)) {
                            leftBottomAggGroupBy.add(slot);
                        }
                    }));
                    List<NamedExpression> leftBottomSums = new ArrayList<>();
                    for (int i = 0; i < leftSums.size(); i++) {
                        leftBottomSums.add(new Alias(new Sum(leftSums.get(i).child()), "left_sum" + i));
                    }
                    Alias leftCnt = new Alias(new Count(), "left_cnt");
                    List<NamedExpression> leftBottomAggOutput = ImmutableList.<NamedExpression>builder()
                            .addAll(leftBottomAggGroupBy).addAll(leftBottomSums).add(leftCnt).build();
                    LogicalAggregate<GroupPlan> leftBottomAgg = new LogicalAggregate<>(
                            ImmutableList.copyOf(leftBottomAggGroupBy), leftBottomAggOutput, join.left());

                    // right bottom agg
                    Set<Slot> rightBottomAggGroupBy = new HashSet<>();
                    agg.getGroupByExpressions().stream().map(e -> (Slot) e).filter(rightOutput::contains)
                            .forEach(rightBottomAggGroupBy::add);
                    join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
                        if (rightOutput.contains(slot)) {
                            rightBottomAggGroupBy.add(slot);
                        }
                    }));
                    List<NamedExpression> rightBottomSums = new ArrayList<>();
                    for (int i = 0; i < rightSums.size(); i++) {
                        rightBottomSums.add(new Alias(new Sum(rightSums.get(i).child()), "right_sum" + i));
                    }
                    Alias rightCnt = new Alias(new Count(), "right_cnt");
                    List<NamedExpression> rightBottomAggOutput = ImmutableList.<NamedExpression>builder()
                            .addAll(rightBottomAggGroupBy).addAll(rightBottomSums).add(rightCnt).build();
                    LogicalAggregate<GroupPlan> rightBottomAgg = new LogicalAggregate<>(
                            ImmutableList.copyOf(rightBottomAggGroupBy), rightBottomAggOutput, join.right());

                    Plan newJoin = join.withChildren(leftBottomAgg, rightBottomAgg);

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
                    Preconditions.checkState(leftSumOutputExprs.size() == leftBottomSums.size());
                    Preconditions.checkState(rightSumOutputExprs.size() == rightBottomSums.size());
                    for (int i = 0; i < leftSumOutputExprs.size(); i++) {
                        Alias oldSum = leftSumOutputExprs.get(i);
                        Slot slot = leftBottomSums.get(i).toSlot();
                        newOutputExprs.add(new Alias(oldSum.getExprId(), new Sum(new Multiply(slot, rightCnt.toSlot())),
                                oldSum.getName()));
                    }
                    for (int i = 0; i < rightSumOutputExprs.size(); i++) {
                        Alias oldSum = rightSumOutputExprs.get(i);
                        Slot bottomSum = rightBottomSums.get(i).toSlot();
                        Alias newSum = new Alias(oldSum.getExprId(), new Sum(new Multiply(bottomSum, leftCnt.toSlot())),
                                oldSum.getName());
                        newOutputExprs.add(newSum);
                    }
                    return agg.withAggOutput(newOutputExprs).withChildren(newJoin);
                }).toRule(RuleType.EAGER_SPLIT);
    }
}
