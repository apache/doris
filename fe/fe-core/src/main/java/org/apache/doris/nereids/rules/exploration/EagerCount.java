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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
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
 * aggregate: SUM(x)
 * |
 * join
 * |   \
 * |    *
 * (x)
 * ->
 * aggregate: SUM(x * cnt)
 * |
 * join
 * |   \
 * |    aggregate COUNT: cnt
 * (x)
 * </pre>
 */
public class EagerCount extends OneExplorationRuleFactory {
    public static final EagerCount INSTANCE = new EagerCount();

    @Override
    public Rule build() {
        return logicalAggregate(logicalJoin())
                .when(agg -> agg.child().getOtherJoinConjuncts().size() == 0)
                .when(agg -> agg.getGroupByExpressions().stream().allMatch(e -> e instanceof Slot))
                .when(agg -> agg.getAggregateFunctions().stream()
                        .allMatch(f -> f instanceof Sum
                                && ((Sum) f).child() instanceof SlotReference
                                && agg.child().left().getOutputSet().contains((SlotReference) ((Sum) f).child())))
                .then(agg -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = agg.child();
                    List<Slot> rightOutput = join.right().getOutput();

                    Set<NamedExpression> cntAggGroupBy = new HashSet<>();
                    agg.getGroupByExpressions().stream().map(e -> (Slot) e).filter(rightOutput::contains)
                            .forEach(cntAggGroupBy::add);
                    join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
                        if (rightOutput.contains(slot)) {
                            cntAggGroupBy.add(slot);
                        }
                    }));
                    Alias cnt = new Alias(new Count(Literal.of(1)), "cnt");
                    List<NamedExpression> cntAggOutput = ImmutableList.<NamedExpression>builder()
                            .addAll(cntAggGroupBy).add(cnt).build();
                    LogicalAggregate<GroupPlan> cntAgg = new LogicalAggregate<>(
                            ImmutableList.copyOf(cntAggGroupBy), cntAggOutput, join.right());
                    Plan newJoin = join.withChildren(join.left(), cntAgg);

                    List<NamedExpression> newOutputExprs = new ArrayList<>();
                    List<Alias> sumOutputExprs = new ArrayList<>();
                    for (NamedExpression ne : agg.getOutputExpressions()) {
                        if (ne instanceof Alias && ((Alias) ne).child() instanceof Sum) {
                            sumOutputExprs.add((Alias) ne);
                        } else {
                            newOutputExprs.add(ne);
                        }
                    }
                    for (AggregateFunction f : agg.getAggregateFunctions()) {
                        Sum sum = (Sum) f;
                        Alias oldSum = sumOutputExprs.stream()
                                .filter(alias -> alias.child().equals(f)).findAny()
                                .orElseThrow(() -> new RuntimeException("Cannot find output expression for " + f));
                        Alias newSum = new Alias(oldSum.getExprId(), new Sum(new Multiply(sum.child(), cnt.toSlot())),
                                oldSum.getName());
                        newOutputExprs.add(newSum);
                    }

                    return agg.withAggOutputChild(newOutputExprs, newJoin);
                }).toRule(RuleType.EAGER_COUNT);
    }
}
