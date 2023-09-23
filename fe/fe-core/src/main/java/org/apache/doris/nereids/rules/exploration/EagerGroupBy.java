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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
 * aggregate: SUM(sum1)
 * |
 * join
 * |   \
 * |    *
 * aggregate: SUM(x) as sum1
 * </pre>
 * After Eager Group By, new plan also can apply `Eager Count`.
 * It's `Double Eager`.
 */
public class EagerGroupBy implements ExplorationRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate(innerLogicalJoin())
                        .when(agg -> agg.child().getOtherJoinConjuncts().size() == 0)
                        .when(agg -> agg.getAggregateFunctions().stream()
                                .allMatch(f -> f instanceof Sum
                                        && ((Sum) f).child() instanceof SlotReference
                                        && agg.child().left().getOutputSet()
                                        .contains((SlotReference) ((Sum) f).child())))
                        .then(agg -> eagerGroupBy(agg, agg.child(), ImmutableList.of()))
                        .toRule(RuleType.EAGER_GROUP_BY),
                logicalAggregate(logicalProject(innerLogicalJoin()))
                        .when(agg -> agg.child().isAllSlots())
                        .when(agg -> agg.child().child().getOtherJoinConjuncts().size() == 0)
                        .when(agg -> agg.getGroupByExpressions().stream().allMatch(e -> e instanceof Slot))
                        .when(agg -> agg.getAggregateFunctions().stream()
                                .allMatch(f -> f instanceof Sum
                                        && ((Sum) f).child() instanceof SlotReference
                                        && agg.child().child().left().getOutputSet()
                                        .contains((SlotReference) ((Sum) f).child())))
                        .then(agg -> eagerGroupBy(agg, agg.child().child(), agg.child().getProjects()))
                        .toRule(RuleType.EAGER_GROUP_BY)
        );
    }

    private LogicalAggregate<Plan> eagerGroupBy(LogicalAggregate<? extends Plan> agg,
            LogicalJoin<GroupPlan, GroupPlan> join, List<NamedExpression> projects) {
        List<Slot> leftOutput = join.left().getOutput();
        List<Sum> sums = agg.getAggregateFunctions().stream().map(Sum.class::cast)
                .collect(Collectors.toList());

        // eager group-by
        Set<Slot> sumAggGroupBy = new HashSet<>();
        agg.getGroupByExpressions().stream().map(e -> (Slot) e).filter(leftOutput::contains)
                .forEach(sumAggGroupBy::add);
        join.getHashJoinConjuncts().forEach(e -> e.getInputSlots().forEach(slot -> {
            if (leftOutput.contains(slot)) {
                sumAggGroupBy.add(slot);
            }
        }));
        List<NamedExpression> bottomSums = new ArrayList<>();
        for (int i = 0; i < sums.size(); i++) {
            bottomSums.add(new Alias(new Sum(sums.get(i).child()), "sum" + i));
        }
        List<NamedExpression> sumAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(sumAggGroupBy).addAll(bottomSums).build();
        LogicalAggregate<GroupPlan> sumAgg = new LogicalAggregate<>(
                ImmutableList.copyOf(sumAggGroupBy), sumAggOutput, join.left());
        Plan newJoin = join.withChildren(sumAgg, join.right());

        List<NamedExpression> newOutputExprs = new ArrayList<>();
        List<Alias> sumOutputExprs = new ArrayList<>();
        for (NamedExpression ne : agg.getOutputExpressions()) {
            if (ne instanceof Alias && ((Alias) ne).child() instanceof Sum) {
                sumOutputExprs.add((Alias) ne);
            } else {
                newOutputExprs.add(ne);
            }
        }
        for (int i = 0; i < sumOutputExprs.size(); i++) {
            Alias oldSum = sumOutputExprs.get(i);
            // sum in bottom Agg
            Slot bottomSum = bottomSums.get(i).toSlot();
            Alias newSum = new Alias(oldSum.getExprId(), new Sum(bottomSum), oldSum.getName());
            newOutputExprs.add(newSum);
        }
        Plan child = PlanUtils.projectOrSelf(projects, newJoin);
        return agg.withAggOutputChild(newOutputExprs, child);
    }
}
