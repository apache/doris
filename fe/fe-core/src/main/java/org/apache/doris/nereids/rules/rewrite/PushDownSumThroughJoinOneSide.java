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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;
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
                            return PushDownMinMaxThroughJoin.pushMinMaxSum(agg, agg.child(), ImmutableList.of());
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
                            return PushDownMinMaxThroughJoin.pushMinMaxSum(agg, agg.child().child(),
                                    agg.child().getProjects());
                        })
                        .toRule(RuleType.PUSH_DOWN_SUM_THROUGH_JOIN)
        );
    }
}
