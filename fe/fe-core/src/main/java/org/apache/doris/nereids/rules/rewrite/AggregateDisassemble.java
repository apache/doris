// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.AggPhase;
import org.apache.doris.nereids.operators.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Used to generate the merge agg node for distributed execution.
 * If we have a query: SELECT SUM(v) + 1 FROM t GROUP BY k + 1
 * the initial plan is:
 *   Aggregate(phase: [FIRST_MERGE], outputExpr: SUM(v1 * v2) + 1, groupByExpr: k + 1)
 *   +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [FIRST_MERGE], outputExpr: [SUM(a) + 1], groupByExpr: [b])
 *   +-- Aggregate(phase: [FIRST], outputExpr: [SUM(v1 * v2) as a, (k + 1) as b], groupByExpr: [k + 1])
 *       +-- childPlan
 *
 * TODO:
 *     1. if instance count is 1, shouldn't disassemble the agg operator
 *     2. we need another rule to removing duplicated expressions in group by expression list
 */
public class AggregateDisassemble extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalAggregate().when(p -> {
            LogicalAggregate logicalAggregation = p.getOperator();
            return !logicalAggregation.isDisassembled();
        }).thenApply(ctx -> {
            Plan plan = ctx.root;
            Operator operator = plan.getOperator();
            LogicalAggregate agg = (LogicalAggregate) operator;
            List<NamedExpression> outputExpressionList = agg.getOutputExpressionList();
            List<Expression> groupByExpressionList = agg.getGroupByExpressionList();

            Map<AggregateFunction, NamedExpression> aggregateFunctionAliasMap = Maps.newHashMap();
            for (NamedExpression outputExpression : outputExpressionList) {
                outputExpression.foreach(e -> {
                    if (e instanceof AggregateFunction) {
                        AggregateFunction a = (AggregateFunction) e;
                        aggregateFunctionAliasMap.put(a, new Alias<>(a, a.sql()));
                    }
                });
            }

            List<Expression> updateGroupByExpressionList = groupByExpressionList;
            List<NamedExpression> updateGroupByAliasList = updateGroupByExpressionList.stream()
                    .map(g -> new Alias<>(g, g.sql()))
                    .collect(Collectors.toList());

            List<NamedExpression> updateOutputExpressionList = Lists.newArrayList();
            updateOutputExpressionList.addAll(updateGroupByAliasList);
            updateOutputExpressionList.addAll(aggregateFunctionAliasMap.values());

            List<Expression> mergeGroupByExpressionList = updateGroupByAliasList.stream()
                    .map(NamedExpression::toSlot).collect(Collectors.toList());

            List<NamedExpression> mergeOutputExpressionList = Lists.newArrayList();
            for (NamedExpression o : outputExpressionList) {
                if (o.anyMatch(AggregateFunction.class::isInstance)) {
                    mergeOutputExpressionList.add((NamedExpression) new AggregateFunctionParamsRewriter()
                            .visit(o, aggregateFunctionAliasMap));
                } else {
                    for (int i = 0; i < updateGroupByAliasList.size(); i++) {
                        // TODO: we need to do sub tree match and replace. but we do not have semanticEquals now.
                        //    e.g. a + 1 + 2 in output expression should be replaced by
                        //    (slot reference to update phase out (a + 1)) + 2, if we do group by a + 1
                        //   currently, we could only handle output expression same with group by expression
                        if (o instanceof SlotReference) {
                            // a in output expression will be SLotReference
                            if (o.equals(updateGroupByExpressionList.get(i))) {
                                mergeOutputExpressionList.add(updateGroupByAliasList.get(i).toSlot());
                                break;
                            }
                        } else if (o instanceof Alias) {
                            // a + 1 in output expression will be Alias
                            if (o.child(0).equals(updateGroupByExpressionList.get(i))) {
                                mergeOutputExpressionList.add(updateGroupByAliasList.get(i).toSlot());
                                break;
                            }
                        }
                    }
                }
            }

            LogicalAggregate localAgg = new LogicalAggregate(
                    updateGroupByExpressionList,
                    updateOutputExpressionList,
                    true,
                    AggPhase.FIRST
            );

            Plan childPlan = plan(localAgg, plan.child(0));

            LogicalAggregate mergeAgg = new LogicalAggregate(
                    mergeGroupByExpressionList,
                    mergeOutputExpressionList,
                    true,
                    AggPhase.FIRST_MERGE
            );
            return plan(mergeAgg, childPlan);
        }).toRule(RuleType.AGGREGATE_DISASSEMBLE);
    }

    private static class AggregateFunctionParamsRewriter
            extends DefaultExpressionRewriter<Map<AggregateFunction, NamedExpression>> {
        @Override
        public Expression visitAggregateFunction(AggregateFunction boundFunction,
                Map<AggregateFunction, NamedExpression> context) {
            return boundFunction.withChildren(Lists.newArrayList(context.get(boundFunction).toSlot()));
        }
    }
}
