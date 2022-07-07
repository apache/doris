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
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Used to generate the merge agg node for distributed execution.
 * If we have a query: SELECT SUM(v) + 1 FROM t GROUP BY k + 1
 * the initial plan is:
 *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(v1 * v2) + 1], groupByExpr: [k + 1])
 *   +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(a) + 1], groupByExpr: [b])
 *   +-- Aggregate(phase: [LOCAL], outputExpr: [SUM(v1 * v2) as a, (k + 1) as b], groupByExpr: [k + 1])
 *       +-- childPlan
 *
 * TODO:
 *     1. use different class represent different phase aggregate
 *     2. if instance count is 1, shouldn't disassemble the agg operator
 *     3. we need another rule to removing duplicated expressions in group by expression list
 */
public class AggregateDisassemble extends OneRewriteRuleFactory {

    @Override
    public Rule<Plan> build() {
        return logicalAggregate().when(p -> {
            LogicalAggregate logicalAggregate = p.getOperator();
            return !logicalAggregate.isDisassembled();
        }).thenApply(ctx -> {
            LogicalUnaryPlan<LogicalAggregate, GroupPlan> plan = ctx.root;
            LogicalAggregate aggregate = plan.getOperator();
            List<NamedExpression> originOutputExprs = aggregate.getOutputExpressionList();
            List<Expression> originGroupByExprs = aggregate.getGroupByExpressionList();

            Map<AggregateFunction, NamedExpression> originAggregateFunctionWithAlias = Maps.newHashMap();
            for (NamedExpression originOutputExpr : originOutputExprs) {
                originOutputExpr.foreach(e -> {
                    if (e instanceof AggregateFunction) {
                        AggregateFunction a = (AggregateFunction) e;
                        originAggregateFunctionWithAlias.put(a, new Alias<>(a, a.sql()));
                    }
                });
            }

            List<Expression> localGroupByExprs = originGroupByExprs;
            List<NamedExpression> localGroupByWrappedAlias = localGroupByExprs.stream()
                    .map(g -> new Alias<>(g, g.sql()))
                    .collect(Collectors.toList());

            List<NamedExpression> localOutputExprs = Lists.newArrayList();
            localOutputExprs.addAll(localGroupByWrappedAlias);
            localOutputExprs.addAll(originAggregateFunctionWithAlias.values());

            List<Expression> mergeGroupByExpressionList = localGroupByWrappedAlias.stream()
                    .map(NamedExpression::toSlot).collect(Collectors.toList());

            List<NamedExpression> globalOutputExprs = Lists.newArrayList();
            for (NamedExpression o : originOutputExprs) {
                if (o.anyMatch(AggregateFunction.class::isInstance)) {
                    globalOutputExprs.add((NamedExpression) new AggregateFunctionParamsRewriter()
                            .visit(o, originAggregateFunctionWithAlias));
                } else {
                    for (int i = 0; i < localGroupByWrappedAlias.size(); i++) {
                        // TODO: we need to do sub tree match and replace. but we do not have semanticEquals now.
                        //    e.g. a + 1 + 2 in output expression should be replaced by
                        //    (slot reference to update phase out (a + 1)) + 2, if we do group by a + 1
                        //   currently, we could only handle output expression same with group by expression
                        if (o instanceof SlotReference) {
                            // a in output expression will be SLotReference
                            if (o.equals(localGroupByExprs.get(i))) {
                                globalOutputExprs.add(localGroupByWrappedAlias.get(i).toSlot());
                                break;
                            }
                        } else if (o instanceof Alias) {
                            // a + 1 in output expression will be Alias
                            if (o.child(0).equals(localGroupByExprs.get(i))) {
                                globalOutputExprs.add(localGroupByWrappedAlias.get(i).toSlot());
                                break;
                            }
                        }
                    }
                }
            }

            LogicalAggregate localAggregate = new LogicalAggregate(
                    localGroupByExprs,
                    localOutputExprs,
                    true,
                    AggPhase.LOCAL
            );

            Plan childPlan = plan(localAggregate, plan.child(0));

            LogicalAggregate globalAggregate = new LogicalAggregate(
                    mergeGroupByExpressionList,
                    globalOutputExprs,
                    true,
                    AggPhase.GLOBAL
            );
            return plan(globalAggregate, childPlan);
        }).toRule(RuleType.AGGREGATE_DISASSEMBLE);
    }

    @SuppressWarnings("InnerClassMayBeStatic")
    private class AggregateFunctionParamsRewriter
            extends DefaultExpressionRewriter<Map<AggregateFunction, NamedExpression>> {
        @Override
        public Expression visitAggregateFunction(AggregateFunction boundFunction,
                Map<AggregateFunction, NamedExpression> context) {
            return boundFunction.withChildren(Lists.newArrayList(context.get(boundFunction).toSlot()));
        }
    }
}
