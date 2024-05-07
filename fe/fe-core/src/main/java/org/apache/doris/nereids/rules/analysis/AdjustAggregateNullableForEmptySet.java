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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * adjust aggregate nullable when: group expr list is empty and function is NullableAggregateFunction,
 * the function in output should be adjusted to nullable and so as having node.
 */
public class AdjustAggregateNullableForEmptySet implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.ADJUST_NULLABLE_FOR_AGGREGATE_SLOT.build(
                        logicalAggregate()
                                .then(agg -> {
                                    List<NamedExpression> outputExprs = agg.getOutputExpressions();
                                    boolean noGroupBy = agg.getGroupByExpressions().isEmpty();
                                    ImmutableList.Builder<NamedExpression> newOutput
                                            = ImmutableList.builderWithExpectedSize(outputExprs.size());
                                    for (NamedExpression ne : outputExprs) {
                                        NamedExpression newExpr =
                                                ((NamedExpression) FunctionReplacer.INSTANCE.replace(ne, noGroupBy));
                                        newOutput.add(newExpr);
                                    }
                                    return agg.withAggOutput(newOutput.build());
                                })
                ),
                RuleType.ADJUST_NULLABLE_FOR_HAVING_SLOT.build(
                        logicalHaving(logicalAggregate())
                                .then(having -> {
                                    Set<Expression> conjuncts = having.getConjuncts();
                                    boolean noGroupBy = having.child().getGroupByExpressions().isEmpty();
                                    ImmutableSet.Builder<Expression> newConjuncts
                                            = ImmutableSet.builderWithExpectedSize(conjuncts.size());
                                    for (Expression expr : conjuncts) {
                                        Expression newExpr = FunctionReplacer.INSTANCE.replace(expr, noGroupBy);
                                        newConjuncts.add(newExpr);
                                    }
                                    return new LogicalHaving<>(newConjuncts.build(), having.child());
                                })
                )
        );
    }

    private static class FunctionReplacer extends DefaultExpressionRewriter<Boolean> {
        public static final FunctionReplacer INSTANCE = new FunctionReplacer();

        public Expression replace(Expression expression, boolean alwaysNullable) {
            return expression.accept(INSTANCE, alwaysNullable);
        }

        @Override
        public Expression visitWindow(WindowExpression windowExpression, Boolean alwaysNullable) {
            return windowExpression.withPartitionKeysOrderKeys(
                    windowExpression.getPartitionKeys().stream()
                            .map(k -> k.accept(INSTANCE, alwaysNullable))
                            .collect(Collectors.toList()),
                    windowExpression.getOrderKeys().stream()
                            .map(k -> (OrderExpression) k.withChildren(k.children().stream()
                                    .map(c -> c.accept(INSTANCE, alwaysNullable))
                                    .collect(Collectors.toList())))
                            .collect(Collectors.toList())
            );
        }

        @Override
        public Expression visitNullableAggregateFunction(NullableAggregateFunction nullableAggregateFunction,
                Boolean alwaysNullable) {
            return nullableAggregateFunction.withAlwaysNullable(alwaysNullable);
        }
    }
}
