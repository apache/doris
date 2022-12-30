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
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;

import com.google.common.collect.ImmutableList;

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
                                .when(agg -> agg.getGroupByExpressions().isEmpty())
                                .then(agg -> {
                                    List<NamedExpression> output = agg.getOutputExpressions().stream()
                                            .map(ne -> ((NamedExpression) FunctionReplacer.INSTANCE.replace(ne)))
                                            .collect(Collectors.toList());
                                    return agg.withAggOutput(output);
                                })
                ),
                RuleType.ADJUST_NULLABLE_FOR_HAVING_SLOT.build(
                        logicalHaving(logicalAggregate())
                                .when(having -> (having.child().getGroupByExpressions().isEmpty()))
                                .then(having -> {
                                    Set<Expression> newConjuncts = having.getConjuncts().stream()
                                            .map(FunctionReplacer.INSTANCE::replace)
                                            .collect(Collectors.toSet());
                                    return new LogicalHaving<>(newConjuncts, having.child());
                                })
                )
        );
    }

    private static class FunctionReplacer extends DefaultExpressionRewriter<Void> {
        public static final FunctionReplacer INSTANCE = new FunctionReplacer();

        public Expression replace(Expression expression) {
            return expression.accept(INSTANCE, null);
        }

        @Override
        public Expression visitNullableAggregateFunction(NullableAggregateFunction nullableAggregateFunction,
                Void unused) {
            return nullableAggregateFunction.withAlwaysNullable(true);
        }
    }
}
