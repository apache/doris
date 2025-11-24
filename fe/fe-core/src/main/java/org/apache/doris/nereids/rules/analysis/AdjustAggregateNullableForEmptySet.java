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

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * adjust aggregate nullable when: group expr list is empty and function is NullableAggregateFunction,
 * the function in output should be adjusted to nullable and so as having node.
 */
public class AdjustAggregateNullableForEmptySet implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.ADJUST_NULLABLE_FOR_HAVING_SLOT.build(
                        logicalHaving(logicalAggregate())
                                .then(having -> replaceHaving(having, having.child().getGroupByExpressions().isEmpty()))
                ),
                RuleType.ADJUST_NULLABLE_FOR_SORT_SLOT.build(
                        logicalSort(logicalAggregate())
                                .then(sort -> replaceSort(sort, sort.child().getGroupByExpressions().isEmpty()))
                ),
                RuleType.ADJUST_NULLABLE_FOR_SORT_SLOT.build(
                        logicalSort(logicalHaving(logicalAggregate()))
                                .then(sort -> replaceSort(sort, sort.child().child().getGroupByExpressions().isEmpty()))
                )
        );
    }

    public static Expression replaceExpression(Expression expression, boolean alwaysNullable) {
        return FunctionReplacer.INSTANCE.replace(expression, alwaysNullable);
    }

    private LogicalPlan replaceSort(LogicalSort<?> sort, boolean alwaysNullable) {
        ImmutableList.Builder<OrderKey> newOrderKeysBuilder
                = ImmutableList.builderWithExpectedSize(sort.getOrderKeys().size());
        sort.getOrderKeys().forEach(
                key -> newOrderKeysBuilder.add(key.withExpression(replaceExpression(key.getExpr(), alwaysNullable))));
        List<OrderKey> newOrderKeys = newOrderKeysBuilder.build();
        if (newOrderKeys.equals(sort.getOrderKeys())) {
            return null;
        }
        return sort.withOrderKeys(newOrderKeys);
    }

    private LogicalPlan replaceHaving(LogicalHaving<?> having, boolean alwaysNullable) {
        Set<Expression> conjuncts = having.getConjuncts();
        ImmutableSet.Builder<Expression> newConjunctsBuilder
                = ImmutableSet.builderWithExpectedSize(conjuncts.size());
        for (Expression expr : conjuncts) {
            Expression newExpr = replaceExpression(expr, alwaysNullable);
            newConjunctsBuilder.add(newExpr);
        }
        ImmutableSet<Expression> newConjuncts = newConjunctsBuilder.build();
        if (newConjuncts.equals(having.getConjuncts())) {
            return null;
        }
        return having.withConjuncts(newConjuncts);
    }

    /**
     * replace NullableAggregateFunction nullable
     */
    private static class FunctionReplacer extends DefaultExpressionRewriter<Boolean> {
        public static final FunctionReplacer INSTANCE = new FunctionReplacer();

        public Expression replace(Expression expression, boolean alwaysNullable) {
            return expression.accept(this, alwaysNullable);
        }

        @Override
        public Expression visitWindow(WindowExpression windowExpression, Boolean alwaysNullable) {
            ImmutableList.Builder<Expression> newFunctionChildrenBuilder
                    = ImmutableList.builderWithExpectedSize(windowExpression.getFunction().children().size());
            for (Expression child : windowExpression.getFunction().children()) {
                newFunctionChildrenBuilder.add(child.accept(this, alwaysNullable));
            }
            Expression newFunction = windowExpression.getFunction().withChildren(newFunctionChildrenBuilder.build());
            ImmutableList.Builder<Expression> newPartitionKeysBuilder
                    = ImmutableList.builderWithExpectedSize(windowExpression.getPartitionKeys().size());
            for (Expression partitionKey : windowExpression.getPartitionKeys()) {
                newPartitionKeysBuilder.add(partitionKey.accept(this, alwaysNullable));
            }
            ImmutableList.Builder<OrderExpression> newOrderKeysBuilder
                    = ImmutableList.builderWithExpectedSize(windowExpression.getOrderKeys().size());
            for (OrderExpression orderKey : windowExpression.getOrderKeys()) {
                ImmutableList.Builder<Expression> newChildrenBuilder
                        = ImmutableList.builderWithExpectedSize(orderKey.children().size());
                for (Expression child : orderKey.children()) {
                    newChildrenBuilder.add(child.accept(this, alwaysNullable));
                }
                newOrderKeysBuilder.add((OrderExpression) orderKey.withChildren(newChildrenBuilder.build()));
            }
            return windowExpression.withFunctionPartitionKeysOrderKeys(
                    newFunction, newPartitionKeysBuilder.build(), newOrderKeysBuilder.build());
        }

        @Override
        public Expression visitNullableAggregateFunction(NullableAggregateFunction nullableAggregateFunction,
                Boolean alwaysNullable) {
            return nullableAggregateFunction.withAlwaysNullable(alwaysNullable);
        }
    }
}
