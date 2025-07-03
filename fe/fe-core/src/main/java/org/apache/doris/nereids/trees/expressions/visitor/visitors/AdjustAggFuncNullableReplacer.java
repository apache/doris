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

package org.apache.doris.nereids.trees.expressions.visitor.visitors;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import java.util.stream.Collectors;

/**
 * Adjust NullableAggregateFunction nullable.
 * For an aggregate, when its group by expressions is empty, its NullableAggregateFunctions should be nullable,
 * otherwise its NullableAggregateFunctions should be not nullable.
 */
public class AdjustAggFuncNullableReplacer extends DefaultExpressionRewriter<Boolean> {
    public static final AdjustAggFuncNullableReplacer INSTANCE = new AdjustAggFuncNullableReplacer();

    public Expression replace(Expression expression, boolean alwaysNullable) {
        return expression.accept(this, alwaysNullable);
    }

    @Override
    public Expression visitWindow(WindowExpression windowExpression, Boolean alwaysNullable) {
        return windowExpression.withPartitionKeysOrderKeys(
                windowExpression.getPartitionKeys().stream()
                        .map(k -> k.accept(this, alwaysNullable))
                        .collect(Collectors.toList()),
                windowExpression.getOrderKeys().stream()
                        .map(k -> (OrderExpression) k.withChildren(k.children().stream()
                                .map(c -> c.accept(this, alwaysNullable))
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
