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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;

import java.util.Map;
import java.util.Optional;

/**
 * This is the factory for all ExpressionVisitor instance.
 * All children instance of DefaultExpressionVisitor or ExpressionVisitor for common usage
 * should be here and expose self by class static final field.
 */
public class ExpressionVisitors {

    public static final NonWindowAggregateGetter NON_WINDOW_AGGREGATE_GETTER = new NonWindowAggregateGetter();
    public static final ExpressionMapReplacer EXPRESSION_MAP_REPLACER = new ExpressionMapReplacer();

    // return non window aggregate function, but return only one, not all
    private static class NonWindowAggregateGetter extends DefaultExpressionVisitor<Optional<AggregateFunction>, Void> {
        @Override
        public Optional<AggregateFunction> visit(Expression expr, Void context) {
            if (!expr.containsType(AggregateFunction.class)) {
                return Optional.empty();
            }
            for (Expression child : expr.children()) {
                Optional<AggregateFunction> aggOpt = child.accept(this, context);
                if (aggOpt.isPresent()) {
                    return aggOpt;
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<AggregateFunction> visitWindow(WindowExpression windowExpression, Void context) {
            for (Expression child : windowExpression.getExpressionsInWindowSpec()) {
                Optional<AggregateFunction> aggOpt = child.accept(this, context);
                if (aggOpt.isPresent()) {
                    return aggOpt;
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<AggregateFunction> visitAggregateFunction(AggregateFunction aggregateFunction, Void context) {
            return Optional.of(aggregateFunction);
        }
    }

    /**
     * replace sub expr by Map
     */
    public static class ExpressionMapReplacer
            extends DefaultExpressionRewriter<Map<Expression, Expression>> {

        private ExpressionMapReplacer() {
        }

        @Override
        public Expression visit(Expression expr, Map<Expression, Expression> replaceMap) {
            if (replaceMap.containsKey(expr)) {
                return replaceMap.get(expr);
            }
            return super.visit(expr, replaceMap);
        }
    }
}
