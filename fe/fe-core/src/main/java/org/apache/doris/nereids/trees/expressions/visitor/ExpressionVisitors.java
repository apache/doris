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
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinction;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is the factory for all ExpressionVisitor instance.
 * All children instance of DefaultExpressionVisitor or ExpressionVisitor for common usage
 * should be here and expose self by class static final field.
 */
public class ExpressionVisitors {

    public static final ContainsAggregateChecker CONTAINS_AGGREGATE_CHECKER = new ContainsAggregateChecker();
    public static final ExpressionMapReplacer EXPRESSION_MAP_REPLACER = new ExpressionMapReplacer();

    /**
     * ContainsAggregateCheckerContext
     */
    public static class ContainsAggregateCheckerContext {
        public boolean needAggregate = false;
    }

    private static class ContainsAggregateChecker extends DefaultExpressionRewriter<ContainsAggregateCheckerContext> {

        @Override
        public Expression visitWindow(WindowExpression windowExpression, ContainsAggregateCheckerContext context) {
            Expression function = windowExpression.getFunction();
            List<Expression> functionChildren = new ArrayList<>();
            for (Expression child : function.children()) {
                functionChildren.add(child.accept(this, context));
            }
            Expression newFunction = function.withChildren(functionChildren);

            List<Expression> partitionKeys = windowExpression.getPartitionKeys();
            List<Expression> newPartitionKeys = new ArrayList<>();
            for (Expression key : partitionKeys) {
                newPartitionKeys.add(key.accept(this, context));
            }

            List<OrderExpression> orderKeys = windowExpression.getOrderKeys();
            List<OrderExpression> newOrderKeys = new ArrayList<>();
            for (OrderExpression orderKey : orderKeys) {
                newOrderKeys.add((OrderExpression) orderKey
                        .withChildren(Lists.newArrayList(orderKey.child().accept(this, context))));
            }

            return windowExpression.withFunctionPartitionKeysOrderKeys(newFunction, newPartitionKeys, newOrderKeys);
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction aggregateFunction,
                                                 ContainsAggregateCheckerContext context) {
            context.needAggregate = true;
            if (aggregateFunction instanceof MultiDistinction) {
                return ((MultiDistinction) aggregateFunction).withMustUseMultiDistinctAgg(true);
            }
            return aggregateFunction;
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
