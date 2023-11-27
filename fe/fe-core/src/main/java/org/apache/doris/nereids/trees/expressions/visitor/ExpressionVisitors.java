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

import org.apache.doris.nereids.rules.exploration.mv.Predicates;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the factory for all ExpressionVisitor instance.
 * All children instance of DefaultExpressionVisitor or ExpressionVisitor for common usage
 * should be here and expose self by class static final field.
 */
public class ExpressionVisitors {

    public static final ContainsAggregateChecker CONTAINS_AGGREGATE_CHECKER = new ContainsAggregateChecker();

    private static class ContainsAggregateChecker extends DefaultExpressionVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Expression expr, Void context) {
            boolean needAggregate = false;
            for (Expression child : expr.children()) {
                needAggregate = needAggregate || child.accept(this, context);
            }
            return needAggregate;
        }

        @Override
        public Boolean visitWindow(WindowExpression windowExpression, Void context) {
            boolean needAggregate = false;
            for (Expression child : windowExpression.getExpressionsInWindowSpec()) {
                needAggregate = needAggregate || child.accept(this, context);
            }
            return needAggregate;
        }

        @Override
        public Boolean visitAggregateFunction(AggregateFunction aggregateFunction, Void context) {
            return true;
        }
    }

    /**
     * Split the expression to equal, range and residual predicate.
     * Should instance when used.
     */
    public static class PredicatesSpliter extends DefaultExpressionVisitor<Void, Void> {

        private List<Expression> equalPredicates = new ArrayList<>();
        private List<Expression> rangePredicates = new ArrayList<>();
        private List<Expression> residualPredicates = new ArrayList<>();
        private final Expression target;

        public PredicatesSpliter(Expression target) {
            this.target = target;
        }

        @Override
        public Void visitComparisonPredicate(ComparisonPredicate comparisonPredicate, Void context) {
            // TODO Smallest implement, complete later
            if (comparisonPredicate instanceof EqualTo) {
                Expression leftArgument = comparisonPredicate.getArgument(0);
                Expression rightArgument = comparisonPredicate.getArgument(1);
                if (leftArgument.isSlot() && rightArgument.isSlot()) {
                    equalPredicates.add(comparisonPredicate);
                } else {
                    rangePredicates.add(comparisonPredicate);
                }
            }
            return super.visit(comparisonPredicate, context);
        }

        public Expression getTarget() {
            return target;
        }

        public Predicates.SplitPredicate getSplitPredicate() {
            return Predicates.SplitPredicate.of(
                    equalPredicates.isEmpty() ? null : ExpressionUtils.and(equalPredicates),
                    rangePredicates.isEmpty() ? null : ExpressionUtils.and(rangePredicates),
                    residualPredicates.isEmpty() ? null : ExpressionUtils.and(residualPredicates));
        }
    }
}
