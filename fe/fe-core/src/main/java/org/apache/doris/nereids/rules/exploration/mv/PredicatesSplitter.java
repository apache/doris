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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Split the expression to equal, range and residual predicate.
 * Should instance when used.
 * TODO support complex predicate split
 */
public class PredicatesSplitter {

    private final List<Expression> equalPredicates = new ArrayList<>();
    private final List<Expression> rangePredicates = new ArrayList<>();
    private final List<Expression> residualPredicates = new ArrayList<>();
    private final List<Expression> conjunctExpressions;

    private final PredicateExtract instance = new PredicateExtract();

    public PredicatesSplitter(Expression target) {
        this.conjunctExpressions = ExpressionUtils.extractConjunction(target);
        for (Expression expression : conjunctExpressions) {
            expression.accept(instance, expression);
        }
    }

    /**
     * PredicateExtract
     */
    public class PredicateExtract extends DefaultExpressionVisitor<Void, Expression> {

        @Override
        public Void visitComparisonPredicate(ComparisonPredicate comparisonPredicate, Expression sourceExpression) {
            Expression leftArg = comparisonPredicate.getArgument(0);
            Expression rightArg = comparisonPredicate.getArgument(1);
            boolean leftArgOnlyContainsColumnRef = containOnlyColumnRef(leftArg, true);
            boolean rightArgOnlyContainsColumnRef = containOnlyColumnRef(rightArg, true);
            if (comparisonPredicate instanceof EqualPredicate) {
                if (leftArgOnlyContainsColumnRef && rightArgOnlyContainsColumnRef) {
                    equalPredicates.add(comparisonPredicate);
                    return null;
                } else {
                    residualPredicates.add(comparisonPredicate);
                }
            } else if ((leftArgOnlyContainsColumnRef && rightArg instanceof Literal)
                    || (rightArgOnlyContainsColumnRef && leftArg instanceof Literal)) {
                rangePredicates.add(comparisonPredicate);
            } else {
                residualPredicates.add(comparisonPredicate);
            }
            return null;
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicate compoundPredicate, Expression context) {
            if (compoundPredicate instanceof Or) {
                residualPredicates.add(compoundPredicate);
                return null;
            }
            return super.visitCompoundPredicate(compoundPredicate, context);
        }
    }

    public Predicates.SplitPredicate getSplitPredicate() {
        return Predicates.SplitPredicate.of(
                equalPredicates.isEmpty() ? null : ExpressionUtils.and(equalPredicates),
                rangePredicates.isEmpty() ? null : ExpressionUtils.and(rangePredicates),
                residualPredicates.isEmpty() ? null : ExpressionUtils.and(residualPredicates));
    }

    private static boolean containOnlyColumnRef(Expression expression, boolean allowCast) {
        if (expression instanceof SlotReference && ((SlotReference) expression).isColumnFromTable()) {
            return true;
        }
        if (allowCast && expression instanceof Cast) {
            return containOnlyColumnRef(((Cast) expression).child(), true);
        }
        if (expression instanceof Alias) {
            return containOnlyColumnRef(((Alias) expression).child(), true);
        }
        return false;
    }
}
