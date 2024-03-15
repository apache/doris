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
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Split the expression to equal, range and residual predicate.
 * Should instance when used.
 * TODO support complex predicate split
 */
public class PredicatesSplitter {

    private final Set<Expression> equalPredicates = new HashSet<>();
    private final Set<Expression> rangePredicates = new HashSet<>();
    private final Set<Expression> residualPredicates = new HashSet<>();
    private final List<Expression> conjunctExpressions;

    public PredicatesSplitter(Expression target) {
        this.conjunctExpressions = ExpressionUtils.extractConjunction(target);
        PredicateExtract instance = new PredicateExtract();
        for (Expression expression : conjunctExpressions) {
            expression.accept(instance, null);
        }
    }

    /**
     * extract to equal, range, residual predicate set
     */
    public class PredicateExtract extends DefaultExpressionVisitor<Void, Void> {

        @Override
        public Void visitComparisonPredicate(ComparisonPredicate comparisonPredicate, Void context) {
            Expression leftArg = comparisonPredicate.getArgument(0);
            Expression rightArg = comparisonPredicate.getArgument(1);
            boolean leftArgOnlyContainsColumnRef = containOnlyColumnRef(leftArg, true);
            boolean rightArgOnlyContainsColumnRef = containOnlyColumnRef(rightArg, true);
            if (comparisonPredicate instanceof EqualPredicate) {
                if (leftArgOnlyContainsColumnRef && rightArgOnlyContainsColumnRef) {
                    equalPredicates.add(comparisonPredicate);
                    return null;
                } else if ((leftArgOnlyContainsColumnRef && rightArg instanceof Literal)
                        || (rightArgOnlyContainsColumnRef && leftArg instanceof Literal)) {
                    rangePredicates.add(comparisonPredicate);
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
        public Void visitInPredicate(InPredicate inPredicate, Void context) {
            if (containOnlyColumnRef(inPredicate.getCompareExpr(), true)
                    && (ExpressionUtils.isAllLiteral(inPredicate.getOptions()))) {
                rangePredicates.add(inPredicate);
            } else {
                residualPredicates.add(inPredicate);
            }
            return null;
        }

        @Override
        public Void visit(Expression expr, Void context) {
            residualPredicates.add(expr);
            return null;
        }
    }

    public Predicates.SplitPredicate getSplitPredicate() {
        return Predicates.SplitPredicate.of(
                equalPredicates.isEmpty() ? null : ExpressionUtils.and(equalPredicates),
                rangePredicates.isEmpty() ? null : ExpressionUtils.and(rangePredicates),
                residualPredicates.isEmpty() ? null : ExpressionUtils.and(residualPredicates));
    }

    private static boolean containOnlyColumnRef(Expression expression, boolean allowCast) {
        if (expression instanceof SlotReference && expression.isColumnFromTable()) {
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
