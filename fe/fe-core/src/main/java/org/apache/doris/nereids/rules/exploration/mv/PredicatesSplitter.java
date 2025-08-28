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

import org.apache.doris.nereids.rules.exploration.mv.Predicates.ExpressionInfo;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Split the expression to equal, range and residual predicate.
 * Should instance when used.
 * TODO support complex predicate split
 */
public class PredicatesSplitter {

    private final Map<Expression, ExpressionInfo> equalPredicates = new HashMap<>();
    private final Map<Expression, ExpressionInfo> rangePredicates = new HashMap<>();
    private final Map<Expression, ExpressionInfo> residualPredicates = new HashMap<>();
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
                    equalPredicates.put(comparisonPredicate, ExpressionInfo.EMPTY);
                    return null;
                } else if ((leftArgOnlyContainsColumnRef && rightArg instanceof Literal)
                        || (rightArgOnlyContainsColumnRef && leftArg instanceof Literal)) {
                    rangePredicates.put(comparisonPredicate, ExpressionInfo.EMPTY);
                } else {
                    residualPredicates.put(comparisonPredicate, ExpressionInfo.EMPTY);
                }
            } else if ((leftArgOnlyContainsColumnRef && rightArg instanceof Literal)
                    || (rightArgOnlyContainsColumnRef && leftArg instanceof Literal)) {
                rangePredicates.put(comparisonPredicate, ExpressionInfo.EMPTY);
            } else {
                residualPredicates.put(comparisonPredicate, ExpressionInfo.EMPTY);
            }
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicate inPredicate, Void context) {
            if (containOnlyColumnRef(inPredicate.getCompareExpr(), true)
                    && (inPredicate.optionsAreLiterals())) {
                rangePredicates.put(inPredicate, ExpressionInfo.EMPTY);
            } else {
                residualPredicates.put(inPredicate, ExpressionInfo.EMPTY);
            }
            return null;
        }

        @Override
        public Void visit(Expression expr, Void context) {
            residualPredicates.put(expr, ExpressionInfo.EMPTY);
            return null;
        }
    }

    public Predicates.SplitPredicate getSplitPredicate() {
        return Predicates.SplitPredicate.of(equalPredicates, rangePredicates, residualPredicates);
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
