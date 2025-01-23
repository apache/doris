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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Remove conflict predicate
 * for example:
 * `a > b and not (a > b)` => `falseOrNull(a > b)`
 * `a > b and b >= a` => `falseOrNull(a > b)`
 * `a > b or not (a > b)` => `trueOrNull(a >b)`
 * `a > b or b >= a` => `trueOrNull(a > b)`
 *  `a is null and not a is null` => `FALSE`
 *  `a is null or not a is null` => `TRUE`
 */
public class SimplifyConflictPredicate implements ExpressionPatternRuleFactory {
    public static final SimplifyConflictPredicate INSTANCE = new SimplifyConflictPredicate();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class).then(this::rewrite)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONFLICT_PREDICATE)
        );
    }

    private Expression rewrite(CompoundPredicate compoundPredicate) {
        List<Expression> flatten = ExpressionUtils.extract(compoundPredicate);
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(flatten.size());
        boolean changed = false;
        // expression -> (exist expression, exist (not expression))
        // if expression -> (true, true) then expression has conflict,
        // ie, predicate contains expression 'expression' and 'not expression'
        Map<Expression, Pair<Boolean, Boolean>> exprExistMarks = Maps.newHashMap();
        for (Expression child : flatten) {
            if (!child.containsNonfoldable()) {
                if (child instanceof CompoundPredicate) {
                    Expression newChild = rewrite((CompoundPredicate) child);
                    if (!child.equals(newChild)) {
                        child = newChild;
                        changed = true;
                    }
                }
                Pair<Expression, Boolean> pair = normalComparisonAndNot(child);
                Expression normalExpr = pair.first;
                boolean isNot = pair.second;
                Pair<Boolean, Boolean> mark = exprExistMarks.computeIfAbsent(normalExpr, k -> Pair.of(false, false));
                if (isNot) {
                    mark = Pair.of(mark.first, true);
                } else {
                    mark = Pair.of(true, mark.second);
                }
                exprExistMarks.put(normalExpr, mark);
            }
            newChildren.add(child);
        }
        // conflict expression -> had written
        Map<Expression, Boolean> conflictExprMarks = exprExistMarks.entrySet().stream()
                .filter(entry -> entry.getValue().first && entry.getValue().second)
                .collect(Collectors.toMap(Entry::getKey, k -> false));
        if (!conflictExprMarks.isEmpty()) {
            List<Expression> finalNewChildren = Lists.newArrayListWithExpectedSize(newChildren.size());
            for (Expression child : newChildren) {
                Expression normalExpr = normalComparisonAndNot(child).first;
                Boolean written = conflictExprMarks.get(normalExpr);
                // no conflict
                if (written == null) {
                    finalNewChildren.add(child);
                } else if (!written) { // had conflict, and not add expression
                    Expression newChild = child;
                    while (newChild instanceof Not) {
                        newChild = ((Not) newChild).child();
                    }
                    newChild = compoundPredicate instanceof And
                            ? ExpressionUtils.falseOrNull(newChild) : ExpressionUtils.trueOrNull(newChild);
                    finalNewChildren.add(newChild);
                    conflictExprMarks.put(normalExpr, true);
                }
            }
            newChildren = finalNewChildren;
            changed = true;
        }
        if (changed) {
            return newChildren.size() == 1 ? newChildren.get(0) : compoundPredicate.withChildren(newChildren);
        }
        return compoundPredicate;
    }

    // normal expr, then return <normalExpr, isNot> will satisfy:
    // if expr is not(not ...()), normalExpr will eliminate all the NOTs.
    // if expr is <, <=, >, >=, normalExpr will convert to '<'
    // if expr is =, <=>, normalExpr left child's hash code <= right child's hash code
    private Pair<Expression, Boolean> normalComparisonAndNot(Expression expr) {
        boolean isNot = false;
        Expression normalExpr = expr;
        while (normalExpr instanceof Not) {
            isNot = !isNot;
            normalExpr = ((Not) normalExpr).child();
        }

        // convert '<, <=, >, >=' to '<'
        if (normalExpr instanceof ComparisonPredicate) {
            Expression left = ((ComparisonPredicate) normalExpr).left();
            Expression right = ((ComparisonPredicate) normalExpr).right();
            if (normalExpr instanceof GreaterThan) {
                normalExpr = new LessThan(right, left);
            } else if (normalExpr instanceof GreaterThanEqual) {
                normalExpr = new LessThan(left, right);
                isNot = !isNot;
            } else if (normalExpr instanceof LessThanEqual) {
                normalExpr = new LessThan(right, left);
                isNot = !isNot;
            } else if ((normalExpr instanceof EqualTo || normalExpr instanceof NullSafeEqual)
                    && (left.hashCode() > right.hashCode())) {
                normalExpr = ((ComparisonPredicate) normalExpr).commute();
            }
        }

        return Pair.of(normalExpr, isNot);
    }
}
