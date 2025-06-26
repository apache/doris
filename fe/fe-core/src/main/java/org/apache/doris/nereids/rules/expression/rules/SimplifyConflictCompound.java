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
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
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
 *  `x and not x` => `falseOrNull(x)`
 *  `x or not x` => `trueOrNull(x)`
 */
public class SimplifyConflictCompound implements ExpressionPatternRuleFactory {
    public static final SimplifyConflictCompound INSTANCE = new SimplifyConflictCompound();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class)
                        .when(c -> c.containsType(Not.class))
                        .then(this::rewrite)
                        .toRule(ExpressionRuleType.SIMPLIFY_CONFLICT_COMPOUND)
        );
    }

    private Expression rewrite(CompoundPredicate compoundPredicate) {
        List<Expression> flatten = ExpressionUtils.extract(compoundPredicate);
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(flatten.size());
        boolean changed = false;
        // expression -> (exist expression, exist (not expression))
        // if expression -> (true, true) then expression has conflict,
        // ie, predicate contains expression 'expression' and 'not expression'
        Map<Expression, Pair<Boolean, Boolean>> exprExistMarks = Maps.newLinkedHashMap();
        boolean canSimplify = false;
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
                    canSimplify |= mark.first;
                    mark = Pair.of(mark.first, true);
                } else {
                    canSimplify |= mark.second;
                    mark = Pair.of(true, mark.second);
                }
                exprExistMarks.put(normalExpr, mark);
            }
            newChildren.add(child);
        }
        if (!canSimplify && !changed) {
            return compoundPredicate;
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
                    Expression newChild = compoundPredicate instanceof And
                            ? ExpressionUtils.falseOrNull(normalExpr) : ExpressionUtils.trueOrNull(normalExpr);
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
    private Pair<Expression, Boolean> normalComparisonAndNot(Expression expr) {
        boolean isNot = false;
        Expression normalExpr = expr;
        while (normalExpr instanceof Not) {
            isNot = !isNot;
            normalExpr = ((Not) normalExpr).child();
        }

        return Pair.of(normalExpr, isNot);
    }
}
