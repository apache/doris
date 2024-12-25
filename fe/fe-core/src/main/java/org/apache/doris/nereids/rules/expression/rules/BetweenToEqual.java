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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * f(A, B) between 1 and 1 => f(A, B) = 1
 *
 */
public class BetweenToEqual implements ExpressionPatternRuleFactory {

    public static BetweenToEqual INSTANCE = new BetweenToEqual();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
            matchesType(And.class).then(BetweenToEqual::rewriteBetweenToEqual)
        );
    }

    private static Expression rewriteBetweenToEqual(And and) {
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(and);
        Map<Expression, List<ComparisonPredicate>> betweenCandidate = Maps.newHashMap();
        for (Expression conj : conjuncts) {
            if (isCandidate(conj)) {
                conj = normalizeCandidate((ComparisonPredicate) conj);
                Expression varPart = conj.child(0);
                betweenCandidate.computeIfAbsent(varPart, k -> Lists.newArrayList());
                betweenCandidate.get(varPart).add((ComparisonPredicate) conj);
            }
        }
        List<EqualTo> equals = Lists.newArrayList();
        List<Expression> equalsKey = Lists.newArrayList();
        for (Expression varPart : betweenCandidate.keySet()) {
            List<ComparisonPredicate> candidates = betweenCandidate.get(varPart);
            if (candidates.size() == 2 && greaterEqualAndLessEqual(candidates.get(0), candidates.get(1))) {
                if (candidates.get(0).child(1).equals(candidates.get(1).child(1))) {
                    equals.add(new EqualTo(candidates.get(0).child(0), candidates.get(0).child(1)));
                    equalsKey.add(candidates.get(0).child(0));
                }
            }
        }
        if (equals.isEmpty()) {
            return null;
        } else {
            List<Expression> newConjuncts = Lists.newArrayList(equals);
            for (Expression conj : conjuncts) {
                if (isCandidate(conj)) {
                    conj = normalizeCandidate((ComparisonPredicate) conj);
                    if (equalsKey.contains(conj.child(0))) {
                        continue;
                    }
                }
                newConjuncts.add(conj);
            }
            return ExpressionUtils.and(newConjuncts);
        }
    }

    // A >= a
    // A <= a
    // A is expr, a is literal
    private static boolean isCandidate(Expression expr) {
        if (expr instanceof GreaterThanEqual || expr instanceof LessThanEqual) {
            return expr.child(0) instanceof Literal && !(expr.child(1) instanceof Literal)
                || expr.child(1) instanceof Literal && !(expr.child(0) instanceof Literal);
        }
        return false;
    }

    private static Expression normalizeCandidate(ComparisonPredicate expr) {
        if (expr.child(1) instanceof Literal) {
            return expr;
        } else {
            return expr.withChildren(expr.child(1), expr.child(0));
        }
    }

    private static boolean greaterEqualAndLessEqual(ComparisonPredicate cmp1, ComparisonPredicate cmp2) {
        return cmp1 instanceof GreaterThanEqual && cmp2 instanceof LessThanEqual
            || (cmp1 instanceof LessThanEqual && cmp2 instanceof GreaterThanEqual);
    }
}
