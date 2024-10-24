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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayContains;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraysOverlap;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 * array_contains ( c_array, '1' )
 *  OR array_contains ( c_array, '2' )
 * =========================================>
 * array_overlap(c_array, ['1', '2'])
 */
public class ArrayContainToArrayOverlap implements ExpressionPatternRuleFactory {

    public static final ArrayContainToArrayOverlap INSTANCE = new ArrayContainToArrayOverlap();

    private static final int REWRITE_PREDICATE_THRESHOLD = 2;

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(Or.class).then(ArrayContainToArrayOverlap::rewrite)
        );
    }

    private static Expression rewrite(Or or) {
        List<Expression> disjuncts = ExpressionUtils.extractDisjunction(or);

        List<Expression> contains = Lists.newArrayList();
        List<Expression> others = Lists.newArrayList();
        for (Expression expr : disjuncts) {
            if (ArrayContainToArrayOverlap.isValidArrayContains(expr)) {
                contains.add(expr);
            } else {
                others.add(expr);
            }
        }

        if (contains.size() <= 1) {
            return or;
        }

        SetMultimap<Expression, Literal> containLiteralSet = Multimaps.newSetMultimap(
                new LinkedHashMap<>(), LinkedHashSet::new
        );
        for (Expression contain : contains) {
            containLiteralSet.put(contain.child(0), (Literal) contain.child(1));
        }

        Builder<Expression> newDisjunctsBuilder = new ImmutableList.Builder<>();
        for (Entry<Expression, Collection<Literal>> kv : containLiteralSet.asMap().entrySet()) {
            Expression left = kv.getKey();
            Collection<Literal> literalSet = kv.getValue();
            if (literalSet.size() > REWRITE_PREDICATE_THRESHOLD) {
                newDisjunctsBuilder.add(
                    new ArraysOverlap(left, new ArrayLiteral(Utils.fastToImmutableList(literalSet)))
                );
            }
        }

        for (Expression contain : contains) {
            if (!canCovertToArrayOverlap(contain, containLiteralSet)) {
                newDisjunctsBuilder.add(contain);
            }
        }
        newDisjunctsBuilder.addAll(others);
        return ExpressionUtils.or(newDisjunctsBuilder.build());
    }

    private static boolean isValidArrayContains(Expression expression) {
        return expression instanceof ArrayContains && expression.child(1) instanceof Literal;
    }

    private static boolean canCovertToArrayOverlap(
            Expression expression, SetMultimap<Expression, Literal> containLiteralSet) {
        if (!(expression instanceof ArrayContains)) {
            return false;
        }
        Set<Literal> containLiteral = containLiteralSet.get(expression.child(0));
        return containLiteral.size() > REWRITE_PREDICATE_THRESHOLD;
    }
}
