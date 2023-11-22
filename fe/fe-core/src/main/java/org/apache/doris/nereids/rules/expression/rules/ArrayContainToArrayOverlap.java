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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayContains;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraysOverlap;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * array_contains ( c_array, '1' )
 *  OR array_contains ( c_array, '2' )
 * =========================================>
 * array_overlap(c_array, ['1', '2'])
 */
public class ArrayContainToArrayOverlap extends DefaultExpressionRewriter<ExpressionRewriteContext> implements
        ExpressionRewriteRule<ExpressionRewriteContext> {

    public static final ArrayContainToArrayOverlap INSTANCE = new ArrayContainToArrayOverlap();

    private static final int REWRITE_PREDICATE_THRESHOLD = 2;

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return expr.accept(this, ctx);
    }

    @Override
    public Expression visitOr(Or or, ExpressionRewriteContext ctx) {
        List<Expression> disjuncts = ExpressionUtils.extractDisjunction(or);
        Map<Boolean, List<Expression>> containFuncAndOtherFunc = disjuncts.stream()
                .collect(Collectors.partitioningBy(this::isValidArrayContains));
        Map<Expression, Set<Literal>> containLiteralSet = new HashMap<>();
        List<Expression> contains = containFuncAndOtherFunc.get(true);
        List<Expression> others = containFuncAndOtherFunc.get(false);

        contains.forEach(containFunc ->
                containLiteralSet.computeIfAbsent(containFunc.child(0), k -> new HashSet<>())
                            .add((Literal) containFunc.child(1)));

        Builder<Expression> newDisjunctsBuilder = new ImmutableList.Builder<>();
        containLiteralSet.forEach((left, literalSet) -> {
            if (literalSet.size() > REWRITE_PREDICATE_THRESHOLD) {
                newDisjunctsBuilder.add(
                        new ArraysOverlap(left,
                                new ArrayLiteral(ImmutableList.copyOf(literalSet))));
            }
        });

        contains.stream()
                .filter(e -> !canCovertToArrayOverlap(e, containLiteralSet))
                .forEach(newDisjunctsBuilder::add);
        others.stream()
                .map(e -> e.accept(this, null))
                .forEach(newDisjunctsBuilder::add);
        return ExpressionUtils.or(newDisjunctsBuilder.build());
    }

    private boolean isValidArrayContains(Expression expression) {
        return expression instanceof ArrayContains && expression.child(1) instanceof Literal;
    }

    private boolean canCovertToArrayOverlap(Expression expression, Map<Expression, Set<Literal>> containLiteralSet) {
        return expression instanceof ArrayContains
                && containLiteralSet.getOrDefault(expression.child(0),
                    new HashSet<>()).size() > REWRITE_PREDICATE_THRESHOLD;
    }
}
