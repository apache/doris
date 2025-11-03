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
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.hadoop.util.Lists;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extract non-constant of InPredicate, For example:
 * where k1 in (k2, k3, 10, 20, 30) ==> where k1 in (10, 20, 30) or k1 = k2 or k1 = k3.
 * It's because backend handle in predicate which contains none-constant column will reduce performance.
 */
public class InPredicateExtractNonConstant implements ExpressionPatternRuleFactory {
    public static final InPredicateExtractNonConstant INSTANCE = new InPredicateExtractNonConstant();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        // If compare expr contains unique function, don't extract it.
        // For example: `a + random() in (x, x + 10)`, if extracted, `a + random() = x or a + random() = x + 10`,
        // then the expression will contain RANDOM two times.
        return ImmutableList.of(
                matchesType(InPredicate.class)
                        .when(inPredicate ->
                                inPredicate.getOptions().size() <= InPredicateDedup.REWRITE_OPTIONS_MAX_SIZE
                                && !inPredicate.getCompareExpr().containsUniqueFunction())
                        .then(this::rewrite)
                        .toRule(ExpressionRuleType.IN_PREDICATE_EXTRACT_NON_CONSTANT)
        );
    }

    private Expression rewrite(InPredicate inPredicate) {
        Set<Expression> nonConstants = Sets.newLinkedHashSetWithExpectedSize(inPredicate.arity());
        for (Expression option : inPredicate.getOptions()) {
            if (!option.isConstant()) {
                nonConstants.add(option);
            }
        }
        if (nonConstants.isEmpty()) {
            return inPredicate;
        }
        Expression key = inPredicate.getCompareExpr();
        List<Expression> disjunctions = Lists.newArrayListWithExpectedSize(inPredicate.getOptions().size());
        List<Expression> constants = inPredicate.getOptions().stream().filter(Expression::isConstant)
                .collect(Collectors.toList());
        if (!constants.isEmpty()) {
            disjunctions.add(ExpressionUtils.toInPredicateOrEqualTo(key, constants));
        }
        for (Expression option : nonConstants) {
            disjunctions.add(new EqualTo(key, option));
        }
        return ExpressionUtils.or(disjunctions);
    }
}
