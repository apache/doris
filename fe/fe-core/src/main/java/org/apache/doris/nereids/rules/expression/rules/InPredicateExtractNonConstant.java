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
import org.apache.hadoop.util.Lists;

import java.util.List;

/**
 * Extract non-constant of InPredicate, For example:
 * where k1 in (k2, k3, 10, 20, 30) ==> where k1 = k2 or k1 = k3 or k1 in (10, 20, 30).
 * It's because backend handle in predicate which contains none-constant column will reduce performance.
 */
public class InPredicateExtractNonConstant implements ExpressionPatternRuleFactory {
    public static final InPredicateExtractNonConstant INSTANCE = new InPredicateExtractNonConstant();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(InPredicate.class)
                        .when(inPredicate -> inPredicate.getOptions().size()
                                <= InPredicateDedup.REWRITE_OPTIONS_MAX_SIZE)
                        .then(this::rewrite)
                        .toRule(ExpressionRuleType.IN_PREDICATE_EXTRACT_NON_CONSTANT)
        );
    }

    private Expression rewrite(InPredicate inPredicate) {
        Expression key = inPredicate.getCompareExpr();
        List<Expression> constants = Lists.newArrayListWithExpectedSize(inPredicate.getOptions().size());
        List<Expression> disjunctions = Lists.newArrayList();
        for (Expression option : inPredicate.getOptions()) {
            if (option.isConstant()) {
                constants.add(option);
            } else {
                disjunctions.add(new EqualTo(key, option));
            }
        }
        if (disjunctions.isEmpty()) {
            return inPredicate;
        }
        if (!constants.isEmpty()) {
            disjunctions.add(ExpressionUtils.toInPredicateOrEqualTo(key, constants));
        }
        return ExpressionUtils.or(disjunctions);
    }
}
