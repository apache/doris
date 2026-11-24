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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Deduplicate InPredicate, For example:
 * where A in (x, x) ==> where A in (x)
 */
public class InPredicateDedup implements ExpressionPatternRuleFactory {
    public static final InPredicateDedup INSTANCE = new InPredicateDedup();

    // Size limit for other IN rewrites and sample size for large-list deduplication.
    public static final int REWRITE_OPTIONS_MAX_SIZE = 200;
    private static final int LARGE_IN_PREFIX_DISTINCT_LIMIT = 100;

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
            matchesType(InPredicate.class)
                    .then(InPredicateDedup::dedup)
                    .toRule(ExpressionRuleType.IN_PREDICATE_DEDUP)
        );
    }

    /** dedup */
    public static Expression dedup(InPredicate inPredicate) {
        List<Expression> options = inPredicate.getOptions();
        if (options.size() > REWRITE_OPTIONS_MAX_SIZE) {
            return dedupLiteralPrefix(inPredicate, options);
        }

        ImmutableSet.Builder<Expression> newOptionsBuilder = ImmutableSet.builderWithExpectedSize(inPredicate.arity());
        for (Expression option : options) {
            newOptionsBuilder.add(option);
        }

        Set<Expression> newOptions = newOptionsBuilder.build();
        if (newOptions.size() == options.size()) {
            return inPredicate;
        }
        return new InPredicate(inPredicate.getCompareExpr(), newOptions);
    }

    private static Expression dedupLiteralPrefix(InPredicate inPredicate, List<Expression> options) {
        Set<Literal> distinctPrefix = new LinkedHashSet<>();
        int prefixEnd = 0;
        for (; prefixEnd < options.size(); prefixEnd++) {
            Expression option = options.get(prefixEnd);
            if (!(option instanceof Literal)) {
                break;
            }
            distinctPrefix.add((Literal) option);
            // Continue past the sample only when it contains enough duplicates.
            if (prefixEnd < REWRITE_OPTIONS_MAX_SIZE && distinctPrefix.size() > LARGE_IN_PREFIX_DISTINCT_LIMIT) {
                return inPredicate;
            }
        }
        if (distinctPrefix.size() == prefixEnd) {
            return inPredicate;
        }

        // Keep the first non-literal option and every option after it in their original order.
        List<Expression> compacted = ImmutableList.<Expression>builderWithExpectedSize(
                        distinctPrefix.size() + (options.size() - prefixEnd))
                .addAll(distinctPrefix)
                .addAll(options.subList(prefixEnd, options.size()))
                .build();
        return new InPredicate(inPredicate.getCompareExpr(), compacted, inPredicate.isInferred());
    }
}
