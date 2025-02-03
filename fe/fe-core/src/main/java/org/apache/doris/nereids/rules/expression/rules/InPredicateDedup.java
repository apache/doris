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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Deduplicate InPredicate, For example:
 * where A in (x, x) ==> where A in (x)
 */
public class InPredicateDedup implements ExpressionPatternRuleFactory {
    public static final InPredicateDedup INSTANCE = new InPredicateDedup();

    // In many BI scenarios, the sql is auto-generated, and hence there may be thousands of options.
    // It takes a long time to apply this rule. So set a threshold for the max number.
    public static final int REWRITE_OPTIONS_MAX_SIZE = 200;

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
            matchesType(InPredicate.class)
                    .when(inPredicate -> inPredicate.getOptions().size() <= REWRITE_OPTIONS_MAX_SIZE)
                    .then(InPredicateDedup::dedup)
                    .toRule(ExpressionRuleType.IN_PREDICATE_DEDUP)
        );
    }

    /** dedup */
    public static Expression dedup(InPredicate inPredicate) {
        ImmutableSet.Builder<Expression> newOptionsBuilder = ImmutableSet.builderWithExpectedSize(inPredicate.arity());
        for (Expression option : inPredicate.getOptions()) {
            newOptionsBuilder.add(option);
        }

        Set<Expression> newOptions = newOptionsBuilder.build();
        if (newOptions.size() == inPredicate.getOptions().size()) {
            return inPredicate;
        }
        return new InPredicate(inPredicate.getCompareExpr(), newOptions);
    }
}
