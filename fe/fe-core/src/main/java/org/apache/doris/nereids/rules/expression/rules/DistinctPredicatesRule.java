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
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Remove redundant expr for 'CompoundPredicate'.
 * for example:
 * transform (a = 1) and (b > 2) and (a = 1)  to (a = 1) and (b > 2)
 * transform (a = 1) or (a = 1) to (a = 1)
 */
public class DistinctPredicatesRule implements ExpressionPatternRuleFactory {
    public static final DistinctPredicatesRule INSTANCE = new DistinctPredicatesRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class).then(DistinctPredicatesRule::distinct)
        );
    }

    private static Expression distinct(CompoundPredicate expr) {
        List<Expression> extractExpressions = ExpressionUtils.extract(expr);
        Set<Expression> distinctExpressions = new LinkedHashSet<>(extractExpressions);
        if (distinctExpressions.size() != extractExpressions.size()) {
            return ExpressionUtils.combineAsLeftDeepTree(expr.getClass(), Lists.newArrayList(distinctExpressions));
        }
        return expr;
    }
}
