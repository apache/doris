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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * simplify with self comparison
 * such as: a = a --> TRUE
 *          a + b > a + b --> FALSE
 */
public class SimplifySelfComparison implements ExpressionPatternRuleFactory {
    public static SimplifySelfComparison INSTANCE = new SimplifySelfComparison();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ComparisonPredicate.class)
                        .then(this::rewrite)
                        .toRule(ExpressionRuleType.SIMPLIFY_SELF_COMPARISON)
        );
    }

    private Expression rewrite(ComparisonPredicate comparison) {
        Expression left = comparison.left();
        if (left.equals(comparison.right())) {
            if (comparison instanceof EqualTo
                    || comparison instanceof GreaterThanEqual
                    || comparison instanceof LessThanEqual) {
                return ExpressionUtils.trueOrNull(left);
            }
            if (comparison instanceof NullSafeEqual) {
                return BooleanLiteral.TRUE;
            }
            if (comparison instanceof GreaterThan || comparison instanceof LessThan) {
                return ExpressionUtils.falseOrNull(left);
            }
        }

        return comparison;
    }

}
