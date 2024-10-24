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
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite rule of NOT expression.
 * For example:
 * not a -> not a.
 * not not a -> a.
 * not not not a -> not a.
 * not a > b -> a <= b.
 * not a < b -> a >= b.
 * not a >= b -> a < b.
 * not a <= b -> a > b.
 * not a=b -> not a=b.
 * not and(a >= b, a <= c) -> or(a < b, a > c)
 * not or(a >= b, a <= c) -> and(a < b, a > c)
 */
public class SimplifyNotExprRule implements ExpressionPatternRuleFactory {

    public static SimplifyNotExprRule INSTANCE = new SimplifyNotExprRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Not.class).then(SimplifyNotExprRule::simplify)
        );
    }

    /** simplifyNot */
    public static Expression simplify(Not not) {
        Expression child = not.child();
        if (child instanceof ComparisonPredicate) {
            ComparisonPredicate cp = (ComparisonPredicate) not.child();
            Expression left = cp.left();
            Expression right = cp.right();

            if (child instanceof GreaterThan) {
                return new LessThanEqual(left, right);
            } else if (child instanceof GreaterThanEqual) {
                return new LessThan(left, right);
            } else if (child instanceof LessThan) {
                return new GreaterThanEqual(left, right);
            } else if (child instanceof LessThanEqual) {
                return new GreaterThan(left, right);
            }
        } else if (child instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) child;
            Not left = new Not(cp.left());
            Not right = new Not(cp.right());
            return cp.flip(left, right);
        } else if (child instanceof Not) {
            return child.child(0);
        }
        return not;
    }
}
