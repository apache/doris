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
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Simplify expression equal to true / false:
 *  1.'expr = true' => 'expr';
 *  2.'expr = false' => 'not expr'.
 *
 *  NOTE: This rule may downgrade the performance for InferPredicate rule,
 *        because InferPredicate will collect predicate `f(xxx) = literal`,
 *        after this rule rewrite `f(xxx) = true/false` to `f(xxx)`/`not f(xxx)`, the predicate will not be collected.
 *
 *        But we think this rule is more useful than harmful.
 *
 *        What's more, for InferPredicate, it will collect f(xxx) = literal, and infer f(yyy) = literal,
 *        but f(yyy) may be very complex, so it is not always useful, so InferPredicate may also cause downgrade.
 *        By the way, if InferPredicate not considering the f(yyy) = literal is complex or not,
 *        the better way for it is to collect all the boolean predicates, not just only the 'xx compare literal' form.
 */
public class SimplifyEqualBooleanLiteral implements ExpressionPatternRuleFactory {
    public static final SimplifyEqualBooleanLiteral INSTANCE = new SimplifyEqualBooleanLiteral();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(EqualTo.class)
                        .when(this::needRewrite)
                        .then(equal -> rewrite(equal, (BooleanLiteral) equal.right()))
                        .toRule(ExpressionRuleType.SIMPLIFY_EQUAL_BOOLEAN_LITERAL)
        );
    }

    private boolean needRewrite(EqualPredicate equal) {
        // we don't rewrite 'slot = true/false' to slot, because:
        // 1. for delete command, the where predicate need slot = xxx;
        // 2. slot = true/false can generate a uniform for this slot, later it can use in constant propagation.
        return !(equal.left() instanceof SlotReference) && equal.right() instanceof BooleanLiteral;
    }

    private Expression rewrite(EqualTo equal, BooleanLiteral right) {
        Expression left = equal.left();
        return right.equals(BooleanLiteral.TRUE) ? left : new Not(left);
    }
}
