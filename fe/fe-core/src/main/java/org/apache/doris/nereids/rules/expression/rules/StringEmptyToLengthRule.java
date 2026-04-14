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
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrites comparisons with empty strings to equivalent length()-based expressions so that
 * the NestedColumnPruning OFFSET optimization can apply.
 *
 * <ul>
 *   <li>{@code str_col = ''}   → {@code length(str_col) = 0}</li>
 *   <li>{@code str_col <> ''}  → {@code length(str_col) != 0}
 *       (represented as {@code NOT(length(str_col) = 0)})</li>
 * </ul>
 *
 * This is a semantics-preserving rewrite: for any non-NULL string, {@code s = ''} is equivalent
 * to {@code length(s) = 0}; for NULL, both sides evaluate to NULL.
 *
 * Only applies when the compared expression is a direct {@link SlotReference} of string-like type,
 * so that the resulting {@code length(slot)} call can benefit from OFFSET-only column reading.
 */
public class StringEmptyToLengthRule implements ExpressionPatternRuleFactory {
    public static final StringEmptyToLengthRule INSTANCE = new StringEmptyToLengthRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                // str_col = '' → length(str_col) = 0
                matchesType(EqualTo.class)
                        .thenApply(ctx -> rewriteEqualToEmpty(ctx.expr))
                        .toRule(ExpressionRuleType.STRING_EMPTY_TO_LENGTH),

                // NOT(str_col = '') → NOT(length(str_col) = 0)  (i.e. str_col <> '')
                matchesType(Not.class)
                        .thenApply(ctx -> {
                            Not not = ctx.expr;
                            if (!(not.child() instanceof EqualTo)) {
                                return not;
                            }
                            Expression rewritten = rewriteEqualToEmpty((EqualTo) not.child());
                            if (rewritten == not.child()) {
                                return not;
                            }
                            return new Not(rewritten);
                        })
                        .toRule(ExpressionRuleType.STRING_EMPTY_TO_LENGTH)
        );
    }

    /**
     * If {@code equalTo} compares a string-typed SlotReference against an empty-string literal,
     * rewrites it to {@code length(slot) = 0}.  Returns the original expression unchanged otherwise.
     */
    private static Expression rewriteEqualToEmpty(EqualTo equalTo) {
        if (ConnectContext.get().getStatementContext().isDelete()) {
            return equalTo;
        }
        Expression left = equalTo.left();
        Expression right = equalTo.right();

        SlotReference slot = null;
        if (isStringSlot(left) && isEmptyStringLiteral(right)) {
            slot = (SlotReference) left;
        } else if (isStringSlot(right) && isEmptyStringLiteral(left)) {
            slot = (SlotReference) right;
        }

        if (slot == null) {
            return equalTo;
        }
        return new EqualTo(new Length(slot), new IntegerLiteral(0));
    }

    private static boolean isStringSlot(Expression expr) {
        return expr instanceof SlotReference && expr.getDataType().isStringLikeType();
    }

    private static boolean isEmptyStringLiteral(Expression expr) {
        return expr instanceof Literal
                && expr.getDataType().isStringLikeType()
                && ((Literal) expr).getStringValue().isEmpty();
    }
}
