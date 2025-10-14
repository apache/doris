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
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Simplify expression equal to true / false:
 *  1.'expr = true' => 'expr';
 *  2.'expr = false' => 'not expr'.
 *  3.'expr <=> true' to 'expr' if expr is not nullable, otherwise try the best effort to simplify it.
 *  4. 'expr <=> false' to 'not expr' if expr is not nullable, otherwise do nothing.
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
                        .toRule(ExpressionRuleType.SIMPLIFY_EQUAL_BOOLEAN_LITERAL),
                matchesType(NullSafeEqual.class)
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

    private Expression rewrite(NullSafeEqual equal, BooleanLiteral right) {
        Expression left = equal.left();
        if (right.equals(BooleanLiteral.TRUE)) {
            return simplifySafeEqualTrue(left).orElse(equal);
        } else {
            if (!left.nullable()) {
                return new Not(left);
            }
        }

        return equal;
    }

    /**
     * try to simplify 'expression <=> TRUE',
     * return the rewritten expression if it can be simplified, otherwise return empty.
     */
    private Optional<Expression> simplifySafeEqualTrue(Expression expression) {
        if (expression.isNullLiteral()) {
            return Optional.of(BooleanLiteral.FALSE);
        } else if (!expression.nullable()) {
            return Optional.of(expression);
        } else if (expression instanceof PropagateNullable) {
            Set<Expression> conjuncts = Sets.newLinkedHashSet();
            conjuncts.add(expression);
            if (tryProcessPropagateNullable(expression, conjuncts)) {
                return Optional.of(ExpressionUtils.and(conjuncts));
            }
        } else if (expression instanceof InPredicate) {
            InPredicate in = (InPredicate) expression;
            Expression compareExpr = in.getCompareExpr();
            if (!compareExpr.isConstant()) {
                Set<Expression> conjuncts = Sets.newLinkedHashSet();
                if (tryProcessPropagateNullable(compareExpr, conjuncts)) {
                    boolean allOptionNonNullLiteral = true;
                    ImmutableList.Builder<Expression> newOptionsBuilder
                            = ImmutableList.builderWithExpectedSize(in.getOptions().size());
                    for (Expression option : in.getOptions()) {
                        if (option.isNullLiteral()) {
                            continue;
                        }
                        if (!option.isLiteral()) {
                            allOptionNonNullLiteral = false;
                            break;
                        }
                        newOptionsBuilder.add(option);
                    }
                    if (allOptionNonNullLiteral) {
                        List<Expression> newOptions = newOptionsBuilder.build();
                        if (newOptions.isEmpty()) {
                            return Optional.of(BooleanLiteral.FALSE);
                        }
                        Expression newIn = newOptions.size() == in.getOptions().size()
                                ? in : ExpressionUtils.toInPredicateOrEqualTo(compareExpr, newOptions);
                        conjuncts.add(newIn);
                        return Optional.of(ExpressionUtils.and(conjuncts));
                    }
                }
            }
        } else if (expression instanceof CompoundPredicate) {
            // process AND / OR
            // c1 and c2 and c3 <=> TRUE  can rewrite to (c1 <=> TRUE) and (c2 <=> TRUE) and (c3 <=> TRUE)
            // the same for OR.
            List<Expression> oldChildren = expression.children();
            ImmutableList.Builder<Expression> newChildrenBuilder
                    = ImmutableList.builderWithExpectedSize(oldChildren.size());
            for (Expression child : expression.children()) {
                // rewrite child to child <=> TRUE
                Expression newChild = simplifySafeEqualTrue(child)
                        .orElse(new NullSafeEqual(child, BooleanLiteral.TRUE));
                if (newChild.getClass() == expression.getClass()) {
                    // flatten
                    newChildrenBuilder.addAll(newChild.children());
                } else {
                    newChildrenBuilder.add(newChild);
                }
            }
            List<Expression> newChildren = newChildrenBuilder.build();
            boolean changed = newChildren.size() != oldChildren.size();
            if (newChildren.size() == oldChildren.size()) {
                for (int i = 0; i < newChildren.size(); i++) {
                    if (newChildren.get(i) != oldChildren.get(i)) {
                        changed = true;
                        break;
                    }
                }
            }
            return Optional.of(changed ? expression.withChildren(newChildren) : expression);
        }
        return Optional.empty();
    }

    private boolean tryProcessPropagateNullable(Expression expression, Set<Expression> conjuncts) {
        if (expression.isLiteral()) {
            // for propagate nullable function, if any of its child is null literal,
            // the fold rule will simplify it to null literal.
            // so here no need to handle with the null literal case.
            return !expression.isNullLiteral();
        } else if (expression instanceof SlotReference) {
            if (expression.nullable()) {
                conjuncts.add(ExpressionUtils.notIsNull(expression));
            }
            return true;
        } else if (expression instanceof PropagateNullable) {
            for (Expression child : expression.children()) {
                if (!tryProcessPropagateNullable(child, conjuncts)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }
}
