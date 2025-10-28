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
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
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
 * convert "A <=> null" to "A is null"
 * null <=> null : true
 * null <=> 1 : false
 * 1 <=> 2 : 1 = 2
 *
 * 1. if null safe equal is in a filter / join / case when / if condition, and at least one side is not nullable,
 *    then null safe equal can be converted to equal.
 * 2. otherwise if both sides are not nullable, then null safe equal can converted to equal too.
 *
 */
public class NullSafeEqualToEqual extends ConditionRewrite {
    public static final NullSafeEqualToEqual INSTANCE = new NullSafeEqualToEqual();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return buildCondRules(ExpressionRuleType.NULL_SAFE_EQUAL_TO_EQUAL);
    }

    // rewrite all the expression tree, not only the condition part.
    @Override
    protected boolean needRewrite(Expression expression, boolean isInsideCondition) {
        return expression.containsType(NullSafeEqual.class);
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, Boolean isInsideCondition) {
        NullSafeEqual newNullSafeEqual = (NullSafeEqual) super.visitNullSafeEqual(nullSafeEqual, isInsideCondition);
        Expression newLeft = newNullSafeEqual.left();
        Expression newRight = newNullSafeEqual.right();
        boolean canConvertToEqual = (!newLeft.nullable() && !newRight.nullable())
                || (isInsideCondition && (!newLeft.nullable() || !newRight.nullable()));
        if (newLeft.equals(newRight)) {
            return BooleanLiteral.TRUE;
        } else if (newLeft.isNullLiteral() && newRight.isNullLiteral()) {
            return BooleanLiteral.TRUE;
        } else if (newLeft.isNullLiteral()) {
            return !newRight.nullable() ? BooleanLiteral.FALSE : new IsNull(newRight);
        } else if (newRight.isNullLiteral()) {
            return !newLeft.nullable() ? BooleanLiteral.FALSE : new IsNull(newLeft);
        } else if (canConvertToEqual) {
            return new EqualTo(newLeft, newRight);
        } else if (newRight.equals(BooleanLiteral.TRUE)) {
            return simplifySafeEqualTrue(newLeft).orElse(newNullSafeEqual);
        } else {
            return newNullSafeEqual;
        }
    }

    /**
     * try to simplify 'expression <=> TRUE',
     * return the rewritten expression if it can be simplified, otherwise return empty.
     */
    private Optional<Expression> simplifySafeEqualTrue(Expression expression) {
        if (expression.isLiteral()) {
            return Optional.of(BooleanLiteral.of(expression.equals(BooleanLiteral.TRUE)));
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
            // (c1 and c2) <=> TRUE rewrite to (c1 <=> TRUE) and (c2 <=> TRUE)
            // (c1 or c2) <=> TRUE rewrite to (c1 <=> TRUE) or (c2 <=> TRUE)
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
