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
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext.ExpressionSource;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * For the condition expression, replace null literal with false literal, replace null safe equal with equal.
 *
 * Here condition expression means the expression used in filter, join condition, case when condition, if condition.
 * And only replace the expression which its ancestors to the condition root are AND/OR/CASE WHEN/IF.
 *
 * for example: for NOT(null) in filter, the null will not be replaced, because its parent NOT is not AND/OR.
 *
 * example:
 *
 * 1. replace null with false for condition expression.
 *    a) if(null and a > 1, ...) => if(false and a > 1, ...)
 *    b) case when null and a > 1 then ... => case when false and a > 1 then ...
 *    c) null or (null and a > 1) or not(null) => false or (false and a > 1) or not(null)
 * 2. replace 'xx <=> yy' to 'xx = yy' for the condition expression if xx is not-nullable or yy is not-nullable,
 *    but not need both are not-nullable. (NOTE: if expression is not a condition, xx <=> yy can rewrite to xx = yy
 *    requires both xx and yy are not-nullable).
 *    a) if(a <=> 1, ...) => if(a = 1,
 *    b) case when a <=> 1 then ... => case when a = 1 then ...
 *    c) a <=> 1 or (a <=> 2 and not (a <=> 3)) =>  a = 1 or (a = 2 and not (a <=> 3))
 */
public class ReplaceNullWithFalseForCond extends DefaultExpressionRewriter<Boolean>
        implements ExpressionPatternRuleFactory {

    public static final ReplaceNullWithFalseForCond INSTANCE = new ReplaceNullWithFalseForCond();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                root(Expression.class)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.REPLACE_NULL_WITH_FALSE_FOR_COND)
        );
    }

    private boolean needRewrite(Expression expression, boolean isInsideCondition) {
        // check have the replaced target
        if (!expression.containsType(NullLiteral.class, NullSafeEqual.class)) {
            return false;
        }
        // check have condition or in a condition
        return isInsideCondition || expression.containsType(WhenClause.class, If.class);
    }

    private Expression rewrite(Expression expression, ExpressionRewriteContext context) {
        return expression.accept(this, rootIsCondition(context));
    }

    private boolean rootIsCondition(ExpressionRewriteContext context) {
        Plan plan = context.plan.orElse(null);
        if (plan instanceof LogicalFilter || plan instanceof LogicalHaving) {
            return true;
        } else if (plan instanceof LogicalJoin) {
            // null aware join can not treat null as false
            ExpressionSource source = context.source.orElse(null);
            return ((LogicalJoin<?, ?>) plan).getJoinType() != JoinType.NULL_AWARE_LEFT_ANTI_JOIN
                    && (source == ExpressionSource.JOIN_HASH_CONDITION
                            || source == ExpressionSource.JOIN_OTHER_CONDITION);
        } else {
            return false;
        }
    }

    @Override
    public Expression visit(Expression expr, Boolean isInsideCondition) {
        if (needRewrite(expr, isInsideCondition)) {
            return super.visit(expr, false);
        } else {
            return expr;
        }
    }

    @Override
    public Expression visitNullLiteral(NullLiteral nullLiteral, Boolean isInsideCondition) {
        if (isInsideCondition
                && (nullLiteral.getDataType().isBooleanType() || nullLiteral.getDataType().isNullType())) {
            return BooleanLiteral.FALSE;
        }
        return nullLiteral;
    }

    @Override
    public Expression visitNullSafeEqual(NullSafeEqual nullSafeEqual, Boolean isInsideCondition) {
        Expression newLeft = nullSafeEqual.left().accept(this, false);
        Expression newRight = nullSafeEqual.right().accept(this, false);
        // x <=> y  => x = y if x or y is not nullable
        if (isInsideCondition && (!newLeft.nullable() || !newRight.nullable())) {
            return new EqualTo(newLeft, newRight);
        } else if (newLeft != nullSafeEqual.left() || newRight != nullSafeEqual.right()) {
            return nullSafeEqual.withChildren(newLeft, newRight);
        } else {
            return nullSafeEqual;
        }
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate predicate, Boolean isInsideCondition) {
        if (!needRewrite(predicate, isInsideCondition)) {
            return predicate;
        }
        boolean changed = false;
        ImmutableList.Builder<Expression> builder
                = ImmutableList.builderWithExpectedSize(predicate.children().size());
        for (Expression child : predicate.children()) {
            Expression newChild = child.accept(this, isInsideCondition);
            if (newChild != child) {
                changed = true;
            }
            builder.add(newChild);
        }
        if (changed) {
            return predicate.withChildren(builder.build());
        } else {
            return predicate;
        }
    }

    @Override
    public Expression visitCaseWhen(CaseWhen caseWhen, Boolean isInsideCondition) {
        if (!needRewrite(caseWhen, isInsideCondition)) {
            return caseWhen;
        }
        boolean changed = false;
        ImmutableList.Builder<WhenClause> whenClausesBuilder
                = ImmutableList.builderWithExpectedSize(caseWhen.getWhenClauses().size());
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            WhenClause newWhenClause = (WhenClause) whenClause.accept(this, isInsideCondition);
            if (newWhenClause != whenClause) {
                changed = true;
            }
            whenClausesBuilder.add(newWhenClause);
        }
        Expression oldDefaultValue = caseWhen.getDefaultValue().orElse(null);
        Expression newDefaultValue = oldDefaultValue;
        if (oldDefaultValue != null) {
            newDefaultValue = oldDefaultValue.accept(this, isInsideCondition);
        } else if (isInsideCondition && caseWhen.getDataType().isBooleanType()) {
            // for case when without else, the else is null, so we need to replace it with false
            newDefaultValue = BooleanLiteral.FALSE;
        }
        if (newDefaultValue != oldDefaultValue) {
            changed = true;
        }
        if (changed) {
            return newDefaultValue != null
                    ? new CaseWhen(whenClausesBuilder.build(), newDefaultValue)
                    : new CaseWhen(whenClausesBuilder.build());
        } else {
            return caseWhen;
        }
    }

    @Override
    public Expression visitWhenClause(WhenClause whenClause, Boolean isInsideCondition) {
        if (!needRewrite(whenClause, isInsideCondition)) {
            return whenClause;
        }
        Expression newOperand = whenClause.getOperand().accept(this, true);
        Expression newResult = whenClause.getResult().accept(this, isInsideCondition);
        if (newOperand != whenClause.getOperand() || newResult != whenClause.getResult()) {
            return new WhenClause(newOperand, newResult);
        } else {
            return whenClause;
        }
    }

    @Override
    public Expression visitIf(If ifExpr, Boolean isInsideCondition) {
        if (!needRewrite(ifExpr, isInsideCondition)) {
            return ifExpr;
        }
        Expression newCondition = ifExpr.getCondition().accept(this, true);
        Expression newTrueValue = ifExpr.getTrueValue().accept(this, isInsideCondition);
        Expression newFalseValue = ifExpr.getFalseValue().accept(this, isInsideCondition);
        if (newCondition != ifExpr.getCondition()
                || newTrueValue != ifExpr.getTrueValue()
                || newFalseValue != ifExpr.getFalseValue()) {
            return new If(newCondition, newTrueValue, newFalseValue);
        } else {
            return ifExpr;
        }
    }
}
