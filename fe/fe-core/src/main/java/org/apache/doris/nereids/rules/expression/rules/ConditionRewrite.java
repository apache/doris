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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 * Here condition expression means the expression used in filter, join condition, case when condition, if condition.
 * And the rewrite argument isInsideCondition means the expression's ancestors to the condition root
 * are AND/OR/CASE WHEN/IF.
 *
 * for example: for 'a and not(b)' in filter, when visit 'a', isInsideCondition is true,
 * while visit 'b' isInsideCondition is false because its parent NOT is not AND/OR/CASE WHEN/IF.
 *
 */
public abstract class ConditionRewrite extends DefaultExpressionRewriter<Boolean>
        implements ExpressionPatternRuleFactory {

    protected List<ExpressionPatternMatcher<? extends Expression>> buildCondRules(ExpressionRuleType ruleType) {
        return ImmutableList.of(
                root(Expression.class)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ruleType)
        );
    }

    protected boolean needRewrite(Expression expression, boolean isInsideCondition) {
        return isInsideCondition || expression.containsType(WhenClause.class, If.class);
    }

    protected Expression rewrite(Expression expression, ExpressionRewriteContext context) {
        return expression.accept(this, rootIsCondition(context));
    }

    // for the expression root, only filter and join expression can treat as condition
    protected boolean rootIsCondition(ExpressionRewriteContext context) {
        Plan plan = context.plan.orElse(null);
        if (plan instanceof LogicalJoin) {
            // null aware join can not treat null as false
            ExpressionSource source = context.source.orElse(null);
            return ((LogicalJoin<?, ?>) plan).getJoinType() != JoinType.NULL_AWARE_LEFT_ANTI_JOIN
                    && (source == ExpressionSource.JOIN_HASH_CONDITION
                            || source == ExpressionSource.JOIN_OTHER_CONDITION);
        } else {
            return PlanUtils.isConditionExpressionPlan(plan);
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
            if (newChild.getClass() == predicate.getClass()) {
                builder.addAll(newChild.children());
                changed = true;
            } else {
                builder.add(newChild);
            }
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
