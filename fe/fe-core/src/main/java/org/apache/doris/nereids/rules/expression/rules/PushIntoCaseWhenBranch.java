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
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Push expression into case when/if/nvl/nullif branch.
 *
 * for expression f with n arguments a1, a2, ..., an, if one of its argument is case when/if/nvl/nullif,
 * and the others are literals, then we can push f into each branch of case when/if/nvl/nullif.
 * The rewrite rule is as follows:
 * f(a1, a2, ..., (case when c1 then p1 when c2 then p2 else p3 end), ..., an)
 * => case when c1 then f(a1, a2, ..., p1, ..., an)
 *         when c2 then f(a1, a2, ..., p2, ..., an)
 *         else  f(a1, a2, ..., p3, ..., an) end
 *
 * For example: 2 > case when TB = 1 then 1 else 3 end
 * can be rewritten to: case when TB = 1 then true else false end.
 * After this rule, the expression will continue to be optimized by other rules.
 * Later rule CASE_WHEN_TO_COMPOUND will rewrite it to: (TB = 1) <=> TRUE,
 * later rule NULL_SAFE_EQUAL_TO_EQUAL will rewrite it to:
 *  a) TB = 1, if expression is in filter or join;
 *  b) TB = 1 and TB is not null, otherwise.
 */
public class PushIntoCaseWhenBranch implements ExpressionPatternRuleFactory {
    public static PushIntoCaseWhenBranch INSTANCE = new PushIntoCaseWhenBranch();
    private static final Class<?>[] CASE_WHEN_LIKE_CLASSES
            = new Class[] {CaseWhen.class, If.class, Nvl.class, NullIf.class};

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Expression.class)
                        .when(this::needRewrite)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.PUSH_INTO_CASE_WHEN_BRANCH));
    }

    private boolean needRewrite(Expression expression) {
        if (!expression.containsType(CASE_WHEN_LIKE_CLASSES) || !expression.foldable()) {
            return false;
        }
        // for expression's children, if one of them is case when/if/nvl/nullif, and the others are literals,
        // then try to push rewrite expression, and push expression into the case when/if/nvl/nullif branch.
        boolean hasCaseWhenLikeChild = false;
        for (Expression child : expression.children()) {
            if (child.isLiteral()) {
                continue;
            }
            boolean isCaseWhenLike = isClassCaseWhenLike(child.getClass());
            if (!isCaseWhenLike) {
                return false;
            }
            // if there are more than one case when/if/nvl/nullif child, do not rewrite
            if (hasCaseWhenLikeChild) {
                return false;
            }
            hasCaseWhenLikeChild = true;
        }
        return hasCaseWhenLikeChild;
    }

    private boolean isClassCaseWhenLike(Class<?> clazz) {
        for (Class<?> pushIntoClass : CASE_WHEN_LIKE_CLASSES) {
            if (clazz == pushIntoClass) {
                return true;
            }
        }
        return false;
    }

    private Expression rewrite(Expression expression, ExpressionRewriteContext context) {
        for (int i = 0; i < expression.children().size(); i++) {
            Expression child = expression.child(i);
            Optional<Expression> newExpr = Optional.empty();
            if (child instanceof CaseWhen) {
                newExpr = tryPushIntoCaseWhen(expression, i, (CaseWhen) child, context);
            } else if (child instanceof If) {
                newExpr = tryPushIntoIf(expression, i, (If) child, context);
            } else if (child instanceof Nvl) {
                newExpr = tryPushIntoNvl(expression, i, (Nvl) child, context);
            } else if (child instanceof NullIf) {
                newExpr = tryPushIntoNullIf(expression, i, (NullIf) child, context);
            }
            if (newExpr.isPresent()) {
                return newExpr.get();
            }
        }
        return expression;
    }

    private Optional<Expression> tryPushIntoCaseWhen(Expression parent, int childIndex, CaseWhen caseWhen,
            ExpressionRewriteContext context) {
        List<Expression> branchValues
                = Lists.newArrayListWithExpectedSize(caseWhen.getWhenClauses().size() + 1);
        for (WhenClause whenClause : caseWhen.getWhenClauses()) {
            branchValues.add(whenClause.getResult());
        }
        branchValues.add(caseWhen.getDefaultValue().orElse(new NullLiteral(caseWhen.getDataType())));
        if (pushIntoBranches(parent, childIndex, branchValues, context)) {
            List<WhenClause> newWhenClauses = Lists.newArrayListWithExpectedSize(caseWhen.getWhenClauses().size());
            for (int i = 0; i < caseWhen.getWhenClauses().size(); i++) {
                newWhenClauses.add(new WhenClause(caseWhen.getWhenClauses().get(i).getOperand(), branchValues.get(i)));
            }
            Expression defaultValue = branchValues.get(branchValues.size() - 1);
            CaseWhen newCaseWhen = new CaseWhen(newWhenClauses, defaultValue);
            return Optional.of(TypeCoercionUtils.ensureSameResultType(parent, newCaseWhen, context));
        } else {
            return Optional.empty();
        }
    }

    private Optional<Expression> tryPushIntoIf(Expression parent, int childIndex, If ifExpr,
            ExpressionRewriteContext context) {
        List<Expression> branchValues = Lists.newArrayList(ifExpr.getTrueValue(), ifExpr.getFalseValue());
        if (pushIntoBranches(parent, childIndex, branchValues, context)) {
            If newIf = new If(ifExpr.getCondition(), branchValues.get(0), branchValues.get(1));
            return Optional.of(TypeCoercionUtils.ensureSameResultType(parent, newIf, context));
        } else {
            return Optional.empty();
        }
    }

    private Optional<Expression> tryPushIntoNvl(Expression parent, int childIndex, Nvl nvl,
            ExpressionRewriteContext context) {
        Expression first = nvl.left();
        Expression second = nvl.right();
        boolean isConditionPlan = PlanUtils.isConditionExpressionPlan(context.plan.orElse(null));
        // after rewrite, nvl(first, second) => if(isnull(first), second, first),
        // so there will exist twice 'first' in the rewritten IF expression, which may increase the computation cost.
        // if the plan is not filter and not join, then push down action may not have positive effect,
        // considering this, we give up the rewrite if the plan is not condition plan or first contains unique function.
        if (first.containsUniqueFunction() || !isConditionPlan) {
            return Optional.empty();
        }
        If ifExpr = new If(new IsNull(first), second, first);
        return tryPushIntoIf(parent, childIndex, ifExpr, context);
    }

    private Optional<Expression> tryPushIntoNullIf(Expression parent, int childIndex, NullIf nullIf,
            ExpressionRewriteContext context) {
        Expression first = nullIf.left();
        Expression second = nullIf.right();
        boolean isConditionPlan = PlanUtils.isConditionExpressionPlan(context.plan.orElse(null));
        // after rewrite, nullif(first, second) => if(first = second, null, first),
        // so there will exist twice 'first' in the rewritten IF expression, which may increase the computation cost.
        // if the plan is not filter and not join, then push down action may not have positive effect,
        // considering this, we give up the rewrite if the plan is not condition plan or first contains unique function.
        if (first.containsUniqueFunction() || !isConditionPlan) {
            return Optional.empty();
        }
        If ifExpr = new If(new EqualTo(first, second), new NullLiteral(nullIf.getDataType()), first);
        return tryPushIntoIf(parent, childIndex, ifExpr, context);
    }

    private boolean pushIntoBranches(Expression parent, int childIndex,
            List<Expression> branchValues, ExpressionRewriteContext context) {
        List<Expression> newChildren = Lists.newArrayList(parent.children());
        // for filter/join condition expression, we allow one non-literal branch after push down,
        // because later rule CASE_WHEN_TO_COMPOUND may rewrite to AND/OR expression.
        // for other plan expression, we require all branches should be literal after push down,
        // just for pass the regression test nereids_rules_p0/mv/agg_without_roll_up.
        final int MAX_NON_LIT_NUM = PlanUtils.isConditionExpressionPlan(context.plan.orElse(null)) ? 1 : 0;
        int nonLiteralBranchNum = 0;
        for (int i = 0; i < branchValues.size(); i++) {
            Expression oldValue = branchValues.get(i);
            newChildren.set(childIndex, oldValue);
            Expression newValue = TypeCoercionUtils.ensureSameResultType(
                    parent,
                    FoldConstantRuleOnFE.evaluate(parent.withChildren(newChildren), context),
                    context);
            if (!newValue.isLiteral()) {
                nonLiteralBranchNum++;
                if (nonLiteralBranchNum > MAX_NON_LIT_NUM) {
                    return false;
                }
            }
            branchValues.set(i, newValue);
        }

        return true;
    }
}
