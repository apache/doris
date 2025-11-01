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
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Push expression into case when/if/nvl/nullif branch to increase the chance of constant folding.
 * For example: 2 > case when TB = 1 then 1 else 3 end
 * can be rewritten to: case when TB = 1 then true else false end
 */
public class PushIntoCaseWhenBranch implements ExpressionPatternRuleFactory {
    public static PushIntoCaseWhenBranch INSTANCE = new PushIntoCaseWhenBranch();
    private static final Class<?>[] PUSH_INTO_CLASSES = new Class[] {CaseWhen.class, If.class, Nvl.class, NullIf.class};

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        List<ExpressionPatternMatcher<? extends Expression>> folderRules
                = FoldConstantRuleOnFE.PATTERN_MATCH_INSTANCE.buildRules();
        ImmutableList.Builder<ExpressionPatternMatcher<? extends Expression>> builder
                = ImmutableList.builderWithExpectedSize(folderRules.size());
        for (ExpressionPatternMatcher<? extends Expression> foldRule : folderRules) {
            if (!LeafExpression.class.isAssignableFrom(foldRule.typePattern)) {
                builder.add(matchesType(foldRule.typePattern)
                        .when(this::needRewrite)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.PUSH_INTO_CASE_WHEN_BRANCH));
            }
        }
        return builder.build();
    }

    private boolean needRewrite(Expression expression) {
        if (!expression.containsType(PUSH_INTO_CLASSES) || !expression.foldable()) {
            return false;
        }
        // contains one case when/if/nvl/nullif child, other children are literals.
        boolean hasPushIntoClassChild = false;
        for (Expression child : expression.children()) {
            if (child.isLiteral()) {
                continue;
            }
            boolean isPushIntoClass = needPushInto(child.getClass());
            if (!isPushIntoClass || hasPushIntoClassChild) {
                return false;
            }
            hasPushIntoClassChild = true;
        }
        return hasPushIntoClassChild;
    }

    private boolean needPushInto(Class<?> clazz) {
        for (Class<?> pushIntoClass : PUSH_INTO_CLASSES) {
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
        if (first.containsUniqueFunction()) {
            return Optional.empty();
        }
        If ifExpr = new If(new IsNull(first), second, first);
        return tryPushIntoIf(parent, childIndex, ifExpr, context);
    }

    private Optional<Expression> tryPushIntoNullIf(Expression parent, int childIndex, NullIf nullIf,
            ExpressionRewriteContext context) {
        Expression first = nullIf.left();
        Expression second = nullIf.right();
        if (first.containsUniqueFunction()) {
            return Optional.empty();
        }
        If ifExpr = new If(new EqualTo(first, second), new NullLiteral(nullIf.getDataType()), first);
        return tryPushIntoIf(parent, childIndex, ifExpr, context);
    }

    private boolean pushIntoBranches(Expression parent, int childIndex,
            List<Expression> branchValues, ExpressionRewriteContext context) {
        List<Expression> newChildren = Lists.newArrayList(parent.children());
        final int MAX_NON_LIT_NUM = 1;
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
