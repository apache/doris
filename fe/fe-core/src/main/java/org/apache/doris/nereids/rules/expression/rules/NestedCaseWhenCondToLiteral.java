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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * For nested CaseWhen/IF expression, replace the inner CaseWhen/IF condition with TRUE/FALSE literal
 * when the condition also exists in the outer CaseWhen/IF conditions.
 * <br/>
 *  1. if it exists in outer case's current branch condition, replace it with TRUE
 *    e.g.
 *      case when A then
 *                  (case when A then 1 else 2 end)
 *                  end
 *          ...
 *      end
 *     then inner case condition A will replace with TRUE:
 *      case when A then
 *                  (case when TRUE then 1 else 2 end)
 *          ...
 *      end
 * </br>
 * <br>
 *  2. if it exists in outer case's previous branch condition, replace it with FALSE
 *    e.g.
 *      case when A then ... end
 *      case when B then
 *                  (case when A then 1 else 2 end)
 *                  end
 *          ...
 *      end
 *     then inner case condition A will replace with FALSE:
 *      case when A then ... end
 *      case when B then
 *                  (case when FALSE then 1 else 2 end)
 *                  end
 *          ...
 *      end
 * </br>
 */
public class NestedCaseWhenCondToLiteral implements ExpressionPatternRuleFactory {

    public static final NestedCaseWhenCondToLiteral INSTANCE = new NestedCaseWhenCondToLiteral();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                root(Expression.class)
                        .when(this::needRewrite)
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.NESTED_CASE_WHEN_COND_TO_LITERAL)
        );
    }

    private boolean needRewrite(Expression expression) {
        return expression.containsType(CaseWhen.class, If.class);
    }

    private Expression rewrite(Expression expression, ExpressionRewriteContext context) {
        return expression.accept(new NestCaseLikeCondReplacer(), context);
    }

    private static class NestCaseLikeCondReplacer extends DefaultExpressionRewriter<ExpressionRewriteContext> {

        // trueConditions/falseConditions is used to record the case/if conditions for the first time occur.
        // when enter a case/if branch, add the condition to trueConditions,
        // when leave a case/if branch, remove the condition from trueConditions, add the condition to falseConditions,
        // when leave the whole case/if statement, remove the condition from falseConditions.
        private final Set<Expression> trueConditions = Sets.newHashSet();
        private final Set<Expression> falseConditions = Sets.newHashSet();

        @Override
        public Expression visit(Expression expr, ExpressionRewriteContext context) {
            if (!INSTANCE.needRewrite(expr)) {
                return expr;
            }
            ImmutableList.Builder<Expression> newChildren
                    = ImmutableList.builderWithExpectedSize(expr.arity());
            boolean hasNewChildren = false;
            for (Expression child : expr.children()) {
                Expression newChild = child.accept(this, context);
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            return hasNewChildren ? expr.withChildren(newChildren.build()) : expr;
        }

        @Override
        public Expression visitCaseWhen(CaseWhen caseWhen, ExpressionRewriteContext context) {
            ImmutableList.Builder<WhenClause> newWhenClausesBuilder
                    = ImmutableList.builderWithExpectedSize(caseWhen.arity());
            List<Expression> firstOccurConds = Lists.newArrayListWithExpectedSize(caseWhen.arity());
            Set<Expression> uniqueConditions = Sets.newHashSet();
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                Expression oldCondition = whenClause.getOperand();
                if (!uniqueConditions.add(oldCondition)) {
                    // remove duplicated when condition
                    continue;
                }
                Pair<Expression, Boolean> replaceResult = replaceCondition(oldCondition, context);
                Expression newCondition = replaceResult.first;
                boolean condFirstOccur = replaceResult.second;
                if (condFirstOccur) {
                    trueConditions.add(oldCondition);
                    firstOccurConds.add(oldCondition);
                }
                Expression newResult = whenClause.getResult().accept(this, context);
                if (condFirstOccur) {
                    trueConditions.remove(oldCondition);
                    falseConditions.add(oldCondition);
                }
                if (whenClause.getOperand() != newCondition || whenClause.getResult() != newResult) {
                    newWhenClausesBuilder.add(new WhenClause(newCondition, newResult));
                } else {
                    newWhenClausesBuilder.add(whenClause);
                }
            }
            Expression oldDefaultValue = caseWhen.getDefaultValue().orElse(null);
            Expression newDefaultValue = oldDefaultValue;
            if (newDefaultValue != null) {
                newDefaultValue = newDefaultValue.accept(this, context);
            }
            for (Expression newAddCond : firstOccurConds) {
                falseConditions.remove(newAddCond);
            }
            List<WhenClause> newWhenClauses = newWhenClausesBuilder.build();
            boolean hasNewChildren = false;
            if (newWhenClauses.size() != caseWhen.getWhenClauses().size()) {
                hasNewChildren = true;
            } else {
                for (int i = 0; i < newWhenClauses.size(); i++) {
                    if (newWhenClauses.get(i) != caseWhen.getWhenClauses().get(i)) {
                        hasNewChildren = true;
                        break;
                    }
                }
            }
            if (newDefaultValue != oldDefaultValue) {
                hasNewChildren = true;
            }
            if (hasNewChildren) {
                return newDefaultValue != null
                        ? new CaseWhen(newWhenClauses, newDefaultValue)
                        : new CaseWhen(newWhenClauses);
            } else {
                return caseWhen;
            }
        }

        @Override
        public Expression visitIf(If ifExpr, ExpressionRewriteContext context) {
            Expression oldCondition = ifExpr.getCondition();
            Pair<Expression, Boolean> replaceResult = replaceCondition(oldCondition, context);
            Expression newCondition = replaceResult.first;
            boolean condFirstOccur = replaceResult.second;
            if (condFirstOccur) {
                trueConditions.add(oldCondition);
            }
            Expression newTrueValue = ifExpr.getTrueValue().accept(this, context);
            if (condFirstOccur) {
                trueConditions.remove(oldCondition);
                falseConditions.add(oldCondition);
            }
            Expression newFalseValue = ifExpr.getFalseValue().accept(this, context);
            if (condFirstOccur) {
                falseConditions.remove(oldCondition);
            }
            if (newCondition != oldCondition
                    || newTrueValue != ifExpr.getTrueValue()
                    || newFalseValue != ifExpr.getFalseValue()) {
                return new If(newCondition, newTrueValue, newFalseValue);
            } else {
                return ifExpr;
            }
        }

        // return newCondition + condition first occur flag
        private Pair<Expression, Boolean> replaceCondition(Expression condition, ExpressionRewriteContext context) {
            if (condition.isLiteral()) {
                // literal condition do not need to replace, and do not record it
                return Pair.of(condition, false);
            } else if (trueConditions.contains(condition)) {
                return Pair.of(BooleanLiteral.TRUE, false);
            } else if (falseConditions.contains(condition)) {
                return Pair.of(BooleanLiteral.FALSE, false);
            } else {
                return Pair.of(condition.accept(this, context), true);
            }
        }
    }
}
