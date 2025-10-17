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
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * For nested CaseWhen/IF expression, replace the inner CaseWhen/IF condition with TRUE/FALSE literal
 * when the condition also exists in the outer CaseWhen/IF conditions.
 *
 * on the nested CASE/IF path, a condition may exist in multiple CASE/IF branches,
 * for any inner case when or if condition, its boolean value is determined by the outermost CASE/IF branch,
 * that is the first occurrence of the condition on the nested CASE/IF path.
 *
 * <br>
 *  1. if it exists in outer case's current branch condition, replace it with TRUE
 *    e.g.
 *      case when A then
 *                  (case when A then 1 else 2 end)
 *          ...
 *      end
 *     then inner case condition A will replace with TRUE:
 *      case when A then
 *                  (case when TRUE then 1 else 2 end)
 *          ...
 *      end
 * <br>
 *  2. if it exists in outer case's previous branch condition, replace it with FALSE
 *    e.g.
 *      case when A then ...
 *           when B then
 *                  (case when A then 1 else 2 end)
 *          ...
 *      end
 *     then inner case condition A will replace with FALSE:
 *      case when A then ...
 *           when B then
 *                  (case when FALSE then 1 else 2 end)
 *          ...
 *      end
 * <br>
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
        return expression.accept(new NestedCondReplacer(), null);
    }

    /** NestedCondReplacer */
    @VisibleForTesting
    public static class NestedCondReplacer extends DefaultExpressionRewriter<Void> {

        // condition literals is used to record the boolean literal for a condition expression,
        // 1. if a condition, if it exists in outer case/if conditions, it will be replaced with the literal.
        // 2. otherwise it's the first time occur, then:
        //    a) when enter a case/if branch, set this condition to TRUE literal
        //    b) when leave a case/if branch, set this condition to FALSE literal
        //    c) when leave the whole case/if statement, remove this condition literal
        protected final Map<Expression, BooleanLiteral> conditionLiterals = Maps.newHashMap();

        @Override
        public Expression visit(Expression expr, Void context) {
            if (INSTANCE.needRewrite(expr)) {
                return super.visit(expr, context);
            } else {
                return expr;
            }
        }

        @Override
        public Expression visitCaseWhen(CaseWhen caseWhen, Void context) {
            ImmutableList.Builder<WhenClause> newWhenClausesBuilder
                    = ImmutableList.builderWithExpectedSize(caseWhen.arity());
            List<Expression> firstOccurConds = Lists.newArrayListWithExpectedSize(caseWhen.arity());
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                Expression oldCondition = whenClause.getOperand();
                Pair<Expression, Boolean> replaceResult = replaceCondition(oldCondition, context);
                Expression newCondition = replaceResult.first;
                boolean condFirstOccur = replaceResult.second;
                if (condFirstOccur) {
                    firstOccurConds.add(oldCondition);
                    conditionLiterals.put(oldCondition, BooleanLiteral.TRUE);
                }
                Expression newResult = whenClause.getResult().accept(this, context);
                if (condFirstOccur) {
                    conditionLiterals.put(oldCondition, BooleanLiteral.FALSE);
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
            for (Expression cond : firstOccurConds) {
                conditionLiterals.remove(cond);
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
        public Expression visitIf(If ifExpr, Void context) {
            Expression oldCondition = ifExpr.getCondition();
            Pair<Expression, Boolean> replaceResult = replaceCondition(oldCondition, context);
            Expression newCondition = replaceResult.first;
            boolean condFirstOccur = replaceResult.second;
            if (condFirstOccur) {
                conditionLiterals.put(oldCondition, BooleanLiteral.TRUE);
            }
            Expression newTrueValue = ifExpr.getTrueValue().accept(this, context);
            if (condFirstOccur) {
                conditionLiterals.put(oldCondition, BooleanLiteral.FALSE);
            }
            Expression newFalseValue = ifExpr.getFalseValue().accept(this, context);
            if (condFirstOccur) {
                conditionLiterals.remove(oldCondition);
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
        private Pair<Expression, Boolean> replaceCondition(Expression condition, Void context) {
            if (condition.isLiteral()) {
                // literal condition do not need to replace, and do not record it
                return Pair.of(condition, false);
            } else if (conditionLiterals.containsKey(condition)) {
                return Pair.of(conditionLiterals.get(condition), false);
            } else if (condition instanceof CompoundPredicate) {
                ImmutableList.Builder<Expression> newChildrenBuilder
                        = ImmutableList.builderWithExpectedSize(condition.arity());
                boolean hasNewChildren = false;
                for (Expression child : condition.children()) {
                    Expression newChild = replaceCondition(child, context).first;
                    hasNewChildren = hasNewChildren || newChild != child;
                    newChildrenBuilder.add(newChild);
                }
                Expression newCondition = hasNewChildren
                        ? condition.withChildren(newChildrenBuilder.build()) : condition;
                return Pair.of(newCondition, true);
            } else {
                return Pair.of(condition.accept(this, context), true);
            }
        }
    }
}
