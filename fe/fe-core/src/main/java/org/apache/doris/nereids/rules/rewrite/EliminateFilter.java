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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;

/**
 * Eliminate filter which is FALSE or TRUE.
 */
public class EliminateFilter implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(logicalFilter()
                .when(filter -> filter.getConjuncts().isEmpty()
                    || ExpressionUtils.containsTypes(filter.getConjuncts(), BooleanLiteral.class, NullLiteral.class))
                .thenApply(ctx -> {
                    LogicalFilter<Plan> filter = ctx.root;
                    ImmutableSet.Builder<Expression> newConjuncts = ImmutableSet.builder();
                    ExpressionRewriteContext context = new ExpressionRewriteContext(ctx.cascadesContext);
                    for (Expression expression : filter.getConjuncts()) {
                        expression = FoldConstantRule.evaluate(eliminateNullLiteral(expression), context);
                        if (expression == BooleanLiteral.FALSE || expression.isNullLiteral()) {
                            return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(),
                                    filter.getOutput());
                        } else if (expression != BooleanLiteral.TRUE) {
                            newConjuncts.addAll(ExpressionUtils.extractConjunction(expression));
                        }
                    }

                    ImmutableSet<Expression> conjuncts = newConjuncts.build();
                    if (conjuncts.isEmpty()) {
                        return filter.child();
                    } else {
                        return new LogicalFilter<>(conjuncts, filter.child());
                    }
                })
                .toRule(RuleType.ELIMINATE_FILTER),
                logicalFilter(logicalOneRowRelation()).thenApply(ctx ->
                        eliminateFilterOnOneRowRelation(ctx.root, ctx.cascadesContext)
                )
                .toRule(RuleType.ELIMINATE_FILTER_ON_ONE_RELATION));
    }

    /** eliminateFilterOnOneRowRelation */
    public static LogicalPlan eliminateFilterOnOneRowRelation(
            LogicalFilter<LogicalOneRowRelation> filter, CascadesContext cascadesContext) {
        Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(filter.child().getOutputs());

        ImmutableSet.Builder<Expression> newConjuncts = ImmutableSet.builder();
        ExpressionRewriteContext context = new ExpressionRewriteContext(cascadesContext);
        for (Expression expression : filter.getConjuncts()) {
            Expression newExpr = ExpressionUtils.replace(expression, replaceMap);
            Expression foldExpression = FoldConstantRule.evaluate(eliminateNullLiteral(newExpr), context);

            if (foldExpression == BooleanLiteral.FALSE || expression.isNullLiteral()) {
                return new LogicalEmptyRelation(
                        cascadesContext.getStatementContext().getNextRelationId(), filter.getOutput());
            } else if (foldExpression != BooleanLiteral.TRUE) {
                newConjuncts.add(expression);
            }
        }

        ImmutableSet<Expression> conjuncts = newConjuncts.build();
        if (conjuncts.isEmpty()) {
            return filter.child();
        } else {
            return new LogicalFilter<>(conjuncts, filter.child());
        }
    }

    @VisibleForTesting
    public static Expression eliminateNullLiteral(Expression expression) {
        if (!expression.anyMatch(e -> ((Expression) e).isNullLiteral())) {
            return expression;
        }

        return replaceNullToFalse(expression);
    }

    // only replace null which its ancestors are all and/or
    // NOTICE: NOT's type is boolean too, if replace null to false in NOT, will got NOT(NULL) = NOT(FALSE) = TRUE,
    // but it is wrong,  NOT(NULL) = NULL. For a filter, only the AND / OR, can keep NULL as FALSE.
    private static Expression replaceNullToFalse(Expression expression) {
        if (expression.isNullLiteral()) {
            return BooleanLiteral.FALSE;
        }

        if (expression instanceof CompoundPredicate) {
            ImmutableList.Builder<Expression> builder = ImmutableList.builderWithExpectedSize(
                    expression.children().size());
            expression.children().forEach(e -> builder.add(replaceNullToFalse(e)));
            return expression.withChildren(builder.build());
        }

        return expression;
    }
}
