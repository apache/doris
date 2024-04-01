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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.util.ExpressionUtils;

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
        return ImmutableList.of(logicalFilter().when(
                filter -> ExpressionUtils.containsType(filter.getConjuncts(), BooleanLiteral.class))
                .thenApply(ctx -> {
                    LogicalFilter<Plan> filter = ctx.root;
                    ImmutableSet.Builder<Expression> newConjuncts = ImmutableSet.builder();
                    for (Expression expression : filter.getConjuncts()) {
                        if (expression == BooleanLiteral.FALSE) {
                            return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(),
                                    filter.getOutput());
                        } else if (expression != BooleanLiteral.TRUE) {
                            newConjuncts.add(expression);
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
                logicalFilter(logicalOneRowRelation()).thenApply(ctx -> {
                    LogicalFilter<LogicalOneRowRelation> filter = ctx.root;
                    Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(filter.child().getOutputs());

                    ImmutableSet.Builder<Expression> newConjuncts = ImmutableSet.builder();
                    ExpressionRewriteContext context =
                            new ExpressionRewriteContext(ctx.cascadesContext);
                    for (Expression expression : filter.getConjuncts()) {
                        Expression newExpr = ExpressionUtils.replace(expression, replaceMap);
                        Expression foldExpression = FoldConstantRule.evaluate(newExpr, context);

                        if (foldExpression == BooleanLiteral.FALSE) {
                            return new LogicalEmptyRelation(
                                    ctx.statementContext.getNextRelationId(), filter.getOutput());
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
                })
                .toRule(RuleType.ELIMINATE_FILTER_ON_ONE_RELATION));
    }
}
