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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Check analysis rule to check semantic correct after analysis by Nereids.
 */
public class CheckAnalysis implements AnalysisRuleFactory {

    private static final Map<Class<? extends LogicalPlan>, Set<Class<? extends Expression>>>
            UNEXPECTED_EXPRESSION_TYPE_MAP = ImmutableMap.<Class<? extends LogicalPlan>,
                Set<Class<? extends Expression>>>builder()
            .put(LogicalAggregate.class, ImmutableSet.of(
                    TableGeneratingFunction.class))
            .put(LogicalFilter.class, ImmutableSet.of(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalGenerate.class, ImmutableSet.of(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    WindowExpression.class))
            .put(LogicalHaving.class, ImmutableSet.of(
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalJoin.class, ImmutableSet.of(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalOneRowRelation.class, LogicalOneRowRelation.FORBIDDEN_EXPRESSIONS)
            .put(LogicalProject.class, ImmutableSet.of(
                    TableGeneratingFunction.class))
            .put(LogicalSort.class, ImmutableSet.of(
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class))
            .put(LogicalWindow.class, ImmutableSet.of(
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class
            ))
            .build();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.CHECK_ANALYSIS.build(
                any().then(plan -> {
                    checkExpressionInputTypes(plan);
                    checkUnexpectedExpressions(plan);
                    return null;
                })
            ),
            RuleType.CHECK_ANALYSIS.build(
                logicalSort().then(this::checkNotContainsNonWindowAggregateFunc)
            ),
            RuleType.CHECK_AGGREGATE_ANALYSIS.build(
                logicalAggregate().then(agg -> {
                    checkAggregate(agg);
                    return agg;
                })
            )
        );
    }

    private void checkUnexpectedExpressions(Plan plan) {
        Class[] unexpectedExpressionTypes = UNEXPECTED_EXPRESSION_TYPE_MAP
                .getOrDefault(plan.getClass(), Collections.emptySet())
                .toArray(new Class[]{});
        if (unexpectedExpressionTypes.length == 0) {
            return;
        }
        for (Expression expr : plan.getExpressions()) {
            if (!expr.containsType(unexpectedExpressionTypes)) {
                continue;
            }
            expr.foreachUp(e -> {
                for (Class<?> type : unexpectedExpressionTypes) {
                    if (type.isInstance(e)) {
                        throw new AnalysisException(plan.getType() + " can not contains "
                                + type.getSimpleName() + " expression: " + ((Expression) e).toSql());
                    }
                }
            });
        }
    }

    private Plan checkNotContainsNonWindowAggregateFunc(Plan plan) {
        for (Expression expr : plan.getExpressions()) {
            Optional<AggregateFunction> aggOpt = expr.accept(ExpressionVisitors.NON_WINDOW_AGGREGATE_GETTER, null);
            if (aggOpt.isPresent()) {
                throw new AnalysisException(plan.getType() + " can not contains "
                        + AggregateFunction.class.getSimpleName() + " expression: " + aggOpt.get().toSql());
            }
        }
        return null;
    }

    private void checkExpressionInputTypes(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            TypeCheckResult firstFailed = expression.checkInputDataTypes();
            if (firstFailed.failed()) {
                throw new AnalysisException(firstFailed.getMessage());
            }
        }
    }

    private void checkAggregate(LogicalAggregate<? extends Plan> aggregate) {
        for (Expression expr : aggregate.getGroupByExpressions()) {
            if (ExpressionUtils.hasNonWindowAggregateFunction(expr)) {
                throw new AnalysisException(
                        "GROUP BY expression must not contain aggregate functions: " + expr.toSql());
            }
        }
    }
}
