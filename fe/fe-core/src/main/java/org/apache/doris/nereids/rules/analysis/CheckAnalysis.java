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
            .put(LogicalOneRowRelation.class, ImmutableSet.of(
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalProject.class, ImmutableSet.of(
                    TableGeneratingFunction.class))
            .put(LogicalSort.class, ImmutableSet.of(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class,
                    WindowExpression.class))
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
            RuleType.CHECK_AGGREGATE_ANALYSIS.build(
                logicalAggregate().then(agg -> {
                    checkAggregate(agg);
                    return agg;
                })
            )
        );
    }

    private void checkUnexpectedExpressions(Plan plan) {
        Set<Class<? extends Expression>> unexpectedExpressionTypes
                = UNEXPECTED_EXPRESSION_TYPE_MAP.getOrDefault(plan.getClass(), Collections.emptySet());
        if (unexpectedExpressionTypes.isEmpty()) {
            return;
        }
        plan.getExpressions().forEach(c -> c.foreachUp(e -> {
            for (Class<? extends Expression> type : unexpectedExpressionTypes) {
                if (type.isInstance(e)) {
                    throw new AnalysisException(plan.getType() + " can not contains "
                            + type.getSimpleName() + " expression: " + ((Expression) e).toSql());
                }
            }
        }));
    }

    private void checkExpressionInputTypes(Plan plan) {
        final Optional<TypeCheckResult> firstFailed = plan.getExpressions().stream()
                .map(Expression::checkInputDataTypes)
                .filter(TypeCheckResult::failed)
                .findFirst();

        if (firstFailed.isPresent()) {
            throw new AnalysisException(firstFailed.get().getMessage());
        }
    }

    private void checkAggregate(LogicalAggregate<? extends Plan> aggregate) {
        Set<AggregateFunction> aggregateFunctions = aggregate.getAggregateFunctions();
        boolean distinctMultiColumns = aggregateFunctions.stream()
                .anyMatch(fun -> fun.isDistinct() && fun.arity() > 1);
        long distinctFunctionNum = aggregateFunctions.stream()
                .filter(AggregateFunction::isDistinct)
                .count();

        if (distinctMultiColumns && distinctFunctionNum > 1) {
            throw new AnalysisException(
                    "The query contains multi count distinct or sum distinct, each can't have multi columns");
        }
        Optional<Expression> expr = aggregate.getGroupByExpressions().stream()
                .filter(expression -> expression.containsType(AggregateFunction.class)).findFirst();
        if (expr.isPresent()) {
            throw new AnalysisException(
                    "GROUP BY expression must not contain aggregate functions: "
                            + expr.get().toSql());
        }
    }
}
