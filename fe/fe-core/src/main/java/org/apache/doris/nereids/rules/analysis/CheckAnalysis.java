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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
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
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Check analysis rule to check semantic correct after analysis by Nereids.
 *
 * Filling missing slot may replace an expression with slot reference, for example:
 *  HAVING(sum(a#12) > 0) => HAVING(sum(a)#34 > 0), aggregate function `sum` will replace with aggregate slot #34.
 * So run checkAnalysis twice, one run before filling missing slot, one run after it.
 */
public class CheckAnalysis implements AnalysisRuleFactory {

    private static final Map<Class<? extends LogicalPlan>, Class<? extends Expression>[]>
            UNEXPECTED_EXPRESSION_TYPE_MAP = ImmutableMap.<Class<? extends LogicalPlan>,
                Class<? extends Expression>[]>builder()
            .put(LogicalAggregate.class, Utils.fastArray(
                    TableGeneratingFunction.class))
            .put(LogicalFilter.class, Utils.fastArray(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalGenerate.class, Utils.fastArray(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    WindowExpression.class))
            .put(LogicalHaving.class, Utils.fastArray(
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalJoin.class, Utils.fastArray(
                    AggregateFunction.class,
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class,
                    WindowExpression.class))
            .put(LogicalOneRowRelation.class, Utils.fastArray(
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class))
            .put(LogicalProject.class, Utils.fastArray(
                    TableGeneratingFunction.class))
            .put(LogicalSort.class, Utils.fastArray(
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class))
            .put(LogicalWindow.class, Utils.fastArray(
                    GroupingScalarFunction.class,
                    TableGeneratingFunction.class))
            .build();

    // after filling missing slot, some expression will be rewritten, so they will not be expected.
    private static final Map<Class<? extends LogicalPlan>, Class<? extends Expression>[]>
            UNEXPECTED_EXPRESSION_TYPE_MAP_AFTER_FILL_MISSING_SLOT = ImmutableMap.<Class<? extends LogicalPlan>,
                    Class<? extends Expression>[]>builder()
            .put(LogicalSort.class, Utils.fastArray(
                    AggregateFunction.class,
                    WindowExpression.class))
            // OneRowRelationToProject will extract window expression
            .put(LogicalOneRowRelation.class, Utils.fastArray(
                    WindowExpression.class))
            .build();

    private final boolean hadFillMissingSlots;

    public CheckAnalysis(boolean hadFillMissingSlots) {
        this.hadFillMissingSlots = hadFillMissingSlots;
    }

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
                aggregate().then(agg -> {
                    checkAggregate(agg);
                    return agg;
                })
            ),
            RuleType.CHECK_OBJECT_TYPE_ANALYSIS.build(
                logicalHaving().thenApply(ctx -> {
                    LogicalHaving<Plan> having = ctx.root;
                    checkHaving(having);
                    return null;
                })
            )
        );
    }

    private void checkUnexpectedExpressions(Plan plan) {
        checkUnexpectedExpressionTypes(plan, UNEXPECTED_EXPRESSION_TYPE_MAP.get(plan.getClass()));
        if (hadFillMissingSlots) {
            checkUnexpectedExpressionTypes(plan,
                    UNEXPECTED_EXPRESSION_TYPE_MAP_AFTER_FILL_MISSING_SLOT.get(plan.getClass()));
        }
    }

    private void checkUnexpectedExpressionTypes(Plan plan, Class<? extends Expression>[] unexpectedExpressionTypes) {
        if (unexpectedExpressionTypes == null || unexpectedExpressionTypes.length == 0) {
            return;
        }
        for (Expression expr : plan.getExpressions()) {
            if (!expr.containsType(unexpectedExpressionTypes)) {
                continue;
            }
            expr.foreachUp(e -> {
                for (Class<? extends Expression> type : unexpectedExpressionTypes) {
                    if (type.isInstance(e)) {
                        throw new AnalysisException(plan.getType() + " can not contains "
                                + type.getSimpleName() + " expression: " + ((Expression) e).toSql());
                    }
                }
            });
        }
    }

    private void checkExpressionInputTypes(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            TypeCheckResult firstFailed = expression.checkInputDataTypes();
            if (firstFailed.failed()) {
                throw new AnalysisException(firstFailed.getMessage());
            }
        }
    }

    private void checkAggregate(Aggregate<? extends Plan> aggregate) {
        for (Expression expr : aggregate.getGroupByExpressions()) {
            if (ExpressionUtils.hasNonWindowAggregateFunction(expr)) {
                throw new AnalysisException(
                        "GROUP BY expression must not contain aggregate functions: " + expr.toSql());
            }
            if (expr.containsType(WindowExpression.class)) {
                throw new AnalysisException(
                        "GROUP BY expression must not contain window functions: " + expr.toSql());
            }
        }
    }

    private void checkHaving(LogicalHaving<Plan> having) {
        // check having not contains window expression slots
        Plan child = having.child();
        if (child instanceof OutputPrunable) {
            OutputPrunable outputPrunable = (OutputPrunable) child;
            if (outputPrunable instanceof Aggregate
                    || outputPrunable instanceof LogicalProject
                    || outputPrunable instanceof LogicalOneRowRelation) {
                Map<Slot, WindowExpression> windowExpressionSlots = Maps.newHashMap();
                for (NamedExpression expr : outputPrunable.getOutputs()) {
                    if (expr.containsType(WindowExpression.class)) {
                        WindowExpression windowExpr = (WindowExpression) ExpressionUtils.collect(
                                ImmutableList.of(expr), WindowExpression.class::isInstance).iterator().next();
                        windowExpressionSlots.put(expr.toSlot(), windowExpr);
                    }
                }
                if (!windowExpressionSlots.isEmpty()) {
                    for (Slot inputSlot : having.getInputSlots()) {
                        WindowExpression windowExpr = windowExpressionSlots.get(inputSlot);
                        if (windowExpr != null) {
                            throw new AnalysisException(
                                    "HAVING expression '" + inputSlot.getName()
                                            + "' must not contain window functions: " + windowExpr.toSql());
                        }
                    }
                }
            }
        }

        // check object type
        Set<Expression> havingConjuncts = having.getConjuncts();
        for (Expression predicate : havingConjuncts) {
            if (predicate instanceof InSubquery) {
                if (((InSubquery) predicate).getSubqueryOutput().getDataType().isObjectType()) {
                    throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
                }
            }
        }
    }
}
