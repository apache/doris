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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.exploration.mv.rollup.AggFunctionRollUpHandler;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewWindowRule
 */
public abstract class AbstractMaterializedViewWindowRule extends AbstractMaterializedViewRule {


    /**
     * compensatePredicates should be pulled up through window
     */
    @Override
    protected boolean rewriteQueryByViewPreCheck(MatchMode matchMode, StructInfo queryStructInfo,
            StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan,
            MaterializationContext materializationContext, ComparisonResult comparisonResult) {
        boolean superCheck = super.rewriteQueryByViewPreCheck(matchMode, queryStructInfo, viewStructInfo,
                viewToQuerySlotMapping, tempRewritedPlan, materializationContext, comparisonResult);
        Optional<LogicalWindow<Plan>> logicalWindow =
                queryStructInfo.getTopPlan().collectFirst(LogicalWindow.class::isInstance);
        if (!logicalWindow.isPresent()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Window rewriteQueryByViewPreCheck fail, logical window is not present",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s, tempRewrittenPlan is %s",
                            queryStructInfo.getExpressions(), materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping, tempRewritedPlan.treeString()));
            return false;
        }
        // if compensatePredicates exists, should be pulled up through window
        Set<SlotReference> queryCommonPartitionKeySet = logicalWindow.get()
                .getCommonPartitionKeyFromWindowExpressions();
        for (Expression conjuct : comparisonResult.getQueryExpressions()) {
            if (!queryCommonPartitionKeySet.containsAll(conjuct.getInputSlots())) {
                return false;
            }
        }
        return superCheck;
    }

    /**
     * Rewrite query by view
     *
     * @param matchMode match mode
     * @param queryStructInfo query struct info
     * @param viewStructInfo view struct info
     * @param viewToQuerySlotMapping slot mapping from view to query
     * @param tempRewrittenPlan temporary rewritten plan
     * @param materializationContext materialization context
     * @param cascadesContext cascades context
     * @return rewritten plan
     */
    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewrittenPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        if (!StructInfo.checkWindowTmpRewrittenPlanIsValid(tempRewrittenPlan)) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Window rewriteQueryByView fail",
                    () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                    + "targetToSourceMapping = %s, tempRewrittenPlan is %s",
                            queryStructInfo.getExpressions(), materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping, tempRewrittenPlan.treeString()));
            return null;
        }
        Map<Expression, Expression> mvExprToMvScanExprQueryBased =
                materializationContext.getShuttledExprToScanExprMapping().keyPermute(viewToQuerySlotMapping)
                        .flattenMap().get(0);
        // Rewrite top projects, represent the query projects by view
        List<Expression> expressionsRewritten = rewriteExpression(
                queryStructInfo.getExpressions(),
                queryStructInfo.getTopPlan(),
                materializationContext.getShuttledExprToScanExprMapping(),
                viewToQuerySlotMapping,
                ImmutableMap.of(), cascadesContext
        );
        // If generic rewrite fails, try roll up from query expressions.
        if (expressionsRewritten.isEmpty()) {
            expressionsRewritten = rollupWindowAggregateFunctions(queryStructInfo.getExpressions(),
                    queryStructInfo.getTopPlan(), mvExprToMvScanExprQueryBased, true, false);
            if (expressionsRewritten.isEmpty()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Rewrite expressions by view in window scan fail",
                        () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                        + "targetToSourceMapping = %s", queryStructInfo.getExpressions(),
                                materializationContext.getShuttledExprToScanExprMapping(),
                                viewToQuerySlotMapping));
                return null;
            }
        }
        return new LogicalProject<>(
                expressionsRewritten.stream()
                        .map(expression -> expression instanceof NamedExpression ? expression : new Alias(expression))
                        .map(NamedExpression.class::cast)
                        .collect(Collectors.toList()), tempRewrittenPlan);
    }

    private static List<Expression> rollupWindowAggregateFunctions(List<? extends Expression> expressions,
            Plan queryTopPlan, Map<Expression, Expression> mvExprToMvScanExprQueryBased,
            boolean needShuttle, boolean strictSlotRewrite) {
        WindowAggregateRollupContext context = new WindowAggregateRollupContext(queryTopPlan,
                mvExprToMvScanExprQueryBased, strictSlotRewrite);
        List<? extends Expression> inputExpressions = needShuttle
                ? ExpressionUtils.shuttleExpressionWithLineage(expressions, queryTopPlan)
                : expressions;
        List<Expression> rewrittenExpressions = inputExpressions.stream()
                .map(expression -> expression.accept(WindowAggregateRollupRewriter.INSTANCE, context))
                .collect(Collectors.toList());
        return context.isValid() ? rewrittenExpressions : ImmutableList.of();
    }

    private static Function rollupWindowAggregateFunction(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled, Map<Expression, Expression> mvExprToMvScanExprQueryBased) {
        for (Map.Entry<Expression, Expression> expressionEntry : mvExprToMvScanExprQueryBased.entrySet()) {
            Expression viewExpression = expressionEntry.getKey();
            // Window mapping keys may be full WindowExpression while rollup handlers match aggregate functions.
            if (viewExpression instanceof WindowExpression) {
                viewExpression = ((WindowExpression) viewExpression).getFunction();
            }
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair = Pair.of(viewExpression,
                    expressionEntry.getValue());
            for (AggFunctionRollUpHandler rollUpHandler : AbstractMaterializedViewAggregateRule.ROLL_UP_HANDLERS) {
                if (!rollUpHandler.canRollup(queryAggregateFunction, queryAggregateFunctionShuttled,
                        mvExprToMvScanExprQueryBasedPair, mvExprToMvScanExprQueryBased)) {
                    continue;
                }
                Function rollupFunction = rollUpHandler.doRollup(queryAggregateFunction,
                        queryAggregateFunctionShuttled, mvExprToMvScanExprQueryBasedPair,
                        mvExprToMvScanExprQueryBased);
                if (rollupFunction != null) {
                    return rollupFunction;
                }
            }
        }
        return null;
    }

    private static class WindowAggregateRollupRewriter
            extends DefaultExpressionRewriter<WindowAggregateRollupContext> {

        private static final WindowAggregateRollupRewriter INSTANCE = new WindowAggregateRollupRewriter();

        @Override
        public Expression visitWindow(WindowExpression windowExpression, WindowAggregateRollupContext context) {
            if (!context.isValid()) {
                return windowExpression;
            }
            Expression rewrittenWindowExpr = context.getMvExprToMvScanExprQueryBased().get(windowExpression);
            if (rewrittenWindowExpr != null) {
                return rewrittenWindowExpr;
            }
            Expression function = windowExpression.getFunction();
            if (!(function instanceof AggregateFunction)) {
                return super.visitWindow(windowExpression, context);
            }
            Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(function,
                    context.getQueryTopPlan());
            Function rewrittenFunction = rollupWindowAggregateFunction((AggregateFunction) function,
                    queryFunctionShuttled, context.getMvExprToMvScanExprQueryBased());
            if (rewrittenFunction == null) {
                context.setValid(false);
                return windowExpression;
            }
            return super.visitWindow(windowExpression.withFunction(rewrittenFunction), context);
        }

        @Override
        public Expression visitSlot(Slot slot, WindowAggregateRollupContext context) {
            if (!context.isValid()) {
                return slot;
            }
            Expression rewritten = context.getMvExprToMvScanExprQueryBased().get(slot);
            if (rewritten == null && context.isStrictSlotRewrite()) {
                context.setValid(false);
                return slot;
            }
            return rewritten == null ? slot : rewritten;
        }
    }

    private static class WindowAggregateRollupContext {
        private boolean valid = true;
        private final Plan queryTopPlan;
        private final Map<Expression, Expression> mvExprToMvScanExprQueryBased;
        private final boolean strictSlotRewrite;

        private WindowAggregateRollupContext(Plan queryTopPlan,
                Map<Expression, Expression> mvExprToMvScanExprQueryBased, boolean strictSlotRewrite) {
            this.queryTopPlan = queryTopPlan;
            this.mvExprToMvScanExprQueryBased = mvExprToMvScanExprQueryBased;
            this.strictSlotRewrite = strictSlotRewrite;
        }

        public boolean isValid() {
            return valid;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public Plan getQueryTopPlan() {
            return queryTopPlan;
        }

        public Map<Expression, Expression> getMvExprToMvScanExprQueryBased() {
            return mvExprToMvScanExprQueryBased;
        }

        public boolean isStrictSlotRewrite() {
            return strictSlotRewrite;
        }
    }
}
