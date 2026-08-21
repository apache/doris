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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        List<? extends Expression> expressionsToRewrite = getWindowRewriteExpressions(queryStructInfo);
        if (expressionsToRewrite.isEmpty()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Rewrite expressions by view in window scan fail, no expressions to rewrite",
                    () -> String.format("topPlan = %s", queryStructInfo.getTopPlan().treeString()));
            return null;
        }
        Plan expressionSourcePlan = queryStructInfo.getOriginalPlan();
        // Rewrite top projects, represent the query projects by view
        List<Expression> expressionsRewritten = rewriteExpression(
                expressionsToRewrite,
                expressionSourcePlan,
                materializationContext.getShuttledExprToScanExprMapping(),
                viewToQuerySlotMapping,
                ImmutableMap.of(), cascadesContext
        );
        // If generic rewrite fails, try roll up from query expressions.
        if (expressionsRewritten.isEmpty()) {
            expressionsRewritten = rollupWindowAggregateFunctions(expressionsToRewrite,
                    expressionSourcePlan, mvExprToMvScanExprQueryBased, true, false);
            if (expressionsRewritten.isEmpty()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Rewrite expressions by view in window scan fail",
                        () -> String.format("expressionToRewritten is %s,\n mvExprToMvScanExprMapping is %s,\n"
                                        + "targetToSourceMapping = %s", expressionsToRewrite,
                                materializationContext.getShuttledExprToScanExprMapping(),
                                viewToQuerySlotMapping));
                return null;
            }
        }
        return buildRewrittenPlanFromExpressions(expressionsRewritten, tempRewrittenPlan);
    }

    private static Plan buildRewrittenPlanFromExpressions(List<Expression> expressionsRewritten, Plan mvScan) {
        boolean hasAggregateFunction = expressionsRewritten.stream()
                .anyMatch(expression -> expression.containsType(AggregateFunction.class));
        if (!hasAggregateFunction) {
            return new LogicalProject<>(toNamedExpressions(expressionsRewritten), mvScan);
        }
        List<Expression> groupByExpressions = new ArrayList<>();
        List<NamedExpression> aggregateFunctionOutputs = new ArrayList<>();
        for (Expression expression : expressionsRewritten) {
            if (expression.containsType(AggregateFunction.class)) {
                aggregateFunctionOutputs.add(toNamedExpression(expression));
            } else if (!(expression instanceof Literal) && !groupByExpressions.contains(expression)) {
                groupByExpressions.add(expression);
            }
        }
        // LogicalAggregate.computeOutput() preserves outputExpressions order; put
        // group-by
        // outputs first so the [groupBy, agg] slot layout matches the projection below.
        List<NamedExpression> aggregateOutputExpressions = new ArrayList<>(
                groupByExpressions.size() + aggregateFunctionOutputs.size());
        for (Expression groupBy : groupByExpressions) {
            aggregateOutputExpressions.add(toNamedExpression(groupBy));
        }
        aggregateOutputExpressions.addAll(aggregateFunctionOutputs);
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(groupByExpressions, aggregateOutputExpressions,
                mvScan);
        List<Slot> aggregateOutput = aggregate.getOutput();
        int groupByOutputSize = groupByExpressions.size();
        int groupByOutputIndex = 0;
        int aggregateFunctionOutputIndex = 0;
        List<NamedExpression> projectExpressions = new ArrayList<>(expressionsRewritten.size());
        for (Expression expression : expressionsRewritten) {
            if (expression instanceof Literal) {
                projectExpressions.add(toNamedExpression(expression));
            } else if (expression.containsType(AggregateFunction.class)) {
                projectExpressions.add(new Alias(aggregateOutput.get(groupByOutputSize
                        + aggregateFunctionOutputIndex++)));
            } else {
                projectExpressions.add(new Alias(aggregateOutput.get(groupByOutputIndex++)));
            }
        }
        return new LogicalProject<>(projectExpressions, aggregate);
    }

    private static List<NamedExpression> toNamedExpressions(List<Expression> expressions) {
        return expressions.stream()
                .map(AbstractMaterializedViewWindowRule::toNamedExpression)
                .collect(Collectors.toList());
    }

    private static NamedExpression toNamedExpression(Expression expression) {
        return expression instanceof NamedExpression ? (NamedExpression) expression : new Alias(expression);
    }

    private static List<? extends Expression> getWindowRewriteExpressions(StructInfo queryStructInfo) {
        Plan topPlan = queryStructInfo.getTopPlan();
        if (topPlan instanceof LogicalProject) {
            return ((LogicalProject<Plan>) topPlan).getProjects();
        }
        Optional<LogicalWindow<Plan>> logicalWindow = topPlan.collectFirst(LogicalWindow.class::isInstance);
        if (!logicalWindow.isPresent()) {
            return queryStructInfo.getExpressions();
        }
        List<? extends Expression> candidates = collectWindowRewriteCandidates(queryStructInfo);
        return buildWindowRewriteExpressionsAlignedWithOutput(candidates, logicalWindow.get());
    }

    private static List<? extends Expression> collectWindowRewriteCandidates(StructInfo queryStructInfo) {
        List<? extends Expression> planOutputShuttled = queryStructInfo.getPlanOutputShuttledExpressions();
        if (!planOutputShuttled.isEmpty()) {
            return planOutputShuttled;
        }
        return queryStructInfo.getExpressions();
    }

    private static List<Expression> buildWindowRewriteExpressionsAlignedWithOutput(
            List<? extends Expression> candidates, LogicalWindow<Plan> window) {
        List<Slot> windowOutputSlots = window.getOutput();
        Set<Slot> intermediateAggStateSlots = getIntermediateAggStateSlots(window);
        List<Expression> result = new ArrayList<>(windowOutputSlots.size());
        if (candidates.size() != windowOutputSlots.size()) {
            return result;
        }
        for (int i = 0; i < windowOutputSlots.size(); i++) {
            Slot outputSlot = windowOutputSlots.get(i);
            if (intermediateAggStateSlots.contains(outputSlot)) {
                result.add(createNullPlaceholder(outputSlot));
            } else {
                result.add(candidates.get(i));
            }
        }
        return result;
    }

    private static Expression createNullPlaceholder(Slot outputSlot) {
        if (outputSlot instanceof SlotReference) {
            return new NullLiteral(((SlotReference) outputSlot).getDataType());
        }
        return NullLiteral.INSTANCE;
    }

    /**
     * Slots produced by aggregate below window but not group-by keys (e.g. per-day uv_state).
     */
    private static Set<Slot> getIntermediateAggStateSlots(LogicalWindow<Plan> window) {
        Plan child = window.child();
        if (!(child instanceof LogicalAggregate)) {
            return ImmutableSet.of();
        }
        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) child;
        Set<Slot> groupBySlots = new HashSet<>();
        for (Expression groupByExpression : aggregate.getGroupByExpressions()) {
            groupBySlots.addAll(groupByExpression.getInputSlots());
        }
        Set<Slot> intermediateAggStateSlots = new HashSet<>();
        for (Slot slot : child.getOutput()) {
            if (!groupBySlots.contains(slot)) {
                intermediateAggStateSlots.add(slot);
            }
        }
        return intermediateAggStateSlots;
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

    private static Function rollupWindowAggregateFunction(WindowExpression queryWindow,
            AggregateFunction queryAggregateFunction, Expression queryAggregateFunctionShuttled,
            Map<Expression, Expression> mvExprToMvScanExprQueryBased) {
        for (Map.Entry<Expression, Expression> expressionEntry : mvExprToMvScanExprQueryBased.entrySet()) {
            Expression viewExpression = expressionEntry.getKey();
            // Window mapping keys may be full WindowExpression while rollup handlers match aggregate functions.
            if (viewExpression instanceof WindowExpression) {
                WindowExpression viewWindow = (WindowExpression) viewExpression;
                if (!windowSpecEquals(queryWindow, viewWindow)) {
                    continue;
                }
                viewExpression = viewWindow.getFunction();
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

    /**
     * Compare window spec only (partition / order / frame), not the aggregate function inside OVER.
     * Query and view may use different combinators (e.g. merge vs union) on the same window spec.
     */
    private static boolean windowSpecEquals(WindowExpression queryWindow, WindowExpression viewWindow) {
        return Objects.equals(queryWindow.getPartitionKeys(), viewWindow.getPartitionKeys())
                && Objects.equals(queryWindow.getOrderKeys(), viewWindow.getOrderKeys())
                && Objects.equals(queryWindow.getWindowFrame(), viewWindow.getWindowFrame());
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
            Function rewrittenFunction = rollupWindowAggregateFunction(windowExpression,
                    (AggregateFunction) function, queryFunctionShuttled,
                    context.getMvExprToMvScanExprQueryBased());
            if (rewrittenFunction == null) {
                context.setValid(false);
                return windowExpression;
            }
            // MV column already holds the windowed result; return rollup scalar (e.g. xx_merge(mv_col))
            // instead of wrapping with OVER to avoid double aggregation on precomputed window column.
            return rewrittenFunction;
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
