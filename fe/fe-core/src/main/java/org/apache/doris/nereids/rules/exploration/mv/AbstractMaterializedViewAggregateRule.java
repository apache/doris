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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.rules.analysis.NormalizeRepeat;
import org.apache.doris.nereids.rules.exploration.mv.AbstractMaterializedViewAggregateRule.AggregateExpressionRewriteContext.ExpressionRewriteMode;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanSplitContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.exploration.mv.rollup.AggFunctionRollUpHandler;
import org.apache.doris.nereids.rules.exploration.mv.rollup.BothCombinatorRollupHandler;
import org.apache.doris.nereids.rules.exploration.mv.rollup.ContainDistinctFunctionRollupHandler;
import org.apache.doris.nereids.rules.exploration.mv.rollup.DirectRollupHandler;
import org.apache.doris.nereids.rules.exploration.mv.rollup.MappingRollupHandler;
import org.apache.doris.nereids.rules.exploration.mv.rollup.SingleCombinatorRollupHandler;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewAggregateRule
 * This is responsible for common aggregate rewriting
 */
public abstract class AbstractMaterializedViewAggregateRule extends AbstractMaterializedViewRule {

    public static final List<AggFunctionRollUpHandler> ROLL_UP_HANDLERS =
            ImmutableList.of(DirectRollupHandler.INSTANCE,
                    MappingRollupHandler.INSTANCE,
                    SingleCombinatorRollupHandler.INSTANCE,
                    BothCombinatorRollupHandler.INSTANCE,
                    ContainDistinctFunctionRollupHandler.INSTANCE);

    protected static final AggregateExpressionRewriter AGGREGATE_EXPRESSION_REWRITER =
            new AggregateExpressionRewriter();

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        // get view and query aggregate and top plan correspondingly
        Pair<Plan, LogicalAggregate<Plan>> viewTopPlanAndAggPair = splitToTopPlanAndAggregate(viewStructInfo);
        if (viewTopPlanAndAggPair == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Split view to top plan and agg fail, view doesn't not contain aggregate",
                    () -> String.format("view plan = %s\n", viewStructInfo.getOriginalPlan().treeString()));
            return null;
        }
        Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair = splitToTopPlanAndAggregate(queryStructInfo);
        if (queryTopPlanAndAggPair == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Split query to top plan and agg fail",
                    () -> String.format("query plan = %s\n", queryStructInfo.getOriginalPlan().treeString()));
            return null;
        }
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        if (!checkCompatibility(queryStructInfo, queryAggregate, viewTopPlanAndAggPair.value(),
                materializationContext)) {
            return null;
        }
        boolean queryContainsGroupSets = queryAggregate.getSourceRepeat().isPresent();
        // If group by expression between query and view is equals, try to rewrite expression directly
        if (!queryContainsGroupSets && isGroupByEquals(queryTopPlanAndAggPair, viewTopPlanAndAggPair,
                viewToQuerySlotMapping, queryStructInfo, viewStructInfo, tempRewritedPlan, materializationContext,
                cascadesContext)) {
            List<Expression> rewrittenQueryExpressions = rewriteExpression(queryTopPlan.getOutput(),
                    queryTopPlan,
                    materializationContext.getShuttledExprToScanExprMapping(),
                    viewToQuerySlotMapping,
                    ImmutableMap.of(), cascadesContext);
            boolean isRewrittenQueryExpressionValid = true;
            if (!rewrittenQueryExpressions.isEmpty()) {
                List<NamedExpression> projects = new ArrayList<>();
                for (Expression expression : rewrittenQueryExpressions) {
                    if (expression.containsType(AggregateFunction.class)) {
                        // record the reason and then try to roll up aggregate function
                        materializationContext.recordFailReason(queryStructInfo,
                                "rewritten expression contains aggregate functions when group equals aggregate rewrite",
                                () -> String.format("aggregate functions = %s\n", rewrittenQueryExpressions));
                        isRewrittenQueryExpressionValid = false;
                    }
                    projects.add(expression instanceof NamedExpression
                            ? (NamedExpression) expression : new Alias(expression));
                }
                if (isRewrittenQueryExpressionValid) {
                    return new LogicalProject<>(projects, tempRewritedPlan);
                }
            }
            // if fails, record the reason and then try to roll up aggregate function
            materializationContext.recordFailReason(queryStructInfo,
                    "Can not rewrite expression when no roll up",
                    () -> String.format("expressionToWrite = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                    + "viewToQuerySlotMapping = %s",
                            queryTopPlan.getOutput(),
                            materializationContext.getShuttledExprToScanExprMapping(),
                            viewToQuerySlotMapping));
        }
        return aggregateRewriteByView(queryStructInfo,
                viewToQuerySlotMapping,
                queryTopPlanAndAggPair,
                tempRewritedPlan,
                materializationContext,
                ExpressionRewriteMode.EXPRESSION_DIRECT,
                ExpressionRewriteMode.EXPRESSION_ROLL_UP);
    }

    /**
     * Aggregate function and group by expression rewrite impl
     */
    protected Plan aggregateRewriteByView(
            StructInfo queryStructInfo,
            SlotMapping viewToQuerySlotMapping,
            Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext,
            ExpressionRewriteMode groupByMode,
            ExpressionRewriteMode aggregateFunctionMode) {
        // try to roll up.
        // try to rewrite, contains both roll up aggregate functions and aggregate group expression
        List<NamedExpression> finalOutputExpressions = new ArrayList<>();
        List<Expression> finalGroupExpressions = new ArrayList<>();
        // permute the mv expr mapping to query based
        Map<Expression, Expression> mvExprToMvScanExprQueryBased =
                materializationContext.getShuttledExprToScanExprMapping().keyPermute(viewToQuerySlotMapping)
                        .flattenMap().get(0);
        Plan queryTopPlan = queryStructInfo.getTopPlan();
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        List<Expression> queryGroupByExpressions = queryAggregate.getGroupByExpressions();
        if (queryAggregate.getSourceRepeat().isPresent()) {
            // The group by/function classification of the query top plan output expressions is only
            // used by the repeat rewrite, so it is computed lazily inside this branch to avoid paying
            // the full plan lineage walk for every ordinary aggregate rewrite.
            // split the query top plan expressions to group expressions and functions, if can not, bail out.
            Pair<Set<? extends Expression>, Set<? extends Expression>> queryGroupAndFunctionPair
                    = topPlanSplitToGroupAndFunction(queryTopPlanAndAggPair, queryStructInfo);
            Set<? extends Expression> queryTopPlanGroupBySet = queryGroupAndFunctionPair.key();
            Set<? extends Expression> queryTopPlanFunctionSet = queryGroupAndFunctionPair.value();
            // try to rewrite the query top plan expressions, the query top plan output expressions
            // are used as the repeat output expressions directly. The repeat output keeps the original top
            // output expr id equivalence classes, see rewriteOutputInOriginalExprIdEquivalenceClass, so the
            // rewritten output set size equals the query output set size and the repeat rewrite is not
            // rejected by the output set size guard of MaterializedViewUtils.rewriteByRules.
            Map<ExprId, NamedExpression> originalExprIdToRewrittenOutput = new HashMap<>();
            Set<ExprId> usedRewrittenOutputExprIds = new HashSet<>();
            for (Slot topPlanOutput : queryTopPlan.getOutput()) {
                NamedExpression rewrittenOutput = originalExprIdToRewrittenOutput.get(topPlanOutput.getExprId());
                if (rewrittenOutput == null) {
                    if (queryTopPlanFunctionSet.contains(topPlanOutput)) {
                        // if agg function, try to roll up and rewrite
                        Expression rollupedExpression = tryRewriteExpression(queryStructInfo, topPlanOutput,
                                mvExprToMvScanExprQueryBased, aggregateFunctionMode, materializationContext,
                                "Query function roll up fail",
                                () -> String.format("queryExpression = %s,\n mvExprToMvScanExprQueryBased = %s",
                                        topPlanOutput, mvExprToMvScanExprQueryBased));
                        if (rollupedExpression == null) {
                            return null;
                        }
                        rewrittenOutput = rewriteOutputInOriginalExprIdEquivalenceClass(
                                originalExprIdToRewrittenOutput, usedRewrittenOutputExprIds,
                                topPlanOutput.getExprId(), rollupedExpression);
                    } else {
                        // if group by dimension, try to rewrite
                        Expression rewrittenGroupByExpression = tryRewriteExpression(
                                queryStructInfo, topPlanOutput, mvExprToMvScanExprQueryBased, groupByMode,
                                materializationContext,
                                "View dimensions doesn't not cover the query dimensions",
                                () -> String.format("mvExprToMvScanExprQueryBased is %s,\n queryExpression is %s",
                                        mvExprToMvScanExprQueryBased, topPlanOutput));
                        if (rewrittenGroupByExpression == null) {
                            // group expr can not rewrite by view
                            return null;
                        }
                        rewrittenOutput = rewriteOutputInOriginalExprIdEquivalenceClass(
                                originalExprIdToRewrittenOutput, usedRewrittenOutputExprIds,
                                topPlanOutput.getExprId(), rewrittenGroupByExpression);
                        finalGroupExpressions.add(rewrittenOutput);
                    }
                }
                finalOutputExpressions.add(rewrittenOutput);
            }
            // handle the scene that query top plan not use the group by in query bottom aggregate
            if (needCompensateGroupBy(queryTopPlanGroupBySet, queryGroupByExpressions)) {
                for (Expression expression : queryGroupByExpressions) {
                    if (queryTopPlanGroupBySet.contains(expression)) {
                        continue;
                    }
                    Expression rewrittenGroupByExpression = tryRewriteExpression(queryStructInfo, expression,
                            mvExprToMvScanExprQueryBased, groupByMode, materializationContext,
                            "View dimensions doesn't not cover the query dimensions in bottom agg ",
                            () -> String.format("mvExprToMvScanExprQueryBased is %s,\n expression is %s",
                                    mvExprToMvScanExprQueryBased, expression));
                    if (rewrittenGroupByExpression == null) {
                        return null;
                    }
                    NamedExpression groupByExpression = rewrittenGroupByExpression instanceof NamedExpression
                            ? (NamedExpression) rewrittenGroupByExpression : new Alias(rewrittenGroupByExpression);
                    finalGroupExpressions.add(groupByExpression);
                }
            }
            // construct group sets for repeat
            List<List<Expression>> rewrittenGroupSetsExpressions = new ArrayList<>();
            List<List<Expression>> groupingSets = queryAggregate.collectFirst(LogicalRepeat.class::isInstance)
                    .map(repeat -> ((LogicalRepeat<? extends Plan>) repeat).getGroupingSets())
                    .orElse(queryAggregate.getSourceRepeat().get().getGroupingSets());
            for (List<Expression> groupingSet : groupingSets) {
                if (groupingSet.isEmpty()) {
                    rewrittenGroupSetsExpressions.add(ImmutableList.of());
                } else {
                    List<Expression> rewrittenGroupSetExpressions = new ArrayList<>();
                    for (Expression expression : groupingSet) {
                        Expression rewrittenGroupByExpression = tryRewriteExpression(queryStructInfo, expression,
                                mvExprToMvScanExprQueryBased, ExpressionRewriteMode.EXPRESSION_DIRECT,
                                materializationContext,
                                "View dimensions doesn't not cover the query group set dimensions",
                                () -> String.format("mvExprToMvScanExprQueryBased is %s,\n queryExpression is %s",
                                        mvExprToMvScanExprQueryBased, expression));
                        if (rewrittenGroupByExpression == null) {
                            return null;
                        }
                        rewrittenGroupSetExpressions.add(rewrittenGroupByExpression);
                    }
                    rewrittenGroupSetsExpressions.add(rewrittenGroupSetExpressions);
                }
            }
            LogicalRepeat<Plan> repeat = new LogicalRepeat<>(rewrittenGroupSetsExpressions,
                    finalOutputExpressions, queryStructInfo.getGroupingId().get(),
                    queryAggregate.getSourceRepeat().get().getGroupingIdValues().orElse(null),
                    queryAggregate.getSourceRepeat().get().getRepeatType(), tempRewritedPlan);
            return NormalizeRepeat.doNormalize(repeat);
        }

        // The rewritten aggregate should group by the query bottom aggregate's group by expressions,
        // and its output expressions should be the rewritten group by expressions and the rolled up
        // aggregate functions. The query top plan output expressions are recomputed by a project above
        // the rewritten aggregate, so the projection of a group by key in the query top plan (such as
        // `select cast(date_trunc(ts, 'day') as string) from t group by date_trunc(ts, 'day')`) will not
        // be wrongly treated as a group by key of the rewritten aggregate.
        // The mapping from the query bottom aggregate output slot to the new aggregate output expression
        // is used to rewrite the query top plan output expressions to reference the new aggregate output.
        // The shuttled query bottom aggregate outputs are used as the map keys and are passed to the
        // rewriter directly, and the shuttled query top plan outputs restore the projection expressions,
        // so the whole top plan is traversed only twice instead of once per output expression.
        // The map values are filled after the rewritten aggregate is constructed, see the comment there.
        List<? extends Expression> shuttledBottomAggOutputs = ExpressionUtils.shuttleExpressionWithLineage(
                queryAggregate.getOutputExpressions(), queryTopPlan);
        List<? extends Expression> shuttledTopPlanOutputs = ExpressionUtils.shuttleExpressionWithLineage(
                queryTopPlan.getOutput(), queryTopPlan);
        Map<Expression, Expression> bottomAggOutputToNewExprMap = new HashMap<>();
        Set<Expression> queryGroupByExpressionSet = new HashSet<>(queryGroupByExpressions);
        List<NamedExpression> queryAggregateOutputs = queryAggregate.getOutputExpressions();
        for (int i = 0; i < queryAggregateOutputs.size(); i++) {
            NamedExpression queryAggregateOutput = queryAggregateOutputs.get(i);
            Expression shuttledQueryAggregateOutput = shuttledBottomAggOutputs.get(i);
            if (queryGroupByExpressionSet.contains(queryAggregateOutput)) {
                // if it is a group by expression, rewrite it to the new aggregate group by key
                Expression rewrittenGroupByExpression = rewriteShuttledExpression(queryStructInfo,
                        shuttledQueryAggregateOutput, mvExprToMvScanExprQueryBased, groupByMode,
                        materializationContext,
                        "View dimensions doesn't not cover the query dimensions",
                        () -> String.format("mvExprToMvScanExprQueryBased is %s,\n queryExpression is %s",
                                mvExprToMvScanExprQueryBased, queryAggregateOutput));
                if (rewrittenGroupByExpression == null) {
                    return null;
                }
                NamedExpression groupByOutput = rewrittenGroupByExpression instanceof NamedExpression
                        ? (NamedExpression) rewrittenGroupByExpression : new Alias(rewrittenGroupByExpression);
                finalGroupExpressions.add(groupByOutput);
                finalOutputExpressions.add(groupByOutput);
            } else {
                // if it is an aggregate function, try to roll up and rewrite
                Expression rewrittenFunction = rewriteShuttledExpression(queryStructInfo,
                        shuttledQueryAggregateOutput, mvExprToMvScanExprQueryBased, aggregateFunctionMode,
                        materializationContext,
                        "Query function roll up fail",
                        () -> String.format("queryExpression = %s,\n mvExprToMvScanExprQueryBased = %s",
                                queryAggregateOutput, mvExprToMvScanExprQueryBased));
                if (rewrittenFunction == null) {
                    return null;
                }
                NamedExpression functionOutput = new Alias(rewrittenFunction);
                finalOutputExpressions.add(functionOutput);
            }
        }

        LogicalAggregate<Plan> rewrittenAggregate =
                new LogicalAggregate<>(finalGroupExpressions, finalOutputExpressions, tempRewritedPlan);

        // Each rewritten aggregate output corresponds to one query aggregate output, so map the query
        // aggregate output to the new output slot by position. The slot must be taken from the constructed
        // aggregate: its constructor forces a NullableAggregateFunction of an aggregate without group by
        // to nullable, since a scalar aggregate over an empty input returns NULL, and a slot built from
        // the expression before that adjustment keeps the old nullability, which AdjustNullable rejects.
        List<NamedExpression> rewrittenAggregateOutputs = rewrittenAggregate.getOutputExpressions();
        for (int i = 0; i < queryAggregateOutputs.size(); i++) {
            NamedExpression rewrittenOutput = rewrittenAggregateOutputs.get(i);
            bottomAggOutputToNewExprMap.put(shuttledBottomAggOutputs.get(i), rewrittenOutput.toSlot());
        }

        // rewrite the query top plan output expressions to reference the rewritten aggregate output,
        // the query top plan output slot is shuttled by lineage firstly to restore the projection
        // expression, so a projection of the group by key in the query top plan can be recomputed
        // by a project above the rewritten aggregate.
        List<NamedExpression> topProjectExpressions = new ArrayList<>();
        // Preserve the original top output expr id equivalence classes, see
        // rewriteOutputInOriginalExprIdEquivalenceClass for the details.
        List<Slot> queryTopPlanOutputs = queryTopPlan.getOutput();
        Map<ExprId, NamedExpression> originalExprIdToRewritten = new HashMap<>();
        Set<ExprId> usedRewrittenExprIds = new HashSet<>();
        for (int i = 0; i < shuttledTopPlanOutputs.size(); i++) {
            ExprId originalExprId = queryTopPlanOutputs.get(i).getExprId();
            NamedExpression groupRewrittenExpression = originalExprIdToRewritten.get(originalExprId);
            if (groupRewrittenExpression == null) {
                groupRewrittenExpression = rewriteOutputInOriginalExprIdEquivalenceClass(
                        originalExprIdToRewritten, usedRewrittenExprIds, originalExprId,
                        ExpressionUtils.replace(shuttledTopPlanOutputs.get(i), bottomAggOutputToNewExprMap));
            }
            topProjectExpressions.add(groupRewrittenExpression);
        }
        // If the query top plan output expressions can be produced by the rewritten aggregate directly,
        // return the aggregate, otherwise compute them by a project above the rewritten aggregate.
        // Note the query top plan output may be a strict matching prefix of the rewritten aggregate output
        // (e.g. `select k1 from t group by k1, k2`), in which case the redundant aggregate outputs must be
        // projected away. Otherwise the rewritten plan output count differs from the query and the candidate
        // is rejected by MaterializedViewUtils.normalizeExpressions, so a valid sync MV is silently not used.
        boolean needTopProject = topProjectExpressions.size() != rewrittenAggregateOutputs.size();
        for (int i = 0; i < topProjectExpressions.size(); i++) {
            if (i >= rewrittenAggregateOutputs.size()
                    || !topProjectExpressions.get(i).toSlot().equals(rewrittenAggregateOutputs.get(i).toSlot())) {
                needTopProject = true;
                break;
            }
        }
        if (!needTopProject) {
            return rewrittenAggregate;
        }
        return new LogicalProject<>(topProjectExpressions, rewrittenAggregate);
    }

    /**
     * handle the scene that query top plan not use the group by in query bottom aggregate
     * If mv is select o_orderdate from  orders group by o_orderdate;
     * query is select 1 from orders group by o_orderdate.
     * Or mv is select o_orderdate from orders group by o_orderdate
     * query is select o_orderdate from  orders group by o_orderdate, o_orderkey;
     * if the slot which query top project use can not cover the slot which query bottom aggregate group by slot
     * should compensate group by to make sure the data is right.
     * For example:
     * mv is select o_orderdate from orders group by o_orderdate;
     * query is select o_orderdate from  orders group by o_orderdate, o_orderkey;
     *
     * @param queryGroupByExpressions query bottom aggregate group by is o_orderdate, o_orderkey
     * @param queryTopProject query top project is o_orderdate
     * @return need to compensate group by if true or not need
     *
     */
    private static boolean needCompensateGroupBy(Set<? extends Expression> queryTopProject,
            List<Expression> queryGroupByExpressions) {
        Set<Expression> queryGroupByExpressionSet = new HashSet<>(queryGroupByExpressions);
        if (queryGroupByExpressionSet.size() != queryTopProject.size()) {
            return true;
        }
        Set<NamedExpression> queryTopPlanGroupByUseNamedExpressions = new HashSet<>();
        Set<NamedExpression> queryGroupByUseNamedExpressions = new HashSet<>();
        for (Expression expr : queryTopProject) {
            queryTopPlanGroupByUseNamedExpressions.addAll(expr.collect(NamedExpression.class::isInstance));
        }
        for (Expression expr : queryGroupByExpressionSet) {
            queryGroupByUseNamedExpressions.addAll(expr.collect(NamedExpression.class::isInstance));
        }
        // if the slots query top project use can not cover the slots which query bottom aggregate use
        // Should compensate.
        return !queryTopPlanGroupByUseNamedExpressions.containsAll(queryGroupByUseNamedExpressions);
    }

    /**
     * Build one rewritten output of a query top plan output position, keeping the rewritten output
     * consistent with the original top output expr id equivalence classes.
     * <p>
     * Positions which share one original expr id (such as the unaliased `select k, k from t group by k`,
     * where both positions reference the same top output slot) must share one rewritten output expr id,
     * otherwise the rewritten output set is inflated. Conversely, positions with distinct original expr ids
     * which are rewritten to the same view output (such as `select k as k1, k as k2 from t group by k`,
     * where both positions rewrite to the same view slot) must keep distinct rewritten output expr ids,
     * otherwise the rewritten output set is collapsed.
     * <p>
     * Both directions matter because the rewritten output set size must equal the query output set size,
     * which is the guard of MaterializedViewUtils.rewriteByRules. When the guard is hit, the whole-tree
     * normalization and partition pruning are skipped, and the repeat rewrite returns an aggregate with
     * normalized=false whose derived projections are still inside the unnormalized aggregate, so that the
     * physical translation can not resolve them when the top plan output is projected.
     *
     * @param originalExprIdToRewritten rewritten output of the original expr id, reused when present
     * @param usedRewrittenExprIds rewritten output expr ids which are used by other expr id classes already
     * @param originalExprId original expr id of the query top plan output position
     * @param rewrittenExpression expression which the query top plan output position is rewritten to
     * @return rewritten output expression of the query top plan output position
     */
    private static NamedExpression rewriteOutputInOriginalExprIdEquivalenceClass(
            Map<ExprId, NamedExpression> originalExprIdToRewritten, Set<ExprId> usedRewrittenExprIds,
            ExprId originalExprId, Expression rewrittenExpression) {
        NamedExpression rewrittenOutput = rewrittenExpression instanceof NamedExpression
                ? (NamedExpression) rewrittenExpression : new Alias(rewrittenExpression);
        if (!usedRewrittenExprIds.add(rewrittenOutput.getExprId())) {
            // The rewritten expr id is used by another original expr id equivalence class already, keep a
            // distinct output expr id for this class.
            rewrittenOutput = new Alias(rewrittenExpression);
        }
        originalExprIdToRewritten.put(originalExprId, rewrittenOutput);
        return rewrittenOutput;
    }

    /**
     * Try to rewrite query expression by view, contains both group by dimension and aggregate function
     */
    protected Expression tryRewriteExpression(StructInfo queryStructInfo, Expression queryExpression,
            Map<Expression, Expression> mvShuttledExprToMvScanExprQueryBased, ExpressionRewriteMode rewriteMode,
            MaterializationContext materializationContext, String summaryIfFail, Supplier<String> detailIfFail) {
        Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                queryExpression,
                queryStructInfo.getTopPlan());
        return rewriteShuttledExpression(queryStructInfo, queryFunctionShuttled, mvShuttledExprToMvScanExprQueryBased,
                rewriteMode, materializationContext, summaryIfFail, detailIfFail);
    }

    /**
     * Rewrite the shuttled query expression by view, contains both group by dimension and aggregate
     * function. The query expression is expected to be shuttled by lineage already, so the top plan is
     * not traversed again here and the batched lineage result can be reused.
     */
    private Expression rewriteShuttledExpression(StructInfo queryStructInfo, Expression queryShuttledExpression,
            Map<Expression, Expression> mvShuttledExprToMvScanExprQueryBased, ExpressionRewriteMode rewriteMode,
            MaterializationContext materializationContext, String summaryIfFail, Supplier<String> detailIfFail) {
        AggregateExpressionRewriteContext expressionRewriteContext = new AggregateExpressionRewriteContext(
                rewriteMode, mvShuttledExprToMvScanExprQueryBased, queryStructInfo.getTopPlan(),
                queryStructInfo.getGroupingId());
        Expression rewrittenExpression = queryShuttledExpression.accept(AGGREGATE_EXPRESSION_REWRITER,
                expressionRewriteContext);
        if (!expressionRewriteContext.isValid()) {
            materializationContext.recordFailReason(queryStructInfo, summaryIfFail, detailIfFail);
            return null;
        }
        return rewrittenExpression;
    }

    /**
     * Not all query after rewritten successfully can compensate union all
     * Such as:
     * mv def sql is as following, partition column is a
     * select a, b, count(*) from t1 group by a, b
     * Query is as following:
     * select b, count(*) from t1 group by b, after rewritten by materialized view successfully
     * If mv part partition is invalid, can not compensate union all, because result is wrong after
     * compensate union all.
     *
     * <p>The compensation unions two finished aggregates: what the materialized view answers for the partitions
     * it keeps, and what the base table answers for the partitions it does not. It is right while no group is
     * answerable by both, and only a grouping that reaches the column the view is partitioned on keeps the two
     * apart -- the view's partitions are disjoint in that column, by value for a list partitioned view and by
     * range for a range partitioned one. A view partitioned by an expression over the column is disjoint in
     * that expression's value instead, so grouping by the same expression splits the branches the same way:
     * {@code date_trunc(d, 'month')} over a view partitioned by it answers one month per group, all of it in
     * one branch. Grouping by another expression over the column does not: {@code date_trunc(d, 'year')} over
     * that view answers one year out of both branches, the view's months of it and the base table's, and the
     * two rows come out beside each other. Nor does a grouping set that leaves it out -- the {@code ()} total
     * of {@code ROLLUP}/{@code CUBE}/{@code GROUPING SETS} puts every partition value in one group, which both
     * branches answer. And the view's own transform keeps them apart only while each partition it keeps holds
     * the whole transform bucket: a window cut or a partition boundary a fraction of a second past the bucket's
     * end leaves part of a bucket in the base branch, which
     * {@code columnsSharingRetainedBucket} reports.
     */
    @Override
    protected boolean canUnionRewrite(Plan queryPlan, AsyncMaterializationContext context,
            CascadesContext cascadesContext, Set<BaseColInfo> columnsSharingRetainedBucket) {
        // The union is built over the whole query plan, so the outermost aggregate is the one whose groups
        // the two branches answer. The plan holds one, since the rule's pattern requires it.
        Optional<LogicalAggregate<Plan>> logicalAggregateOptional =
                queryPlan.collectFirst(planTreeNode -> planTreeNode instanceof LogicalAggregate);
        if (!logicalAggregateOptional.isPresent()) {
            return true;
        }
        LogicalAggregate<Plan> queryAggregate = logicalAggregateOptional.get();
        List<Expression> groupByExpressions = queryAggregate.getGroupByExpressions();
        if (groupByExpressions.isEmpty()) {
            // Scalar aggregate can not compensate union all
            return false;
        }
        MTMVPartitionInfo partitionInfo = context.getMtmv().getMvPartitionInfo();
        List<BaseColInfo> pctInfos = partitionInfo.getPctInfos();
        // The view has one partition column but it can be tracked by several base columns at once: an inner
        // join on A.d = B.d records both, and they are one dimension as long as the query holds them equal.
        Map<BaseColInfo, Set<BaseColInfo>> equalColumns = columnsTheQueryHoldsEqual(queryPlan);
        Optional<String> partitionTimeUnit = viewPartitionTimeUnit(partitionInfo);
        Optional<LogicalRepeat<?>> logicalRepeatOptional = queryAggregate.getSourceRepeat();
        if (!logicalRepeatOptional.isPresent()) {
            return groupingCoversPartitionColumns(groupByExpressions, queryPlan, pctInfos, equalColumns,
                    partitionTimeUnit, columnsSharingRetainedBucket);
        }
        // The group by list holds every grouping set's expressions beside the grouping id, so it can not tell
        // which set a partition column was read from; each set is asked on its own instead.
        for (List<Expression> groupingSet : logicalRepeatOptional.get().getGroupingSets()) {
            if (!groupingCoversPartitionColumns(groupingSet, queryPlan, pctInfos, equalColumns,
                    partitionTimeUnit, columnsSharingRetainedBucket)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The time unit of the view's own partition transform, empty when it partitions by the bare column. An
     * expression partitioned view always is a date_trunc, the only function its partition clause takes, and
     * the second argument is the unit.
     */
    private static Optional<String> viewPartitionTimeUnit(MTMVPartitionInfo partitionInfo) {
        Expr partitionExpression = partitionInfo.getExpr();
        if (partitionExpression == null) {
            return Optional.empty();
        }
        List<Expr> arguments = ((FunctionCallExpr) partitionExpression).getParams().exprs();
        return Optional.of(((StringLiteral) arguments.get(1)).getStringValue());
    }

    /**
     * Whether these grouping expressions reach every column the view is partitioned on, each of them by itself
     * or equal to one that is -- through another column too, since an equality chain is one equality -- or
     * under the view's own partition transform. A tracked column that is neither grouped nor tied to a grouped
     * one by the query varies freely within a group.
     *
     * <p>The transform counts only while the view's partition holds the whole transform bucket: over a column
     * whose compensation shares a retained bucket it forms a group out of both branches, so it covers nothing.
     * The columns the query holds equal to such a column share the group's value, so a transform over any of
     * them is no better.
     */
    private static boolean groupingCoversPartitionColumns(List<Expression> groupingExpressions, Plan queryPlan,
            List<BaseColInfo> pctInfos, Map<BaseColInfo, Set<BaseColInfo>> equalColumns,
            Optional<String> partitionTimeUnit, Set<BaseColInfo> columnsSharingRetainedBucket) {
        List<? extends Expression> groupingShuttledExpressions =
                ExpressionUtils.shuttleExpressionWithLineage(groupingExpressions, queryPlan);
        Set<BaseColInfo> partitionColumns = Sets.newHashSet(pctInfos);
        Set<BaseColInfo> groupedByItself = Sets.newHashSet();
        Set<BaseColInfo> groupedByTransform = Sets.newHashSet();
        for (Expression expression : groupingShuttledExpressions) {
            Optional<BaseColInfo> grouped = columnGroupedByItself(expression);
            if (grouped.isPresent()) {
                if (partitionColumns.contains(grouped.get())) {
                    groupedByItself.add(grouped.get());
                }
                continue;
            }
            Optional<BaseColInfo> transformed = columnUnderPartitionTransform(expression, partitionTimeUnit);
            if (transformed.isPresent() && partitionColumns.contains(transformed.get())) {
                groupedByTransform.add(transformed.get());
            }
        }
        Set<BaseColInfo> groupedTogether = equalColumnsClosure(groupedByItself, equalColumns);
        for (BaseColInfo transformed : groupedByTransform) {
            if (groupedTogether.contains(transformed)) {
                continue;
            }
            Set<BaseColInfo> transformedClass = equalColumnsClosure(Sets.newHashSet(transformed), equalColumns);
            if (!Sets.intersection(transformedClass, columnsSharingRetainedBucket).isEmpty()) {
                continue;
            }
            groupedTogether.addAll(transformedClass);
        }
        return groupedTogether.containsAll(partitionColumns);
    }

    /** The columns reachable from these through the equalities the query holds, the seeds included. */
    private static Set<BaseColInfo> equalColumnsClosure(Set<BaseColInfo> seeds,
            Map<BaseColInfo, Set<BaseColInfo>> equalColumns) {
        Set<BaseColInfo> groupedTogether = Sets.newHashSet();
        Deque<BaseColInfo> unvisited = new ArrayDeque<>(seeds);
        while (!unvisited.isEmpty()) {
            BaseColInfo column = unvisited.poll();
            if (!groupedTogether.add(column)) {
                continue;
            }
            for (BaseColInfo equal : equalColumns.getOrDefault(column, Collections.emptySet())) {
                if (!groupedTogether.contains(equal)) {
                    unvisited.add(equal);
                }
            }
        }
        return groupedTogether;
    }

    /**
     * The base column this expression groups by under the view's own partition transform, if it is that
     * transform itself: the same date_trunc, in the same unit, over a column of a table. A group it forms
     * lies within one of the view's partitions, which are disjoint in the transform's value, while a coarser
     * truncation rolls several of them up into a group both branches answer.
     */
    private static Optional<BaseColInfo> columnUnderPartitionTransform(Expression expression,
            Optional<String> partitionTimeUnit) {
        if (!partitionTimeUnit.isPresent() || !(expression instanceof DateTrunc)) {
            return Optional.empty();
        }
        DateTrunc dateTrunc = (DateTrunc) expression;
        int dateArgumentIndex = dateTrunc.getMonotonicFunctionChildIndex();
        // A timezone aware value is truncated in the session timezone, while the view's partition boundaries
        // are fixed in UTC, so the two can disagree on which of the view's buckets a row's group falls in.
        if (dateTrunc.getArgument(dateArgumentIndex).getDataType() instanceof TimeStampTzType) {
            return Optional.empty();
        }
        Expression timeUnit = dateTrunc.getArgument(1 - dateArgumentIndex);
        if (!(timeUnit instanceof StringLikeLiteral)
                || !((StringLikeLiteral) timeUnit).getStringValue()
                        .equalsIgnoreCase(partitionTimeUnit.get())) {
            return Optional.empty();
        }
        return columnGroupedByItself(dateTrunc.getArgument(dateArgumentIndex));
    }

    /**
     * The base column this expression is by itself: a slot of a column of a table, with nothing over it.
     */
    private static Optional<BaseColInfo> columnGroupedByItself(Expression expression) {
        if (!(expression instanceof SlotReference) || !((SlotReference) expression).isColumnFromTable()) {
            return Optional.empty();
        }
        SlotReference slotReference = (SlotReference) expression;
        String columnName = slotReference.getOriginalColumn().map(Column::getName).orElse(null);
        BaseTableInfo tableInfo = slotReference.getOriginalTable().map(BaseTableInfo::new).orElse(null);
        if (columnName == null || tableInfo == null) {
            return Optional.empty();
        }
        return Optional.of(new BaseColInfo(columnName, tableInfo));
    }

    /**
     * The base columns the query holds equal in every row of it: the equalities between two columns that its
     * inner joins and its filters put on. An outer join's on-predicate is not one of them, since a
     * null-supplied row does not satisfy it, and neither is an equality under a disjunction.
     */
    private static Map<BaseColInfo, Set<BaseColInfo>> columnsTheQueryHoldsEqual(Plan queryPlan) {
        List<Expression> guaranteed = new ArrayList<>();
        for (Plan plan : queryPlan.<Plan>collectToList(candidate ->
                candidate instanceof LogicalFilter<?> || isInnerJoin(candidate))) {
            if (plan instanceof LogicalFilter<?>) {
                guaranteed.addAll(((LogicalFilter<?>) plan).getConjuncts());
            } else {
                ((LogicalJoin<?, ?>) plan).getOnClauseCondition().ifPresent(guaranteed::add);
            }
        }
        Map<BaseColInfo, Set<BaseColInfo>> equalColumns = Maps.newHashMap();
        if (guaranteed.isEmpty()) {
            return equalColumns;
        }
        for (Expression expression : Predicates.splitPredicates(ExpressionUtils.and(guaranteed))
                .getEqualPredicateMap().keySet()) {
            if (!(expression instanceof EqualTo)) {
                continue;
            }
            EqualTo equalTo = (EqualTo) expression;
            Optional<BaseColInfo> left = columnGroupedByItself(equalTo.getArgument(0));
            Optional<BaseColInfo> right = columnGroupedByItself(equalTo.getArgument(1));
            if (!left.isPresent() || !right.isPresent()) {
                continue;
            }
            equalColumns.computeIfAbsent(left.get(), key -> Sets.newHashSet()).add(right.get());
            equalColumns.computeIfAbsent(right.get(), key -> Sets.newHashSet()).add(left.get());
        }
        return equalColumns;
    }

    private static boolean isInnerJoin(TreeNode<?> candidate) {
        return candidate instanceof LogicalJoin<?, ?>
                && ((LogicalJoin<?, ?>) candidate).getJoinType() == JoinType.INNER_JOIN;
    }

    /**
     * Check query and view aggregate compatibility
     */
    private static boolean checkCompatibility(
            StructInfo queryStructInfo,
            LogicalAggregate<Plan> queryAggregate, LogicalAggregate<Plan> viewAggregate,
            MaterializationContext materializationContext) {
        // if view is scalar aggregate but query is not. Or if query is scalar aggregate but view is not
        // Should not rewrite
        List<Expression> queryGroupByExpressions = queryAggregate.getGroupByExpressions();
        List<Expression> viewGroupByExpressions = viewAggregate.getGroupByExpressions();
        if (!queryGroupByExpressions.isEmpty() && viewGroupByExpressions.isEmpty()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "only one the of query or view is scalar aggregate and "
                            + "can not rewrite expression meanwhile",
                    () -> String.format("query aggregate = %s,\n view aggregate = %s,\n",
                            queryAggregate.treeString(),
                            viewAggregate.treeString()));
            return false;
        }
        boolean viewHasGroupSets = viewAggregate.getSourceRepeat()
                .map(repeat -> repeat.getGroupingSets().size()).orElse(0) > 0;
        // if both query and view has group sets, or query doesn't hava, mv have, not supported
        if (viewHasGroupSets) {
            materializationContext.recordFailReason(queryStructInfo,
                    "both query and view have group sets, or query doesn't have but view have, not supported",
                    () -> String.format("query aggregate = %s,\n view aggregate = %s,\n",
                            queryAggregate.treeString(),
                            viewAggregate.treeString()));
            return false;
        }
        return true;
    }

    @Override
    protected boolean aggregateOutputIsRecomputed(StructInfo queryStructInfo, StructInfo viewStructInfo) {
        // A predicate the view enforces over its own aggregate output answers the query only while the query
        // reads the value the view stored. isGroupByEquals keeps the aggregate for a query without grouping
        // over a view which groups by something, and that roll up aggregates the stored values, of no row at
        // all when the filter matches none. The roll ups of the other shapes are the ones upstream has.
        Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair = splitToTopPlanAndAggregate(queryStructInfo);
        Pair<Plan, LogicalAggregate<Plan>> viewTopPlanAndAggPair = splitToTopPlanAndAggregate(viewStructInfo);
        if (queryTopPlanAndAggPair == null || viewTopPlanAndAggPair == null) {
            // Without an aggregate on both sides, the view has no aggregate output of its own to have
            // enforced a predicate over, so there is no recomputed value to protect.
            return false;
        }
        return queryTopPlanAndAggPair.value().getGroupByExpressions().isEmpty()
                && !viewTopPlanAndAggPair.value().getGroupByExpressions().isEmpty();
    }

    private boolean isGroupByEquals(Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair,
            Pair<Plan, LogicalAggregate<Plan>> viewTopPlanAndAggPair,
            SlotMapping viewToQuerySlotMapping,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            Plan tempRewrittenPlan,
            MaterializationContext materializationContext,
            CascadesContext cascadesContext) {

        if (materializationContext instanceof SyncMaterializationContext) {
            // For data correctness, should always add aggregate node if rewritten by sync materialized view
            return false;
        }
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        Plan viewTopPlan = viewTopPlanAndAggPair.key();
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        LogicalAggregate<Plan> viewAggregate = viewTopPlanAndAggPair.value();

        // If the query is a scalar aggregate but the view groups by extra dimensions, the dimensions
        // removed by the uniform function dependency below only guarantee at most one group, not that a
        // group exists. Eliminating the aggregate into a bare project would lose the single row a scalar
        // aggregate must always emit when the filter matches zero rows. Keep the aggregate by rejecting
        // this fast path and falling back to the roll up rewrite.
        if (queryAggregate.getGroupByExpressions().isEmpty()
                && !viewAggregate.getGroupByExpressions().isEmpty()) {
            return false;
        }

        Set<Expression> queryGroupByShuttledExpression = new HashSet<>(ExpressionUtils.shuttleExpressionWithLineage(
                queryAggregate.getGroupByExpressions(), queryTopPlan));

        // try to eliminate group by dimension by function dependency if group by expression is not in query
        Map<Expression, Expression> viewShuttledExpressionQueryBasedToGroupByExpressionMap = new HashMap<>();
        List<Expression> viewGroupByExpressions = viewAggregate.getGroupByExpressions();
        List<? extends Expression> viewGroupByShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                viewGroupByExpressions, viewTopPlan);

        for (int index = 0; index < viewGroupByExpressions.size(); index++) {
            Expression viewExpression = viewGroupByExpressions.get(index);
            Expression viewGroupExpressionQueryBased = ExpressionUtils.replace(
                    viewGroupByShuttledExpressions.get(index),
                    viewToQuerySlotMapping.toSlotReferenceMap());
            viewShuttledExpressionQueryBasedToGroupByExpressionMap.put(viewGroupExpressionQueryBased,
                    viewExpression);
        }
        if (queryGroupByShuttledExpression.equals(viewShuttledExpressionQueryBasedToGroupByExpressionMap.keySet())) {
            // return true, if equals directly
            return true;
        }
        // Check is equals by equal filter eliminate
        return isGroupByEqualsByFunctionDependency(
                (LogicalPlan) tempRewrittenPlan,
                queryGroupByShuttledExpression,
                viewShuttledExpressionQueryBasedToGroupByExpressionMap,
                materializationContext);
    }

    /**
     * Check group by is equals by uniform function dependency
     * For example query is:
     * select
     * a, b, c from t1
     * where a = 1 and d = 'xx'
     * group by a, b, c;
     * mv is :
     * select a, b, c, d
     * from t1
     * group by a, b, c, d;
     * After group by key eliminate, the query group by is b, c
     * but mv is group by a, b, c, d, the group by a and d of mv is more dimensions than the query
     * But in tempRewrittenPlan is as following:
     * select *
     * from mv
     * where a = 1 and d = 'xx'
     * We can get group by a and d is uniform by function dependency info,
     * so the group by expression between query and view is equals, should not aggregate roll up
     * */
    private static boolean isGroupByEqualsByFunctionDependency(
            LogicalPlan tempRewrittenPlan,
            Set<Expression> queryGroupShuttledExpression,
            Map<Expression, Expression> viewShuttledExprQueryBasedToViewGroupByExprMap,
            MaterializationContext materializationContext) {

        Map<Expression, Expression> viewShuttledExprToScanExprMapping =
                materializationContext.getShuttledExprToScanExprMapping().flattenMap().get(0);
        Set<Expression> viewShuttledExprQueryBasedSet = viewShuttledExprQueryBasedToViewGroupByExprMap.keySet();
        // view group by expr can not cover query group by expr
        if (!viewShuttledExprQueryBasedSet.containsAll(queryGroupShuttledExpression)) {
            return false;
        }
        Set<Expression> viewShouldRemovedExpressionSet = new HashSet<>();
        Set<Expression> viewScanShouldReservedExpressionSet = new HashSet<>();
        // calc the group by expr which is needed to roll up and should be uniform
        for (Map.Entry<Expression, Expression> expressionMappingEntry :
                viewShuttledExprQueryBasedToViewGroupByExprMap.entrySet()) {
            if (queryGroupShuttledExpression.contains(expressionMappingEntry.getKey())) {
                // the group expr which query has, do not require eliminate
                viewScanShouldReservedExpressionSet.add(
                        viewShuttledExprToScanExprMapping.get(expressionMappingEntry.getValue()));
            } else {
                // the view expression which is more than query's expression, should try to eliminate
                viewShouldRemovedExpressionSet.add(expressionMappingEntry.getValue());
            }
        }

        DataTrait dataTrait = tempRewrittenPlan.computeDataTrait();
        for (Expression viewShouldRemovedExpr : viewShouldRemovedExpressionSet) {
            Expression viewScanExpression = viewShuttledExprToScanExprMapping.get(viewShouldRemovedExpr);
            if (viewScanExpression == null) {
                return false;
            }
            if (!(viewScanExpression instanceof Slot)) {
                return false;
            }
            if (!dataTrait.isUniform((Slot) viewScanExpression)
                    && Sets.intersection(dataTrait.calEqualSet((Slot) viewScanExpression),
                    viewScanShouldReservedExpressionSet).isEmpty()) {
                // Such as query is l_orderkey#0, l_linenumber#1, o_custkey#17, l_partkey#2
                // view is ps_partkey#25, o_orderkey#16, l_orderkey#0, l_linenumber#1, o_custkey#17, l_partkey#2
                // If want to check the group by equality, ps_partkey#25, o_orderkey#16 should be uniform
                // or should be equal any of [ l_orderkey#0, l_linenumber#1, o_custkey#17, l_partkey#2]
                return false;
            }
        }
        return true;
    }

    /**
     * Roll up query aggregate function when query dimension num is less than mv dimension num,
     *
     * @param queryAggregateFunction query aggregate function to roll up.
     * @param queryAggregateFunctionShuttled query aggregate function shuttled by lineage.
     * @param mvExprToMvScanExprQueryBased mv def sql output expressions to mv result data output mapping.
     *         <p>
     *         Such as query is
     *         select max(a) + 1 from table group by b.
     *         mv is
     *         select max(a) from table group by a, b.
     *         the queryAggregateFunction is max(a), queryAggregateFunctionShuttled is max(a) + 1
     *         mvExprToMvScanExprQueryBased is { max(a) : MTMVScan(output#0) }
     */
    private static Function rollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Map<Expression, Expression> mvExprToMvScanExprQueryBased) {
        for (Map.Entry<Expression, Expression> expressionEntry : mvExprToMvScanExprQueryBased.entrySet()) {
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair = Pair.of(expressionEntry.getKey(),
                    expressionEntry.getValue());
            for (AggFunctionRollUpHandler rollUpHandler : ROLL_UP_HANDLERS) {
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

    protected Pair<Set<? extends Expression>, Set<? extends Expression>> topPlanSplitToGroupAndFunction(
            Pair<Plan, LogicalAggregate<Plan>> topPlanAndAggPair, StructInfo queryStructInfo) {
        LogicalAggregate<Plan> bottomQueryAggregate = topPlanAndAggPair.value();
        Set<Expression> groupByExpressionSet = new HashSet<>(bottomQueryAggregate.getGroupByExpressions());
        // when query is bitmap_count(bitmap_union), the plan is as following:
        // project(bitmap_count()#1)
        //    aggregate(bitmap_union()#2)
        // we should use exprId which query top plan used to decide the query top plan is use the
        // bottom agg function or not
        Set<ExprId> bottomAggregateFunctionExprIdSet = bottomQueryAggregate.getOutput().stream()
                .filter(expr -> !groupByExpressionSet.contains(expr))
                .map(NamedExpression::getExprId)
                .collect(Collectors.toSet());

        Plan queryTopPlan = topPlanAndAggPair.key();
        Set<Expression> topGroupByExpressions = new HashSet<>();
        Set<Expression> topFunctionExpressions = new HashSet<>();
        queryTopPlan.getOutput().forEach(expression -> {
            ExpressionLineageReplacer.ExpressionReplaceContext replaceContext =
                    new ExpressionLineageReplacer.ExpressionReplaceContext(ImmutableList.of(expression));
            queryTopPlan.accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
            if (Collections.disjoint(bottomAggregateFunctionExprIdSet, replaceContext.getUsedExprIdSet())) {
                topGroupByExpressions.add(expression);
            } else {
                // if query top plan expression use any aggregate function, then consider it is aggregate function
                topFunctionExpressions.add(expression);

            }
        });
        return Pair.of(topGroupByExpressions, topFunctionExpressions);
    }

    protected Pair<Plan, LogicalAggregate<Plan>> splitToTopPlanAndAggregate(StructInfo structInfo) {
        Plan topPlan = structInfo.getTopPlan();
        PlanSplitContext splitContext = new PlanSplitContext(Sets.newHashSet(LogicalAggregate.class));
        topPlan.accept(StructInfo.PLAN_SPLITTER, splitContext);
        if (!(splitContext.getBottomPlan() instanceof LogicalAggregate)) {
            return null;
        } else {
            return Pair.of(topPlan, (LogicalAggregate<Plan>) splitContext.getBottomPlan());
        }
    }

    /**
     * Check Aggregate is simple or not and check join is whether valid or not.
     * Support project, filter, join, logical relation node and join condition should only contain
     * slot reference equals currently.
     */
    @Override
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        PlanCheckContext checkContext = PlanCheckContext.of(SUPPORTED_JOIN_TYPE_SET);
        // if query or mv contains more than one top aggregate, should fail
        return structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext)
                && checkContext.isContainsTopAggregate() && checkContext.getTopAggregateNum() <= 1
                && !checkContext.isContainsTopLimit() && !checkContext.isContainsTopTopN()
                && !checkContext.isContainsTopWindow();
    }

    /**
     * Aggregate expression rewriter which is responsible for rewriting group by and
     * aggregate function expression
     */
    protected static class AggregateExpressionRewriter
            extends DefaultExpressionRewriter<AggregateExpressionRewriteContext> {

        @Override
        public Expression visitAggregateFunction(AggregateFunction aggregateFunction,
                AggregateExpressionRewriteContext rewriteContext) {
            if (!rewriteContext.isValid()) {
                return aggregateFunction;
            }
            if (ExpressionRewriteMode.EXPRESSION_DIRECT.equals(rewriteContext.getExpressionRewriteMode())) {
                rewriteContext.setValid(false);
                return aggregateFunction;
            }
            Function rewrittenFunction;
            if (ExpressionRewriteMode.EXPRESSION_ROLL_UP.equals(rewriteContext.getExpressionRewriteMode())) {
                Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                        aggregateFunction,
                        rewriteContext.getQueryTopPlan());
                rewrittenFunction = rollup(aggregateFunction, queryFunctionShuttled,
                        rewriteContext.getMvExprToMvScanExprQueryBasedMapping());
                if (rewrittenFunction == null) {
                    rewriteContext.setValid(false);
                    return aggregateFunction;
                }
                return rewrittenFunction;
            }
            if (ExpressionRewriteMode.EXPRESSION_DIRECT_ALL.equals(rewriteContext.getExpressionRewriteMode())) {
                List<Expression> children = aggregateFunction.children();
                List<Expression> rewrittenChildren = new ArrayList<>();
                for (Expression child : children) {
                    Expression rewrittenExpression = child.accept(this, rewriteContext);
                    if (!rewriteContext.isValid()) {
                        return aggregateFunction;
                    }
                    rewrittenChildren.add(rewrittenExpression);
                }
                return aggregateFunction.withChildren(rewrittenChildren);
            }
            rewriteContext.setValid(false);
            return aggregateFunction;
        }

        @Override
        public Expression visitSlot(Slot slot, AggregateExpressionRewriteContext rewriteContext) {
            if (!rewriteContext.isValid()) {
                return slot;
            }
            if (rewriteContext.getGroupingId().isPresent() && slot.equals(rewriteContext.getGroupingId().get())) {
                return slot;
            }
            if (rewriteContext.getMvExprToMvScanExprQueryBasedMapping().containsKey(slot)) {
                return rewriteContext.getMvExprToMvScanExprQueryBasedMapping().get(slot);
            }
            rewriteContext.setValid(false);
            return slot;
        }

        @Override
        public Expression visit(Expression expr, AggregateExpressionRewriteContext rewriteContext) {
            if (!rewriteContext.isValid()) {
                return expr;
            }
            // for group by expression try to get corresponding expression directly
            if ((ExpressionRewriteMode.EXPRESSION_DIRECT.equals(rewriteContext.getExpressionRewriteMode())
                    || ExpressionRewriteMode.EXPRESSION_DIRECT_ALL.equals(rewriteContext.getExpressionRewriteMode()))
                    && rewriteContext.getMvExprToMvScanExprQueryBasedMapping().containsKey(expr)) {
                return rewriteContext.getMvExprToMvScanExprQueryBasedMapping().get(expr);
            }
            List<Expression> newChildren = new ArrayList<>(expr.arity());
            boolean hasNewChildren = false;
            for (Expression child : expr.children()) {
                Expression newChild = child.accept(this, rewriteContext);
                if (!rewriteContext.isValid()) {
                    return expr;
                }
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            return hasNewChildren ? expr.withChildren(newChildren) : expr;
        }
    }

    /**
     * AggregateExpressionRewriteContext
     */
    public static class AggregateExpressionRewriteContext {
        private boolean valid = true;
        private final ExpressionRewriteMode expressionRewriteMode;
        private final Map<Expression, Expression> mvExprToMvScanExprQueryBasedMapping;
        private final Plan queryTopPlan;
        private final Optional<SlotReference> groupingId;

        public AggregateExpressionRewriteContext(ExpressionRewriteMode expressionRewriteMode,
                Map<Expression, Expression> mvExprToMvScanExprQueryBasedMapping, Plan queryTopPlan,
                                                 Optional<SlotReference> groupingId) {
            this.expressionRewriteMode = expressionRewriteMode;
            this.mvExprToMvScanExprQueryBasedMapping = mvExprToMvScanExprQueryBasedMapping;
            this.queryTopPlan = queryTopPlan;
            this.groupingId = groupingId;
        }

        public boolean isValid() {
            return valid;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public ExpressionRewriteMode getExpressionRewriteMode() {
            return expressionRewriteMode;
        }

        public Map<Expression, Expression> getMvExprToMvScanExprQueryBasedMapping() {
            return mvExprToMvScanExprQueryBasedMapping;
        }

        public Plan getQueryTopPlan() {
            return queryTopPlan;
        }

        public Optional<SlotReference> getGroupingId() {
            return groupingId;
        }

        /**
         * The expression rewrite mode, which decide how the expression in query is rewritten by mv
         */
        public enum ExpressionRewriteMode {
            /**
             * Try to use the expression in mv directly, and doesn't handle aggregate function
             */
            EXPRESSION_DIRECT,

            /**
             * Try to use the expression in mv directly, and try to rewrite the arguments in aggregate function except
             * the aggregate function
             */
            EXPRESSION_DIRECT_ALL,

            /**
             * Try to roll up aggregate function
             */
            EXPRESSION_ROLL_UP
        }
    }
}
