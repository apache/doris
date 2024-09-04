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
import org.apache.doris.nereids.jobs.executor.Rewriter;
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
import org.apache.doris.nereids.rules.rewrite.EliminateGroupByKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
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
                    queryStructInfo.getTableBitSet());
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
        return doRewriteQueryByView(queryStructInfo,
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
    protected LogicalAggregate<Plan> doRewriteQueryByView(
            StructInfo queryStructInfo,
            SlotMapping viewToQuerySlotMapping,
            Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext,
            ExpressionRewriteMode groupByMode,
            ExpressionRewriteMode aggregateFunctionMode) {

        // try to roll up.
        // split the query top plan expressions to group expressions and functions, if can not, bail out.
        Pair<Set<? extends Expression>, Set<? extends Expression>> queryGroupAndFunctionPair
                = topPlanSplitToGroupAndFunction(queryTopPlanAndAggPair, queryStructInfo);
        Set<? extends Expression> queryTopPlanGroupBySet = queryGroupAndFunctionPair.key();
        Set<? extends Expression> queryTopPlanFunctionSet = queryGroupAndFunctionPair.value();
        // try to rewrite, contains both roll up aggregate functions and aggregate group expression
        List<NamedExpression> finalOutputExpressions = new ArrayList<>();
        List<Expression> finalGroupExpressions = new ArrayList<>();
        // permute the mv expr mapping to query based
        Map<Expression, Expression> mvExprToMvScanExprQueryBased =
                materializationContext.getShuttledExprToScanExprMapping().keyPermute(viewToQuerySlotMapping)
                        .flattenMap().get(0);
        Plan queryTopPlan = queryStructInfo.getTopPlan();
        for (Expression topExpression : queryTopPlan.getOutput()) {
            if (queryTopPlanFunctionSet.contains(topExpression)) {
                // if agg function, try to roll up and rewrite
                Expression rollupedExpression = tryRewriteExpression(queryStructInfo, topExpression,
                        mvExprToMvScanExprQueryBased, aggregateFunctionMode, materializationContext,
                        "Query function roll up fail",
                        () -> String.format("queryExpression = %s,\n mvExprToMvScanExprQueryBased = %s",
                                topExpression, mvExprToMvScanExprQueryBased));
                if (rollupedExpression == null) {
                    return null;
                }
                finalOutputExpressions.add(new Alias(rollupedExpression));
            } else {
                // if group by dimension, try to rewrite
                Expression rewrittenGroupByExpression = tryRewriteExpression(queryStructInfo, topExpression,
                        mvExprToMvScanExprQueryBased, groupByMode, materializationContext,
                        "View dimensions doesn't not cover the query dimensions",
                        () -> String.format("mvExprToMvScanExprQueryBased is %s,\n queryExpression is %s",
                                mvExprToMvScanExprQueryBased, topExpression));
                if (rewrittenGroupByExpression == null) {
                    // group expr can not rewrite by view
                    return null;
                }
                NamedExpression groupByExpression = rewrittenGroupByExpression instanceof NamedExpression
                        ? (NamedExpression) rewrittenGroupByExpression : new Alias(rewrittenGroupByExpression);
                finalOutputExpressions.add(groupByExpression);
                finalGroupExpressions.add(groupByExpression);
            }
        }
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        List<Expression> queryGroupByExpressions = queryAggregate.getGroupByExpressions();
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
        if (queryAggregate.getSourceRepeat().isPresent()) {
            // construct group sets for repeat
            List<List<Expression>> rewrittenGroupSetsExpressions = new ArrayList<>();
            List<List<Expression>> groupingSets = queryAggregate.getSourceRepeat().get().getGroupingSets();
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
                    finalOutputExpressions, tempRewritedPlan);
            return NormalizeRepeat.doNormalize(repeat);
        }
        return new LogicalAggregate<>(finalGroupExpressions, finalOutputExpressions, tempRewritedPlan);
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
     * Try to rewrite query expression by view, contains both group by dimension and aggregate function
     */
    protected Expression tryRewriteExpression(StructInfo queryStructInfo, Expression queryExpression,
            Map<Expression, Expression> mvShuttledExprToMvScanExprQueryBased, ExpressionRewriteMode rewriteMode,
            MaterializationContext materializationContext, String summaryIfFail, Supplier<String> detailIfFail) {
        Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                queryExpression,
                queryStructInfo.getTopPlan(),
                queryStructInfo.getTableBitSet());
        AggregateExpressionRewriteContext expressionRewriteContext = new AggregateExpressionRewriteContext(
                rewriteMode, mvShuttledExprToMvScanExprQueryBased, queryStructInfo.getTopPlan(),
                queryStructInfo.getTableBitSet());
        Expression rewrittenExpression = queryFunctionShuttled.accept(AGGREGATE_EXPRESSION_REWRITER,
                expressionRewriteContext);
        if (!expressionRewriteContext.isValid()) {
            materializationContext.recordFailReason(queryStructInfo, summaryIfFail, detailIfFail);
            return null;
        }
        return rewrittenExpression;
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

        Set<Expression> queryGroupShuttledExpression = new HashSet<>(ExpressionUtils.shuttleExpressionWithLineage(
                queryAggregate.getGroupByExpressions(), queryTopPlan, queryStructInfo.getTableBitSet()));

        // try to eliminate group by dimension by function dependency if group by expression is not in query
        Map<Expression, Expression> viewShuttledExpressionQueryBasedToGroupByExpressionMap = new HashMap<>();
        Map<Expression, Expression> groupByExpressionToViewShuttledExpressionQueryBasedMap = new HashMap<>();
        List<Expression> viewGroupByExpressions = viewAggregate.getGroupByExpressions();
        List<? extends Expression> viewGroupByShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                viewGroupByExpressions, viewTopPlan, viewStructInfo.getTableBitSet());

        for (int index = 0; index < viewGroupByExpressions.size(); index++) {
            Expression viewExpression = viewGroupByExpressions.get(index);
            Expression viewGroupExpressionQueryBased = ExpressionUtils.replace(
                    viewGroupByShuttledExpressions.get(index),
                    viewToQuerySlotMapping.toSlotReferenceMap());
            viewShuttledExpressionQueryBasedToGroupByExpressionMap.put(viewGroupExpressionQueryBased,
                    viewExpression);
            groupByExpressionToViewShuttledExpressionQueryBasedMap.put(viewExpression,
                    viewGroupExpressionQueryBased
            );
        }
        if (queryGroupShuttledExpression.equals(viewShuttledExpressionQueryBasedToGroupByExpressionMap.keySet())) {
            // return true, if equals directly
            return true;
        }

        boolean isGroupByEquals = false;
        // check is equals by group by eliminate
        isGroupByEquals |= isGroupByEqualsAfterGroupByEliminate(queryGroupShuttledExpression,
                viewShuttledExpressionQueryBasedToGroupByExpressionMap,
                groupByExpressionToViewShuttledExpressionQueryBasedMap,
                viewAggregate,
                cascadesContext);
        // check is equals by equal filter eliminate
        Optional<LogicalFilter<Plan>> filterOptional = tempRewrittenPlan.collectFirst(LogicalFilter.class::isInstance);
        if (!filterOptional.isPresent()) {
            return isGroupByEquals;
        }
        isGroupByEquals |= isGroupByEqualsAfterEqualFilterEliminate(
                (LogicalPlan) tempRewrittenPlan,
                queryGroupShuttledExpression,
                viewShuttledExpressionQueryBasedToGroupByExpressionMap,
                materializationContext);
        return isGroupByEquals;
    }

    /**
     * Check group by is equals by equal filter eliminate
     * For example query is select a, b, c from t1 where a = 1 and d = 'xx' group by a, b, c;
     * mv is select a, b, c, d from t1 group by a, b, c, d;
     * the group by expression between query and view is equals after equal filter eliminate
     * should not aggregate roll up
     * */
    private static boolean isGroupByEqualsAfterEqualFilterEliminate(
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
        Set<Expression> viewShouldUniformExpressionSet = new HashSet<>();
        // calc the group by expr which is needed to roll up and should be uniform
        for (Map.Entry<Expression, Expression> expressionEntry :
                viewShuttledExprQueryBasedToViewGroupByExprMap.entrySet()) {
            if (queryGroupShuttledExpression.contains(expressionEntry.getKey())) {
                // the group expr which query has, do not require uniform
                continue;
            }
            viewShouldUniformExpressionSet.add(expressionEntry.getValue());
        }

        DataTrait dataTrait = tempRewrittenPlan.computeDataTrait();
        for (Expression shouldUniformExpr : viewShouldUniformExpressionSet) {
            Expression viewScanExpression = viewShuttledExprToScanExprMapping.get(shouldUniformExpr);
            if (viewScanExpression == null) {
                return false;
            }
            if (!(viewScanExpression instanceof Slot)) {
                return false;
            }
            if (!dataTrait.isUniform((Slot) viewScanExpression)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check group by is equal or not after group by eliminate by functional dependency
     * Such as query is select l_orderdate, l_supperkey, count(*) from table group by l_orderdate, l_supperkey;
     * materialized view is select l_orderdate, l_supperkey, l_partkey count(*) from table
     * group by l_orderdate, l_supperkey, l_partkey;
     * Would check the extra l_partkey is can be eliminated by functional dependency.
     * The process step and  data is as following:
     * group by expression is (l_orderdate#1, l_supperkey#2)
     * materialized view is group by expression is (l_orderdate#4, l_supperkey#5, l_partkey#6)
     * materialized view expression mapping is
     * {l_orderdate#4:l_orderdate#10, l_supperkey#5:l_supperkey#11, l_partkey#6:l_partkey#12}
     * 1. viewShuttledExpressionQueryBasedToGroupByExpressionMap
     * is {l_orderdate#1:l_orderdate#10,  l_supperkey#2:l_supperkey#11}
     * groupByExpressionToViewShuttledExpressionQueryBasedMap
     * is {l_orderdate#10:l_orderdate#1,  l_supperkey#11:l_supperkey#2:}
     * 2. construct projects query used by view group expressions
     * projects (l_orderdate#10, l_supperkey#11)
     * 3. try to eliminate materialized view group expression
     * projects (l_orderdate#10, l_supperkey#11)
     * viewAggregate
     * 4. check the viewAggregate group by expression is equals queryAggregate expression or not
     */
    private static boolean isGroupByEqualsAfterGroupByEliminate(Set<Expression> queryGroupShuttledExpression,
            Map<Expression, Expression> viewShuttledExpressionQueryBasedToGroupByExpressionMap,
            Map<Expression, Expression> groupByExpressionToViewShuttledExpressionQueryBasedMap,
            LogicalAggregate<Plan> viewAggregate,
            CascadesContext cascadesContext) {
        List<NamedExpression> projects = new ArrayList<>();
        // construct projects query used by view group expressions
        for (Expression expression : queryGroupShuttledExpression) {
            Expression chosenExpression = viewShuttledExpressionQueryBasedToGroupByExpressionMap.get(expression);
            if (chosenExpression == null) {
                return false;
            }
            projects.add(chosenExpression instanceof NamedExpression
                    ? (NamedExpression) chosenExpression : new Alias(chosenExpression));
        }
        LogicalProject<LogicalAggregate<Plan>> project = new LogicalProject<>(projects, viewAggregate);
        // try to eliminate view group by expression which is not in query group by expression
        Plan rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                childContext -> {
                    Rewriter.getCteChildrenRewriter(childContext,
                            ImmutableList.of(Rewriter.topDown(new EliminateGroupByKey()))).execute();
                    return childContext.getRewritePlan();
                }, project, project);

        Optional<LogicalAggregate<Plan>> aggreagateOptional =
                rewrittenPlan.collectFirst(LogicalAggregate.class::isInstance);
        if (!aggreagateOptional.isPresent()) {
            return false;
        }
        // check result after view group by eliminate by functional dependency
        List<Expression> viewEliminatedGroupByExpressions = aggreagateOptional.get().getGroupByExpressions();
        if (viewEliminatedGroupByExpressions.size() != queryGroupShuttledExpression.size()) {
            return false;
        }
        Set<Expression> viewGroupShuttledExpressionQueryBased = new HashSet<>();
        for (Expression viewExpression : aggreagateOptional.get().getGroupByExpressions()) {
            Expression viewExpressionQueryBased =
                    groupByExpressionToViewShuttledExpressionQueryBasedMap.get(viewExpression);
            if (viewExpressionQueryBased == null) {
                return false;
            }
            viewGroupShuttledExpressionQueryBased.add(viewExpressionQueryBased);
        }
        return queryGroupShuttledExpression.equals(viewGroupShuttledExpressionQueryBased);
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
                    new ExpressionLineageReplacer.ExpressionReplaceContext(ImmutableList.of(expression),
                            ImmutableSet.of(), ImmutableSet.of(), queryStructInfo.getTableBitSet());
            queryTopPlan.accept(ExpressionLineageReplacer.INSTANCE, replaceContext);
            if (!Sets.intersection(bottomAggregateFunctionExprIdSet,
                    replaceContext.getExprIdExpressionMap().keySet()).isEmpty()) {
                // if query top plan expression use any aggregate function, then consider it is aggregate function
                topFunctionExpressions.add(expression);
            } else {
                topGroupByExpressions.add(expression);
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
        // if query or mv contains more then one top aggregate, should fail
        return structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext)
                && checkContext.isContainsTopAggregate() && checkContext.getTopAggregateNum() <= 1;
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
                        rewriteContext.getQueryTopPlan(),
                        rewriteContext.getQueryTableBitSet());
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
        public Expression visitGroupingScalarFunction(GroupingScalarFunction groupingScalarFunction,
                AggregateExpressionRewriteContext context) {
            List<Expression> children = groupingScalarFunction.children();
            List<Expression> rewrittenChildren = new ArrayList<>();
            for (Expression child : children) {
                Expression rewrittenChild = child.accept(this, context);
                if (!context.isValid()) {
                    return groupingScalarFunction;
                }
                rewrittenChildren.add(rewrittenChild);
            }
            return groupingScalarFunction.withChildren(rewrittenChildren);
        }

        @Override
        public Expression visitSlot(Slot slot, AggregateExpressionRewriteContext rewriteContext) {
            if (!rewriteContext.isValid()) {
                return slot;
            }
            if (slot instanceof VirtualSlotReference) {
                Optional<GroupingScalarFunction> originExpression = ((VirtualSlotReference) slot).getOriginExpression();
                if (!originExpression.isPresent()) {
                    return Repeat.generateVirtualGroupingIdSlot();
                } else {
                    GroupingScalarFunction groupingScalarFunction = originExpression.get();
                    groupingScalarFunction =
                            (GroupingScalarFunction) groupingScalarFunction.accept(this, rewriteContext);
                    if (!rewriteContext.isValid()) {
                        return slot;
                    }
                    return Repeat.generateVirtualSlotByFunction(groupingScalarFunction);
                }
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
        private final BitSet queryTableBitSet;

        public AggregateExpressionRewriteContext(ExpressionRewriteMode expressionRewriteMode,
                Map<Expression, Expression> mvExprToMvScanExprQueryBasedMapping, Plan queryTopPlan,
                BitSet queryTableBitSet) {
            this.expressionRewriteMode = expressionRewriteMode;
            this.mvExprToMvScanExprQueryBasedMapping = mvExprToMvScanExprQueryBasedMapping;
            this.queryTopPlan = queryTopPlan;
            this.queryTableBitSet = queryTableBitSet;
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

        public BitSet getQueryTableBitSet() {
            return queryTableBitSet;
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
