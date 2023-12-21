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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanSplitContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Any;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.CouldRollUp;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewAggregateRule
 * This is responsible for common aggregate rewriting
 */
public abstract class AbstractMaterializedViewAggregateRule extends AbstractMaterializedViewRule {

    protected static final Map<Expression, Expression>
            AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP = new HashMap<>();
    protected final String currentClassName = this.getClass().getSimpleName();

    private final Logger logger = LogManager.getLogger(this.getClass());

    static {
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(Any.INSTANCE));
    }

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping queryToViewSlotMapping,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext) {
        // get view and query aggregate and top plan correspondingly
        Pair<Plan, LogicalAggregate<Plan>> viewTopPlanAndAggPair = splitToTopPlanAndAggregate(viewStructInfo);
        if (viewTopPlanAndAggPair == null) {
            logger.warn(currentClassName + " split to view to top plan and agg fail so return null");
            return null;
        }
        Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair = splitToTopPlanAndAggregate(queryStructInfo);
        if (queryTopPlanAndAggPair == null) {
            logger.warn(currentClassName + " split to query to top plan and agg fail so return null");
            return null;
        }
        // Firstly, handle query group by expression rewrite
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        // query and view have the same dimension, try to rewrite rewrittenQueryGroupExpr
        LogicalAggregate<Plan> viewAggregate = viewTopPlanAndAggPair.value();
        Plan viewTopPlan = viewTopPlanAndAggPair.key();
        boolean needRollUp =
                queryAggregate.getGroupByExpressions().size() != viewAggregate.getGroupByExpressions().size();
        if (queryAggregate.getGroupByExpressions().size() == viewAggregate.getGroupByExpressions().size()) {
            List<? extends Expression> queryGroupShuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                    queryAggregate.getGroupByExpressions(), queryTopPlan);
            List<? extends Expression> viewGroupShuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                            viewAggregate.getGroupByExpressions(), viewTopPlan)
                    .stream()
                    .map(expr -> ExpressionUtils.replace(expr, queryToViewSlotMapping.inverse().toSlotReferenceMap()))
                    .collect(Collectors.toList());
            needRollUp = !queryGroupShuttledExpression.equals(viewGroupShuttledExpression);
        }
        if (!needRollUp) {
            List<Expression> rewrittenQueryGroupExpr = rewriteExpression(queryTopPlan.getExpressions(),
                    queryTopPlan,
                    materializationContext.getMvExprToMvScanExprMapping(),
                    queryToViewSlotMapping,
                    true);
            if (rewrittenQueryGroupExpr.isEmpty()) {
                // can not rewrite, bail out.
                logger.debug(currentClassName + " can not rewrite expression when not need roll up");
                return null;
            }
            return new LogicalProject<>(
                    rewrittenQueryGroupExpr.stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                    tempRewritedPlan);
        }
        // the dimension in query and view are different, try to roll up
        // Split query aggregate to group expression and agg function
        // Firstly, find the query top output rewrite function expr list which only use query aggregate function,
        // This will be used to roll up
        if (viewAggregate.getOutputExpressions().stream().anyMatch(
                viewExpr -> viewExpr.anyMatch(expr -> expr instanceof AggregateFunction
                        && ((AggregateFunction) expr).isDistinct()))) {
            // if mv aggregate function contains distinct, can not roll up, bail out.
            logger.debug(currentClassName + " view contains distinct function so can not roll up");
            return null;
        }
        // split the query top plan expressions to group expressions and functions, if can not, bail out.
        Pair<Set<? extends Expression>, Set<? extends Expression>> queryGroupAndFunctionPair
                = topPlanSplitToGroupAndFunction(queryTopPlanAndAggPair);
        if (queryGroupAndFunctionPair == null) {
            logger.warn(currentClassName + " query top plan split to group by and function fail so return null");
            return null;
        }
        // Secondly, try to roll up the agg functions
        // this map will be used to rewrite expression
        Multimap<Expression, Expression> needRollupExprMap = HashMultimap.create();
        Multimap<Expression, Expression> groupRewrittenExprMap = HashMultimap.create();
        Map<Expression, Expression> mvExprToMvScanExprQueryBased =
                materializationContext.getMvExprToMvScanExprMapping().keyPermute(
                        queryToViewSlotMapping.inverse()).flattenMap().get(0);

        Set<? extends Expression> queryTopPlanFunctionSet = queryGroupAndFunctionPair.value();
        // try to rewrite, contains both roll up aggregate functions and aggregate group expression
        List<NamedExpression> finalAggregateExpressions = new ArrayList<>();
        List<Expression> finalGroupExpressions = new ArrayList<>();
        for (Expression topExpression : queryTopPlan.getExpressions()) {
            // is agg function, try to roll up and rewrite
            if (queryTopPlanFunctionSet.contains(topExpression)) {
                Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                        topExpression,
                        queryTopPlan);
                // try to roll up
                AggregateFunction queryFunction = (AggregateFunction) topExpression.firstMatch(
                        expr -> expr instanceof AggregateFunction);
                Function rollupAggregateFunction = rollup(queryFunction, queryFunctionShuttled,
                        mvExprToMvScanExprQueryBased);
                if (rollupAggregateFunction == null) {
                    return null;
                }
                // key is query need roll up expr, value is mv scan based roll up expr
                needRollupExprMap.put(queryFunctionShuttled, rollupAggregateFunction);
                // rewrite query function expression by mv expression
                Expression rewrittenFunctionExpression = rewriteExpression(topExpression,
                        queryTopPlan,
                        new ExpressionMapping(needRollupExprMap),
                        queryToViewSlotMapping,
                        false);
                if (rewrittenFunctionExpression == null) {
                    logger.debug(currentClassName + " roll up expression can not rewrite by view so return null");
                    return null;
                }
                finalAggregateExpressions.add((NamedExpression) rewrittenFunctionExpression);
            } else {
                // try to rewrite group expression
                Expression queryGroupShuttledExpr =
                        ExpressionUtils.shuttleExpressionWithLineage(topExpression, queryTopPlan);
                if (!mvExprToMvScanExprQueryBased.containsKey(queryGroupShuttledExpr)) {
                    // group expr can not rewrite by view
                    logger.debug(currentClassName
                            + " view group expressions can not contains the query group by expression so return null");
                    return null;
                }
                groupRewrittenExprMap.put(queryGroupShuttledExpr,
                        mvExprToMvScanExprQueryBased.get(queryGroupShuttledExpr));
                // rewrite query group expression by mv expression
                Expression rewrittenGroupExpression = rewriteExpression(
                        topExpression,
                        queryTopPlan,
                        new ExpressionMapping(groupRewrittenExprMap),
                        queryToViewSlotMapping,
                        true);
                if (rewrittenGroupExpression == null) {
                    logger.debug(currentClassName
                            + " query top expression can not be rewritten by view so return null");
                    return null;
                }
                finalAggregateExpressions.add((NamedExpression) rewrittenGroupExpression);
                finalGroupExpressions.add(rewrittenGroupExpression);
            }
        }
        // add project to guarantee group by column ref is slot reference,
        // this is necessary because physical createHash will need slotReference later
        List<Expression> copiedFinalGroupExpressions = new ArrayList<>(finalGroupExpressions);
        List<NamedExpression> projectsUnderAggregate = copiedFinalGroupExpressions.stream()
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        projectsUnderAggregate.addAll(tempRewritedPlan.getOutput());
        LogicalProject<Plan> mvProject = new LogicalProject<>(projectsUnderAggregate, tempRewritedPlan);
        // add agg rewrite
        Map<ExprId, Slot> projectOutPutExprIdMap = mvProject.getOutput().stream()
                .distinct()
                .collect(Collectors.toMap(NamedExpression::getExprId, slot -> slot));
        // make the expressions to re reference project output
        finalGroupExpressions = finalGroupExpressions.stream()
                .map(expr -> {
                    ExprId exprId = ((NamedExpression) expr).getExprId();
                    if (projectOutPutExprIdMap.containsKey(exprId)) {
                        return projectOutPutExprIdMap.get(exprId);
                    }
                    return (NamedExpression) expr;
                })
                .collect(Collectors.toList());
        finalAggregateExpressions = finalAggregateExpressions.stream()
                .map(expr -> {
                    ExprId exprId = expr.getExprId();
                    if (projectOutPutExprIdMap.containsKey(exprId)) {
                        return projectOutPutExprIdMap.get(exprId);
                    }
                    return expr;
                })
                .collect(Collectors.toList());
        LogicalAggregate rewrittenAggregate = new LogicalAggregate(finalGroupExpressions,
                finalAggregateExpressions, mvProject);
        // record the group id in materializationContext, and when rewrite again in
        // the same group, bail out quickly.
        if (queryStructInfo.getOriginalPlan().getGroupExpression().isPresent()) {
            materializationContext.addMatchedGroup(
                    queryStructInfo.getOriginalPlan().getGroupExpression().get().getOwnerGroup().getGroupId());
        }
        return rewrittenAggregate;
    }

    // only support sum roll up, support other agg functions later.
    private Function rollup(AggregateFunction queryFunction,
            Expression queryFunctionShuttled,
            Map<Expression, Expression> mvExprToMvScanExprQueryBased) {
        if (!(queryFunction instanceof CouldRollUp)) {
            return null;
        }
        Expression rollupParam = null;
        if (mvExprToMvScanExprQueryBased.containsKey(queryFunctionShuttled)) {
            // function can rewrite by view
            rollupParam = mvExprToMvScanExprQueryBased.get(queryFunctionShuttled);
        } else {
            // function can not rewrite by view, try to use complex roll up param
            // eg: query is count(distinct param), mv sql is bitmap_union(to_bitmap(param))
            for (Expression mvExprShuttled : mvExprToMvScanExprQueryBased.keySet()) {
                if (!(mvExprShuttled instanceof Function)) {
                    continue;
                }
                if (isAggregateFunctionEquivalent(queryFunction, (Function) mvExprShuttled)) {
                    rollupParam = mvExprToMvScanExprQueryBased.get(mvExprShuttled);
                }
            }
        }
        if (rollupParam == null) {
            return null;
        }
        // do roll up
        return ((CouldRollUp) queryFunction).constructRollUp(rollupParam);
    }

    private Pair<Set<? extends Expression>, Set<? extends Expression>> topPlanSplitToGroupAndFunction(
            Pair<Plan, LogicalAggregate<Plan>> topPlanAndAggPair) {

        LogicalAggregate<Plan> queryAggregate = topPlanAndAggPair.value();
        Set<Expression> queryAggGroupSet = new HashSet<>(queryAggregate.getGroupByExpressions());
        Set<Expression> queryAggFunctionSet = queryAggregate.getOutputExpressions().stream()
                .filter(expr -> !queryAggGroupSet.contains(expr))
                .collect(Collectors.toSet());

        Plan queryTopPlan = topPlanAndAggPair.key();
        Set<Expression> topGroupByExpressions = new HashSet<>();
        Set<Expression> topFunctionExpressions = new HashSet<>();
        queryTopPlan.getExpressions().forEach(
                expression -> {
                    if (expression.anyMatch(expr -> expr instanceof NamedExpression
                            && queryAggFunctionSet.contains((NamedExpression) expr))) {
                        topFunctionExpressions.add(expression);
                    } else {
                        topGroupByExpressions.add(expression);
                    }
                });
        // only support to reference the aggregate function directly in top, will support expression later.
        if (topFunctionExpressions.stream().anyMatch(
                topAggFunc -> !(topAggFunc instanceof NamedExpression) && (!queryAggFunctionSet.contains(topAggFunc)
                        || !queryAggFunctionSet.contains(topAggFunc.child(0))))) {
            return null;
        }
        return Pair.of(topGroupByExpressions, topFunctionExpressions);
    }

    private Pair<Plan, LogicalAggregate<Plan>> splitToTopPlanAndAggregate(StructInfo structInfo) {
        Plan topPlan = structInfo.getTopPlan();
        PlanSplitContext splitContext = new PlanSplitContext(Sets.newHashSet(LogicalAggregate.class));
        topPlan.accept(StructInfo.PLAN_SPLITTER, splitContext);
        if (!(splitContext.getBottomPlan() instanceof LogicalAggregate)) {
            return null;
        } else {
            return Pair.of(topPlan, (LogicalAggregate<Plan>) splitContext.getBottomPlan());
        }
    }

    // Check Aggregate is simple or not and check join is whether valid or not.
    // Support join's input can not contain aggregate Only support project, filter, join, logical relation node and
    // join condition should be slot reference equals currently
    @Override
    protected boolean checkPattern(StructInfo structInfo) {

        Plan topPlan = structInfo.getTopPlan();
        Boolean valid = topPlan.accept(StructInfo.AGGREGATE_PATTERN_CHECKER, null);
        if (!valid) {
            return false;
        }
        HyperGraph hyperGraph = structInfo.getHyperGraph();
        for (AbstractNode node : hyperGraph.getNodes()) {
            StructInfoNode structInfoNode = (StructInfoNode) node;
            if (!structInfoNode.getPlan().accept(StructInfo.JOIN_PATTERN_CHECKER,
                    SUPPORTED_JOIN_TYPE_SET)) {
                return false;
            }
            for (JoinEdge edge : hyperGraph.getJoinEdges()) {
                if (!edge.getJoin().accept(StructInfo.JOIN_PATTERN_CHECKER, SUPPORTED_JOIN_TYPE_SET)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isAggregateFunctionEquivalent(Function queryFunction, Function viewFunction) {
        if (queryFunction.equals(viewFunction)) {
            return true;
        }
        // get query equivalent function
        Expression equivalentFunction = null;
        for (Map.Entry<Expression, Expression> entry : AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.entrySet()) {
            if (entry.getKey().equals(queryFunction)) {
                equivalentFunction = entry.getValue();
            }
        }
        // check is have equivalent function or not
        if (equivalentFunction == null) {
            return false;
        }
        // current compare
        return equivalentFunction.equals(viewFunction);
    }
}
