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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.CouldRollUp;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewAggregateRule
 * This is responsible for common aggregate rewriting
 */
public abstract class AbstractMaterializedViewAggregateRule extends AbstractMaterializedViewRule {

    protected static final Multimap<Expression, Expression>
            AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP = ArrayListMultimap.create();
    protected final String currentClassName = this.getClass().getSimpleName();
    private final Logger logger = LogManager.getLogger(this.getClass());

    static {
        // support count distinct roll up
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(new ToBitmap(Any.INSTANCE)));
        // with cast
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))));

        // support bitmap_union_count roll up
        // field is already bitmap with only bitmap_union
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(Any.INSTANCE),
                new BitmapUnion(Any.INSTANCE));
        // with bitmap_union and to_bitmap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(new ToBitmap(Any.INSTANCE)),
                new BitmapUnion(new ToBitmap(Any.INSTANCE)));
        // with bitmap_union, to_bitmap and cast
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))),
                new BitmapUnion(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))));
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
            materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                    Pair.of("Split view to top plan and agg fail",
                            String.format("view plan = %s\n", viewStructInfo.getOriginalPlan().treeString())));
            return null;
        }
        Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair = splitToTopPlanAndAggregate(queryStructInfo);
        if (queryTopPlanAndAggPair == null) {
            materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                    Pair.of("Split query to top plan and agg fail",
                            String.format("query plan = %s\n", queryStructInfo.getOriginalPlan().treeString())));
            return null;
        }
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        SlotMapping viewToQurySlotMapping = queryToViewSlotMapping.inverse();
        if (isGroupByEquals(queryTopPlanAndAggPair, viewTopPlanAndAggPair, viewToQurySlotMapping)) {
            // if group by expression between query and view is equals, try to rewrite expression directly
            List<Expression> rewrittenQueryGroupExpr = rewriteExpression(queryTopPlan.getExpressions(),
                    queryTopPlan,
                    materializationContext.getMvExprToMvScanExprMapping(),
                    queryToViewSlotMapping,
                    true);
            if (rewrittenQueryGroupExpr.isEmpty()) {
                // if fails, record the reason and then try to roll up
                materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                        Pair.of("Can not rewrite expression when no roll up",
                                String.format("expressionToWrite = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                                + "queryToViewSlotMapping = %s",
                                        queryTopPlan.getExpressions(),
                                        materializationContext.getMvExprToMvScanExprMapping(),
                                        queryToViewSlotMapping)));
            } else {
                return new LogicalProject<>(
                        rewrittenQueryGroupExpr.stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                        tempRewritedPlan);
            }
        }
        // try to roll up.
        // split the query top plan expressions to group expressions and functions, if can not, bail out.
        Pair<Set<? extends Expression>, Set<? extends Expression>> queryGroupAndFunctionPair
                = topPlanSplitToGroupAndFunction(queryTopPlanAndAggPair);
        // this map will be used to rewrite expression
        Multimap<Expression, Expression> needRollupExprMap = HashMultimap.create();
        Multimap<Expression, Expression> groupRewrittenExprMap = HashMultimap.create();
        // permute the mv expr mapping to query based
        Map<Expression, Expression> mvExprToMvScanExprQueryBased =
                materializationContext.getMvExprToMvScanExprMapping().keyPermute(viewToQurySlotMapping)
                        .flattenMap().get(0);
        Set<? extends Expression> queryTopPlanFunctionSet = queryGroupAndFunctionPair.value();
        // try to rewrite, contains both roll up aggregate functions and aggregate group expression
        List<NamedExpression> finalAggregateExpressions = new ArrayList<>();
        List<Expression> finalGroupExpressions = new ArrayList<>();
        for (Expression topExpression : queryTopPlan.getExpressions()) {
            // if agg function, try to roll up and rewrite
            if (queryTopPlanFunctionSet.contains(topExpression)) {
                Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                        topExpression,
                        queryTopPlan);
                // try to roll up
                AggregateFunction queryFunction = (AggregateFunction) queryFunctionShuttled.firstMatch(
                        expr -> expr instanceof AggregateFunction);
                Function rollupAggregateFunction = rollup(queryFunction, queryFunctionShuttled,
                        mvExprToMvScanExprQueryBased);
                if (rollupAggregateFunction == null) {
                    materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                            Pair.of("Query function roll up fail",
                                    String.format("queryFunction = %s,\n queryFunctionShuttled = %s,\n"
                                                    + "mvExprToMvScanExprQueryBased = %s",
                                            queryFunction, queryFunctionShuttled, mvExprToMvScanExprQueryBased)));
                    return null;
                }
                // key is query need roll up expr, value is mv scan based roll up expr
                needRollupExprMap.put(queryFunctionShuttled, rollupAggregateFunction);
                // rewrite query function expression by mv expression
                ExpressionMapping needRollupExprMapping = new ExpressionMapping(needRollupExprMap);
                Expression rewrittenFunctionExpression = rewriteExpression(topExpression,
                        queryTopPlan,
                        needRollupExprMapping,
                        queryToViewSlotMapping,
                        false);
                if (rewrittenFunctionExpression == null) {
                    materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                            Pair.of("Roll up expression can not rewrite by view", String.format(
                                    "topExpression = %s,\n needRollupExprMapping = %s,\n queryToViewSlotMapping = %s",
                                    topExpression, needRollupExprMapping, queryToViewSlotMapping)));
                    return null;
                }
                finalAggregateExpressions.add((NamedExpression) rewrittenFunctionExpression);
            } else {
                // if group by expression, try to rewrite group by expression
                Expression queryGroupShuttledExpr =
                        ExpressionUtils.shuttleExpressionWithLineage(topExpression, queryTopPlan);
                if (!mvExprToMvScanExprQueryBased.containsKey(queryGroupShuttledExpr)) {
                    // group expr can not rewrite by view
                    materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                            Pair.of("View dimensions doesn't not cover the query dimensions",
                                    String.format("mvExprToMvScanExprQueryBased is %s,\n queryGroupShuttledExpr is %s",
                                            mvExprToMvScanExprQueryBased, queryGroupShuttledExpr)));
                    return null;
                }
                groupRewrittenExprMap.put(queryGroupShuttledExpr,
                        mvExprToMvScanExprQueryBased.get(queryGroupShuttledExpr));
                // rewrite query group expression by mv expression
                ExpressionMapping groupRewrittenExprMapping = new ExpressionMapping(groupRewrittenExprMap);
                Expression rewrittenGroupExpression = rewriteExpression(
                        topExpression,
                        queryTopPlan,
                        groupRewrittenExprMapping,
                        queryToViewSlotMapping,
                        true);
                if (rewrittenGroupExpression == null) {
                    materializationContext.recordFailReason(queryStructInfo.getOriginalPlanId(),
                            Pair.of("Query dimensions can not be rewritten by view",
                                    String.format("topExpression is %s,\n groupRewrittenExprMapping is %s,\n"
                                                    + "queryToViewSlotMapping = %s",
                                            topExpression, groupRewrittenExprMapping, queryToViewSlotMapping)));
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
        return new LogicalAggregate(finalGroupExpressions, finalAggregateExpressions, mvProject);
    }

    private boolean isGroupByEquals(Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair,
            Pair<Plan, LogicalAggregate<Plan>> viewTopPlanAndAggPair,
            SlotMapping viewToQurySlotMapping) {
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        Plan viewTopPlan = viewTopPlanAndAggPair.key();
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        LogicalAggregate<Plan> viewAggregate = viewTopPlanAndAggPair.value();
        List<? extends Expression> queryGroupShuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                queryAggregate.getGroupByExpressions(), queryTopPlan);
        List<? extends Expression> viewGroupShuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                        viewAggregate.getGroupByExpressions(), viewTopPlan)
                .stream()
                .map(expr -> ExpressionUtils.replace(expr, viewToQurySlotMapping.toSlotReferenceMap()))
                .collect(Collectors.toList());
        return queryAggregate.getGroupByExpressions().size() == viewAggregate.getGroupByExpressions().size()
                && queryGroupShuttledExpression.equals(viewGroupShuttledExpression);
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
    private Function rollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Map<Expression, Expression> mvExprToMvScanExprQueryBased) {
        if (!(queryAggregateFunction instanceof CouldRollUp)) {
            return null;
        }
        Expression rollupParam = null;
        Expression viewRollupFunction = null;
        if (mvExprToMvScanExprQueryBased.containsKey(queryAggregateFunctionShuttled)) {
            // function can rewrite by view
            rollupParam = mvExprToMvScanExprQueryBased.get(queryAggregateFunctionShuttled);
            viewRollupFunction = queryAggregateFunctionShuttled;
        } else {
            // function can not rewrite by view, try to use complex roll up param
            // eg: query is count(distinct param), mv sql is bitmap_union(to_bitmap(param))
            for (Expression mvExprShuttled : mvExprToMvScanExprQueryBased.keySet()) {
                if (!(mvExprShuttled instanceof Function)) {
                    continue;
                }
                if (isAggregateFunctionEquivalent(queryAggregateFunction, queryAggregateFunctionShuttled,
                        (Function) mvExprShuttled)) {
                    rollupParam = mvExprToMvScanExprQueryBased.get(mvExprShuttled);
                    viewRollupFunction = mvExprShuttled;
                }
            }
        }
        if (rollupParam == null || !canRollup(viewRollupFunction)) {
            return null;
        }
        // do roll up
        return ((CouldRollUp) queryAggregateFunction).constructRollUp(rollupParam);
    }

    // Check the aggregate function can roll up or not, return true if could roll up
    // if view aggregate function is distinct or is in the un supported rollup functions, it doesn't support
    // roll up.
    private boolean canRollup(Expression rollupExpression) {
        if (rollupExpression == null) {
            return false;
        }
        if (rollupExpression instanceof Function && !(rollupExpression instanceof AggregateFunction)) {
            return false;
        }
        if (rollupExpression instanceof AggregateFunction) {
            AggregateFunction aggregateFunction = (AggregateFunction) rollupExpression;
            return !aggregateFunction.isDistinct() && aggregateFunction instanceof CouldRollUp;
        }
        return true;
    }

    private Pair<Set<? extends Expression>, Set<? extends Expression>> topPlanSplitToGroupAndFunction(
            Pair<Plan, LogicalAggregate<Plan>> topPlanAndAggPair) {
        LogicalAggregate<Plan> queryAggregate = topPlanAndAggPair.value();
        Set<Expression> queryAggGroupSet = new HashSet<>(queryAggregate.getGroupByExpressions());
        // when query is bitmap_count(bitmap_union), the plan is as following:
        // project(bitmap_count()#1)
        //    aggregate(bitmap_union()#2)
        // we should use exprId which query top plan used to decide the query top plan is use the
        // bottom agg function or not
        Set<ExprId> queryAggFunctionSet = queryAggregate.getOutputExpressions().stream()
                .filter(expr -> !queryAggGroupSet.contains(expr))
                .map(NamedExpression::getExprId)
                .collect(Collectors.toSet());

        Plan queryTopPlan = topPlanAndAggPair.key();
        Set<Expression> topGroupByExpressions = new HashSet<>();
        Set<Expression> topFunctionExpressions = new HashSet<>();
        queryTopPlan.getExpressions().forEach(expression -> {
            if (expression.anyMatch(expr -> expr instanceof NamedExpression
                    && queryAggFunctionSet.contains(((NamedExpression) expr).getExprId()))) {
                topFunctionExpressions.add(expression);
            } else {
                topGroupByExpressions.add(expression);
            }
        });
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

    /**
     * Check the queryFunction is equivalent to view function when function roll up.
     * Not only check the function name but also check the argument between query and view aggregate function.
     * Such as query is
     * select count(distinct a) + 1 from table group by b.
     * mv is
     * select bitmap_union(to_bitmap(a)) from table group by a, b.
     * the queryAggregateFunction is count(distinct a), queryAggregateFunctionShuttled is count(distinct a) + 1
     * mvExprToMvScanExprQueryBased is { bitmap_union(to_bitmap(a)) : MTMVScan(output#0) }
     * This will check the count(distinct a) in query is equivalent to  bitmap_union(to_bitmap(a)) in mv,
     * and then check their arguments is equivalent.
     */
    private boolean isAggregateFunctionEquivalent(Function queryFunction, Expression queryFunctionShuttled,
            Function viewFunction) {
        if (queryFunction.equals(viewFunction)) {
            return true;
        }
        // check the argument of rollup function is equivalent to view function or not
        for (Map.Entry<Expression, Collection<Expression>> equivalentFunctionEntry :
                AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.asMap().entrySet()) {
            if (equivalentFunctionEntry.getKey().equals(queryFunction)) {
                // check is have equivalent function or not
                for (Expression equivalentFunction : equivalentFunctionEntry.getValue()) {
                    if (!Any.equals(equivalentFunction, viewFunction)) {
                        continue;
                    }
                    // check param in query function is same as the view function
                    List<Expression> viewFunctionArguments = extractArguments(equivalentFunction, viewFunction);
                    List<Expression> queryFunctionArguments =
                            extractArguments(equivalentFunctionEntry.getKey(), queryFunction);
                    // check argument size,we only support roll up function which has only one argument currently
                    if (queryFunctionArguments.size() != 1 || viewFunctionArguments.size() != 1) {
                        continue;
                    }
                    if (Objects.equals(queryFunctionArguments.get(0), viewFunctionArguments.get(0))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Extract the function arguments by functionWithAny pattern
     * Such as functionWithAny def is bitmap_union(to_bitmap(Any.INSTANCE)),
     * actualFunction is bitmap_union(to_bitmap(case when a = 5 then 1 else 2 end))
     * after extracting, the return argument is: case when a = 5 then 1 else 2 end
     */
    private List<Expression> extractArguments(Expression functionWithAny, Function actualFunction) {
        Set<Object> exprSetToRemove = functionWithAny.collectToSet(expr -> !(expr instanceof Any));
        return actualFunction.collectFirst(expr ->
                exprSetToRemove.stream().noneMatch(exprToRemove -> exprToRemove.equals(expr)));
    }
}
