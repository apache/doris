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
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PlanSplitContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Alias;
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
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllCardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
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

    protected static final Multimap<Function, Expression>
            AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP = ArrayListMultimap.create();
    protected static final AggregateExpressionRewriter AGGREGATE_EXPRESSION_REWRITER =
            new AggregateExpressionRewriter();

    static {
        // support roll up when count distinct is in query
        // the column type is not bitMap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(new ToBitmap(Any.INSTANCE)));
        // with bitmap_union, to_bitmap and cast
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))));

        // support roll up when bitmap_union_count is in query
        // the column type is bitMap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(Any.INSTANCE),
                new BitmapUnion(Any.INSTANCE));
        // the column type is not bitMap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(new ToBitmap(Any.INSTANCE)),
                new BitmapUnion(new ToBitmap(Any.INSTANCE)));
        // with bitmap_union, to_bitmap and cast
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))),
                new BitmapUnion(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))));

        // support roll up when the column type is not hll
        // query is approx_count_distinct
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Ndv(Any.INSTANCE),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Ndv(Any.INSTANCE),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // query is HLL_UNION_AGG
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnionAgg(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnionAgg(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnionAgg(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnionAgg(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // query is HLL_CARDINALITY
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllCardinality(new HllUnion(new HllHash(Any.INSTANCE))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllCardinality(new HllUnion(new HllHash(Any.INSTANCE))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllCardinality(new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT)))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllCardinality(new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT)))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // query is HLL_RAW_AGG or HLL_UNION
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnion(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnion(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // support roll up when the column type is hll
        // query is HLL_UNION_AGG
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnionAgg(Any.INSTANCE),
                new HllUnion(Any.INSTANCE));

        // query is HLL_CARDINALITY
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllCardinality(new HllUnion(Any.INSTANCE)),
                new HllUnion(Any.INSTANCE));

        // query is HLL_RAW_AGG or HLL_UNION
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnion(Any.INSTANCE),
                new HllUnion(Any.INSTANCE));

    }

    @Override
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            Plan tempRewritedPlan,
            MaterializationContext materializationContext) {
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
        // Firstly,if group by expression between query and view is equals, try to rewrite expression directly
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        if (isGroupByEquals(queryTopPlanAndAggPair, viewTopPlanAndAggPair, viewToQuerySlotMapping, queryStructInfo,
                viewStructInfo)) {
            List<Expression> rewrittenQueryExpressions = rewriteExpression(queryTopPlan.getOutput(),
                    queryTopPlan,
                    materializationContext.getMvExprToMvScanExprMapping(),
                    viewToQuerySlotMapping,
                    true,
                    queryStructInfo.getTableBitSet());
            if (!rewrittenQueryExpressions.isEmpty()) {
                List<NamedExpression> projects = new ArrayList<>();
                for (Expression expression : rewrittenQueryExpressions) {
                    if (expression.containsType(AggregateFunction.class)) {
                        materializationContext.recordFailReason(queryStructInfo,
                                "rewritten expression contains aggregate functions when group equals aggregate rewrite",
                                () -> String.format("aggregate functions = %s\n", rewrittenQueryExpressions));
                        return null;
                    }
                    projects.add(expression instanceof NamedExpression
                            ? (NamedExpression) expression : new Alias(expression));
                }
                return new LogicalProject<>(projects, tempRewritedPlan);
            }
            // if fails, record the reason and then try to roll up aggregate function
            materializationContext.recordFailReason(queryStructInfo,
                    "Can not rewrite expression when no roll up",
                    () -> String.format("expressionToWrite = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                    + "viewToQuerySlotMapping = %s",
                            queryTopPlan.getOutput(),
                            materializationContext.getMvExprToMvScanExprMapping(),
                            viewToQuerySlotMapping));
        }
        // if view is scalar aggregate but query is not. Or if query is scalar aggregate but view is not
        // Should not rewrite
        List<Expression> queryGroupByExpressions = queryTopPlanAndAggPair.value().getGroupByExpressions();
        List<Expression> viewGroupByExpressions = viewTopPlanAndAggPair.value().getGroupByExpressions();
        if ((queryGroupByExpressions.isEmpty() && !viewGroupByExpressions.isEmpty())
                || (!queryGroupByExpressions.isEmpty() && viewGroupByExpressions.isEmpty())) {
            materializationContext.recordFailReason(queryStructInfo,
                    "only one the of query or view is scalar aggregate and "
                            + "can not rewrite expression meanwhile",
                    () -> String.format("query aggregate = %s,\n view aggregate = %s,\n",
                            queryTopPlanAndAggPair.value().treeString(),
                            viewTopPlanAndAggPair.value().treeString()));
            return null;
        }
        // try to roll up.
        // split the query top plan expressions to group expressions and functions, if can not, bail out.
        Pair<Set<? extends Expression>, Set<? extends Expression>> queryGroupAndFunctionPair
                = topPlanSplitToGroupAndFunction(queryTopPlanAndAggPair, queryStructInfo);
        Set<? extends Expression> queryTopPlanGroupBySet = queryGroupAndFunctionPair.key();
        Set<? extends Expression> queryTopPlanFunctionSet = queryGroupAndFunctionPair.value();
        // try to rewrite, contains both roll up aggregate functions and aggregate group expression
        List<NamedExpression> finalOutputExpressions = new ArrayList<>();
        List<Expression> finalGroupExpressions = new ArrayList<>();
        List<? extends Expression> queryExpressions = queryTopPlan.getOutput();
        // permute the mv expr mapping to query based
        Map<Expression, Expression> mvExprToMvScanExprQueryBased =
                materializationContext.getMvExprToMvScanExprMapping().keyPermute(viewToQuerySlotMapping)
                        .flattenMap().get(0);
        for (Expression topExpression : queryExpressions) {
            // if agg function, try to roll up and rewrite
            if (queryTopPlanFunctionSet.contains(topExpression)) {
                Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                        topExpression,
                        queryTopPlan,
                        queryStructInfo.getTableBitSet());
                AggregateExpressionRewriteContext context = new AggregateExpressionRewriteContext(
                        false, mvExprToMvScanExprQueryBased, queryTopPlan, queryStructInfo.getTableBitSet());
                // queryFunctionShuttled maybe sum(column) + count(*), so need to use expression rewriter
                Expression rollupedExpression = queryFunctionShuttled.accept(AGGREGATE_EXPRESSION_REWRITER,
                        context);
                if (!context.isValid()) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Query function roll up fail",
                            () -> String.format("queryFunctionShuttled = %s,\n mvExprToMvScanExprQueryBased = %s",
                                    queryFunctionShuttled, mvExprToMvScanExprQueryBased));
                    return null;
                }
                finalOutputExpressions.add(new Alias(rollupedExpression));
            } else {
                // if group by expression, try to rewrite group by expression
                Expression queryGroupShuttledExpr = ExpressionUtils.shuttleExpressionWithLineage(
                        topExpression, queryTopPlan, queryStructInfo.getTableBitSet());
                AggregateExpressionRewriteContext context = new AggregateExpressionRewriteContext(true,
                        mvExprToMvScanExprQueryBased, queryTopPlan, queryStructInfo.getTableBitSet());
                // group by expression maybe group by a + b, so we need expression rewriter
                Expression rewrittenGroupByExpression = queryGroupShuttledExpr.accept(AGGREGATE_EXPRESSION_REWRITER,
                        context);
                if (!context.isValid()) {
                    // group expr can not rewrite by view
                    materializationContext.recordFailReason(queryStructInfo,
                            "View dimensions doesn't not cover the query dimensions",
                            () -> String.format("mvExprToMvScanExprQueryBased is %s,\n queryGroupShuttledExpr is %s",
                                    mvExprToMvScanExprQueryBased, queryGroupShuttledExpr));
                    return null;
                }
                NamedExpression groupByExpression = rewrittenGroupByExpression instanceof NamedExpression
                        ? (NamedExpression) rewrittenGroupByExpression : new Alias(rewrittenGroupByExpression);
                finalOutputExpressions.add(groupByExpression);
                finalGroupExpressions.add(groupByExpression);
            }
        }
        // add project to guarantee group by column ref is slot reference,
        // this is necessary because physical createHash will need slotReference later
        if (queryGroupByExpressions.size() != queryTopPlanGroupBySet.size()) {
            for (Expression expression : queryGroupByExpressions) {
                if (queryTopPlanGroupBySet.contains(expression)) {
                    continue;
                }
                Expression queryGroupShuttledExpr = ExpressionUtils.shuttleExpressionWithLineage(
                        expression, queryTopPlan, queryStructInfo.getTableBitSet());
                AggregateExpressionRewriteContext context = new AggregateExpressionRewriteContext(true,
                        mvExprToMvScanExprQueryBased, queryTopPlan, queryStructInfo.getTableBitSet());
                // group by expression maybe group by a + b, so we need expression rewriter
                Expression rewrittenGroupByExpression = queryGroupShuttledExpr.accept(AGGREGATE_EXPRESSION_REWRITER,
                        context);
                if (!context.isValid()) {
                    // group expr can not rewrite by view
                    materializationContext.recordFailReason(queryStructInfo,
                            "View dimensions doesn't not cover the query dimensions in bottom agg ",
                            () -> String.format("mvExprToMvScanExprQueryBased is %s,\n queryGroupShuttledExpr is %s",
                                    mvExprToMvScanExprQueryBased, queryGroupShuttledExpr));
                    return null;
                }
                NamedExpression groupByExpression = rewrittenGroupByExpression instanceof NamedExpression
                        ? (NamedExpression) rewrittenGroupByExpression : new Alias(rewrittenGroupByExpression);
                finalGroupExpressions.add(groupByExpression);
            }
        }
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
        finalOutputExpressions = finalOutputExpressions.stream()
                .map(expr -> projectOutPutExprIdMap.containsKey(expr.getExprId())
                        ? projectOutPutExprIdMap.get(expr.getExprId()) : expr)
                .collect(Collectors.toList());
        return new LogicalAggregate(finalGroupExpressions, finalOutputExpressions, mvProject);
    }

    private boolean isGroupByEquals(Pair<Plan, LogicalAggregate<Plan>> queryTopPlanAndAggPair,
            Pair<Plan, LogicalAggregate<Plan>> viewTopPlanAndAggPair,
            SlotMapping viewToQuerySlotMapping,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo) {
        Plan queryTopPlan = queryTopPlanAndAggPair.key();
        Plan viewTopPlan = viewTopPlanAndAggPair.key();
        LogicalAggregate<Plan> queryAggregate = queryTopPlanAndAggPair.value();
        LogicalAggregate<Plan> viewAggregate = viewTopPlanAndAggPair.value();
        Set<? extends Expression> queryGroupShuttledExpression = new HashSet<>(
                ExpressionUtils.shuttleExpressionWithLineage(
                        queryAggregate.getGroupByExpressions(), queryTopPlan, queryStructInfo.getTableBitSet()));
        Set<? extends Expression> viewGroupShuttledExpressionQueryBased = ExpressionUtils.shuttleExpressionWithLineage(
                        viewAggregate.getGroupByExpressions(), viewTopPlan, viewStructInfo.getTableBitSet())
                .stream()
                .map(expr -> ExpressionUtils.replace(expr, viewToQuerySlotMapping.toSlotReferenceMap()))
                .collect(Collectors.toSet());
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
        if (!(queryAggregateFunction instanceof CouldRollUp)) {
            return null;
        }
        Expression rollupParam = null;
        Expression viewRollupFunction = null;
        // handle simple aggregate function roll up which is not in the AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP
        if (mvExprToMvScanExprQueryBased.containsKey(queryAggregateFunctionShuttled)
                && AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.keySet().stream()
                .noneMatch(aggFunction -> aggFunction.equals(queryAggregateFunction))) {
            rollupParam = mvExprToMvScanExprQueryBased.get(queryAggregateFunctionShuttled);
            viewRollupFunction = queryAggregateFunctionShuttled;
        } else {
            // handle complex functions roll up
            // eg: query is count(distinct param), mv sql is bitmap_union(to_bitmap(param))
            for (Expression mvExprShuttled : mvExprToMvScanExprQueryBased.keySet()) {
                if (!(mvExprShuttled instanceof Function)) {
                    continue;
                }
                if (isAggregateFunctionEquivalent(queryAggregateFunction, (Function) mvExprShuttled)) {
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
    private static boolean canRollup(Expression rollupExpression) {
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

    /**
     * Check Aggregate is simple or not and check join is whether valid or not.
     * Support project, filter, join, logical relation node and join condition should only contain
     * slot reference equals currently.
     */
    @Override
    protected boolean checkPattern(StructInfo structInfo) {
        PlanCheckContext checkContext = PlanCheckContext.of(SUPPORTED_JOIN_TYPE_SET);
        // if query or mv contains more then one top aggregate, should fail
        return structInfo.getTopPlan().accept(StructInfo.PLAN_PATTERN_CHECKER, checkContext)
                && checkContext.isContainsTopAggregate() && checkContext.getTopAggregateNum() <= 1;
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
    private static boolean isAggregateFunctionEquivalent(Function queryFunction, Function viewFunction) {
        if (queryFunction.equals(viewFunction)) {
            return true;
        }
        // check the argument of rollup function is equivalent to view function or not
        for (Map.Entry<Function, Collection<Expression>> equivalentFunctionEntry :
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
    private static List<Expression> extractArguments(Expression functionWithAny, Function actualFunction) {
        Set<Object> exprSetToRemove = functionWithAny.collectToSet(expr -> !(expr instanceof Any));
        return actualFunction.collectFirst(expr ->
                exprSetToRemove.stream().noneMatch(exprToRemove -> exprToRemove.equals(expr)));
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
            Expression queryFunctionShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                    aggregateFunction,
                    rewriteContext.getQueryTopPlan(),
                    rewriteContext.getQueryTableBitSet());
            Function rollupAggregateFunction = rollup(aggregateFunction, queryFunctionShuttled,
                    rewriteContext.getMvExprToMvScanExprQueryBasedMapping());
            if (rollupAggregateFunction == null) {
                rewriteContext.setValid(false);
                return aggregateFunction;
            }
            return rollupAggregateFunction;
        }

        @Override
        public Expression visitSlot(Slot slot, AggregateExpressionRewriteContext rewriteContext) {
            if (!rewriteContext.isValid()) {
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
            if (rewriteContext.isOnlyContainGroupByExpression()
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
    protected static class AggregateExpressionRewriteContext {
        private boolean valid = true;
        private final boolean onlyContainGroupByExpression;
        private final Map<Expression, Expression> mvExprToMvScanExprQueryBasedMapping;
        private final Plan queryTopPlan;
        private final BitSet queryTableBitSet;

        public AggregateExpressionRewriteContext(boolean onlyContainGroupByExpression,
                Map<Expression, Expression> mvExprToMvScanExprQueryBasedMapping, Plan queryTopPlan,
                BitSet queryTableBitSet) {
            this.onlyContainGroupByExpression = onlyContainGroupByExpression;
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

        public boolean isOnlyContainGroupByExpression() {
            return onlyContainGroupByExpression;
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
    }
}
