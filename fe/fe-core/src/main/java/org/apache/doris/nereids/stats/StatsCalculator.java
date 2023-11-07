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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.PartitionTopN;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.algebra.Union;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalJdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.StatisticRange;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to calculate the stats for each plan
 */
public class StatsCalculator extends DefaultPlanVisitor<Statistics, Void> {
    public static double DEFAULT_AGGREGATE_RATIO = 0.5;
    public static double DEFAULT_AGGREGATE_EXPAND_RATIO = 1.05;

    public static double AGGREGATE_COLUMN_CORRELATION_COEFFICIENT = 0.75;
    public static double DEFAULT_COLUMN_NDV_RATIO = 0.5;

    private static final Logger LOG = LogManager.getLogger(StatsCalculator.class);
    private final GroupExpression groupExpression;

    private boolean forbidUnknownColStats = false;

    private Map<String, ColumnStatistic> totalColumnStatisticMap = new HashMap<>();

    private boolean isPlayNereidsDump = false;

    private Map<String, Histogram> totalHistogramMap = new HashMap<>();

    private Map<CTEId, Statistics> cteIdToStats;

    private CascadesContext cascadesContext;

    private StatsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
                                Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        this.groupExpression = groupExpression;
        this.forbidUnknownColStats = forbidUnknownColStats;
        this.totalColumnStatisticMap = columnStatisticMap;
        this.isPlayNereidsDump = isPlayNereidsDump;
        this.cteIdToStats = Objects.requireNonNull(cteIdToStats, "CTEIdToStats can't be null");
        this.cascadesContext = context;
    }

    public Map<String, Histogram> getTotalHistogramMap() {
        return totalHistogramMap;
    }

    public void setTotalHistogramMap(Map<String, Histogram> totalHistogramMap) {
        this.totalHistogramMap = totalHistogramMap;
    }

    public Map<String, ColumnStatistic> getTotalColumnStatisticMap() {
        return totalColumnStatisticMap;
    }

    public void setTotalColumnStatisticMap(Map<String, ColumnStatistic> totalColumnStatisticMap) {
        this.totalColumnStatisticMap = totalColumnStatisticMap;
    }

    /**
     * estimate stats
     */
    public static StatsCalculator estimate(GroupExpression groupExpression, boolean forbidUnknownColStats,
                                           Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        StatsCalculator statsCalculator = new StatsCalculator(
                groupExpression, forbidUnknownColStats, columnStatisticMap, isPlayNereidsDump, cteIdToStats, context);
        statsCalculator.estimate();
        return statsCalculator;
    }

    public static StatsCalculator estimate(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump, CascadesContext context) {
        return StatsCalculator.estimate(groupExpression,
                forbidUnknownColStats,
                columnStatisticMap,
                isPlayNereidsDump,
                new HashMap<>(), context);
    }

    // For unit test only
    public static void estimate(GroupExpression groupExpression, CascadesContext context) {
        StatsCalculator statsCalculator = new StatsCalculator(groupExpression, false,
                new HashMap<>(), false, Collections.emptyMap(), context);
        statsCalculator.estimate();
    }

    private void estimate() {
        Plan plan = groupExpression.getPlan();
        Statistics stats = plan.accept(this, null);
        Statistics originStats = groupExpression.getOwnerGroup().getStatistics();
        /*
        in an ideal cost model, every group expression in a group are equivalent, but in fact the cost are different.
        we record the lowest expression cost as group cost to avoid missing this group.
        */
        if (originStats == null || originStats.getRowCount() > stats.getRowCount()) {
            groupExpression.getOwnerGroup().setStatistics(stats);
        } else {
            // the reason why we update col stats here.
            // consider join between 3 tables: A/B/C with join condition: A.id=B.id=C.id and a filter: C.id=1
            // in the final join result, the ndv of A.id/B.id/C.id should be 1
            // suppose we have 2 candidate plans
            // plan1: (A join B on A.id=B.id) join C on B.id=C.id
            // plan2:(B join C)join A
            // suppose plan1 is estimated before plan2
            //
            // after estimate the outer join of plan1 (join C), we update B.id.ndv=1, but A.id.ndv is not updated
            // then we estimate plan2. the stats of plan2 is denoted by stats2. obviously, stats2.A.id.ndv is 1
            // now we update OwnerGroup().getStatistics().A.id.ndv to 1
            if (originStats.getRowCount() > stats.getRowCount()) {
                stats.updateNdv(originStats);
                groupExpression.getOwnerGroup().setStatistics(stats);
            } else {
                originStats.updateNdv(stats);
            }
        }
        groupExpression.setEstOutputRowCount(stats.getRowCount());
        groupExpression.setStatDerived(true);
    }

    @Override
    public Statistics visitLogicalSink(LogicalSink<? extends Plan> logicalSink, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public Statistics visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, Void context) {
        return computeEmptyRelation(emptyRelation);
    }

    @Override
    public Statistics visitLogicalLimit(LogicalLimit<? extends Plan> limit, Void context) {
        return computeLimit(limit);
    }

    @Override
    public Statistics visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, Void context) {
        return computeLimit(limit);
    }

    @Override
    public Statistics visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Void context) {
        return computeOneRowRelation(oneRowRelation.getProjects());
    }

    @Override
    public Statistics visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        return computeAggregate(aggregate);
    }

    @Override
    public Statistics visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void context) {
        return computeRepeat(repeat);
    }

    @Override
    public Statistics visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        return computeFilter(filter);
    }

    @Override
    public Statistics visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        return computeCatalogRelation(olapScan);
    }

    @Override
    public Statistics visitLogicalDeferMaterializeOlapScan(LogicalDeferMaterializeOlapScan deferMaterializeOlapScan,
            Void context) {
        return computeCatalogRelation(deferMaterializeOlapScan.getLogicalOlapScan());
    }

    @Override
    public Statistics visitLogicalSchemaScan(LogicalSchemaScan schemaScan, Void context) {
        return computeCatalogRelation(schemaScan);
    }

    @Override
    public Statistics visitLogicalFileScan(LogicalFileScan fileScan, Void context) {
        fileScan.getExpressions();
        return computeCatalogRelation(fileScan);
    }

    @Override
    public Statistics visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, Void context) {
        return tvfRelation.getFunction().computeStats(tvfRelation.getOutput());
    }

    @Override
    public Statistics visitLogicalJdbcScan(LogicalJdbcScan jdbcScan, Void context) {
        jdbcScan.getExpressions();
        return computeCatalogRelation(jdbcScan);
    }

    @Override
    public Statistics visitLogicalEsScan(LogicalEsScan esScan, Void context) {
        esScan.getExpressions();
        return computeCatalogRelation(esScan);
    }

    @Override
    public Statistics visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public Statistics visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public Statistics visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN);
    }

    @Override
    public Statistics visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN.getLogicalTopN());
    }

    @Override
    public Statistics visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> partitionTopN, Void context) {
        return computePartitionTopN(partitionTopN);
    }

    @Override
    public Statistics visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        return JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join);
    }

    @Override
    public Statistics visitLogicalAssertNumRows(
            LogicalAssertNumRows<? extends Plan> assertNumRows, Void context) {
        return computeAssertNumRows(assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows());
    }

    @Override
    public Statistics visitLogicalUnion(
            LogicalUnion union, Void context) {
        return computeUnion(union);
    }

    @Override
    public Statistics visitLogicalExcept(
            LogicalExcept except, Void context) {
        return computeExcept(except);
    }

    @Override
    public Statistics visitLogicalIntersect(
            LogicalIntersect intersect, Void context) {
        return computeIntersect(intersect);
    }

    @Override
    public Statistics visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Void context) {
        return computeGenerate(generate);
    }

    public Statistics visitLogicalWindow(LogicalWindow<? extends Plan> window, Void context) {
        return computeWindow(window);
    }

    @Override
    public Statistics visitPhysicalSink(PhysicalSink<? extends Plan> physicalSink, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public Statistics visitPhysicalWindow(PhysicalWindow window, Void context) {
        return computeWindow(window);
    }

    @Override
    public Statistics visitPhysicalPartitionTopN(PhysicalPartitionTopN partitionTopN, Void context) {
        return computePartitionTopN(partitionTopN);
    }

    @Override
    public Statistics visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, Void context) {
        return computeEmptyRelation(emptyRelation);
    }

    @Override
    public Statistics visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, Void context) {
        return computeAggregate(agg);
    }

    @Override
    public Statistics visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, Void context) {
        return computeRepeat(repeat);
    }

    @Override
    public Statistics visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, Void context) {
        return computeOneRowRelation(oneRowRelation.getProjects());
    }

    @Override
    public Statistics visitPhysicalOlapScan(PhysicalOlapScan olapScan, Void context) {
        return computeCatalogRelation(olapScan);
    }

    @Override
    public Statistics visitPhysicalDeferMaterializeOlapScan(PhysicalDeferMaterializeOlapScan deferMaterializeOlapScan,
            Void context) {
        return computeCatalogRelation(deferMaterializeOlapScan.getPhysicalOlapScan());
    }

    @Override
    public Statistics visitPhysicalSchemaScan(PhysicalSchemaScan schemaScan, Void context) {
        return computeCatalogRelation(schemaScan);
    }

    @Override
    public Statistics visitPhysicalFileScan(PhysicalFileScan fileScan, Void context) {
        return computeCatalogRelation(fileScan);
    }

    @Override
    public Statistics visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, Void context) {
        return storageLayerAggregate.getRelation().accept(this, context);
    }

    @Override
    public Statistics visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, Void context) {
        return tvfRelation.getFunction().computeStats(tvfRelation.getOutput());
    }

    @Override
    public Statistics visitPhysicalJdbcScan(PhysicalJdbcScan jdbcScan, Void context) {
        return computeCatalogRelation(jdbcScan);
    }

    @Override
    public Statistics visitPhysicalEsScan(PhysicalEsScan esScan, Void context) {
        return computeCatalogRelation(esScan);
    }

    @Override
    public Statistics visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public Statistics visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, Void context) {
        return computeTopN(topN);
    }

    @Override
    public Statistics visitPhysicalDeferMaterializeTopN(PhysicalDeferMaterializeTopN<? extends Plan> topN,
            Void context) {
        return computeTopN(topN.getPhysicalTopN());
    }

    @Override
    public Statistics visitPhysicalHashJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, Void context) {
        return JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), hashJoin);
    }

    @Override
    public Statistics visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            Void context) {
        return JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), nestedLoopJoin);
    }

    // TODO: We should subtract those pruned column, and consider the expression transformations in the node.
    @Override
    public Statistics visitPhysicalProject(PhysicalProject<? extends Plan> project, Void context) {
        return computeProject(project);
    }

    @Override
    public Statistics visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, Void context) {
        return computeFilter(filter);
    }

    @Override
    public Statistics visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute,
            Void context) {
        return groupExpression.childStatistics(0);
    }

    @Override
    public Statistics visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows,
            Void context) {
        return computeAssertNumRows(assertNumRows.getAssertNumRowsElement().getDesiredNumOfRows());
    }

    @Override
    public Statistics visitPhysicalUnion(PhysicalUnion union, Void context) {
        return computeUnion(union);
    }

    @Override
    public Statistics visitPhysicalExcept(PhysicalExcept except, Void context) {
        return computeExcept(except);
    }

    @Override
    public Statistics visitPhysicalIntersect(PhysicalIntersect intersect, Void context) {
        return computeIntersect(intersect);
    }

    @Override
    public Statistics visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, Void context) {
        return computeGenerate(generate);
    }

    private Statistics computeAssertNumRows(long desiredNumOfRows) {
        Statistics statistics = groupExpression.childStatistics(0);
        statistics.withRowCount(Math.min(1, statistics.getRowCount()));
        return statistics;
    }

    private Statistics computeFilter(Filter filter) {
        Statistics stats = groupExpression.childStatistics(0);
        Plan plan = tryToFindChild(groupExpression);
        if (plan != null) {
            if (plan instanceof Aggregate) {
                Aggregate agg = ((Aggregate<?>) plan);
                List<NamedExpression> expressions = agg.getOutputExpressions();
                Set<Slot> slots = expressions
                        .stream()
                        .filter(Alias.class::isInstance)
                        .filter(s -> ((Alias) s).child().anyMatch(AggregateFunction.class::isInstance))
                        .map(NamedExpression::toSlot).collect(Collectors.toSet());
                Expression predicate = filter.getPredicate();
                if (predicate.anyMatch(s -> slots.contains(s))) {
                    return new FilterEstimation(slots).estimate(filter.getPredicate(), stats);
                }
            }
        }
        return new FilterEstimation().estimate(filter.getPredicate(), stats);
    }

    private ColumnStatistic getColumnStatistic(TableIf table, String colName) {
        if (totalColumnStatisticMap.get(table.getName() + colName) != null) {
            return totalColumnStatisticMap.get(table.getName() + colName);
        } else if (isPlayNereidsDump) {
            return ColumnStatistic.UNKNOWN;
        } else {
            long catalogId;
            long dbId;
            try {
                catalogId = table.getDatabase().getCatalog().getId();
                dbId = table.getDatabase().getId();
            } catch (Exception e) {
                // Use -1 for catalog id and db id when failed to get them from metadata.
                // This is OK because catalog id and db id is not in the hashcode function of ColumnStatistics cache
                // and the table id is globally unique.
                LOG.debug(String.format("Fail to get catalog id and db id for table %s", table.getName()));
                catalogId = -1;
                dbId = -1;
            }
            return Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
                catalogId, dbId, table.getId(), colName);
        }
    }

    private Histogram getColumnHistogram(TableIf table, String colName) {
        // if (totalHistogramMap.get(table.getName() + colName) != null) {
        //     return totalHistogramMap.get(table.getName() + colName);
        // } else if (isPlayNereidsDump) {
        //     return null;
        // } else {
        //     return Env.getCurrentEnv().getStatisticsCache().getHistogram(table.getId(), colName);
        // }
        return null;
    }

    // TODO: 1. Subtract the pruned partition
    //       2. Consider the influence of runtime filter
    //       3. Get NDV and column data size from StatisticManger, StatisticManager doesn't support it now.
    private Statistics computeCatalogRelation(CatalogRelation catalogRelation) {
        Set<SlotReference> slotSet = catalogRelation.getOutput().stream().filter(SlotReference.class::isInstance)
                .map(s -> (SlotReference) s).collect(Collectors.toSet());
        Map<Expression, ColumnStatistic> columnStatisticMap = new HashMap<>();
        TableIf table = catalogRelation.getTable();
        double rowCount = catalogRelation.getTable().estimatedRowCount();
        for (SlotReference slotReference : slotSet) {
            String colName = slotReference.getName();
            boolean shouldIgnoreThisCol = StatisticConstants.shouldIgnoreCol(table, slotReference.getColumn().get());

            if (colName == null) {
                throw new RuntimeException(String.format("Invalid slot: %s", slotReference.getExprId()));
            }
            ColumnStatistic cache;
            if (ConnectContext.get() == null || !ConnectContext.get().getSessionVariable().enableStats
                    || !FeConstants.enableInternalSchemaDb
                    || shouldIgnoreThisCol) {
                cache = ColumnStatistic.UNKNOWN;
            } else {
                cache = getColumnStatistic(table, colName);
            }
            if (cache.avgSizeByte <= 0) {
                cache = new ColumnStatisticBuilder(cache)
                        .setAvgSizeByte(slotReference.getColumn().get().getType().getSlotSize())
                        .build();
            }
            if (!cache.isUnKnown) {
                rowCount = Math.max(rowCount, cache.count);
                Histogram histogram = getColumnHistogram(table, colName);
                if (histogram != null) {
                    ColumnStatisticBuilder columnStatisticBuilder =
                            new ColumnStatisticBuilder(cache).setHistogram(histogram);
                    cache = columnStatisticBuilder.build();
                    if (ConnectContext.get().getSessionVariable().isEnableMinidump()
                            && !ConnectContext.get().getSessionVariable().isPlayNereidsDump()) {
                        totalColumnStatisticMap.put(table.getName() + ":" + colName, cache);
                        totalHistogramMap.put(table.getName() + colName, histogram);
                    }
                }
            }
            columnStatisticMap.put(slotReference, cache);
        }
        return new Statistics(rowCount, columnStatisticMap);
    }

    private Statistics computeTopN(TopN topN) {
        Statistics stats = groupExpression.childStatistics(0);
        return stats.withRowCount(Math.min(stats.getRowCount(), topN.getLimit()));
    }

    private Statistics computePartitionTopN(PartitionTopN partitionTopN) {
        Statistics childStats = groupExpression.childStatistics(0);
        double rowCount = childStats.getRowCount();
        List<Expression> partitionKeys = partitionTopN.getPartitionKeys();
        if (!partitionTopN.hasGlobalLimit() && !partitionKeys.isEmpty()) {
            // If there is no global limit. So result for the cardinality estimation is:
            // NDV(partition key) * partitionLimit
            List<ColumnStatistic> partitionByKeyStats = partitionKeys.stream()
                    .map(partitionKey -> {
                        ColumnStatistic partitionKeyStats = childStats.findColumnStatistics(partitionKey);
                        if (partitionKeyStats == null) {
                            partitionKeyStats = new ExpressionEstimation().visit(partitionKey, childStats);
                        }
                        return partitionKeyStats;
                    })
                    .filter(s -> !s.isUnKnown)
                    .collect(Collectors.toList());
            if (partitionByKeyStats.isEmpty()) {
                // all column stats are unknown, use default ratio
                rowCount = rowCount * DEFAULT_COLUMN_NDV_RATIO;
            } else {
                rowCount = Math.min(rowCount, partitionByKeyStats.stream().map(s -> s.ndv)
                        .max(Double::compare).get() * partitionTopN.getPartitionLimit());
            }
        } else {
            rowCount = Math.min(rowCount, partitionTopN.getPartitionLimit());
        }
        // TODO: for the filter push down window situation, we will prune the row count twice
        //  because we keep the pushed down filter. And it will be calculated twice, one of them in 'PartitionTopN'
        //  and the other is in 'Filter'. It's hard to dismiss.
        return childStats.withRowCountAndEnforceValid(rowCount);
    }

    private Statistics computeLimit(Limit limit) {
        Statistics stats = groupExpression.childStatistics(0);
        return stats.withRowCount(Math.min(stats.getRowCount(), limit.getLimit()));
    }

    private double estimateGroupByRowCount(List<Expression> groupByExpressions, Statistics childStats) {
        double rowCount = 1;
        Map<Expression, ColumnStatistic> groupByColStats = new HashMap<>();
        for (Expression groupByExpr : groupByExpressions) {
            ColumnStatistic colStats = childStats.findColumnStatistics(groupByExpr);
            if (colStats == null) {
                colStats = ExpressionEstimation.estimate(groupByExpr, childStats);
            }
            groupByColStats.put(groupByExpr, colStats);
        }
        int groupByCount = groupByExpressions.size();
        if (groupByColStats.values().stream().anyMatch(ColumnStatistic::isUnKnown)) {
            // if there is group-bys, output row count is childStats.getRowCount() * DEFAULT_AGGREGATE_RATIO,
            // o.w. output row count is 1
            // example: select sum(A) from T;
            if (groupByCount > 0) {
                rowCount = childStats.getRowCount() * DEFAULT_AGGREGATE_RATIO;
            } else {
                rowCount = 1;
            }
        } else {
            if (groupByCount > 0) {
                List<Double> groupByNdvs = groupByColStats.values().stream()
                        .map(colStats -> colStats.ndv)
                        .sorted(Comparator.reverseOrder()).collect(Collectors.toList());
                rowCount = groupByNdvs.get(0);
                for (int groupByIndex = 1; groupByIndex < groupByCount; ++groupByIndex) {
                    rowCount *= Math.max(1, groupByNdvs.get(groupByIndex) * Math.pow(
                            AGGREGATE_COLUMN_CORRELATION_COEFFICIENT, groupByIndex + 1D));
                    if (rowCount > childStats.getRowCount()) {
                        rowCount = childStats.getRowCount();
                        break;
                    }
                }
            }
        }
        rowCount = Math.max(1, rowCount);
        rowCount = Math.min(rowCount, childStats.getRowCount());
        return rowCount;
    }

    private Statistics computeAggregate(Aggregate<? extends Plan> aggregate) {
        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        Statistics childStats = groupExpression.childStatistics(0);
        double rowCount = estimateGroupByRowCount(groupByExpressions, childStats);
        Map<Expression, ColumnStatistic> slotToColumnStats = Maps.newHashMap();
        List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();
        // TODO: 1. Estimate the output unit size by the type of corresponding AggregateFunction
        //       2. Handle alias, literal in the output expression list
        double factor = childStats.getRowCount() / rowCount;
        for (NamedExpression outputExpression : outputExpressions) {
            ColumnStatistic columnStat = ExpressionEstimation.estimate(outputExpression, childStats);
            ColumnStatisticBuilder builder = new ColumnStatisticBuilder(columnStat);
            builder.setMinValue(columnStat.minValue / factor);
            builder.setMaxValue(columnStat.maxValue / factor);
            if (columnStat.ndv > rowCount) {
                builder.setNdv(rowCount);
            }
            builder.setDataSize(rowCount * outputExpression.getDataType().width());
            slotToColumnStats.put(outputExpression.toSlot(), columnStat);
        }
        return new Statistics(rowCount, slotToColumnStats);
    }

    private Statistics computeRepeat(Repeat<? extends Plan> repeat) {
        Statistics childStats = groupExpression.childStatistics(0);
        Map<Expression, ColumnStatistic> slotIdToColumnStats = childStats.columnStatistics();
        int groupingSetNum = repeat.getGroupingSets().size();
        double rowCount = childStats.getRowCount();
        Map<Expression, ColumnStatistic> columnStatisticMap = slotIdToColumnStats.entrySet()
                .stream().map(kv -> {
                    ColumnStatistic stats = kv.getValue();
                    ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder(stats);
                    columnStatisticBuilder
                            .setCount(stats.count < 0 ? stats.count : stats.count * groupingSetNum)
                            .setNumNulls(stats.numNulls < 0 ? stats.numNulls : stats.numNulls * groupingSetNum)
                            .setDataSize(stats.dataSize < 0 ? stats.dataSize : stats.dataSize * groupingSetNum);
                    return Pair.of(kv.getKey(), columnStatisticBuilder.build());
                }).collect(Collectors.toMap(Pair::key, Pair::value));
        return new Statistics(rowCount < 0 ? rowCount : rowCount * groupingSetNum, columnStatisticMap);
    }

    private Statistics computeProject(Project project) {
        List<NamedExpression> projections = project.getProjects();
        Statistics childStats = groupExpression.childStatistics(0);
        Map<Expression, ColumnStatistic> columnsStats = projections.stream().map(projection -> {
            ColumnStatistic columnStatistic = ExpressionEstimation.estimate(projection, childStats);
            return new SimpleEntry<>(projection.toSlot(), columnStatistic);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (item1, item2) -> item1));
        return new Statistics(childStats.getRowCount(), columnsStats);
    }

    private Statistics computeOneRowRelation(List<NamedExpression> projects) {
        Map<Expression, ColumnStatistic> columnStatsMap = projects.stream()
                .map(project -> {
                    ColumnStatistic statistic = new ColumnStatisticBuilder().setNdv(1).build();
                    // TODO: compute the literal size
                    return Pair.of(project.toSlot(), statistic);
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));
        int rowCount = 1;
        return new Statistics(rowCount, columnStatsMap);
    }

    private Statistics computeEmptyRelation(EmptyRelation emptyRelation) {
        Map<Expression, ColumnStatistic> columnStatsMap = emptyRelation.getProjects()
                .stream()
                .map(project -> {
                    ColumnStatisticBuilder columnStat = new ColumnStatisticBuilder()
                            .setNdv(0)
                            .setNumNulls(0)
                            .setAvgSizeByte(0);
                    return Pair.of(project.toSlot(), columnStat.build());
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));
        int rowCount = 0;
        return new Statistics(rowCount, columnStatsMap);
    }

    private Statistics computeUnion(Union union) {
        // TODO: refactor this for one row relation
        List<SlotReference> head = null;
        Statistics headStats = null;
        List<List<SlotReference>> childOutputs = Lists.newArrayList(union.getRegularChildrenOutputs());
        List<Statistics> childStats =
                groupExpression.children().stream().map(Group::getStatistics).collect(Collectors.toList());

        if (!union.getConstantExprsList().isEmpty()) {
            childOutputs.addAll(union.getConstantExprsList().stream()
                    .map(l -> l.stream().map(NamedExpression::toSlot)
                            .map(SlotReference.class::cast)
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList()));
            childStats.addAll(union.getConstantExprsList().stream()
                    .map(this::computeOneRowRelation)
                    .collect(Collectors.toList()));
            if (!union.getConstantExprsList().isEmpty()) {
                head = union.getConstantExprsList().get(0).stream()
                        .map(NamedExpression::toSlot)
                        .map(SlotReference.class::cast)
                        .collect(Collectors.toList());
                headStats = computeOneRowRelation(union.getConstantExprsList().get(0));
            }
        }
        if (head == null) {
            head = (List) groupExpression.child(0).getLogicalProperties().getOutput();
            headStats = groupExpression.childStatistics(0);
        }

        StatisticsBuilder statisticsBuilder = new StatisticsBuilder();
        List<NamedExpression> unionOutput = union.getOutputs();
        for (int i = 0; i < head.size(); i++) {
            double leftRowCount = headStats.getRowCount();
            Slot headSlot = head.get(i);
            for (int j = 1; j < childOutputs.size(); j++) {
                Slot slot = childOutputs.get(j).get(i);
                ColumnStatistic rightStatistic = childStats.get(j).findColumnStatistics(slot);
                double rightRowCount = childStats.get(j).getRowCount();
                ColumnStatistic estimatedColumnStatistics
                        = unionColumn(headStats.findColumnStatistics(headSlot),
                        headStats.getRowCount(), rightStatistic, rightRowCount, headSlot.getDataType());
                headStats.addColumnStats(headSlot, estimatedColumnStatistics);
                leftRowCount += childStats.get(j).getRowCount();
            }
            statisticsBuilder.setRowCount(leftRowCount);
            statisticsBuilder.putColumnStatistics(unionOutput.get(i), headStats.findColumnStatistics(headSlot));
        }
        return statisticsBuilder.build();
    }

    private Statistics computeExcept(SetOperation setOperation) {
        Statistics leftStats = groupExpression.childStatistics(0);
        List<NamedExpression> operatorOutput = setOperation.getOutputs();
        List<SlotReference> childSlots = setOperation.getRegularChildOutput(0);
        StatisticsBuilder statisticsBuilder = new StatisticsBuilder();
        for (int i = 0; i < operatorOutput.size(); i++) {
            ColumnStatistic columnStatistic = leftStats.findColumnStatistics(childSlots.get(i));
            statisticsBuilder.putColumnStatistics(operatorOutput.get(i), columnStatistic);
        }
        statisticsBuilder.setRowCount(leftStats.getRowCount());
        return statisticsBuilder.build();
    }

    private Statistics computeIntersect(SetOperation setOperation) {
        Statistics leftChildStats = groupExpression.childStatistics(0);
        double rowCount = leftChildStats.getRowCount();
        for (int i = 1; i < setOperation.getArity(); ++i) {
            rowCount = Math.min(rowCount, groupExpression.childStatistics(i).getRowCount());
        }
        double minProd = Double.MAX_VALUE;
        for (Group group : groupExpression.children()) {
            Statistics statistics = group.getStatistics();
            double prod = 1.0;
            for (ColumnStatistic columnStatistic : statistics.columnStatistics().values()) {
                prod *= columnStatistic.ndv;
            }
            if (minProd < prod) {
                minProd = prod;
            }
        }
        rowCount = Math.min(rowCount, minProd);
        List<NamedExpression> outputs = setOperation.getOutputs();
        List<SlotReference> leftChildOutputs = setOperation.getRegularChildOutput(0);
        for (int i = 0; i < outputs.size(); i++) {
            leftChildStats.addColumnStats(outputs.get(i),
                    leftChildStats.findColumnStatistics(leftChildOutputs.get(i)));
        }
        return leftChildStats.withRowCount(rowCount);
    }

    private Statistics computeGenerate(Generate generate) {
        Statistics stats = groupExpression.childStatistics(0);
        double count = stats.getRowCount() * generate.getGeneratorOutput().size() * 5;
        Map<Expression, ColumnStatistic> columnStatsMap = Maps.newHashMap();
        for (Map.Entry<Expression, ColumnStatistic> entry : stats.columnStatistics().entrySet()) {
            ColumnStatistic columnStatistic = new ColumnStatisticBuilder(entry.getValue()).setCount(count).build();
            columnStatsMap.put(entry.getKey(), columnStatistic);
        }
        for (Slot output : generate.getGeneratorOutput()) {
            ColumnStatistic columnStatistic = new ColumnStatisticBuilder()
                    .setCount(count)
                    .setMinValue(Double.MAX_VALUE)
                    .setMaxValue(Double.MIN_VALUE)
                    .setNdv(count)
                    .setNumNulls(0)
                    .setAvgSizeByte(output.getDataType().width())
                    .build();
            columnStatsMap.put(output, columnStatistic);
        }
        return new Statistics(count, columnStatsMap);
    }

    private Statistics computeWindow(Window windowOperator) {
        Statistics stats = groupExpression.childStatistics(0);
        Map<Expression, ColumnStatistic> childColumnStats = stats.columnStatistics();
        Map<Expression, ColumnStatistic> columnStatisticMap = windowOperator.getWindowExpressions().stream()
                .map(expr -> {
                    //estimate rank()
                    if (expr instanceof Alias && expr.child(0) instanceof WindowExpression
                            && ((WindowExpression) expr.child(0)).getFunction() instanceof Rank) {
                        ColumnStatisticBuilder colBuilder = new ColumnStatisticBuilder();
                        colBuilder.setNdv(stats.getRowCount())
                                .setOriginal(null)
                                .setCount(stats.getRowCount())
                                .setMinValue(0)
                                .setMaxValue(stats.getRowCount());
                        return Pair.of(expr.toSlot(), colBuilder.build());
                    }
                    //estimate other expressions
                    ColumnStatistic value = null;
                    Set<Slot> slots = expr.getInputSlots();
                    if (slots.isEmpty()) {
                        value = ColumnStatistic.UNKNOWN;
                    } else {
                        for (Slot slot : slots) {
                            if (childColumnStats.containsKey(slot)) {
                                value = childColumnStats.get(slot);
                                break;
                            }
                        }
                        if (value == null) {
                            // todo: how to set stats?
                            value = ColumnStatistic.UNKNOWN;
                        }
                    }
                    return Pair.of(expr.toSlot(), value);
                }).collect(Collectors.toMap(Pair::key, Pair::value));
        columnStatisticMap.putAll(childColumnStats);
        return new Statistics(stats.getRowCount(), columnStatisticMap);
    }

    private ColumnStatistic unionColumn(ColumnStatistic leftStats, double leftRowCount, ColumnStatistic rightStats,
            double rightRowCount, DataType dataType) {
        ColumnStatisticBuilder columnStatisticBuilder = new ColumnStatisticBuilder();
        columnStatisticBuilder.setMaxValue(Math.max(leftStats.maxValue, rightStats.maxValue));
        columnStatisticBuilder.setMinValue(Math.min(leftStats.minValue, rightStats.minValue));
        StatisticRange leftRange = StatisticRange.from(leftStats, dataType);
        StatisticRange rightRange = StatisticRange.from(rightStats, dataType);
        StatisticRange newRange = leftRange.union(rightRange);
        double newRowCount = leftRowCount + rightRowCount;
        double leftSize = (leftRowCount - leftStats.numNulls) * leftStats.avgSizeByte;
        double rightSize = (rightRowCount - rightStats.numNulls) * rightStats.avgSizeByte;
        double newNullFraction = (leftStats.numNulls + rightStats.numNulls) / StatsMathUtil.maxNonNaN(1, newRowCount);
        double newNonNullRowCount = newRowCount * (1 - newNullFraction);

        double newAverageRowSize = newNonNullRowCount == 0 ? 0 : (leftSize + rightSize) / newNonNullRowCount;
        columnStatisticBuilder.setMinValue(newRange.getLow())
                .setMaxValue(newRange.getHigh())
                .setNdv(newRange.getDistinctValues())
                .setNumNulls(leftStats.numNulls + rightStats.numNulls)
                .setAvgSizeByte(newAverageRowSize);
        return columnStatisticBuilder.build();
    }

    private Plan tryToFindChild(GroupExpression groupExpression) {
        List<GroupExpression> groupExprs = groupExpression.child(0).getLogicalExpressions();
        if (CollectionUtils.isEmpty(groupExprs)) {
            groupExprs = groupExpression.child(0).getPhysicalExpressions();
            if (CollectionUtils.isEmpty(groupExprs)) {
                return null;
            }
        }
        return groupExprs.get(0).getPlan();
    }

    @Override
    public Statistics visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, Void context) {
        Statistics statistics = groupExpression.childStatistics(0);
        cteIdToStats.put(cteProducer.getCteId(), statistics);
        return statistics;
    }

    @Override
    public Statistics visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Void context) {
        CTEId cteId = cteConsumer.getCteId();
        cascadesContext.addCTEConsumerGroup(cteConsumer.getCteId(), groupExpression.getOwnerGroup(),
                cteConsumer.getProducerToConsumerOutputMap());
        Statistics prodStats = cteIdToStats.get(cteId);
        Preconditions.checkArgument(prodStats != null, String.format("Stats for CTE: %s not found", cteId));
        Statistics consumerStats = new Statistics(prodStats.getRowCount(), new HashMap<>());
        for (Slot slot : cteConsumer.getOutput()) {
            Slot prodSlot = cteConsumer.getProducerSlot(slot);
            ColumnStatistic colStats = prodStats.columnStatistics().get(prodSlot);
            if (colStats == null) {
                continue;
            }
            consumerStats.addColumnStats(slot, colStats);
        }
        return consumerStats;
    }

    @Override
    public Statistics visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, Void context) {
        return groupExpression.childStatistics(1);
    }

    @Override
    public Statistics visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> cteProducer,
            Void context) {
        Statistics statistics = groupExpression.childStatistics(0);
        cteIdToStats.put(cteProducer.getCteId(), statistics);
        cascadesContext.updateConsumerStats(cteProducer.getCteId(), statistics);
        return statistics;
    }

    @Override
    public Statistics visitPhysicalCTEConsumer(PhysicalCTEConsumer cteConsumer, Void context) {
        cascadesContext.addCTEConsumerGroup(cteConsumer.getCteId(), groupExpression.getOwnerGroup(),
                cteConsumer.getProducerToConsumerSlotMap());
        CTEId cteId = cteConsumer.getCteId();
        Statistics prodStats = cteIdToStats.get(cteId);
        if (prodStats == null) {
            prodStats = groupExpression.getOwnerGroup().getStatistics();
        }
        Preconditions.checkArgument(prodStats != null, String.format("Stats for CTE: %s not found", cteId));
        Statistics consumerStats = new Statistics(prodStats.getRowCount(), new HashMap<>());
        for (Slot slot : cteConsumer.getOutput()) {
            Slot prodSlot = cteConsumer.getProducerSlot(slot);
            ColumnStatistic colStats = prodStats.columnStatistics().get(prodSlot);
            if (colStats == null) {
                continue;
            }
            consumerStats.addColumnStats(slot, colStats);
        }
        return consumerStats;
    }

    @Override
    public Statistics visitPhysicalCTEAnchor(
            PhysicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor, Void context) {
        return groupExpression.childStatistics(1);
    }

}
