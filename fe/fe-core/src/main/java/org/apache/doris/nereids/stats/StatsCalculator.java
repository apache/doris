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

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.EmptyRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.algebra.Limit;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.algebra.PartitionTopN;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalOdbcScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
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
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.Histogram;
import org.apache.doris.statistics.PartitionColumnStatistic;
import org.apache.doris.statistics.PartitionColumnStatisticBuilder;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.StatisticRange;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsBuilder;
import org.apache.doris.statistics.TableStatsMeta;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to calculate the stats for each plan
 */
public class StatsCalculator extends DefaultPlanVisitor<Statistics, Void> {
    public static double DEFAULT_AGGREGATE_RATIO = 1 / 3.0;
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
        Statistics newStats = plan.accept(this, null);
        newStats.enforceValid();

        // We ensure that the rowCount remains unchanged in order to make the cost of each plan comparable.
        if (groupExpression.getOwnerGroup().getStatistics() == null) {
            boolean isReliable = groupExpression.getPlan().getExpressions().stream()
                    .noneMatch(e -> newStats.isInputSlotsUnknown(e.getInputSlots()));
            groupExpression.getOwnerGroup().setStatsReliable(isReliable);
            groupExpression.getOwnerGroup().setStatistics(newStats);
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
            groupExpression.getOwnerGroup().getStatistics().updateNdv(newStats);
        }
        groupExpression.setEstOutputRowCount(newStats.getRowCount());
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

    /**
     * returns the sum of deltaRowCount for all selected partitions or for the table.
     */
    private long computeDeltaRowCount(OlapScan olapScan) {
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        TableStatsMeta tableMeta = analysisManager.findTableStatsStatus(olapScan.getTable().getId());
        long deltaRowCount = 0;
        if (tableMeta != null) {
            deltaRowCount = tableMeta.getBaseIndexDeltaRowCount(olapScan.getTable());
        }
        return deltaRowCount;
    }

    private ColumnStatistic getColumnStatsFromTableCache(CatalogRelation catalogRelation, SlotReference slot) {
        long idxId = -1;
        if (catalogRelation instanceof OlapScan) {
            idxId = ((OlapScan) catalogRelation).getSelectedIndexId();
        }
        return getColumnStatistic(catalogRelation.getTable(), slot.getName(), idxId);
    }

    private ColumnStatistic getColumnStatsFromPartitionCache(OlapScan catalogRelation, SlotReference slot,
            List<String> partitionNames) {
        long idxId = catalogRelation.getSelectedIndexId();

        return getColumnStatistic(catalogRelation.getTable(), slot.getName(), idxId, partitionNames);
    }

    private long getSelectedPartitionRowCount(OlapScan olapScan) {
        long partRowCountSum = 0;
        for (long id : olapScan.getSelectedPartitionIds()) {
            long partRowCount = olapScan.getTable().getPartition(id)
                    .getIndex(olapScan.getSelectedIndexId()).getRowCount();
            // if we cannot get any partition's rowCount, return -1 to fallback to table level stats
            if (partRowCount <= 0) {
                return -1;
            }
            partRowCountSum += partRowCount;
        }
        return partRowCountSum;
    }

    private void setHasUnknownColStatsInStatementContext() {
        if (ConnectContext.get() != null && ConnectContext.get().getStatementContext() != null) {
            ConnectContext.get().getStatementContext().setHasUnknownColStats(true);
        }
    }

    private void checkIfUnknownStatsUsedAsKey(StatisticsBuilder builder) {
        if (ConnectContext.get() != null && ConnectContext.get().getStatementContext() != null) {
            for (Map.Entry<Expression, ColumnStatistic> entry : builder.getExpressionColumnStatsEntries()) {
                if (entry.getKey() instanceof SlotReference
                        && ConnectContext.get().getStatementContext().isKeySlot((SlotReference) entry.getKey())) {
                    if (entry.getValue().isUnKnown) {
                        ConnectContext.get().getStatementContext().setHasUnknownColStats(true);
                        break;
                    }
                }
            }
        }
    }

    private double getOlapTableRowCount(OlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        TableStatsMeta tableMeta = analysisManager.findTableStatsStatus(olapScan.getTable().getId());
        double rowCount = -1;
        if (tableMeta != null && tableMeta.userInjected) {
            rowCount = tableMeta.getRowCount(olapScan.getSelectedIndexId());
        } else {
            rowCount = olapTable.getRowCountForIndex(olapScan.getSelectedIndexId(), true);
            if (rowCount == -1) {
                if (tableMeta != null) {
                    rowCount = tableMeta.getRowCount(olapScan.getSelectedIndexId());
                }
            }
        }
        return rowCount;
    }

    private Statistics computeOlapScan(OlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        double tableRowCount = getOlapTableRowCount(olapScan);
        tableRowCount = Math.max(1, tableRowCount);

        if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId() || olapTable instanceof MTMV) {
            // mv is selected, return its estimated stats
            Optional<Statistics> optStats = cascadesContext.getStatementContext()
                    .getStatistics(((Relation) olapScan).getRelationId());
            if (optStats.isPresent()) {
                double selectedPartitionsRowCount = getSelectedPartitionRowCount(olapScan);
                if (selectedPartitionsRowCount == -1) {
                    selectedPartitionsRowCount = tableRowCount;
                }
                // if estimated mv rowCount is more than actual row count, fall back to base table stats
                if (selectedPartitionsRowCount >= optStats.get().getRowCount()) {
                    Statistics derivedStats = optStats.get();
                    double derivedRowCount = derivedStats.getRowCount();
                    for (Slot slot : ((Relation) olapScan).getOutput()) {
                        if (derivedStats.findColumnStatistics(slot) == null) {
                            derivedStats.addColumnStats(slot,
                                    new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN)
                                            .setCount(derivedRowCount).build());
                        }
                    }
                    return derivedStats;
                }
            }
        }

        StatisticsBuilder builder = new StatisticsBuilder();

        // for system table or FeUt, use ColumnStatistic.UNKNOWN
        if (StatisticConstants.isSystemTable(olapTable) || !FeConstants.enableInternalSchemaDb
                || ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable().internalSession) {
            for (Slot slot : ((Plan) olapScan).getOutput()) {
                builder.putColumnStatistics(slot, ColumnStatistic.UNKNOWN);
            }
            setHasUnknownColStatsInStatementContext();
            builder.setRowCount(tableRowCount);
            return builder.build();
        }

        // for regression shape test
        if (ConnectContext.get() == null || !ConnectContext.get().getSessionVariable().enableStats) {
            // get row count from any visible slotReference's colStats
            for (Slot slot : ((Plan) olapScan).getOutput()) {
                builder.putColumnStatistics(slot,
                        new ColumnStatisticBuilder(ColumnStatistic.UNKNOWN).setCount(tableRowCount).build());
            }
            setHasUnknownColStatsInStatementContext();
            return builder.setRowCount(tableRowCount).build();
        }

        // build Stats for olapScan
        double deltaRowCount = computeDeltaRowCount(olapScan);
        builder.setDeltaRowCount(deltaRowCount);
        // if slot is invisible, use UNKNOWN
        List<SlotReference> visibleOutputSlots = new ArrayList<>();
        for (Slot slot : ((Plan) olapScan).getOutput()) {
            if (isVisibleSlotReference(slot)) {
                visibleOutputSlots.add((SlotReference) slot);
            } else {
                builder.putColumnStatistics(slot, ColumnStatistic.UNKNOWN);
            }
        }

        boolean useTableLevelStats = true;
        if (olapScan.getSelectedPartitionIds().size() < olapScan.getTable().getPartitionNum()) {
            // partition pruned
            // try to use selected partition stats, if failed, fall back to table stats
            double selectedPartitionsRowCount = getSelectedPartitionRowCount(olapScan);
            if (selectedPartitionsRowCount >= 0) {
                useTableLevelStats = false;
                List<String> selectedPartitionNames = new ArrayList<>(olapScan.getSelectedPartitionIds().size());
                olapScan.getSelectedPartitionIds().forEach(id -> {
                    selectedPartitionNames.add(olapScan.getTable().getPartition(id).getName());
                });
                for (SlotReference slot : visibleOutputSlots) {
                    ColumnStatistic cache = getColumnStatsFromPartitionCache(olapScan, slot, selectedPartitionNames);
                    ColumnStatisticBuilder colStatsBuilder = new ColumnStatisticBuilder(cache);
                    colStatsBuilder.setCount(selectedPartitionsRowCount);
                    colStatsBuilder.normalizeAvgSizeByte(slot);
                    builder.putColumnStatistics(slot, colStatsBuilder.build());
                }
                checkIfUnknownStatsUsedAsKey(builder);
                builder.setRowCount(selectedPartitionsRowCount + deltaRowCount);
            }
        }
        // 1. no partition is pruned, or
        // 2. fall back to table stats
        if (useTableLevelStats) {
            // get table level stats
            for (SlotReference slot : visibleOutputSlots) {
                ColumnStatistic cache = getColumnStatsFromTableCache((CatalogRelation) olapScan, slot);
                ColumnStatisticBuilder colStatsBuilder = new ColumnStatisticBuilder(cache);
                colStatsBuilder.setCount(tableRowCount);
                colStatsBuilder.normalizeAvgSizeByte(slot);
                builder.putColumnStatistics(slot, colStatsBuilder.build());
            }
            checkIfUnknownStatsUsedAsKey(builder);
            builder.setRowCount(tableRowCount + deltaRowCount);
        }
        return builder.build();
    }

    @Override
    public Statistics visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
        return computeOlapScan(olapScan);
    }

    private boolean isVisibleSlotReference(Slot slot) {
        if (slot instanceof SlotReference) {
            Optional<Column> colOpt = ((SlotReference) slot).getColumn();
            if (colOpt.isPresent()) {
                return colOpt.get().isVisible();
            }
        }
        return false;
    }

    @Override
    public Statistics visitLogicalDeferMaterializeOlapScan(LogicalDeferMaterializeOlapScan deferMaterializeOlapScan,
            Void context) {
        return computeOlapScan(deferMaterializeOlapScan.getLogicalOlapScan());
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
    public Statistics visitLogicalHudiScan(LogicalHudiScan fileScan, Void context) {
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
    public Statistics visitLogicalOdbcScan(LogicalOdbcScan odbcScan, Void context) {
        odbcScan.getExpressions();
        return computeCatalogRelation(odbcScan);
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
        Statistics joinStats = JoinEstimation.estimate(groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join);
        joinStats = new StatisticsBuilder(joinStats).setWidthInJoinCluster(
                groupExpression.childStatistics(0).getWidthInJoinCluster()
                        + groupExpression.childStatistics(1).getWidthInJoinCluster()).build();
        return joinStats;
    }

    @Override
    public Statistics visitLogicalAssertNumRows(
            LogicalAssertNumRows<? extends Plan> assertNumRows, Void context) {
        return computeAssertNumRows(assertNumRows.getAssertNumRowsElement());
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

    @Override
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
        return computeOlapScan(olapScan);
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
        PhysicalRelation relation = storageLayerAggregate.getRelation();
        return relation.accept(this, context);

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
    public Statistics visitPhysicalOdbcScan(PhysicalOdbcScan odbcScan, Void context) {
        return computeCatalogRelation(odbcScan);
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
        return computeAssertNumRows(assertNumRows.getAssertNumRowsElement());
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

    private Statistics computeAssertNumRows(AssertNumRowsElement assertNumRowsElement) {
        Statistics statistics = groupExpression.childStatistics(0);
        long newRowCount;
        long rowCount = (long) statistics.getRowCount();
        long desiredNumOfRows = assertNumRowsElement.getDesiredNumOfRows();
        switch (assertNumRowsElement.getAssertion()) {
            case EQ:
                newRowCount = desiredNumOfRows;
                break;
            case GE:
                newRowCount = statistics.getRowCount() >= desiredNumOfRows ? rowCount : desiredNumOfRows;
                break;
            case GT:
                newRowCount = statistics.getRowCount() > desiredNumOfRows ? rowCount : desiredNumOfRows;
                break;
            case LE:
                newRowCount = statistics.getRowCount() <= desiredNumOfRows ? rowCount : desiredNumOfRows;
                break;
            case LT:
                newRowCount = statistics.getRowCount() < desiredNumOfRows ? rowCount : desiredNumOfRows;
                break;
            case NE:
                return statistics;
            default:
                throw new IllegalArgumentException("Unknown assertion: " + assertNumRowsElement.getAssertion());
        }
        Statistics newStatistics = statistics.withRowCountAndEnforceValid(newRowCount);
        return new StatisticsBuilder(newStatistics).setWidthInJoinCluster(1).build();
    }

    private Statistics computeFilter(Filter filter) {
        Statistics stats = groupExpression.childStatistics(0);
        if (groupExpression.getFirstChildPlan(OlapScan.class) != null) {
            return new FilterEstimation(true).estimate(filter.getPredicate(), stats);
        }
        if (groupExpression.getFirstChildPlan(Aggregate.class) != null) {
            Aggregate agg = (Aggregate<?>) groupExpression.getFirstChildPlan(Aggregate.class);
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
        } else if (groupExpression.getFirstChildPlan(LogicalJoin.class) != null) {
            LogicalJoin plan = (LogicalJoin) groupExpression.getFirstChildPlan(LogicalJoin.class);
            if (filter instanceof LogicalFilter
                    && filter.getConjuncts().stream().anyMatch(e -> e instanceof IsNull)) {
                Statistics isNullStats = computeGeneratedIsNullStats((LogicalJoin) plan, filter);
                if (isNullStats != null) {
                    // overwrite the stats corrected as above before passing to filter estimation
                    Set<Expression> newConjuncts = filter.getConjuncts().stream()
                            .filter(e -> !(e instanceof IsNull))
                            .collect(Collectors.toSet());
                    if (newConjuncts.isEmpty()) {
                        return isNullStats;
                    } else {
                        // overwrite the filter by removing is null and remain the others
                        filter = ((LogicalFilter<?>) filter).withConjunctsAndProps(newConjuncts,
                                ((LogicalFilter<?>) filter).getGroupExpression(),
                                Optional.of(((LogicalFilter<?>) filter).getLogicalProperties()), plan);
                        // add update is-null related column stats for other predicate derive
                        StatisticsBuilder builder = new StatisticsBuilder(stats);
                        for (Expression expr : isNullStats.columnStatistics().keySet()) {
                            builder.putColumnStatistics(expr, isNullStats.findColumnStatistics(expr));
                        }
                        builder.setRowCount(isNullStats.getRowCount());
                        stats = builder.build();
                        stats.enforceValid();
                    }
                }
            }
        }
        return new FilterEstimation(false).estimate(filter.getPredicate(), stats);
    }

    private Statistics computeGeneratedIsNullStats(LogicalJoin join, Filter filter) {
        JoinType joinType = join.getJoinType();
        Plan left = join.left();
        Plan right = join.right();
        if (left == null || right == null
                || ((GroupPlan) left).getGroup() == null || ((GroupPlan) right).getGroup() == null
                || ((GroupPlan) left).getGroup().getStatistics() == null
                || ((GroupPlan) right).getGroup().getStatistics() == null
                || !join.getGroupExpression().isPresent()) {
            return null;
        }

        double leftRowCount = ((GroupPlan) left).getGroup().getStatistics().getRowCount();
        double rightRowCount = ((GroupPlan) right).getGroup().getStatistics().getRowCount();
        if (leftRowCount < 0 || Double.isInfinite(leftRowCount)
                || rightRowCount < 0 || Double.isInfinite(rightRowCount)) {
            return null;
        }

        Statistics origJoinStats = join.getGroupExpression().get().getOwnerGroup().getStatistics();

        // for outer join which is anti-like, use anti join to re-estimate the stats
        // otherwise, return null and pass through to use the normal filter estimation logical
        if (joinType.isOuterJoin()) {
            boolean leftHasIsNull = false;
            boolean rightHasIsNull = false;
            boolean isLeftOuterJoin = join.getJoinType() == JoinType.LEFT_OUTER_JOIN;
            boolean isRightOuterJoin = join.getJoinType() == JoinType.RIGHT_OUTER_JOIN;
            boolean isFullOuterJoin = join.getJoinType() == JoinType.FULL_OUTER_JOIN;

            for (Expression expr : filter.getConjuncts()) {
                if (expr instanceof IsNull) {
                    Expression child = ((IsNull) expr).child();
                    if (PlanUtils.isColumnRef(child)) {
                        LogicalPlan leftChild = (LogicalPlan) join.left();
                        LogicalPlan rightChild = (LogicalPlan) join.right();
                        leftHasIsNull = PlanUtils.checkSlotFrom(((GroupPlan) leftChild)
                                .getGroup().getLogicalExpression().getPlan(), (SlotReference) child);
                        rightHasIsNull = PlanUtils.checkSlotFrom(((GroupPlan) rightChild)
                                .getGroup().getLogicalExpression().getPlan(), (SlotReference) child);
                    }
                }
            }

            boolean isLeftAntiLikeJoin = (isLeftOuterJoin && rightHasIsNull) || (isFullOuterJoin && rightHasIsNull);
            boolean isRightAntiLikeJoin = (isRightOuterJoin && leftHasIsNull) || (isFullOuterJoin && leftHasIsNull);
            if (isLeftAntiLikeJoin || isRightAntiLikeJoin) {
                // transform to anti estimation
                Statistics newStats = null;
                if (isLeftAntiLikeJoin) {
                    LogicalJoin<GroupPlan, GroupPlan> newJoin = join.withJoinType(JoinType.LEFT_ANTI_JOIN);
                    StatsCalculator statsCalculator = new StatsCalculator(join.getGroupExpression().get(),
                            false, getTotalColumnStatisticMap(), false,
                            cteIdToStats, cascadesContext);

                    newStats = ((Plan) newJoin).accept(statsCalculator, null);
                } else if (isRightAntiLikeJoin) {
                    LogicalJoin<GroupPlan, GroupPlan> newJoin = join.withJoinType(JoinType.RIGHT_ANTI_JOIN);
                    StatsCalculator statsCalculator = new StatsCalculator(join.getGroupExpression().get(),
                            false, this.getTotalColumnStatisticMap(), false,
                            this.cteIdToStats, this.cascadesContext);

                    newStats = ((Plan) newJoin).accept(statsCalculator, null);
                }
                newStats.enforceValid();

                double selectivity = Statistics.getValidSelectivity(
                        newStats.getRowCount() / (leftRowCount * rightRowCount));
                double newRows = origJoinStats.getRowCount() * selectivity;

                newStats.withRowCount(newRows);
                return newStats;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private ColumnStatistic getColumnStatistic(TableIf table, String colName, long idxId) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().internalSession) {
            return ColumnStatistic.UNKNOWN;
        }
        long catalogId;
        long dbId;
        try {
            catalogId = table.getDatabase().getCatalog().getId();
            dbId = table.getDatabase().getId();
        } catch (Exception e) {
            // Use -1 for catalog id and db id when failed to get them from metadata.
            // This is OK because catalog id and db id is not in the hashcode function of ColumnStatistics cache
            // and the table id is globally unique.
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Fail to get catalog id and db id for table %s", table.getName()));
            }
            catalogId = -1;
            dbId = -1;
        }
        return Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
                catalogId, dbId, table.getId(), idxId, colName);
    }

    private ColumnStatistic getColumnStatistic(TableIf table, String colName, long idxId, List<String> partitionNames) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().internalSession) {
            return ColumnStatistic.UNKNOWN;
        }
        long catalogId;
        long dbId;
        try {
            catalogId = table.getDatabase().getCatalog().getId();
            dbId = table.getDatabase().getId();
        } catch (Exception e) {
            // Use -1 for catalog id and db id when failed to get them from metadata.
            // This is OK because catalog id and db id is not in the hashcode function of ColumnStatistics cache
            // and the table id is globally unique.
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Fail to get catalog id and db id for table %s", table.getName()));
            }
            catalogId = -1;
            dbId = -1;
        }
        if (isPlayNereidsDump) {
            if (totalColumnStatisticMap.get(table.getName() + colName) != null) {
                return totalColumnStatisticMap.get(table.getName() + colName);
            } else {
                return ColumnStatistic.UNKNOWN;
            }
        } else {
            if (!partitionNames.isEmpty()) {
                PartitionColumnStatisticBuilder builder = new PartitionColumnStatisticBuilder();
                boolean hasUnknown = false;
                // check if there is any unknown stats to avoid unnecessary partition column stats merge.
                List<PartitionColumnStatistic> pColStatsLists = new ArrayList<>(partitionNames.size());
                for (String partitionName : partitionNames) {
                    PartitionColumnStatistic pcolStats = Env.getCurrentEnv().getStatisticsCache()
                            .getPartitionColumnStatistics(
                                    catalogId, dbId, table.getId(), idxId, partitionName, colName);
                    if (pcolStats.isUnKnown) {
                        hasUnknown = true;
                        break;
                    } else {
                        pColStatsLists.add(pcolStats);
                    }
                }
                if (!hasUnknown) {
                    boolean isFirst = true;
                    // try to merge partition column stats
                    for (PartitionColumnStatistic pcolStats : pColStatsLists) {
                        if (isFirst) {
                            builder = new PartitionColumnStatisticBuilder(pcolStats);
                            isFirst = false;
                        } else {
                            builder.merge(pcolStats);
                        }
                    }
                    return builder.toColumnStatistics();
                }
            }
            // if any partition-col-stats is unknown, fall back to table level col stats
            return Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
                    catalogId, dbId, table.getId(), idxId, colName);
        }
    }

    /**
     * compute stats for catalogRelations except OlapScan
     */
    private Statistics computeCatalogRelation(CatalogRelation catalogRelation) {
        StatisticsBuilder builder = new StatisticsBuilder();
        // for FeUt, use ColumnStatistic.UNKNOWN
        if (!FeConstants.enableInternalSchemaDb
                || ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable().internalSession) {
            builder.setRowCount(catalogRelation.getTable().getRowCountForNereids());
            for (Slot slot : catalogRelation.getOutput()) {
                builder.putColumnStatistics(slot, ColumnStatistic.UNKNOWN);
            }
            setHasUnknownColStatsInStatementContext();
            return builder.build();
        }

        List<Slot> output = catalogRelation.getOutput();
        ImmutableSet.Builder<SlotReference> slotSetBuilder = ImmutableSet.builderWithExpectedSize(output.size());
        for (Slot slot : output) {
            if (slot instanceof SlotReference) {
                slotSetBuilder.add((SlotReference) slot);
            }
        }
        Set<SlotReference> slotSet = slotSetBuilder.build();
        double tableRowCount = catalogRelation.getTable().getRowCount();
        if (tableRowCount <= 0) {
            // try to get row count from col stats
            for (SlotReference slot : slotSet) {
                ColumnStatistic cache = getColumnStatsFromTableCache(catalogRelation, slot);
                tableRowCount = Math.max(cache.count, tableRowCount);
            }
        }
        for (SlotReference slot : slotSet) {
            ColumnStatistic cache;
            if (ConnectContext.get() != null && ! ConnectContext.get().getSessionVariable().enableStats) {
                cache = ColumnStatistic.UNKNOWN;
            } else {
                cache = getColumnStatsFromTableCache(catalogRelation, slot);
            }
            ColumnStatisticBuilder colStatsBuilder = new ColumnStatisticBuilder(cache);
            colStatsBuilder.setCount(tableRowCount);
            builder.putColumnStatistics(slot, colStatsBuilder.build());
        }
        checkIfUnknownStatsUsedAsKey(builder);
        return builder.setRowCount(tableRowCount).build();
    }

    private Statistics computeTopN(TopN topN) {
        Statistics stats = groupExpression.childStatistics(0);
        return stats.withRowCountAndEnforceValid(Math.min(stats.getRowCount(), topN.getLimit()));
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
        return stats.withRowCountAndEnforceValid(Math.min(stats.getRowCount(), limit.getLimit()));
    }

    private double estimateGroupByRowCount(List<Expression> groupByExpressions, Statistics childStats) {
        double rowCount = 1;
        // if there is group-bys, output row count is childStats.getRowCount() * DEFAULT_AGGREGATE_RATIO,
        // o.w. output row count is 1
        // example: select sum(A) from T;
        if (groupByExpressions.isEmpty()) {
            return 1;
        }
        List<Double> groupByNdvs = new ArrayList<>();
        for (Expression groupByExpr : groupByExpressions) {
            ColumnStatistic colStats = childStats.findColumnStatistics(groupByExpr);
            if (colStats == null) {
                colStats = ExpressionEstimation.estimate(groupByExpr, childStats);
            }
            if (colStats.isUnKnown()) {
                rowCount = childStats.getRowCount() * DEFAULT_AGGREGATE_RATIO;
                rowCount = Math.max(1, rowCount);
                rowCount = Math.min(rowCount, childStats.getRowCount());
                return rowCount;
            }
            double ndv = colStats.ndv;
            groupByNdvs.add(ndv);
        }
        groupByNdvs.sort(Collections.reverseOrder());

        rowCount = groupByNdvs.get(0);
        for (int groupByIndex = 1; groupByIndex < groupByExpressions.size(); ++groupByIndex) {
            rowCount *= Math.max(1, groupByNdvs.get(groupByIndex) * Math.pow(
                    AGGREGATE_COLUMN_CORRELATION_COEFFICIENT, groupByIndex + 1D));
            if (rowCount > childStats.getRowCount()) {
                rowCount = childStats.getRowCount();
                break;
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
        Statistics aggOutputStats = new Statistics(rowCount, 1, slotToColumnStats);
        aggOutputStats.enforceValid();
        return aggOutputStats;
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
                }).collect(Collectors.toMap(Pair::key, Pair::value, (item1, item2) -> item1));
        return new Statistics(rowCount < 0 ? rowCount : rowCount * groupingSetNum, 1, columnStatisticMap);
    }

    private Statistics computeProject(Project project) {
        List<NamedExpression> projections = project.getProjects();
        Statistics childStats = groupExpression.childStatistics(0);
        Map<Expression, ColumnStatistic> columnsStats = projections.stream().map(projection -> {
            ColumnStatistic columnStatistic = ExpressionEstimation.estimate(projection, childStats);
            return new SimpleEntry<>(projection.toSlot(), columnStatistic);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (item1, item2) -> item1));
        return new Statistics(childStats.getRowCount(), childStats.getWidthInJoinCluster(), columnsStats);
    }

    private Statistics computeOneRowRelation(List<NamedExpression> projects) {
        Map<Expression, ColumnStatistic> columnStatsMap = projects.stream()
                .map(project -> {
                    ColumnStatistic statistic = new ColumnStatisticBuilder().setNdv(1).build();
                    // TODO: compute the literal size
                    return Pair.of(project.toSlot(), statistic);
                })
                .collect(Collectors.toMap(Pair::key, Pair::value, (item1, item2) -> item1));
        int rowCount = 1;
        return new Statistics(rowCount, 1, columnStatsMap);
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
                .collect(Collectors.toMap(Pair::key, Pair::value, (item1, item2) -> item1));
        int rowCount = 0;
        return new Statistics(rowCount, 1, columnStatsMap);
    }

    private Statistics computeUnion(Union union) {
        // TODO: refactor this for one row relation
        List<SlotReference> head;
        Statistics headStats;
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
        }

        head = childOutputs.get(0);
        headStats = childStats.get(0);

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
        return statisticsBuilder.setWidthInJoinCluster(1).build();
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
        return statisticsBuilder.setWidthInJoinCluster(1).build();
    }

    private Statistics computeIntersect(SetOperation setOperation) {
        Statistics leftChildStats = groupExpression.childStatistics(0);
        double rowCount = leftChildStats.getRowCount();
        for (int i = 1; i < setOperation.getArity(); ++i) {
            rowCount = Math.min(rowCount, groupExpression.childStatistics(i).getRowCount());
        }
        double minProd = Double.POSITIVE_INFINITY;
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
        return new StatisticsBuilder(leftChildStats.withRowCountAndEnforceValid(rowCount))
                .setWidthInJoinCluster(1).build();
    }

    private Statistics computeGenerate(Generate generate) {
        Statistics stats = groupExpression.childStatistics(0);
        int statsFactor = ConnectContext.get().getSessionVariable().generateStatsFactor;
        double count = stats.getRowCount() * generate.getGeneratorOutput().size() * statsFactor;
        Map<Expression, ColumnStatistic> columnStatsMap = Maps.newHashMap();
        for (Map.Entry<Expression, ColumnStatistic> entry : stats.columnStatistics().entrySet()) {
            ColumnStatistic columnStatistic = new ColumnStatisticBuilder(entry.getValue()).setCount(count).build();
            columnStatsMap.put(entry.getKey(), columnStatistic);
        }
        for (Slot output : generate.getGeneratorOutput()) {
            ColumnStatistic columnStatistic = new ColumnStatisticBuilder()
                    .setCount(count)
                    .setMinValue(Double.NEGATIVE_INFINITY)
                    .setMaxValue(Double.POSITIVE_INFINITY)
                    .setNdv(count)
                    .setNumNulls(0)
                    .setAvgSizeByte(output.getDataType().width())
                    .build();
            columnStatsMap.put(output, columnStatistic);
        }
        return new Statistics(count, 1, columnStatsMap);
    }

    private Statistics computeWindow(Window windowOperator) {
        Statistics childStats = groupExpression.childStatistics(0);
        Map<Expression, ColumnStatistic> childColumnStats = childStats.columnStatistics();
        Map<Expression, ColumnStatistic> columnStatisticMap = windowOperator.getWindowExpressions().stream()
                .map(expr -> {
                    Preconditions.checkArgument(expr instanceof Alias
                                    && expr.child(0) instanceof WindowExpression,
                            "need WindowExpression, but we meet " + expr);
                    WindowExpression windExpr = (WindowExpression) expr.child(0);
                    ColumnStatisticBuilder colStatsBuilder = new ColumnStatisticBuilder();
                    colStatsBuilder.setCount(childStats.getRowCount())
                            .setOriginal(null);

                    Double partitionCount = windExpr.getPartitionKeys().stream().map(key -> {
                        ColumnStatistic keyStats = childStats.findColumnStatistics(key);
                        if (keyStats == null) {
                            keyStats = new ExpressionEstimation().visit(key, childStats);
                        }
                        return keyStats;
                    })
                            .filter(columnStatistic -> !columnStatistic.isUnKnown)
                            .map(colStats -> colStats.ndv).max(Double::compare)
                            .orElseGet(() -> -1.0);

                    if (partitionCount == -1.0) {
                        // partition key stats are all unknown
                        colStatsBuilder.setCount(childStats.getRowCount())
                                .setNdv(1)
                                .setMinValue(Double.NEGATIVE_INFINITY)
                                .setMaxValue(Double.POSITIVE_INFINITY);
                    } else {
                        partitionCount = Math.max(1, partitionCount);
                        if (windExpr.getFunction() instanceof AggregateFunction) {
                            if (windExpr.getFunction() instanceof Count) {
                                colStatsBuilder.setNdv(1)
                                        .setMinValue(0)
                                        .setMinExpr(new IntLiteral(0))
                                        .setMaxValue(childStats.getRowCount())
                                        .setMaxExpr(new IntLiteral((long) childStats.getRowCount()));
                            } else if (windExpr.getFunction() instanceof Min
                                    || windExpr.getFunction() instanceof Max) {
                                Expression minmaxChild = windExpr.getFunction().child(0);
                                ColumnStatistic minChildStats = new ExpressionEstimation()
                                        .visit(minmaxChild, childStats);
                                colStatsBuilder.setNdv(1)
                                        .setMinValue(minChildStats.minValue)
                                        .setMinExpr(minChildStats.minExpr)
                                        .setMaxValue(minChildStats.maxValue)
                                        .setMaxExpr(minChildStats.maxExpr);
                            } else {
                                // sum/avg
                                colStatsBuilder.setNdv(1).setMinValue(Double.NEGATIVE_INFINITY)
                                        .setMaxValue(Double.POSITIVE_INFINITY);
                            }
                        } else {
                            // rank/dense_rank/row_num ...
                            colStatsBuilder.setNdv(childStats.getRowCount() / partitionCount)
                                    .setMinValue(0)
                                    .setMinExpr(new IntLiteral(0))
                                    .setMaxValue(childStats.getRowCount())
                                    .setMaxExpr(new IntLiteral((long) childStats.getRowCount()));
                        }
                    }
                    return Pair.of(expr.toSlot(), colStatsBuilder.build());
                }).collect(Collectors.toMap(Pair::key, Pair::value, (item1, item2) -> item1));
        columnStatisticMap.putAll(childColumnStats);
        return new Statistics(childStats.getRowCount(), 1, columnStatisticMap);
    }

    private ColumnStatistic unionColumn(ColumnStatistic leftStats, double leftRowCount, ColumnStatistic rightStats,
            double rightRowCount, DataType dataType) {
        if (leftStats.isUnKnown() || rightStats.isUnKnown()) {
            return new ColumnStatisticBuilder(leftStats).setCount(leftRowCount + rightRowCount).build();
        }
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

    @Override
    public Statistics visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, Void context) {
        StatisticsBuilder builder = new StatisticsBuilder(groupExpression.childStatistics(0));
        Statistics statistics = builder.setWidthInJoinCluster(1).build();
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
        Statistics consumerStats = new Statistics(prodStats.getRowCount(), 1, new HashMap<>());
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
        Statistics statistics = new StatisticsBuilder(groupExpression.childStatistics(0))
                .setWidthInJoinCluster(1).build();
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
        Statistics consumerStats = new Statistics(prodStats.getRowCount(), 1, new HashMap<>());
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
