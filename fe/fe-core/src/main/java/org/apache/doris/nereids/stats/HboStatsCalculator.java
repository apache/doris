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

import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanNodeAndHash;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.Statistics;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * HboStatsCalculator
 */
public class HboStatsCalculator extends StatsCalculator {
    private final HboPlanStatisticsProvider hboPlanStatisticsProvider;
    public HboStatsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        super(groupExpression, forbidUnknownColStats, columnStatisticMap, isPlayNereidsDump,
                cteIdToStats, context);
        this.hboPlanStatisticsProvider = requireNonNull(HboPlanStatisticsManager.getInstance()
                        .getHboPlanStatisticsProvider(), "HboPlanStatisticsProvider is null");
    }

    @Override
    public void estimate() {
        super.estimate();
    }

    // NOTE: can't override computeScan since the publishing side's plan hash of scan node
    // use the scan's hbo string but embedding the filter info into the input table structure.
    // if the matching logic here want to support filter node's hbo info's reusing, it only needs
    // to hook the computeFilter and use original scan's plan hash string, and also embedding the
    // parent filter info into the scan node also.
    @Override
    protected Statistics computeFilter(Filter filter) {
        Statistics legacyStats = super.computeFilter(filter);
        boolean isLogicalFilterOnTs = isLogicalFilterOnLogicalScan(filter);
        boolean isPhysicalFilterOnTs = isPhysicalFilterOnPhysicalScan(filter);
        if (isLogicalFilterOnTs || isPhysicalFilterOnTs) {
            return getStatsFromHbo((AbstractPlan) filter, legacyStats);
        } else {
            return legacyStats;
        }
    }

    @Override
    protected Statistics computeJoin(Join join) {
        Statistics legacyStats = super.computeJoin(join);
        return getStatsFromHbo((AbstractPlan) join, legacyStats);
    }

    @Override
    protected Statistics computeAggregate(Aggregate<? extends Plan> aggregate) {
        Statistics legacyStats = super.computeAggregate(aggregate);
        // TODO: aggr has two times matching, one is the global but logical aggr
        // another is local but physical aggr.
        // the physical one can be matched but the logical one is hard
        // e.g, logical one is like "count(*) AS `count(*)`#4"
        //      local physical one is like "partial_count(*) AS `partial_count(*)`#5"
        //      global physical one is like "count(partial_count(*)#5) AS `count(*)`#4"
        return getStatsFromHbo((AbstractPlan) aggregate, legacyStats);
    }

    private boolean isLogicalFilterOnLogicalScan(Filter filter) {
        if (filter instanceof LogicalFilter
            && ((LogicalFilter) filter).child() instanceof GroupPlan
            && ((GroupPlan) ((LogicalFilter) filter).child()).getGroup() != null
            && !((GroupPlan) ((LogicalFilter) filter).child()).getGroup().getLogicalExpressions().isEmpty()
            && ((GroupPlan) ((LogicalFilter) filter).child()).getGroup().getLogicalExpressions().get(0).getPlan() instanceof LogicalOlapScan) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isPhysicalFilterOnPhysicalScan(Filter filter) {
        if (filter instanceof PhysicalFilter
                && ((PhysicalFilter) filter).child() instanceof GroupPlan
                && ((GroupPlan) ((PhysicalFilter) filter).child()).getGroup() != null
                && !((GroupPlan) ((PhysicalFilter) filter).child()).getGroup().getPhysicalExpressions().isEmpty()
                && ((GroupPlan) ((PhysicalFilter) filter).child()).getGroup().getPhysicalExpressions().get(0).getPlan() instanceof PhysicalOlapScan) {
            return true;
        } else {
            return false;
        }
    }

    private Statistics getStatsFromHbo(AbstractPlan planNode, Statistics delegateStats) {
        if (planNode instanceof Filter) {
            // handle filter to access scan, whose child is scan which ensured before
            if (isLogicalFilterOnLogicalScan((Filter) planNode)) {
                planNode = (LogicalOlapScan) ((GroupPlan) ((LogicalFilter) planNode).child())
                        .getGroup().getLogicalExpressions().get(0).getPlan();
            } else if (isPhysicalFilterOnPhysicalScan((Filter) planNode)) {
                planNode = (PhysicalOlapScan) ((GroupPlan) ((PhysicalFilter) planNode).child())
                        .getGroup().getPhysicalExpressions().get(0).getPlan();
            } else {
                throw new AnalysisException("unexpected filter type");
            }
        }
        PlanNodeAndHash PlanNodeAndHash = HboUtils.getPlanNodeHash(planNode);
        RecentRunsPlanStatistics planStatistics = hboPlanStatisticsProvider.getHboStats(PlanNodeAndHash);
        PlanStatistics matchedPlanStatistics = HboUtils.getMatchedPlanStatistics(planStatistics,
                cascadesContext.getConnectContext());
        if (matchedPlanStatistics != null) {
            delegateStats = delegateStats.withRowCountAndHboFlag(matchedPlanStatistics.getOutputRows());
        }
        return delegateStats;
    }

    /*
    private List<PlanStatistics> updateCurrentInputTableStatisticsByTableExprMap(List<PlanStatistics> currentInputTableStatistics) {
        HboPlanStatisticsManager hboManager = HboPlanStatisticsManager.getInstance();
        HboPlanInfoProvider idToMapProvider = hboManager.getHboPlanInfoProvider();
        String queryId = DebugUtil.printId(cascadesContext.getConnectContext().queryId());
        Map<TableIf, Set<Expression>> tableToExprMap = idToMapProvider.getTableToExprMap(queryId);
    }

    private List<PlanStatistics> updateCurrentInputTableStatisticsWithFilter(List<PlanStatistics> currentInputTableStatistics,
            Filter filter, OlapScan scan) {
        if (currentInputTableStatistics.size() != 1) {
            throw new AnalysisException("unexpected status that filter's inputPlanStatistics doesn't have 1 entry");
        }
        ImmutableList.Builder<PlanStatistics> outputTableStatisticsBuilder = ImmutableList.builder();
        Set<Expression> tableFilterSet = filter.getConjuncts();
        PlanStatistics inputPlanStatistics = currentInputTableStatistics.get(0);
        ScanPlanStatistics newInputPlanStatistics = new ScanPlanStatistics(inputPlanStatistics, tableFilterSet,
                scan.getTable().isPartitionedTable(), scan.getTable().getPartitionInfo(), scan.getSelectedPartitionIds());
        outputTableStatisticsBuilder.add(newInputPlanStatistics);
        return outputTableStatisticsBuilder.build();
    }*/



    /*
    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(AbstractPlan planNode,
                List<PlanStatistics> currentInputTableStatistics, boolean cacheOnly) {
        ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
        Set<LogicalOlapScan> scans = new HashSet<>();
        // TODO: cbo stage may miss some scan during memo tree visiting, use the entry 0 instead + replacement
        HboUtils.collectScans(planNode, scans);
        for (LogicalOlapScan scan : scans) {
            // FIXME: logical scan not contains filter info and can't match the physical filter's info
            // consider the case that date_dim first with d_moy = 7 but the second not, it will find the entry 0
            // but correct entry is 1
            String hash = scan.hboTreeString();
            hash = HboUtils.hashCanonicalPlan(hash);
            PlanNodeAndHash PlanNodeAndHash = new PlanNodeAndHash(scan, Optional.of(hash));
            RecentRunsPlanStatistics RecentRunsPlanStatistics = HboPlanStatisticsProvider
                    .getHboStats(PlanNodeAndHash);
            if (RecentRunsPlanStatistics.equals(RecentRunsPlanStatistics.empty())) {
                return Optional.empty();
            } else {
                // TODO: first match checking based on accurate partition info
                // otherwise, use the entry 0 since the param number has been considered in plan hash
                // note: next round, the accurate matching entry will be added into and will be matched next time (TODO: testing)
                PlanStatistics planStatistics = RecentRunsPlanStatistics.getLastRunsStatistics()
                        .get(0).getPlanStatistics();
                inputTableStatisticsBuilder.add(planStatistics);
            }
        }

        return Optional.of(inputTableStatisticsBuilder.build());
    }*/
}

