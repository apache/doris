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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.planner.PlanNodeAndHash;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatisticsEntry;
import org.apache.doris.statistics.HboPlanInfoProvider;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.ScanPlanStatistics;

import com.google.common.collect.ImmutableList;
import static com.google.common.hash.Hashing.sha256;
import static java.lang.Double.isNaN;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HboUtils {

    public static Optional<Integer> getAccurateStatsIndex(
            RecentRunsPlanStatistics RecentRunsPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            // TODO: wrap the following multiple control config into a stretagy structure as a whole
            double rowThreshold,
            double hboRfSafeThreshold,
            boolean needMatchPartition,
            boolean needMatchPartitionColumnPredicate,
            boolean needMatchOtherColumnPredicate)
    {
        List<RecentRunsPlanStatisticsEntry> lastRunsStatistics = RecentRunsPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int lastRunsIndex = 0; lastRunsIndex < lastRunsStatistics.size(); ++lastRunsIndex) {
            if (inputTableStatistics.size() != lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().size()) {
                continue;
            }
            boolean accurateMatch = true;
            for (int inputTablesIndex = 0; accurateMatch && inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                ScanPlanStatistics currentInputStatistics = (ScanPlanStatistics) inputTableStatistics.get(inputTablesIndex);
                ScanPlanStatistics historicalInputStatistics = (ScanPlanStatistics) lastRunsStatistics.get(lastRunsIndex)
                        .getInputTableStatistics().get(inputTablesIndex);
                // check if rf safe
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(hboRfSafeThreshold);
                if (!isRFSafe) {
                    accurateMatch = false;
                } else if (!currentInputStatistics.isPartitionedTable() && !historicalInputStatistics.isPartitionedTable()) {
                    accurateMatch = canAccurateMatchForNonPartitionTable(currentInputStatistics, historicalInputStatistics);
                } else if (currentInputStatistics.isPartitionedTable() && historicalInputStatistics.isPartitionedTable()) {
                    // find the first full matching entry in lastRunEntries
                    accurateMatch = canAccurateMatchForPartitionTable(currentInputStatistics, historicalInputStatistics, rowThreshold,
                            needMatchPartition, needMatchPartitionColumnPredicate, needMatchOtherColumnPredicate);
                } else {
                    throw new RuntimeException("unexpected state during hbo input table stats matching");
                }
            }
            if (accurateMatch) {
                return Optional.of(lastRunsIndex);
            }
        }
        return Optional.empty();
    }

    public static boolean canAccurateMatchForNonPartitionTable(ScanPlanStatistics currentInputStatistics,
            ScanPlanStatistics historicalInputStatistics) {
        return currentInputStatistics.hasSameOtherPredicates(historicalInputStatistics);
    }

    public static boolean canAccurateMatchForPartitionTable(ScanPlanStatistics currentInputStatistics,
            ScanPlanStatistics historicalInputStatistics, double rowThreshold,
            boolean needMatchSelectedPartition, boolean needMatchPartitionColumnPredicate, boolean needMatchOtherPredicate) {
        // for partition table, must ensure
        // 1. the pruned partition is the same
        // 2. partition column predicate is the exactly same(for single value partition it is not sepecial, but for range partition,
        //    although the select partition id is the same, but the partition column filter may not be the same, which may have impact
        //    on the hbo cache searching and matching)
        // 3. the other predicate with the constant is the same
        boolean hasSamePartition = currentInputStatistics.hasSamePartitionId(historicalInputStatistics);
        boolean hasSamePartitionColumnPredicate = currentInputStatistics.hasSamePartitionColumnPredicates(historicalInputStatistics);
        boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(historicalInputStatistics);
        boolean hasSimilarStats = true;//similarStats(currentInputStatistics.getOutputRows(),
                //historicalInputStatistics.getOutputRows(), rowThreshold);
        if (needMatchSelectedPartition && needMatchPartitionColumnPredicate && needMatchOtherPredicate) {
            return hasSamePartition && hasSamePartitionColumnPredicate && hasSameOtherPredicate;
        } else if (needMatchSelectedPartition && !needMatchPartitionColumnPredicate && needMatchOtherPredicate) {
            return hasSamePartition && hasSameOtherPredicate && hasSimilarStats;
        } else if (needMatchSelectedPartition && needMatchPartitionColumnPredicate && !needMatchOtherPredicate) {
            return hasSamePartition && hasSamePartitionColumnPredicate && hasSimilarStats;
        } else if (needMatchSelectedPartition && !needMatchPartitionColumnPredicate && !needMatchOtherPredicate) {
            return hasSamePartition && hasSimilarStats;
        } else {
            return hasSimilarStats;
        }
    }

    public static Optional<Integer> getSimilarStatsIndex(
            RecentRunsPlanStatistics RecentRunsPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double rowThreshold, double hboRfSafeThreshold)
    {
        List<RecentRunsPlanStatisticsEntry> lastRunsStatistics = RecentRunsPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int lastRunsIndex = 0; lastRunsIndex < lastRunsStatistics.size(); ++lastRunsIndex) {
            if (inputTableStatistics.size() != lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().size()) {
                // This is not expected, but may happen when changing thrift definitions.
                continue;
            }
            boolean rowSimilarity = true;
            //boolean outputSizeSimilarity = true;

            // Match to historical stats only when size of input tables are similar to those of historical runs.
            for (int inputTablesIndex = 0; rowSimilarity && inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                PlanStatistics currentInputStatistics = inputTableStatistics.get(inputTablesIndex);
                PlanStatistics historicalInputStatistics = lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().get(inputTablesIndex);
                // check if rf safe
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(hboRfSafeThreshold);
                if (!isRFSafe) {
                    rowSimilarity = false;
                } else {
                    rowSimilarity = similarStats(currentInputStatistics.getOutputRows(),
                            historicalInputStatistics.getOutputRows(), rowThreshold);
                }
                //outputSizeSimilarity = outputSizeSimilarity
                // && similarStats(currentInputStatistics.getOutputSize().getValue(),
                // historicalInputStatistics.getOutputSize().getValue(), threshold);
            }
            // Write information if both rows and output size are similar.
            if (rowSimilarity/* && outputSizeSimilarity*/) {
                return Optional.of(lastRunsIndex);
            }
        }
        return Optional.empty();
    }

    public static boolean similarStats(double stats1, double stats2, double threshold)
    {
        if (isNaN(stats1) && isNaN(stats2)) {
            return true;
        }
        return stats1 >= (1 - threshold) * stats2 && stats1 <= (1 + threshold) * stats2;
    }

    public static void collectScans(AbstractPlan planNode, List<String> scanList) {
        if (planNode instanceof LogicalOlapScan) {
            scanList.add(((OlapScan) planNode).getTable().getNameWithFullQualifiers() + ((LogicalOlapScan) planNode).getRelationId().toString());
        } else if (planNode instanceof PhysicalOlapScan) {
            scanList.add(((OlapScan) planNode).getTable().getNameWithFullQualifiers() + ((PhysicalOlapScan) planNode).getRelationId().toString());
        } else if (planNode instanceof GroupPlan
                && !((GroupPlan) planNode).getGroup().getLogicalExpressions().isEmpty()
                && ((GroupPlan) planNode).getGroup()
                .getLogicalExpressions().get(0).getPlan() instanceof AbstractLogicalPlan) {
            Plan logicalPlan = ((GroupPlan) planNode).getGroup().getLogicalExpressions().get(0).getPlan();
            collectScans((AbstractPlan) logicalPlan, scanList);
        } else if (planNode instanceof GroupPlan
                && ((GroupPlan) planNode).getGroup().getLogicalExpressions().isEmpty()
                && !((GroupPlan) planNode).getGroup().getPhysicalExpressions().isEmpty()
                && ((GroupPlan) planNode).getGroup()
                .getPhysicalExpressions().get(0).getPlan() instanceof AbstractPhysicalPlan) {
            Plan physicalPlan = ((GroupPlan) planNode).getGroup().getPhysicalExpressions().get(0).getPlan();
            collectScans((AbstractPlan) physicalPlan, scanList);
        } else {
            for (Object child : planNode.children()) {
                collectScans((AbstractPlan) child, scanList);
            }
        }
    }

    public static String hashCanonicalPlan(String planString) {
        return sha256().hashString(planString, UTF_8).toString();
    }

    public static Optional<RecentRunsPlanStatisticsEntry> getSelectedHboPlanStatisticsEntry(
            RecentRunsPlanStatistics oldHboPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double historyMatchingThreshold,
            double hboRfSafeThreshold,
            boolean isEnableHboNonStrictMatchingMode) {
        List<RecentRunsPlanStatisticsEntry> lastRunsStatistics = oldHboPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        // TODO: if only non-partition table exists, the following 4 steps may be redundant
        // MATCH 1:
        // firstly full matching, i.e, the same partition ids,
        //                             the same other predicate with the same constant
        // it is mainly for accurate matching under RETRY
        // by design, the accurate entry in the lastRunEntries will have only ONE entry
        Optional<Integer> accurateStatsIndex = HboUtils.getAccurateStatsIndex(
                oldHboPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold,
                true, true, true);
        if (accurateStatsIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(accurateStatsIndex.get()));
        }

        // MATCH 2:
        Optional<Integer> accurateStatsMatchPartitionIdAndOtherPredicateIndex = HboUtils.getAccurateStatsIndex(
                oldHboPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold,
                true, false, true);
        if (accurateStatsMatchPartitionIdAndOtherPredicateIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(accurateStatsMatchPartitionIdAndOtherPredicateIndex.get()));
        }

        if (isEnableHboNonStrictMatchingMode) {
            // MATCH 3: TODO: reconsider this option's safety
            Optional<Integer> accurateStatsOnlyMatchPartitionIdIndex
                    = HboUtils.getAccurateStatsIndex(
                    oldHboPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold,
                    true, false, false);
            if (accurateStatsOnlyMatchPartitionIdIndex.isPresent()) {
                return Optional.of(lastRunsStatistics.get(accurateStatsOnlyMatchPartitionIdIndex.get()));
            }

            // MATCH 4: TODO: this option is actually useless since the inputTableStatistics is not exactly the current input
            // but actually a mocked one with the non-current row count (TODO: NEED TO check presto's action otherwise it will always match and introduce risk)
            Optional<Integer> similarStatsIndex = HboUtils.getSimilarStatsIndex(
                    oldHboPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold);
            if (similarStatsIndex.isPresent()) {
                return Optional.of(lastRunsStatistics.get(similarStatsIndex.get()));
            }
        }
        // TODO: Use linear regression to predict stats if we have only 1 table.
        return Optional.empty();
    }

    public static PlanNodeAndHash getPlanNodeHash(AbstractPlan planNode) {
        String hash;
        if (planNode instanceof AbstractPhysicalPlan) {
            hash = planNode.hboTreeString();
            hash = HboUtils.hashCanonicalPlan(hash);
        } else if (planNode instanceof AbstractLogicalPlan) {
            hash = planNode.hboTreeString();
            hash = HboUtils.hashCanonicalPlan(hash);
        } else {
            throw new IllegalStateException("hbo get neither physical plan nor logical plan");
        }
        PlanNodeAndHash PlanNodeAndHash = new PlanNodeAndHash(planNode, Optional.of(hash));
        return PlanNodeAndHash;
    }

    private static Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(
            List<PlanStatistics> currentInputTableStatistics, ConnectContext connectContext, boolean cacheOnly) {
        HboPlanStatisticsManager hboManager = HboPlanStatisticsManager.getInstance();
        HboPlanInfoProvider idToMapProvider = hboManager.getHboPlanInfoProvider();

        String queryId = DebugUtil.printId(connectContext.queryId());
        Map<RelationId, Set<Expression>> tableToExprMap = idToMapProvider.getTableToExprMap(queryId);
        // FIXME: current queryId's idToPlanMap is NOT available NOW
        //Map<Integer, PhysicalPlan> idToPlanMap = idToMapProvider.getIdToPlanMap(queryId);
        ImmutableList.Builder<PlanStatistics> outputTableStatisticsBuilder = ImmutableList.builder();

        for (PlanStatistics inputTableStatistics : currentInputTableStatistics) {
            //int tableNodeId = inputTableStatistics.getNodeId();
            //PhysicalPlan planNode = idToPlanMap.get(tableNodeId);
            //if (!(planNode instanceof PhysicalOlapScan)) {
            //    throw new RuntimeException("unexpected plan node type");
            //}
            PhysicalOlapScan tableScan = ((ScanPlanStatistics) inputTableStatistics).getTable();
            Set<Expression> tableFilterSet = tableToExprMap.get(tableScan.getRelationId());

            // here is the assumption that the table is always same with different table id(TODO: verify this)
            ScanPlanStatistics newInputPlanStatistics = new ScanPlanStatistics(inputTableStatistics, tableScan, tableFilterSet,
                    tableScan.getTable().isPartitionedTable(), tableScan.getTable().getPartitionInfo(),
                    tableScan.getSelectedPartitionIds());
            outputTableStatisticsBuilder.add(newInputPlanStatistics);
        }
        return Optional.of(outputTableStatisticsBuilder.build());
    }

    public static PlanStatistics getMatchedPlanStatistics(RecentRunsPlanStatistics planStatistics,
            ConnectContext connectContext) {
        PlanStatistics matchedPlanStatistics = null;
        // TODO: get current inputTableStatistics
        // use the following getPlanNodeInputTableStatistics may lead a problem
        // case1: select xxx from t where c1 = 1 vs. select xxx from t where c1 = 1 and c2 = 2
        // since the input table t will have two entries in lastRunEntries list
        // if the getPlanNodeInputTableStatistics only find the table entry by the table name, etc
        // it may find the wrong entry for the different filter pattern
        // by contract, use current planStatistics is safe because the plan hash has ensure the plan hbo string
        // is matched, although the detailed constant may note be same
        if (!planStatistics.getLastRunsStatistics().isEmpty()) {
            // TODO: the currentInputTableStatistics is the mocked one which needs to be updated with current filter, etc
            // use entry 0 or the last entry will be considered
            // entry 0: the oldest entry
            // entry last: the newest entry
            int initialSelectedIndex = planStatistics.getLastRunsStatistics().size() - 1;
            List<PlanStatistics> currentInputTableStatistics = planStatistics.getLastRunsStatistics()
                    .get(initialSelectedIndex).getInputTableStatistics();
            if (!currentInputTableStatistics.isEmpty()) {
                Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(
                        currentInputTableStatistics, connectContext, true);
                if (inputTableStatistics.isPresent()) {
                    //if (!planStatistics.getLastRunsStatistics().isEmpty() && inputTableStatistics.isPresent()) {
                    // FIXME: always get 0 will be wrong
                    // since the existing cache always has entry 0 and it will always hit entry 0 all the time
                    // TODO: try to use the last entry as a replacement, refer updatePlanStatistics(latest insertion as the last index)
                    //Optional<List<PlanStatistics>> currentInputTableStatistics = Optional.of(planStatistics
                    //        .getLastRunsStatistics().get(0).getInputTableStatistics());
                    // NOTE: must update the partition and common filter at input plan statistics
                    // in order to match the filter(but actually a ScanPlanStatistics) entry in the hbo cache
                    // i.e, logical filter node is mapped to ScanPlanStatistics in hbo cache(important!!!)
                    //if (currentInputTableStatistics.isPresent()) {
                    // extract filters info out to update inputTableStatistics as a current search key
                    //List<PlanStatistics> inputTableStatistics = currentInputTableStatistics.get();
                    // TODO: for join node, it will update the filter info for the input plan statistics
                    // it will find the wrong cache entry
                    //if (isFilterOnTs) {
                    //    inputTableStatistics = updateCurrentInputTableStatisticsWithFilter(inputTableStatistics,
                    //            (Filter) originalPlanNode, (OlapScan) planNode);
                    //} else {
                    //}
                    double hboRfsafeThreshold = -1.0;
                    double rowCountMatchingThreshold = 0.1;
                    boolean isEnableHboNonStrictMatchingMode = false;
                    if (connectContext != null && connectContext.getSessionVariable() != null) {
                        hboRfsafeThreshold = connectContext.getSessionVariable().getHboRfSafeThreshold();
                        rowCountMatchingThreshold = connectContext.getSessionVariable().getHboRowMatchingThreshold();
                        isEnableHboNonStrictMatchingMode = connectContext.getSessionVariable().isEnableHboNonStrictMatchingMode();
                    }
                    Optional<RecentRunsPlanStatisticsEntry> RecentRunsPlanStatisticsEntry
                            = HboUtils.getSelectedHboPlanStatisticsEntry
                            (planStatistics, inputTableStatistics.get(), rowCountMatchingThreshold, hboRfsafeThreshold,
                                    isEnableHboNonStrictMatchingMode);
                    if (RecentRunsPlanStatisticsEntry.isPresent()) {
                        matchedPlanStatistics = RecentRunsPlanStatisticsEntry.get().getPlanStatistics();
                    }
                }
            }
        }
        return matchedPlanStatistics;
    }
}