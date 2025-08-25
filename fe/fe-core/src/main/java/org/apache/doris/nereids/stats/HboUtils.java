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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanNodeAndHash;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.hbo.InputTableStatisticsInfo;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.PlanStatisticsMatchStrategy;
import org.apache.doris.statistics.hbo.PlanStatisticsWithInputInfo;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatisticsEntry;
import org.apache.doris.statistics.hbo.ScanPlanStatistics;
import org.apache.doris.thrift.TPlanNodeRuntimeStatsItem;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Hbo utils.
 */
public class HboUtils {

    /**
     * Get accurate stats index
     * @param recentRunsPlanStatistics recentRunsPlanStatistics
     * @param inputTableStatistics inputTableStatistics
     * @param rfSafeThreshold rfSafeThreshold
     * @param isEnableHboNonStrictMatchingMode isEnableHboNonStrictMatchingMode
     * @param strategy match strategy
     * @return accurate stats index
     */
    public static Optional<Integer> getAccurateStatsIndex(
            RecentRunsPlanStatistics recentRunsPlanStatistics,
            List<PlanStatistics> inputTableStatistics, double rfSafeThreshold,
            boolean isEnableHboNonStrictMatchingMode, PlanStatisticsMatchStrategy strategy) {
        List<RecentRunsPlanStatisticsEntry> recentRunsStatistics = recentRunsPlanStatistics.getRecentRunsStatistics();
        if (recentRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int recentRunsIndex = 0; recentRunsIndex < recentRunsStatistics.size(); ++recentRunsIndex) {
            if (inputTableStatistics.size() != recentRunsStatistics.get(recentRunsIndex)
                    .getInputTableStatistics().size()) {
                continue;
            }
            boolean accurateMatch = true;
            for (int inputTableIndex = 0; accurateMatch
                    && inputTableIndex < inputTableStatistics.size(); ++inputTableIndex) {
                ScanPlanStatistics curInputStatistics = (ScanPlanStatistics) inputTableStatistics.get(inputTableIndex);
                ScanPlanStatistics historicalInputStatistics =
                        (ScanPlanStatistics) recentRunsStatistics.get(recentRunsIndex)
                                .getInputTableStatistics().get(inputTableIndex);
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(rfSafeThreshold);
                if (!isRFSafe) {
                    accurateMatch = false;
                } else if (!curInputStatistics.isPartitionedTable()
                        && !historicalInputStatistics.isPartitionedTable()) {
                    accurateMatch = canAccurateMatchForNonPartitionTable(curInputStatistics,
                            historicalInputStatistics, strategy);
                } else if (curInputStatistics.isPartitionedTable()
                        && historicalInputStatistics.isPartitionedTable()) {
                    // find the first full matching entry in recentRunEntries
                    accurateMatch = canAccurateMatchForPartitionTable(curInputStatistics,
                            historicalInputStatistics, isEnableHboNonStrictMatchingMode, strategy);
                } else {
                    throw new RuntimeException("unexpected state during hbo input table stats matching");
                }
            }
            if (accurateMatch) {
                return Optional.of(recentRunsIndex);
            }
        }
        return Optional.empty();
    }

    /**
     * Can accurate match for Non-Partition table
     * @param currentInputStatistics currentInputStatistics
     * @param recentRunsInputStatistics recentRunsInputStatistics
     * @return can accurate match for Non-Partition table.
     */
    public static boolean canAccurateMatchForNonPartitionTable(ScanPlanStatistics currentInputStatistics,
            ScanPlanStatistics recentRunsInputStatistics, PlanStatisticsMatchStrategy strategy) {
        boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(recentRunsInputStatistics);
        if (strategy.equals(PlanStatisticsMatchStrategy.OTHER_ONLY_MATCH)
                || strategy.equals(PlanStatisticsMatchStrategy.FULL_MATCH)
                || strategy.equals(PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH)) {
            return hasSameOtherPredicate;
        } else {
            return false;
        }
    }

    /**
     * Can accurate match for partition table
     * @param currentInputStatistics currentInputStatistics
     * @param recentRunsInputStatistics historicalInputStatistics
     * @param strategy strategy
     * @return can accurate match for partition table
     */
    public static boolean canAccurateMatchForPartitionTable(ScanPlanStatistics currentInputStatistics,
            ScanPlanStatistics recentRunsInputStatistics, boolean isEnableHboNonStrictMatchingMode,
            PlanStatisticsMatchStrategy strategy) {
        // For partition table, must ensure
        // 1. the pruned partition is the same
        // 2. partition column predicate is the exactly same(for single value partition it is not special,
        // but for range partition, although the select partition id is the same, but the partition column
        // filter may not be the same, which may have impact on the hbo cache matching)
        // 3. the other predicate with the constant is the same
        boolean hasSamePartition = currentInputStatistics.hasSamePartitionId(recentRunsInputStatistics);
        boolean hasSamePartitionColumnPredicate = currentInputStatistics
                .hasSamePartitionColumnPredicates(recentRunsInputStatistics);
        boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(recentRunsInputStatistics);
        //boolean hasSimilarStats = true;
        //hasSimilarStats = similarStats(currentInputStatistics.getOutputRows(),
        // recentRunsInputStatistics.getOutputRows(), rowThreshold);
        if (strategy.equals(PlanStatisticsMatchStrategy.FULL_MATCH)) {
            return hasSamePartition && hasSamePartitionColumnPredicate && hasSameOtherPredicate;
        } else if (strategy.equals(PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH)) {
            return hasSamePartition && hasSameOtherPredicate/* && hasSimilarStats*/;
        } else if (isEnableHboNonStrictMatchingMode
                && strategy.equals(PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH)) {
            return hasSamePartition;
        //} else if (needMatchSelectedPartition && needMatchPartitionColumnPredicate && !needMatchOtherPredicate) {
        //    return hasSamePartition && hasSamePartitionColumnPredicate && hasSimilarStats;
        //} else if (needMatchSelectedPartition && !needMatchPartitionColumnPredicate && !needMatchOtherPredicate) {
        //    return hasSamePartition && hasSimilarStats;
        } else {
            return false;
        }
    }

    /**
     * Get similar stats index
     * @param recentRunsPlanStatistics recentRunsPlanStatistics
     * @param inputTableStatistics inputTableStatistics
     * @param rowThreshold rowThreshold
     * @param hboRfSafeThreshold hboRfSafeThreshold
     * @return similar StatsIndex
     */
    public static Optional<Integer> getSimilarStatsIndex(
            RecentRunsPlanStatistics recentRunsPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double rowThreshold, double hboRfSafeThreshold) {
        List<RecentRunsPlanStatisticsEntry> recentRunsStatistics = recentRunsPlanStatistics.getRecentRunsStatistics();
        if (recentRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int recentRunsIndex = 0; recentRunsIndex < recentRunsStatistics.size(); ++recentRunsIndex) {
            if (inputTableStatistics.size() != recentRunsStatistics.get(recentRunsIndex)
                    .getInputTableStatistics().size()) {
                continue;
            }
            boolean rowSimilarity = true;
            for (int inputTablesIndex = 0; rowSimilarity
                    && inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                PlanStatistics currentInputStatistics = inputTableStatistics.get(inputTablesIndex);
                PlanStatistics historicalInputStatistics = recentRunsStatistics.get(recentRunsIndex)
                        .getInputTableStatistics().get(inputTablesIndex);
                // check if rf safe
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(hboRfSafeThreshold);
                if (!isRFSafe) {
                    rowSimilarity = false;
                } else {
                    rowSimilarity = similarStats(currentInputStatistics.getOutputRows(),
                            historicalInputStatistics.getOutputRows(), rowThreshold);
                }
            }
            if (rowSimilarity) {
                return Optional.of(recentRunsIndex);
            }
        }
        return Optional.empty();
    }

    /**
     * similarStats
     * @param stats1 stats1
     * @param stats2 stats2
     * @param threshold threshold
     * @return similar Stats
     */
    public static boolean similarStats(double stats1, double stats2, double threshold) {
        if (Double.isNaN(stats1) && Double.isNaN(stats2)) {
            return true;
        }
        return stats1 >= (1 - threshold) * stats2 && stats1 <= (1 + threshold) * stats2;
    }

    /**
     * Collect full qualifier of scan with relation id info, for distinguishing the same table in the query.
     * This is mainly for pattern matching of hbo, supporting different join orders, such as a join b and b join a
     * matching the same hbo stats. info.
     * @param planNode planNode
     * @param scanQualifierList scan qualifier list
     */
    public static void collectScanQualifierList(AbstractPlan planNode, List<String> scanQualifierList) {
        if (planNode instanceof LogicalOlapScan) {
            scanQualifierList.add(((LogicalOlapScan) planNode).getQualifierWithRelationId());
        } else if (planNode instanceof PhysicalOlapScan) {
            scanQualifierList.add(((PhysicalOlapScan) planNode).getQualifierWithRelationId());
        } else if (planNode instanceof GroupPlan
                && !((GroupPlan) planNode).getGroup().getLogicalExpressions().isEmpty()
                && ((GroupPlan) planNode).getGroup()
                .getLogicalExpressions().get(0).getPlan() instanceof AbstractLogicalPlan) {
            Plan logicalPlan = ((GroupPlan) planNode).getGroup().getLogicalExpressions().get(0).getPlan();
            collectScanQualifierList((AbstractPlan) logicalPlan, scanQualifierList);
        } else if (planNode instanceof GroupPlan
                && ((GroupPlan) planNode).getGroup().getLogicalExpressions().isEmpty()
                && !((GroupPlan) planNode).getGroup().getPhysicalExpressions().isEmpty()
                && ((GroupPlan) planNode).getGroup()
                .getPhysicalExpressions().get(0).getPlan() instanceof AbstractPhysicalPlan) {
            Plan physicalPlan = ((GroupPlan) planNode).getGroup().getPhysicalExpressions().get(0).getPlan();
            collectScanQualifierList((AbstractPlan) physicalPlan, scanQualifierList);
        } else {
            for (Object child : planNode.children()) {
                collectScanQualifierList((AbstractPlan) child, scanQualifierList);
            }
        }
    }

    /**
     * Get plan fingerprint hash value
     * @param planFingerprint plan fingerprint
     * @return plan fingerprint hash value
     */
    public static String getPlanFingerprintHash(String planFingerprint) {
        return Hashing.sha256().hashString(planFingerprint, StandardCharsets.UTF_8).toString();
    }

    /**
     * Get matched hbo plan stats. entry.
     * @param recentHboPlanStatistics recentHboPlanStatistics
     * @param inputTableStatistics inputTableStatistics
     * @param hboRfSafeThreshold hboRfSafeThreshold
     * @param isEnableHboNonStrictMatchingMode isEnableHboNonStrictMatchingMode
     * @return matched hbo plan stats. entry.
     */
    public static Optional<RecentRunsPlanStatisticsEntry> getMatchedHboPlanStatisticsEntry(
            RecentRunsPlanStatistics recentHboPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double hboRfSafeThreshold,
            boolean isEnableHboNonStrictMatchingMode) {
        List<RecentRunsPlanStatisticsEntry> recentRunsStatistics = recentHboPlanStatistics.getRecentRunsStatistics();
        if (recentRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        // TODO: if only non-partition table exists, the following steps may be redundant.
        // MATCH 1: PlanStatisticsMatchStrategy.FULL_MATCH
        Optional<Integer> accurateStatsIndex = HboUtils.getAccurateStatsIndex(
                recentHboPlanStatistics, inputTableStatistics, hboRfSafeThreshold,
                isEnableHboNonStrictMatchingMode, PlanStatisticsMatchStrategy.FULL_MATCH);
        if (accurateStatsIndex.isPresent()) {
            return Optional.of(recentRunsStatistics.get(accurateStatsIndex.get()));
        }

        // MATCH 2: PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH
        Optional<Integer> accurateStatsMatchPartitionIdAndOtherPredicateIndex = HboUtils.getAccurateStatsIndex(
                recentHboPlanStatistics, inputTableStatistics, hboRfSafeThreshold,
                isEnableHboNonStrictMatchingMode, PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH);
        if (accurateStatsMatchPartitionIdAndOtherPredicateIndex.isPresent()) {
            return Optional.of(recentRunsStatistics.get(accurateStatsMatchPartitionIdAndOtherPredicateIndex.get()));
        }

        // MATCH 3: PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH
        if (isEnableHboNonStrictMatchingMode) {
            Optional<Integer> accurateStatsOnlyMatchPartitionIdIndex
                    = HboUtils.getAccurateStatsIndex(
                    recentHboPlanStatistics, inputTableStatistics, hboRfSafeThreshold,
                    true, PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH);
            if (accurateStatsOnlyMatchPartitionIdIndex.isPresent()) {
                return Optional.of(recentRunsStatistics.get(accurateStatsOnlyMatchPartitionIdIndex.get()));
            }

            // MATCH 4: TODO: this option is actually useless since the inputTableStatistics is not exactly
            // the current input, but actually a mocked one with the non-current row count.
            //Optional<Integer> similarStatsIndex = HboUtils.getSimilarStatsIndex(
            //        recentHboPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold);
            //if (similarStatsIndex.isPresent()) {
            //    return Optional.of(recentRunsStatistics.get(similarStatsIndex.get()));
            //}
        }
        return Optional.empty();
    }

    /**
     * getPlanNodeHash
     * @param planNode planNode
     * @return planNode And Hash
     */
    public static PlanNodeAndHash getPlanNodeHash(AbstractPlan planNode) {
        String planFingerprint;
        String planHash;
        if (planNode instanceof AbstractPhysicalPlan) {
            planFingerprint = planNode.getPlanTreeFingerprint();
            planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        } else if (planNode instanceof AbstractLogicalPlan) {
            planFingerprint = planNode.getPlanTreeFingerprint();
            planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        } else {
            throw new IllegalStateException("hbo get neither physical plan nor logical plan");
        }
        return new PlanNodeAndHash(planNode, Optional.of(planHash));
    }

    private static Optional<List<PlanStatistics>> getFilterAdjustedInputTableStatistics(
            List<PlanStatistics> currentInputTableStatistics, ConnectContext connectContext) {
        ImmutableList.Builder<PlanStatistics> outputTableStatisticsBuilder = ImmutableList.builder();
        HboPlanStatisticsManager hboManager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        HboPlanInfoProvider planInfoProvider = hboManager.getHboPlanInfoProvider();

        if (planInfoProvider != null) {
            String queryId = DebugUtil.printId(connectContext.queryId());
            Map<RelationId, Set<Expression>> scanToFilterMap = planInfoProvider.getScanToFilterMap(queryId);
            // here allows scanToFilterMap is empty when no filter on the scan and upper plan node
            // can also reuse the recent run's plan stats. info.
            for (PlanStatistics inputTableStatistics : currentInputTableStatistics) {
                PhysicalOlapScan tableScan = ((ScanPlanStatistics) inputTableStatistics).getScan();
                Set<Expression> tableFilterSet = scanToFilterMap.get(tableScan.getRelationId());
                ScanPlanStatistics newInputPlanStatistics = new ScanPlanStatistics(inputTableStatistics, tableScan,
                        tableFilterSet, tableScan.getTable().isPartitionedTable(),
                        tableScan.getTable().getPartitionInfo(),
                        tableScan.getSelectedPartitionIds());
                outputTableStatisticsBuilder.add(newInputPlanStatistics);
            }
        }
        return Optional.of(outputTableStatisticsBuilder.build());
    }

    /**
     * getMatchedPlanStatistics
     * @param planStatistics planStatistics
     * @param connectContext connectContext
     * @return planStatistics
     */
    public static PlanStatistics getMatchedPlanStatistics(RecentRunsPlanStatistics planStatistics,
            ConnectContext connectContext) {
        PlanStatistics matchedPlanStatistics = null;
        // NOTE: get current inputTableStatistics is difficult, consider the case:
        // select ... from t where c1 = 1 followed by select ... from t where c1 = 1 and c2 = 2
        // since the input table t will have two entries in recentRunEntries list,
        // if the getPlanNodeInputTableStatistics only find the table entry by the table name,
        // it may find the wrong entry for the different filter pattern. By contract, use current planStatistics
        // is relative safe because the plan hash has ensured the plan pattern, such as partition number,
        // filter pattern are matched, although the constant in filter may not be same.
        // After considering the above situation, current solution is as following:
        // firstly find an initial entry, e.g, the latest entry, of currentInputTablesStatistics.
        // then adjust the initial info based on the filter info.
        if (!planStatistics.getRecentRunsStatistics().isEmpty()) {
            // use the latest entry as initial entry, which will be updated with the filter info.
            int initialIndex = planStatistics.getRecentRunsStatistics().size() - 1;
            List<PlanStatistics> initialInputTableStatistics = planStatistics.getRecentRunsStatistics()
                    .get(initialIndex).getInputTableStatistics();
            if (!initialInputTableStatistics.isEmpty()) {
                Optional<List<PlanStatistics>> inputTableStatistics = getFilterAdjustedInputTableStatistics(
                        initialInputTableStatistics, connectContext);
                if (inputTableStatistics.isPresent()) {
                    double rfsafeThreshold = -1.0;
                    //double rowCountMatchingThreshold = 0.1;
                    boolean isEnableHboNonStrictMatchingMode = false;
                    if (connectContext != null && connectContext.getSessionVariable() != null) {
                        rfsafeThreshold = connectContext.getSessionVariable().getHboRfSafeThreshold();
                        //rowCountMatchingThreshold = connectContext.getSessionVariable().getHboRowMatchingThreshold();
                        isEnableHboNonStrictMatchingMode = connectContext.getSessionVariable()
                                .isEnableHboNonStrictMatchingMode();
                    }
                    Optional<RecentRunsPlanStatisticsEntry> recentRunsPlanStatisticsEntry
                            = HboUtils.getMatchedHboPlanStatisticsEntry(
                                    planStatistics, inputTableStatistics.get(),
                                    rfsafeThreshold, isEnableHboNonStrictMatchingMode);
                    if (recentRunsPlanStatisticsEntry.isPresent()) {
                        matchedPlanStatistics = recentRunsPlanStatisticsEntry.get().getPlanStatistics();
                    }
                }
            }
        }
        return matchedPlanStatistics;
    }

    /**
     * Judge whether this filter is on logical scan.
     * @param filter filter
     * @return whether this filter is on logical scan.
     */
    public static boolean isLogicalFilterOnLogicalScan(Filter filter) {
        return filter instanceof LogicalFilter
                && ((LogicalFilter<?>) filter).child() instanceof GroupPlan
                && ((GroupPlan) ((LogicalFilter<?>) filter).child()).getGroup() != null
                && !((GroupPlan) ((LogicalFilter<?>) filter).child()).getGroup().getLogicalExpressions().isEmpty()
                && ((GroupPlan) ((LogicalFilter<?>) filter).child()).getGroup().getLogicalExpressions().get(0)
                .getPlan() instanceof LogicalOlapScan;
    }

    /**
     * Judge whether this filter is on physical scan.
     * @param filter filter node
     * @return whether this filter is on physical scan.
     */
    public static boolean isPhysicalFilterOnPhysicalScan(Filter filter) {
        return filter instanceof PhysicalFilter
                && ((PhysicalFilter<?>) filter).child() instanceof GroupPlan
                && ((GroupPlan) ((PhysicalFilter<?>) filter).child()).getGroup() != null
                && !((GroupPlan) ((PhysicalFilter<?>) filter).child()).getGroup().getPhysicalExpressions().isEmpty()
                && ((GroupPlan) ((PhysicalFilter<?>) filter).child()).getGroup().getPhysicalExpressions().get(0)
                .getPlan() instanceof PhysicalOlapScan;
    }

    /**
     * Get scan node of under filter node, during stats. calculation stage.
     * @param filterNode filter node
     * @return scan node
     */
    public static AbstractPlan getScanUnderFilterNode(Filter filterNode) {
        AbstractPlan scanNodePlan;
        if (isLogicalFilterOnLogicalScan(filterNode)) {
            scanNodePlan = (LogicalOlapScan) ((GroupPlan) ((LogicalFilter<?>) filterNode).child())
                    .getGroup().getLogicalExpressions().get(0).getPlan();
        } else if (isPhysicalFilterOnPhysicalScan(filterNode)) {
            scanNodePlan = (PhysicalOlapScan) ((GroupPlan) ((PhysicalFilter<?>) filterNode).child())
                    .getGroup().getPhysicalExpressions().get(0).getPlan();
        } else {
            throw new AnalysisException("unexpected filter type");
        }
        return scanNodePlan;
    }

    private static PlanStatistics generateScanPlanStatistics(int nodeId,
            List<TPlanNodeRuntimeStatsItem> planNodeRuntimeStats,
            PhysicalOlapScan scan, Map<RelationId, Set<Expression>> scanToFilterMap) {
        for (TPlanNodeRuntimeStatsItem item : planNodeRuntimeStats) {
            if (item.node_id == nodeId) {
                return PlanStatistics.buildFromStatsItem(item, scan, scanToFilterMap);
            }
        }
        return PlanStatistics.EMPTY;
    }

    private static InputTableStatisticsInfo buildHboInputTableStatisticsInfo(PhysicalPlan root,
            Map<PhysicalPlan, Integer> planToIdMap, Map<RelationId, Set<Expression>> scanToFilterMap,
            List<TPlanNodeRuntimeStatsItem> statsItem) {
        String planFingerprint = ((AbstractPhysicalPlan) root).getPlanTreeFingerprint();
        String planHash = HboUtils.getPlanFingerprintHash(planFingerprint);
        ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
        List<PhysicalOlapScan> scans = root.collectToList(PhysicalOlapScan.class::isInstance);
        for (PhysicalOlapScan scan : scans) {
            Integer nodeId = planToIdMap.get(scan);
            if (nodeId != null) {
                PlanStatistics planStatistics = generateScanPlanStatistics(
                        nodeId, statsItem, scan, scanToFilterMap);
                if (!planStatistics.equals(PlanStatistics.EMPTY)) {
                    inputTableStatisticsBuilder.add(planStatistics);
                }
            }
        }
        return new InputTableStatisticsInfo(Optional.of(planHash), Optional.of(inputTableStatisticsBuilder.build()));
    }

    /**
     * Generate plan statistics map.
     * @param idToPlanMap idToPlanMap
     * @param planToIdMap planToIdMap
     * @param scanToFilterMap scanToFilterMap
     * @param curPlanNodeRuntimeStats curPlanNodeRuntimeStats
     * @return plan statistics map
     */
    public static Map<PlanNodeAndHash, PlanStatisticsWithInputInfo> genPlanStatisticsMap(
            Map<Integer, PhysicalPlan> idToPlanMap, Map<PhysicalPlan, Integer> planToIdMap,
            Map<RelationId, Set<Expression>> scanToFilterMap,
            List<TPlanNodeRuntimeStatsItem> curPlanNodeRuntimeStats) {
        Map<PlanNodeAndHash, PlanStatisticsWithInputInfo> outputPlanStatisticsMap = new HashMap<>();
        for (TPlanNodeRuntimeStatsItem nodeStats : curPlanNodeRuntimeStats) {
            int nodeId = nodeStats.node_id;
            PhysicalPlan planNode = idToPlanMap.get(nodeId);
            if (planNode != null) {
                PlanStatistics curPlanStatistics = PlanStatistics.buildFromStatsItem(
                        nodeStats, planNode, scanToFilterMap);
                InputTableStatisticsInfo inputTableStatisticsInfo = buildHboInputTableStatisticsInfo(
                        planNode, planToIdMap, scanToFilterMap, curPlanNodeRuntimeStats);
                Optional<String> hash = inputTableStatisticsInfo.getHash();
                PlanNodeAndHash planNodeAndHash = new PlanNodeAndHash((AbstractPlan) planNode, hash);
                PlanStatisticsWithInputInfo planHashWithInputInfo = new PlanStatisticsWithInputInfo(
                        nodeId, curPlanStatistics, inputTableStatisticsInfo);
                outputPlanStatisticsMap.put(planNodeAndHash, planHashWithInputInfo);
            }
        }
        return outputPlanStatisticsMap;
    }
}
