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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Re-estimate memo logical row counts and rebuild physical costs
 * after DPHyp join enumeration, so the subsequent cascades optimization
 * phase (OptimizeGroupJob) sees accurate statistics.
 *
 * Why this is needed:
 * DPHyp copies projected join alternatives into the memo using a
 * lightweight heuristic cost model.  Once all alternatives are in
 * place, the memo needs a full bottom-up statistics refresh.  Without
 * this step, parent-group costs are computed against stale row counts
 * and the final plan may be suboptimal.
 *
 * Overall flow (recompute):
 *   1. Seed CTE-producer stats -- snapshot existing group statistics
 *      into cteIdToStats so LogicalCTEConsumer expressions can look up
 *      their producer's statistics during estimation.
 *   2. First logical-stats pass -- bottom-up re-estimation.  For each
 *      group, every logical expression is estimated independently;
 *      candidates are aggregated according to the configured
 *      LogicalRowCountAggregationPolicy (average, median, min, or
 *      trust-join-count).
 *   3. Second logical-stats pass (CTE queries only) -- CTE consumer
 *      groups may have been processed before their producers in the
 *      first pass because they sit in different memo sub-trees.  A
 *      second pass lets consumers re-estimate with the now-refreshed
 *      producer stats.
 *   4. Physical-cost recomputation -- bottom-up.  Clears every group's
 *      lowest-cost-plan table, recalculates CostCalculator.calculateCost
 *      and child-cost addition for each physical expression, then
 *      rebuilds the per-group best plan map.
 *
 * Key sub-components:
 *   - reestimateCurrentGroup: estimates each candidate logical
 *     expression, then aggregates row counts and resolves isStatsReliable.
 *   - filterCandidateStatisticsByPolicy: TRUST_JOIN_COUNT policy uses
 *     countTrustJoins/getGroupTrustJoinCount to keep only the candidates
 *     with the most trustable joins.
 *   - resolveChosenProjectStatistics: when a project group's
 *     alternatives diverge by >1000x in row count, the stats from the
 *     currently-best PhysicalProject are preserved to avoid a misleading
 *     average.
 *   - recomputeGroupExpressionCost: re-costs a single physical
 *     expression against its original lowest-cost table and restores any
 *     entries that were not reachable after the refresh.
 *
 * @see JoinEstimation#hasTrustableEqualCondition
 * @see org.apache.doris.nereids.jobs.executor.Optimizer#dpHypOptimize
 */
public final class MemoStatsAndCostRecomputer {
    private static final double CHOSEN_PROJECT_STATS_DIVERGENCE_RATIO_THRESHOLD = 1_000D;
    final Map<Group, Integer> groupTrustJoinCountCache = new HashMap<>();
    private final CascadesContext cascadesContext;
    private final Map<CTEId, Statistics> cteIdToStats = new HashMap<>();
    private final LogicalExpressionRowCountSyncPolicy logicalExpressionRowCountSyncPolicy;

    MemoStatsAndCostRecomputer(CascadesContext cascadesContext,
            LogicalExpressionRowCountSyncPolicy logicalExpressionRowCountSyncPolicy) {
        this.cascadesContext = cascadesContext;
        this.logicalExpressionRowCountSyncPolicy = logicalExpressionRowCountSyncPolicy;
    }

    /**
     * Re-estimate logical row counts and rebuild physical costs for the entire memo,
     * then reconstruct the lowest-cost plan table for every group.
     *
     * This is called after DPHyp join enumeration which copies projected join
     * alternatives into the memo.  DPHyp's own cost model is a lightweight
     * heuristic; once all alternatives are in place the memo needs a full
     * bottom-up statistics refresh so that the subsequent cascades optimization
     * phase (OptimizeGroupJob) sees accurate row counts and costs.
     *
     * Steps:
     * 1. Seed CTE-producer stats so consumers can look them up.
     * 2. Re-estimate logical stats bottom-up (with a second pass for CTE
     *    consumers whose producers were refreshed in the first pass).
     * 3. Recompute physical costs bottom-up from the refreshed stats.
     */
    public static void recompute(Group rootGroup, PhysicalProperties physicalProperties,
            CascadesContext cascadesContext) {
        recompute(rootGroup, physicalProperties, cascadesContext,
                LogicalExpressionRowCountSyncPolicy.KEEP_INDIVIDUAL_EXPRESSION_ROW_COUNT);
    }

    /**
     * recompute with configurable logical expression row count sync behavior.
     */
    public static void recompute(Group rootGroup, PhysicalProperties physicalProperties,
            CascadesContext cascadesContext,
            LogicalExpressionRowCountSyncPolicy logicalExpressionRowCountSyncPolicy) {
        MemoStatsAndCostRecomputer recomputer = new MemoStatsAndCostRecomputer(cascadesContext,
                logicalExpressionRowCountSyncPolicy);
        recomputer.seedProducerStats(rootGroup, new HashSet<>());
        recomputer.groupTrustJoinCountCache.clear();
        recomputer.reestimateLogicalStatsBottomUp(rootGroup, new HashSet<>());
        // Run a second pass so CTE consumers and their ancestors can settle on producer stats refreshed above.
        // Skip when no CTEs exist — the bottom-up first pass already produces settled stats for non-CTE queries.
        if (!recomputer.cteIdToStats.isEmpty()) {
            recomputer.groupTrustJoinCountCache.clear();
            recomputer.reestimateLogicalStatsBottomUp(rootGroup, new HashSet<>());
        }
        recomputer.recomputePhysicalCostsBottomUp(rootGroup, new HashSet<>());
    }

    private void seedProducerStats(Group group, Set<Group> visited) {
        if (!visited.add(group)) {
            return;
        }
        Statistics statistics = group.getStatistics();
        if (statistics != null) {
            recordProducerStats(group, statistics);
        }
        for (Group child : getTraversalChildren(group)) {
            seedProducerStats(child, visited);
        }
    }

    private void reestimateLogicalStatsBottomUp(Group group, Set<Group> visited) {
        if (!visited.add(group)) {
            return;
        }
        for (Group child : getTraversalChildren(group)) {
            reestimateLogicalStatsBottomUp(child, visited);
        }
        reestimateCurrentGroup(group);
        refreshEnforcerRowCount(group);
    }

    private void reestimateCurrentGroup(Group group) {
        List<GroupExpression> estimableExpressions = getEstimableLogicalExpressions(group);
        if (estimableExpressions.isEmpty()) {
            if (group.getLogicalExpressions().isEmpty()) {
                reestimatePhysicalOnlyGroup(group);
            }
            return;
        }
        Statistics originalStatistics = group.getStatistics();
        boolean originalStatsReliable = group.isStatsReliable();
        Map<GroupExpression, Statistics> candidateStatisticsByExpression = new LinkedHashMap<>();
        Map<GroupExpression, Boolean> candidateStatsReliableByExpression = new LinkedHashMap<>();
        for (GroupExpression logicalExpression : estimableExpressions) {
            group.setStatistics(null);
            estimateStats(logicalExpression);
            Statistics estimatedStatistics = group.getStatistics();
            if (estimatedStatistics == null || !isValidCandidateStatistics(estimatedStatistics)) {
                continue;
            }
            logicalExpression.setEstOutputRowCount(estimatedStatistics.getRowCount());
            // Safe to store the reference: group.setStatistics(null) above releases the group's
            // hold, and the next iteration will overwrite with a new object from estimateStats().
            candidateStatisticsByExpression.put(logicalExpression, estimatedStatistics);
            candidateStatsReliableByExpression.put(logicalExpression, group.isStatsReliable());
        }
        if (candidateStatisticsByExpression.isEmpty()) {
            group.setStatistics(originalStatistics);
            group.setStatsReliable(originalStatsReliable);
            return;
        }
        LogicalRowCountAggregationPolicy aggregationPolicy = getLogicalRowCountAggregationPolicy();
        Map<GroupExpression, Statistics> selectedCandidateStatisticsByExpression = filterCandidateStatisticsByPolicy(
                aggregationPolicy, candidateStatisticsByExpression);
        List<Statistics> candidateStatistics = new ArrayList<>(selectedCandidateStatisticsByExpression.values());
        double aggregatedRowCount = aggregationPolicy.aggregate(candidateStatistics);
        Statistics updatedStatistics = resolveUpdatedGroupStatistics(group, selectedCandidateStatisticsByExpression,
                candidateStatistics, aggregatedRowCount, originalStatistics);
        boolean resolvedStatsReliable = resolveUpdatedGroupStatsReliability(group,
                selectedCandidateStatisticsByExpression, candidateStatsReliableByExpression,
                aggregatedRowCount);
        group.setStatsReliable(resolvedStatsReliable);
        group.setStatistics(updatedStatistics);
        repairInvalidLogicalExpressionRowCounts(group, aggregatedRowCount);
        refreshPhysicalExpressionRowCount(group, updatedStatistics.getRowCount());
        recordProducerStats(group, updatedStatistics);
        if (shouldSyncLogicalExpressionRowCount()) {
            syncLogicalExpressionRowCount(group, updatedStatistics.getRowCount());
        }
    }

    private void reestimatePhysicalOnlyGroup(Group group) {
        List<GroupExpression> estimableExpressions = getEstimablePhysicalExpressions(group);
        if (estimableExpressions.isEmpty()) {
            return;
        }
        Statistics originalStatistics = group.getStatistics();
        boolean originalStatsReliable = group.isStatsReliable();
        Map<GroupExpression, Statistics> candidateStatisticsByExpression = new LinkedHashMap<>();
        Map<GroupExpression, Boolean> candidateStatsReliableByExpression = new LinkedHashMap<>();
        for (GroupExpression physicalExpression : estimableExpressions) {
            group.setStatistics(null);
            estimateStats(physicalExpression);
            Statistics estimatedStatistics = group.getStatistics();
            if (estimatedStatistics == null || !isValidCandidateStatistics(estimatedStatistics)) {
                continue;
            }
            physicalExpression.setEstOutputRowCount(estimatedStatistics.getRowCount());
            candidateStatisticsByExpression.put(physicalExpression, new Statistics(estimatedStatistics));
            candidateStatsReliableByExpression.put(physicalExpression, group.isStatsReliable());
        }
        if (candidateStatisticsByExpression.isEmpty()) {
            group.setStatistics(originalStatistics);
            group.setStatsReliable(originalStatsReliable);
            return;
        }
        Statistics updatedStatistics = choosePhysicalOnlyGroupStatistics(group, candidateStatisticsByExpression,
                originalStatistics);
        boolean resolvedStatsReliable = resolvePhysicalOnlyGroupStatsReliability(group,
                candidateStatisticsByExpression, candidateStatsReliableByExpression,
                originalStatistics);
        group.setStatsReliable(resolvedStatsReliable);
        group.setStatistics(updatedStatistics);
        refreshPhysicalExpressionRowCount(group, updatedStatistics.getRowCount());
        recordProducerStats(group, updatedStatistics);
    }

    private boolean isValidCandidateStatistics(Statistics statistics) {
        return Double.isFinite(statistics.getRowCount()) && statistics.getRowCount() >= 0;
    }

    private void estimateStats(GroupExpression groupExpression) {
        ConnectContext connectContext = cascadesContext.getConnectContext();
        StatsCalculator statsCalculator = new StatsCalculator(
                groupExpression,
                connectContext.getSessionVariable().getForbidUnknownColStats(),
                connectContext.getTotalColumnStatisticMap(),
                connectContext.getSessionVariable().isPlayNereidsDump(),
                cteIdToStats,
                cascadesContext);
        statsCalculator.estimate();
    }

    private void recomputePhysicalCostsBottomUp(Group group, Set<Group> visited) {
        if (!visited.add(group)) {
            return;
        }
        for (Group child : getTraversalChildren(group)) {
            recomputePhysicalCostsBottomUp(child, visited);
        }
        if (group.getStatistics() == null
                || (group.getPhysicalExpressions().isEmpty() && group.getEnforcers().isEmpty())) {
            refreshEnforcerRowCount(group);
            return;
        }
        Map<PhysicalProperties, Pair<Cost, GroupExpression>> originalLowestCostPlans =
                snapshotLowestCostPlans(group);
        group.clearLowestCostPlans();
        for (GroupExpression physicalExpression : group.getPhysicalExpressions()) {
            recomputeGroupExpressionCost(group, physicalExpression);
        }
        refreshEnforcerRowCount(group);
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            recomputeGroupExpressionCost(group, enforcer);
        }
        restoreMissingLowestCostPlans(group, originalLowestCostPlans);
    }

    private void recomputeGroupExpressionCost(Group ownerGroup, GroupExpression groupExpression) {
        if (ownerGroup.getStatistics() == null || !hasCompleteChildStatistics(groupExpression)) {
            return;
        }
        Cost originalCost = groupExpression.getCost();
        Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> originalLowestCostTable
                = new LinkedHashMap<>(groupExpression.getLowestCostTable());
        Map<PhysicalProperties, PhysicalProperties> originalRequestPropertiesMap
                = new LinkedHashMap<>(groupExpression.getRequestPropertiesMap());
        groupExpression.clearCostState();

        Cost bestNodeCost = null;
        for (Map.Entry<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> entry
                : originalLowestCostTable.entrySet()) {
            PhysicalProperties outputProperties = entry.getKey();
            List<PhysicalProperties> childInputProperties = entry.getValue().second;
            if (!hasAvailableChildBestPlan(groupExpression, childInputProperties)) {
                continue;
            }
            Cost nodeCost = CostCalculator.calculateCost(cascadesContext.getConnectContext(),
                    groupExpression, childInputProperties);
            Cost totalCost = nodeCost;
            for (int i = 0; i < childInputProperties.size(); i++) {
                Optional<Pair<Cost, GroupExpression>> childBestPlan = groupExpression.child(i)
                        .getLowestCostPlan(childInputProperties.get(i));
                if (!childBestPlan.isPresent()) {
                    totalCost = null;
                    break;
                }
                totalCost = CostCalculator.addChildCost(cascadesContext.getConnectContext(),
                        groupExpression.getPlan(), totalCost, childBestPlan.get().first, i);
            }
            if (totalCost == null) {
                continue;
            }
            groupExpression.updateLowestCostTable(
                    outputProperties, childInputProperties, totalCost);
            ownerGroup.setBestPlan(groupExpression, totalCost, outputProperties);
            if (bestNodeCost == null || nodeCost.getValue() < bestNodeCost.getValue()) {
                bestNodeCost = nodeCost;
            }
        }
        restoreMissingExpressionCostState(groupExpression, originalLowestCostTable, originalRequestPropertiesMap);
        if (bestNodeCost != null) {
            groupExpression.setCost(bestNodeCost);
        } else if (originalCost != null) {
            groupExpression.setCost(originalCost);
        }
    }

    private void restoreMissingExpressionCostState(GroupExpression groupExpression,
            Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> originalLowestCostTable,
            Map<PhysicalProperties, PhysicalProperties> originalRequestPropertiesMap) {
        for (Map.Entry<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> entry
                : originalLowestCostTable.entrySet()) {
            if (!groupExpression.getLowestCostTable().containsKey(entry.getKey())) {
                groupExpression.updateLowestCostTable(entry.getKey(), entry.getValue().second, entry.getValue().first);
            }
        }
        for (Map.Entry<PhysicalProperties, PhysicalProperties> entry : originalRequestPropertiesMap.entrySet()) {
            if (groupExpression.getLowestCostTable().containsKey(entry.getKey())) {
                groupExpression.putOutputPropertiesMap(entry.getValue(), entry.getKey());
            }
        }
    }

    private boolean hasAvailableChildBestPlan(GroupExpression groupExpression,
            List<PhysicalProperties> childInputProperties) {
        if (childInputProperties.size() != groupExpression.arity()) {
            return false;
        }
        for (int i = 0; i < childInputProperties.size(); i++) {
            if (!groupExpression.child(i)
                    .getLowestCostPlan(childInputProperties.get(i)).isPresent()) {
                return false;
            }
        }
        return true;
    }

    private void syncLogicalExpressionRowCount(Group group, double rowCount) {
        for (GroupExpression logicalExpression : group.getLogicalExpressions()) {
            if (logicalExpression.getEstOutputRowCount() > 0
                    || !Double.isFinite(logicalExpression.getEstOutputRowCount())) {
                logicalExpression.setEstOutputRowCount(rowCount);
            }
        }
    }

    private void refreshPhysicalExpressionRowCount(Group group, double rowCount) {
        for (GroupExpression physicalExpression : group.getPhysicalExpressions()) {
            physicalExpression.setEstOutputRowCount(getPhysicalExpressionRowCount(physicalExpression, rowCount));
        }
    }

    private double getPhysicalExpressionRowCount(GroupExpression physicalExpression, double rowCount) {
        if (physicalExpression.getPlan() instanceof PhysicalProject && physicalExpression.arity() == 1) {
            Statistics childStatistics = physicalExpression.child(0).getStatistics();
            if (childStatistics != null && Double.isFinite(childStatistics.getRowCount())
                    && childStatistics.getRowCount() >= 0) {
                return childStatistics.getRowCount();
            }
        }
        return rowCount;
    }

    private boolean shouldSyncLogicalExpressionRowCount() {
        return logicalExpressionRowCountSyncPolicy
                == LogicalExpressionRowCountSyncPolicy.SYNC_WITH_GROUP_ROW_COUNT;
    }

    private Map<GroupExpression, Statistics> filterCandidateStatisticsByPolicy(
            LogicalRowCountAggregationPolicy aggregationPolicy,
            Map<GroupExpression, Statistics> candidateStatisticsByExpression) {
        if (aggregationPolicy != LogicalRowCountAggregationPolicy.TRUST_JOIN_COUNT
                || candidateStatisticsByExpression.size() < 2) {
            return candidateStatisticsByExpression;
        }
        int maxTrustJoinCount = Integer.MIN_VALUE;
        Map<GroupExpression, Statistics> selectedCandidateStatisticsByExpression = new LinkedHashMap<>();
        for (Map.Entry<GroupExpression, Statistics> entry : candidateStatisticsByExpression.entrySet()) {
            int trustJoinCount = countTrustJoins(entry.getKey());
            if (trustJoinCount > maxTrustJoinCount) {
                selectedCandidateStatisticsByExpression.clear();
                maxTrustJoinCount = trustJoinCount;
            }
            if (trustJoinCount == maxTrustJoinCount) {
                selectedCandidateStatisticsByExpression.put(entry.getKey(), entry.getValue());
            }
        }
        return selectedCandidateStatisticsByExpression;
    }

    int countTrustJoins(GroupExpression groupExpression) {
        int trustJoinCount = isTrustJoin(groupExpression) ? 1 : 0;
        for (Group child : groupExpression.children()) {
            if (!child.getLogicalExpressions().isEmpty()) {
                trustJoinCount += getGroupTrustJoinCount(child);
            }
        }
        return trustJoinCount;
    }

    int getGroupTrustJoinCount(Group group) {
        Integer cached = groupTrustJoinCountCache.get(group);
        if (cached != null) {
            return cached;
        }
        // Placeholder to break recursion cycles through the memo group DAG.
        groupTrustJoinCountCache.put(group, 0);
        int maxCount = 0;
        for (GroupExpression logicalExpression : group.getLogicalExpressions()) {
            int count = countTrustJoins(logicalExpression);
            maxCount = Math.max(maxCount, count);
        }
        groupTrustJoinCountCache.put(group, maxCount);
        return maxCount;
    }

    boolean isTrustJoin(GroupExpression groupExpression) {
        if (groupExpression.arity() != 2 || !(groupExpression.getPlan() instanceof Join)) {
            return false;
        }
        Statistics leftStats = groupExpression.child(0).getStatistics();
        Statistics rightStats = groupExpression.child(1).getStatistics();
        if (leftStats == null || rightStats == null) {
            return false;
        }
        return JoinEstimation.hasTrustableEqualCondition(leftStats, rightStats,
                (Join) groupExpression.getPlan());
    }

    private LogicalRowCountAggregationPolicy getLogicalRowCountAggregationPolicy() {
        ConnectContext connectContext = cascadesContext == null ? null : cascadesContext.getConnectContext();
        if (connectContext == null || connectContext.getSessionVariable() == null) {
            return LogicalRowCountAggregationPolicy.AVERAGE;
        }
        return LogicalRowCountAggregationPolicy.fromSessionValue(
                connectContext.getSessionVariable().getMemoLogicalRowCountAggregationPolicy());
    }

    private void refreshEnforcerRowCount(Group group) {
        Statistics statistics = group.getStatistics();
        if (statistics == null) {
            return;
        }
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            enforcer.setEstOutputRowCount(statistics.getRowCount());
        }
    }

    private void recordProducerStats(Group group, Statistics statistics) {
        if (cascadesContext == null || statistics == null) {
            return;
        }
        for (GroupExpression logicalExpression : group.getLogicalExpressions()) {
            Plan plan = logicalExpression.getPlan();
            if (plan instanceof LogicalCTEProducer) {
                cteIdToStats.put(((LogicalCTEProducer<?>) plan).getCteId(), new Statistics(statistics));
            }
        }
        for (GroupExpression physicalExpression : group.getPhysicalExpressions()) {
            Plan plan = physicalExpression.getPlan();
            if (plan instanceof PhysicalCTEProducer) {
                cteIdToStats.put(((PhysicalCTEProducer<?>) plan).getCteId(), new Statistics(statistics));
            }
        }
    }

    private Map<PhysicalProperties, Pair<Cost, GroupExpression>> snapshotLowestCostPlans(Group group) {
        Map<PhysicalProperties, Pair<Cost, GroupExpression>> snapshot = new LinkedHashMap<>();
        for (PhysicalProperties properties : group.getAllProperties()) {
            group.getLowestCostPlan(properties).ifPresent(plan -> snapshot.put(properties, plan));
        }
        return snapshot;
    }

    private void restoreMissingLowestCostPlans(Group group,
            Map<PhysicalProperties, Pair<Cost, GroupExpression>> lowestCostPlans) {
        for (Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry : lowestCostPlans.entrySet()) {
            if (!group.getLowestCostPlan(entry.getKey()).isPresent()) {
                group.putBestPlan(entry.getValue().second, entry.getValue().first, entry.getKey());
            }
        }
    }

    private Statistics chooseRepresentativeStatistics(List<Statistics> candidateStatistics,
            double aggregatedRowCount, Statistics originalStatistics) {
        Statistics bestMatch = null;
        double bestDistance = Double.POSITIVE_INFINITY;
        for (Statistics candidate : candidateStatistics) {
            double distance = Math.abs(candidate.getRowCount() - aggregatedRowCount);
            if (distance < bestDistance) {
                bestDistance = distance;
                bestMatch = candidate;
            }
        }
        if (bestMatch != null) {
            return bestMatch;
        }
        if (originalStatistics != null) {
            return new Statistics(originalStatistics);
        }
        return new Statistics(aggregatedRowCount, new HashMap<>());
    }

    private Statistics resolveUpdatedGroupStatistics(Group group,
            Map<GroupExpression, Statistics> candidateStatisticsByExpression,
            List<Statistics> candidateStatistics, double aggregatedRowCount,
            Statistics originalStatistics) {
        Statistics chosenProjectStatistics = resolveChosenProjectStatistics(group, candidateStatisticsByExpression,
                aggregatedRowCount);
        if (chosenProjectStatistics != null) {
            return chosenProjectStatistics;
        }
        Statistics representativeStatistics = chooseRepresentativeStatistics(
                candidateStatistics, aggregatedRowCount, originalStatistics);
        return representativeStatistics.withRowCountAndEnforceValid(aggregatedRowCount);
    }

    private Statistics resolveChosenProjectStatistics(Group group,
            Map<GroupExpression, Statistics> candidateStatisticsByExpression,
            double aggregatedRowCount) {
        if (!shouldPreserveChosenProjectStatistics(group, candidateStatisticsByExpression, aggregatedRowCount)) {
            return null;
        }
        Optional<Pair<Cost, GroupExpression>> lowestCostPlan = group.getLowestCostPlan(PhysicalProperties.ANY);
        if (!lowestCostPlan.isPresent()) {
            return null;
        }
        GroupExpression chosenPhysicalExpression = lowestCostPlan.get().second;
        for (Map.Entry<GroupExpression, Statistics> entry : candidateStatisticsByExpression.entrySet()) {
            if (entry.getKey().children().equals(chosenPhysicalExpression.children())) {
                // The candidate map owns the only reference; returning directly avoids a deep copy.
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean resolveUpdatedGroupStatsReliability(Group group,
            Map<GroupExpression, Statistics> selectedCandidateStatisticsByExpression,
            Map<GroupExpression, Boolean> candidateStatsReliableByExpression,
            double aggregatedRowCount) {
        // If resolveChosenProjectStatistics would return non-null, use that specific candidate's reliability
        if (shouldPreserveChosenProjectStatistics(group, selectedCandidateStatisticsByExpression, aggregatedRowCount)) {
            Optional<Pair<Cost, GroupExpression>> lowestCostPlan = group.getLowestCostPlan(PhysicalProperties.ANY);
            if (lowestCostPlan.isPresent()) {
                GroupExpression chosenPhysicalExpression = lowestCostPlan.get().second;
                for (Map.Entry<GroupExpression, Statistics> entry
                        : selectedCandidateStatisticsByExpression.entrySet()) {
                    if (entry.getKey().children().equals(chosenPhysicalExpression.children())) {
                        Boolean reliable = candidateStatsReliableByExpression.get(entry.getKey());
                        if (reliable != null) {
                            return reliable;
                        }
                    }
                }
            }
        }
        // Conservative: false if any selected candidate is unreliable
        for (Map.Entry<GroupExpression, Boolean> entry : candidateStatsReliableByExpression.entrySet()) {
            if (selectedCandidateStatisticsByExpression.containsKey(entry.getKey()) && !entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    private boolean resolvePhysicalOnlyGroupStatsReliability(Group group,
            Map<GroupExpression, Statistics> candidateStatisticsByExpression,
            Map<GroupExpression, Boolean> candidateStatsReliableByExpression,
            Statistics originalStatistics) {
        // Mirror the selection logic of choosePhysicalOnlyGroupStatistics:
        // prefer the lowest-cost plan's expression, then closest to original row count, then first candidate
        Optional<Pair<Cost, GroupExpression>> lowestCostPlan = group.getLowestCostPlan(PhysicalProperties.ANY);
        if (lowestCostPlan.isPresent()) {
            Boolean reliable = candidateStatsReliableByExpression.get(lowestCostPlan.get().second);
            if (reliable != null) {
                return reliable;
            }
        }
        if (originalStatistics != null && Double.isFinite(originalStatistics.getRowCount())) {
            double bestDistance = Double.POSITIVE_INFINITY;
            GroupExpression bestExpression = null;
            for (Map.Entry<GroupExpression, Statistics> entry : candidateStatisticsByExpression.entrySet()) {
                double distance = Math.abs(entry.getValue().getRowCount() - originalStatistics.getRowCount());
                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestExpression = entry.getKey();
                }
            }
            if (bestExpression != null) {
                Boolean reliable = candidateStatsReliableByExpression.get(bestExpression);
                return reliable != null ? reliable : false;
            }
        }
        Boolean reliable = candidateStatsReliableByExpression.values().iterator().next();
        return reliable != null ? reliable : false;
    }

    private boolean shouldPreserveChosenProjectStatistics(Group group,
            Map<GroupExpression, Statistics> candidateStatisticsByExpression,
            double aggregatedRowCount) {
        if (candidateStatisticsByExpression.size() < 2 || !(aggregatedRowCount > 0)) {
            return false;
        }
        if (!group.getLowestCostPlan(PhysicalProperties.ANY).isPresent()) {
            return false;
        }
        GroupExpression chosenPhysicalExpression = group.getLowestCostPlan(PhysicalProperties.ANY).get().second;
        if (!(chosenPhysicalExpression.getPlan() instanceof PhysicalProject)) {
            return false;
        }
        for (GroupExpression logicalExpression : candidateStatisticsByExpression.keySet()) {
            if (!(logicalExpression.getPlan() instanceof LogicalProject)) {
                return false;
            }
        }
        double minRowCount = Double.POSITIVE_INFINITY;
        double maxRowCount = 0;
        double chosenRowCount = Double.NaN;
        for (Map.Entry<GroupExpression, Statistics> entry : candidateStatisticsByExpression.entrySet()) {
            double rowCount = entry.getValue().getRowCount();
            if (!(rowCount > 0)) {
                return false;
            }
            minRowCount = Math.min(minRowCount, rowCount);
            maxRowCount = Math.max(maxRowCount, rowCount);
            if (entry.getKey().children().equals(chosenPhysicalExpression.children())) {
                chosenRowCount = rowCount;
            }
        }
        if (!Double.isFinite(chosenRowCount) || chosenRowCount >= aggregatedRowCount) {
            return false;
        }
        return maxRowCount / minRowCount >= CHOSEN_PROJECT_STATS_DIVERGENCE_RATIO_THRESHOLD;
    }

    private List<GroupExpression> getEstimableLogicalExpressions(Group group) {
        List<GroupExpression> logicalExpressions = group.getLogicalExpressions();
        if (logicalExpressions.isEmpty()) {
            return Collections.emptyList();
        }
        List<GroupExpression> estimableExpressions = new ArrayList<>();
        for (GroupExpression logicalExpression : logicalExpressions) {
            if (hasEstimableLogicalExpressionRowCount(group, logicalExpression)
                    && hasCompleteChildStatistics(logicalExpression)
                    && hasAvailableCteStatistics(logicalExpression)) {
                estimableExpressions.add(logicalExpression);
            }
        }
        return estimableExpressions;
    }

    private List<GroupExpression> getEstimablePhysicalExpressions(Group group) {
        List<GroupExpression> estimableExpressions = new ArrayList<>();
        for (GroupExpression physicalExpression : group.getPhysicalExpressions()) {
            if (physicalExpression.arity() > 0
                    && !dependsOnOwnerGroupStatistics(group, physicalExpression)
                    && hasCompleteChildStatistics(physicalExpression)
                    && hasAvailableCteStatistics(physicalExpression)) {
                estimableExpressions.add(physicalExpression);
            }
        }
        for (GroupExpression enforcer : group.getEnforcers().values()) {
            if (enforcer.arity() > 0
                    && !dependsOnOwnerGroupStatistics(group, enforcer)
                    && hasCompleteChildStatistics(enforcer)
                    && hasAvailableCteStatistics(enforcer)) {
                estimableExpressions.add(enforcer);
            }
        }
        return estimableExpressions;
    }

    private Statistics choosePhysicalOnlyGroupStatistics(Group group,
            Map<GroupExpression, Statistics> candidateStatisticsByExpression,
            Statistics originalStatistics) {
        Optional<Pair<Cost, GroupExpression>> lowestCostPlan = group.getLowestCostPlan(PhysicalProperties.ANY);
        if (lowestCostPlan.isPresent()) {
            Statistics chosenStatistics = candidateStatisticsByExpression.get(lowestCostPlan.get().second);
            if (chosenStatistics != null) {
                return new Statistics(chosenStatistics);
            }
        }
        if (originalStatistics != null && Double.isFinite(originalStatistics.getRowCount())) {
            Statistics bestMatch = null;
            double bestDistance = Double.POSITIVE_INFINITY;
            for (Statistics candidateStatistics : candidateStatisticsByExpression.values()) {
                double distance = Math.abs(candidateStatistics.getRowCount() - originalStatistics.getRowCount());
                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestMatch = candidateStatistics;
                }
            }
            if (bestMatch != null) {
                return new Statistics(bestMatch);
            }
        }
        return new Statistics(candidateStatisticsByExpression.values().iterator().next());
    }

    private void repairInvalidLogicalExpressionRowCounts(Group group, double rowCount) {
        for (GroupExpression logicalExpression : group.getLogicalExpressions()) {
            double expressionRowCount = logicalExpression.getEstOutputRowCount();
            if (!Double.isFinite(expressionRowCount) || expressionRowCount <= 0) {
                logicalExpression.setEstOutputRowCount(rowCount);
            }
        }
    }

    private boolean hasEstimableLogicalExpressionRowCount(Group group, GroupExpression logicalExpression) {
        return logicalExpression.getEstOutputRowCount() > 0
                || (!shouldSyncLogicalExpressionRowCount() && group.getStatistics() != null);
    }

    private boolean hasAvailableCteStatistics(GroupExpression groupExpression) {
        Plan plan = groupExpression.getPlan();
        if (plan instanceof LogicalCTEConsumer) {
            return cteIdToStats.containsKey(((LogicalCTEConsumer) plan).getCteId());
        }
        if (plan instanceof PhysicalCTEConsumer) {
            return cteIdToStats.containsKey(((PhysicalCTEConsumer) plan).getCteId());
        }
        return true;
    }

    private List<Group> getTraversalChildren(Group group) {
        List<Group> children = Lists.newArrayList();
        addChildren(children, group.getLogicalExpressions(), group);
        addChildren(children, group.getPhysicalExpressions(), group);
        addChildren(children, group.getEnforcers().values(), group);
        return children;
    }

    private void addChildren(List<Group> children, Iterable<GroupExpression> groupExpressions,
            Group ownerGroup) {
        for (GroupExpression groupExpression : groupExpressions) {
            for (Group child : groupExpression.children()) {
                if (child != ownerGroup) {
                    children.add(child);
                }
            }
        }
    }

    private boolean hasCompleteChildStatistics(GroupExpression groupExpression) {
        for (Group child : groupExpression.children()) {
            if (child.getStatistics() == null) {
                return false;
            }
        }
        return true;
    }

    private boolean dependsOnOwnerGroupStatistics(Group ownerGroup, GroupExpression groupExpression) {
        for (Group child : groupExpression.children()) {
            if (child == ownerGroup) {
                return true;
            }
        }
        return false;
    }

    /**
     * LogicalExpressionRowCountSyncPolicy
     */
    public enum LogicalExpressionRowCountSyncPolicy {
        SYNC_WITH_GROUP_ROW_COUNT,
        KEEP_INDIVIDUAL_EXPRESSION_ROW_COUNT
    }

    private enum LogicalRowCountAggregationPolicy {
        AVERAGE {
            @Override
            double aggregate(List<Statistics> candidateStatistics) {
                return candidateStatistics.stream()
                        .mapToDouble(Statistics::getRowCount)
                        .average()
                        .orElse(Double.NaN);
            }
        },
        MEDIAN {
            @Override
            double aggregate(List<Statistics> candidateStatistics) {
                double[] rowCounts = candidateStatistics.stream()
                        .mapToDouble(Statistics::getRowCount)
                        .sorted()
                        .toArray();
                if (rowCounts.length == 0) {
                    return Double.NaN;
                }
                int middle = rowCounts.length / 2;
                if ((rowCounts.length & 1) == 1) {
                    return rowCounts[middle];
                }
                return (rowCounts[middle - 1] + rowCounts[middle]) / 2;
            }
        },
        MIN {
            @Override
            double aggregate(List<Statistics> candidateStatistics) {
                return candidateStatistics.stream()
                        .mapToDouble(Statistics::getRowCount)
                        .min()
                        .orElse(Double.NaN);
            }
        },
        TRUST_JOIN_COUNT {
            @Override
            double aggregate(List<Statistics> candidateStatistics) {
                return MEDIAN.aggregate(candidateStatistics);
            }
        };

        static LogicalRowCountAggregationPolicy fromSessionValue(String value) {
            if (value == null) {
                return AVERAGE;
            }
            switch (value.toLowerCase()) {
                case "average":
                    return AVERAGE;
                case "median":
                    return MEDIAN;
                case "min":
                    return MIN;
                case "trust_join_count":
                    return TRUST_JOIN_COUNT;
                default:
                    throw new IllegalArgumentException("Unknown logical row count aggregation policy: " + value);
            }
        }

        abstract double aggregate(List<Statistics> candidateStatistics);
    }
}
