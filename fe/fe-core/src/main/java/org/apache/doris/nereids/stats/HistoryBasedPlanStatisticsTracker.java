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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.DPhyperNode;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.proto.Data.PQueryStatistics;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.HistoryBasedSourceInfo;
import org.apache.doris.statistics.PlanNodeCanonicalInfo;
import org.apache.doris.statistics.PlanStatistics;
import org.apache.doris.statistics.PlanStatisticsWithSourceInfo;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.thrift.TNodeExecStatsItemPB;
import org.apache.doris.thrift.TQueryStatistics;

import com.google.common.collect.ImmutableList;
import static com.google.common.collect.ImmutableList.toImmutableList;
import com.google.common.collect.ImmutableMap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.graph.Traverser.forTree;
import static com.google.common.hash.Hashing.sha256;
import static java.lang.Double.isNaN;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class HistoryBasedPlanStatisticsTracker {
    private final HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatisticsMap = new HashMap<>();

    private CascadesContext context;

    private ConnectContext connectContext;
    private PhysicalPlan root;

    public HistoryBasedPlanStatisticsTracker(
            ConnectContext context,
            HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager) {
        this.connectContext = context;
        this.historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsProvider;
        this.historyBasedStatisticsCacheManager = historyBasedStatisticsCacheManager;
    }

    public HistoryBasedPlanStatisticsProvider getHistoryBasedPlanStatisticsProvider() {
        return this.historyBasedPlanStatisticsProvider;
    }

    public HistoryBasedStatisticsCacheManager getHistoryBasedStatisticsCacheManager() {
        return this.historyBasedStatisticsCacheManager;
    }

    public void setContext(CascadesContext context, PhysicalPlan root) {
        this.context = context;
        this.root = root;
    }

    private String hashCanonicalPlan(String planString)
    {
        return sha256().hashString(planString, UTF_8).toString();
    }


    public void buildPlanNodeToInfoMap(PlanNode root, List<TNodeExecStatsItemPB> pbList,
            Map<PhysicalPlan, PlanNodeCanonicalInfo> infos) {
        for (PlanNode planNode : forTree(PlanNode::getChildren).depthFirstPreOrder(root)) {
            String canonicalPlanString = planNode.toString();
            String hashValue = hashCanonicalPlan(canonicalPlanString);
            ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
            List<PhysicalOlapScan> scans = ((PhysicalPlan) planNode).collectToList(PhysicalOlapScan.class::isInstance);
            scans.stream().map(scan -> inputTableStatisticsBuilder.add(getPlanStatistics(scan.getId(), pbList)));
            PlanNodeCanonicalInfo info = new PlanNodeCanonicalInfo(hashValue, inputTableStatisticsBuilder.build());
            infos.putIfAbsent((PhysicalPlan) planNode, info);
        }
    }

    public PlanStatistics getPlanStatistics(int nodeId, List<TNodeExecStatsItemPB> pbList) {
        for (TNodeExecStatsItemPB item : pbList) {
            if (item.node_id == nodeId) {
                return PlanStatistics.buildFromPB(item);
            }
        }
        return PlanStatistics.EMPTY;
    }

    public Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> getQueryStats(String queryId) {
        Map<Integer, PhysicalPlan> idToPlanMap = context.getNeedStatsPlanIdNodeMap();

        WorkloadRuntimeStatusMgr mgr = Env.getCurrentEnv().getWorkloadRuntimeStatusMgr();
        Map<String, TQueryStatistics> queryStatisticsMap = mgr.getQueryStatisticsMap();
        Map<PhysicalPlan, PlanNodeCanonicalInfo> planToInfoMap = new HashMap<>();

        //for (AuditEvent event : auditEventList) {
            TQueryStatistics qs = queryStatisticsMap.get(queryId);
            List<TNodeExecStatsItemPB> pbList = qs.getNodeExecStatsItems();
            for (TNodeExecStatsItemPB nodeStats : pbList) {
                int nodeId = nodeStats.node_id;
                PlanStatistics planStatistics = PlanStatistics.buildFromPB(nodeStats);
                PhysicalPlan planNode = idToPlanMap.get(nodeId);
                buildPlanNodeToInfoMap((PlanNode) planNode, pbList, planToInfoMap);
                Optional<PlanNodeCanonicalInfo> planNodeCanonicalInfo = Optional.ofNullable(planToInfoMap.get(planNode));
                if (planNodeCanonicalInfo.isPresent()) {
                    String hash = planNodeCanonicalInfo.get().getHash();
                    PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash((PlanNode) planNode, Optional.of(hash));
                    List<PlanStatistics> inputTableStatistics = planNodeCanonicalInfo.get().getInputTableStatistics();
                    HistoryBasedSourceInfo sourceInfo = new HistoryBasedSourceInfo(Optional.of(hash), Optional.of(inputTableStatistics));
                    PlanStatisticsWithSourceInfo planStatsWithSourceInfo = new PlanStatisticsWithSourceInfo(
                            nodeId, planStatistics, sourceInfo);

                    planStatisticsMap.put(planNodeWithHash, planStatsWithSourceInfo);
                }
            }
        //}
        return ImmutableMap.copyOf(planStatisticsMap);
    }

    public void updateStatistics() {
        WorkloadRuntimeStatusMgr mgr = Env.getCurrentEnv().getWorkloadRuntimeStatusMgr();
        List<AuditEvent> auditEventList = mgr.getQueryNeedAudit();
        // TODO: choose which audit event
        if (!auditEventList.isEmpty()) {
            AuditEvent event = auditEventList.get(0);
            Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatistics = getQueryStats(event.queryId);
            Map<PlanNodeWithHash, HistoricalPlanStatistics> historicalPlanStatisticsMap =
                    historyBasedPlanStatisticsProvider.getStats(
                            planStatistics.keySet().stream().collect(toImmutableList()), 1000);

            Map<PlanNodeWithHash, HistoricalPlanStatistics> newPlanStatistics = planStatistics.entrySet().stream()
                    .filter(entry -> entry.getKey().getHash().isPresent() &&
                            entry.getValue().getSourceInfo().getInputTableStatistics().isPresent())
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            entry -> {
                                HistoricalPlanStatistics oldPlanStatistics = Optional.ofNullable(
                                                historicalPlanStatisticsMap.get(entry.getKey()))
                                        .orElseGet(HistoricalPlanStatistics::empty);
                                HistoryBasedSourceInfo historyBasedSourceInfo = entry.getValue().getSourceInfo();
                                return updatePlanStatistics(
                                        oldPlanStatistics,
                                        historyBasedSourceInfo.getInputTableStatistics().get(),
                                        entry.getValue().getPlanStatistics());
                            }));

            if (!newPlanStatistics.isEmpty()) {
                historyBasedPlanStatisticsProvider.putStats(ImmutableMap.copyOf(newPlanStatistics));
            }
            historyBasedStatisticsCacheManager.invalidate(event.queryId);
        }
    }

    public static HistoricalPlanStatistics updatePlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            PlanStatistics current)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

        List<HistoricalPlanStatisticsEntry> newLastRunsStatistics = new ArrayList<>(lastRunsStatistics);

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics,
                inputTableStatistics, 0.1);
        if (similarStatsIndex.isPresent()) {
            newLastRunsStatistics.remove(similarStatsIndex.get().intValue());
        }

        newLastRunsStatistics.add(new HistoricalPlanStatisticsEntry(current, inputTableStatistics));
        int maxLastRuns = inputTableStatistics.isEmpty() ? 1 : 10;//config.getMaxLastRunsHistory();
        if (newLastRunsStatistics.size() > maxLastRuns) {
            newLastRunsStatistics.remove(0);
        }

        return new HistoricalPlanStatistics(newLastRunsStatistics);
    }

    public static Optional<Integer> getSimilarStatsIndex(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double threshold)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

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
            for (int inputTablesIndex = 0; inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                PlanStatistics currentInputStatistics = inputTableStatistics.get(inputTablesIndex);
                PlanStatistics historicalInputStatistics = lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().get(inputTablesIndex);

                rowSimilarity = rowSimilarity && similarStats(currentInputStatistics.getPushRows(), historicalInputStatistics.getPushRows(), threshold);
                //outputSizeSimilarity = outputSizeSimilarity && similarStats(currentInputStatistics.getOutputSize().getValue(), historicalInputStatistics.getOutputSize().getValue(), threshold);
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

}