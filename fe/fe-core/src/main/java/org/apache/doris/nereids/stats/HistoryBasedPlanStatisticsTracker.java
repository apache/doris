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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class HistoryBasedPlanStatisticsTracker {
    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatisticsMap = new HashMap<>();

    private CascadesContext context;

    private ConnectContext connectContext;
    private PhysicalPlan root;

    public HistoryBasedPlanStatisticsTracker(
            ConnectContext context,
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager) {
        this.connectContext = context;
        this.historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsProvider;
        this.historyBasedStatisticsCacheManager = historyBasedStatisticsCacheManager;
    }

    public Supplier<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProvider() {
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
        AuditEvent event = auditEventList.get(0);
        Map<PlanNodeWithHash, PlanStatisticsWithSourceInfo> planStatistics = getQueryStats(event.queryId);
        Map<PlanNodeWithHash, HistoricalPlanStatistics> historicalPlanStatisticsMap =
                historyBasedPlanStatisticsProvider.get().getStats(
                        planStatistics.keySet().stream().collect(toImmutableList()), 1000);

        Map<PlanNodeWithHash, HistoricalPlanStatistics> newPlanStatistics = planStatistics.entrySet().stream()
                .filter(entry -> entry.getKey().getHash().isPresent() &&
                        entry.getValue().getSourceInfo().getInputTableStatistics().isPresent())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            HistoricalPlanStatistics historicalPlanStatistics = Optional.ofNullable(historicalPlanStatisticsMap.get(entry.getKey()))
                                    .orElseGet(HistoricalPlanStatistics::empty);
                            HistoryBasedSourceInfo historyBasedSourceInfo = entry.getValue().getSourceInfo();
                            return updatePlanStatistics(
                                    historicalPlanStatistics,
                                    historyBasedSourceInfo.getInputTableStatistics().get(),
                                    entry.getValue().getPlanStatistics());
                        }));

        if (!newPlanStatistics.isEmpty()) {
            historyBasedPlanStatisticsProvider.get().putStats(ImmutableMap.copyOf(newPlanStatistics));
        }
        historyBasedStatisticsCacheManager.invalidate(event.queryId);
    }

    public static HistoricalPlanStatistics updatePlanStatistics(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            PlanStatistics current)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

        List<HistoricalPlanStatisticsEntry> newLastRunsStatistics = new ArrayList<>(lastRunsStatistics);

        //Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics,
        //        inputTableStatistics, config.getHistoryMatchingThreshold());
        //if (similarStatsIndex.isPresent()) {
        //    newLastRunsStatistics.remove(similarStatsIndex.get().intValue());
        //}

        newLastRunsStatistics.add(new HistoricalPlanStatisticsEntry(current, inputTableStatistics));
        int maxLastRuns = inputTableStatistics.isEmpty() ? 1 : 10;//config.getMaxLastRunsHistory();
        if (newLastRunsStatistics.size() > maxLastRuns) {
            newLastRunsStatistics.remove(0);
        }

        return new HistoricalPlanStatistics(newLastRunsStatistics);
    }

}