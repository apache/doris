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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** UnassignedScanSingleOlapTableJob */
public class UnassignedScanSingleOlapTableJob extends AbstractUnassignedScanJob {
    private static final Logger LOG = LogManager.getLogger(UnassignedScanSingleOlapTableJob.class);

    private OlapScanNode olapScanNode;
    private final ScanWorkerSelector scanWorkerSelector;

    public UnassignedScanSingleOlapTableJob(
            StatementContext statementContext, PlanFragment fragment, OlapScanNode olapScanNode,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob,
            ScanWorkerSelector scanWorkerSelector) {
        super(statementContext, fragment, ImmutableList.of(olapScanNode), exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(
                scanWorkerSelector, "scanWorkerSelector cat not be null");
        this.olapScanNode = olapScanNode;
    }

    @Override
    protected Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        // for every tablet, select its replica and worker.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"):
        //          olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //    BackendWorker("172.0.0.2"):
        //          olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008, tablet_10009])
        // }
        return scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(
                olapScanNode, statementContext.getConnectContext()
        );
    }

    @Override
    protected List<AssignedJob> insideMachineParallelization(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs, DistributeContext distributeContext) {
        // for each worker, compute how many instances should be generated, and which data should be scanned.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"): [
        //        instance 1: olapScanNode1: ScanRanges([tablet_10001, tablet_10003])
        //        instance 2: olapScanNode1: ScanRanges([tablet_10002, tablet_10004])
        //    ],
        //    BackendWorker("172.0.0.2"): [
        //        instance 3: olapScanNode1: ScanRanges([tablet_10005, tablet_10008])
        //        instance 4: olapScanNode1: ScanRanges([tablet_10006, tablet_10009])
        //        instance 5: olapScanNode1: ScanRanges([tablet_10007])
        //    ],
        // }
        if (usePartitionParallelismForQueryCache(workerToScanRanges, distributeContext)) {
            try {
                // Best effort optimization for query cache: keep tablets in same partition
                // on the same instance to reduce BE concurrency pressure.
                List<AssignedJob> partitionInstances = insideMachineParallelizationByPartition(workerToScanRanges);
                if (partitionInstances != null) {
                    return partitionInstances;
                }
            } catch (Exception e) {
                LOG.warn("Failed to assign query cache instances by partition, fallback to default planning",
                        e);
            }
        }

        return super.insideMachineParallelization(workerToScanRanges, inputJobs, distributeContext);
    }

    private List<AssignedJob> insideMachineParallelizationByPartition(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges) {
        List<Long> selectedPartitionIds = new ArrayList<>(olapScanNode.getSelectedPartitionIds());
        Map<Long, Long> tabletToPartitionId = buildTabletToPartitionId(selectedPartitionIds);
        if (tabletToPartitionId.size() != olapScanNode.getScanTabletIds().size()) {
            return null;
        }

        ConnectContext context = statementContext.getConnectContext();
        List<AssignedJob> instances = new ArrayList<>();
        for (Map.Entry<DistributedPlanWorker, UninstancedScanSource> entry : workerToScanRanges.entrySet()) {
            DistributedPlanWorker worker = entry.getKey();
            ScanSource scanSource = entry.getValue().scanSource;
            if (!(scanSource instanceof DefaultScanSource)) {
                return null;
            }

            DefaultScanSource defaultScanSource = (DefaultScanSource) scanSource;
            ScanRanges scanRanges = defaultScanSource.scanNodeToScanRanges.get(olapScanNode);
            if (scanRanges == null) {
                return null;
            }
            if (scanRanges.params.isEmpty()) {
                continue;
            }

            Map<Long, ScanRanges> partitionToScanRanges = splitScanRangesByPartition(scanRanges, tabletToPartitionId);
            if (partitionToScanRanges == null) {
                return null;
            }

            // One partition on one BE maps to one instance. Different BEs may miss some partitions.
            for (Long partitionId : selectedPartitionIds) {
                ScanRanges partitionScanRanges = partitionToScanRanges.remove(partitionId);
                if (partitionScanRanges == null || partitionScanRanges.params.isEmpty()) {
                    continue;
                }
                instances.add(assignWorkerAndDataSources(
                        instances.size(), context.nextInstanceId(), worker,
                        new DefaultScanSource(ImmutableMap.of(olapScanNode, partitionScanRanges))));
            }

            if (!partitionToScanRanges.isEmpty()) {
                return null;
            }
        }
        return instances;
    }

    private boolean usePartitionParallelismForQueryCache(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            DistributeContext distributeContext) {
        if (fragment.queryCacheParam == null || workerToScanRanges.isEmpty()) {
            return false;
        }

        ConnectContext context = statementContext.getConnectContext();
        if (context == null || useLocalShuffleToAddParallel(distributeContext)) {
            return false;
        }

        long totalTabletNum = olapScanNode.getScanTabletIds().size();
        int parallelPipelineTaskNum = Math.max(
                context.getSessionVariable().getParallelExecInstanceNum(
                        olapScanNode.getScanContext().getClusterName()), 1);
        long threshold = (long) parallelPipelineTaskNum * workerToScanRanges.size();
        return totalTabletNum > threshold;
    }

    private Map<Long, Long> buildTabletToPartitionId(List<Long> selectedPartitionIds) {
        long selectedIndexId = olapScanNode.getSelectedIndexId();
        if (selectedIndexId == -1) {
            selectedIndexId = olapScanNode.getOlapTable().getBaseIndexId();
        }

        Set<Long> scanTabletIds = new LinkedHashSet<>(olapScanNode.getScanTabletIds());
        Map<Long, Long> tabletToPartitionId = new LinkedHashMap<>(scanTabletIds.size());
        for (Long partitionId : selectedPartitionIds) {
            Partition partition = olapScanNode.getOlapTable().getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            MaterializedIndex index = partition.getIndex(selectedIndexId);
            if (index == null) {
                continue;
            }
            for (Tablet tablet : index.getTablets()) {
                long tabletId = tablet.getId();
                if (scanTabletIds.contains(tabletId)) {
                    tabletToPartitionId.put(tabletId, partitionId);
                }
            }
        }
        return tabletToPartitionId;
    }

    private Map<Long, ScanRanges> splitScanRangesByPartition(
            ScanRanges scanRanges, Map<Long, Long> tabletToPartitionId) {
        Map<Long, ScanRanges> partitionToScanRanges = new LinkedHashMap<>();
        for (int i = 0; i < scanRanges.params.size(); i++) {
            TScanRangeParams scanRangeParams = scanRanges.params.get(i);
            long tabletId = scanRangeParams.getScanRange().getPaloScanRange().getTabletId();
            Long partitionId = tabletToPartitionId.get(tabletId);
            if (partitionId == null) {
                return null;
            }
            partitionToScanRanges
                    .computeIfAbsent(partitionId, id -> new ScanRanges())
                    .addScanRange(scanRangeParams, scanRanges.bytes.get(i));
        }
        return partitionToScanRanges;
    }

    @Override
    protected List<AssignedJob> fillUpAssignedJobs(List<AssignedJob> assignedJobs,
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        if (assignedJobs.isEmpty()) {
            // the tablets have pruned, so no assignedJobs,
            // we should allocate an instance of it,
            //
            // for example: SELECT * FROM tbl TABLET(1234)
            // if the tablet 1234 not exists
            assignedJobs = fillUpSingleEmptyInstance(workerManager);
        }
        return assignedJobs;
    }
}
