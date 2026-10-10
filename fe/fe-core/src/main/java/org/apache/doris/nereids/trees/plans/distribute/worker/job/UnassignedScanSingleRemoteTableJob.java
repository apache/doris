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

import org.apache.doris.datasource.tvf.source.TVFScanNode;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * UnassignedScanSingleRemoteTableJob
 * it should be a leaf job which not contains scan native olap table node,
 * for example, select literal without table, or scan an external table
 */
public class UnassignedScanSingleRemoteTableJob extends AbstractUnassignedScanJob {
    private final ScanWorkerSelector scanWorkerSelector;

    public UnassignedScanSingleRemoteTableJob(
            StatementContext statementContext, PlanFragment fragment, ScanNode scanNode,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob, ScanWorkerSelector scanWorkerSelector) {
        super(statementContext, fragment, ImmutableList.of(scanNode), exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(scanWorkerSelector, "scanWorkerSelector is not null");
    }

    /**
     * Select a worker for each scan range of the external / remote table scan node.
     * For external tables (Hive, Iceberg, etc.), scan ranges represent file splits
     * rather than tablets, and workers are selected based on data locality or
     * workload balancing.
     *
     * @param distributeContext the distribute context
     * @param inputJobs multimap from child exchange nodes to their assigned jobs
     * @return a map from worker to its assigned file scan ranges
     */
    @Override
    protected Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        return scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(
                scanNodes.get(0), statementContext.getConnectContext()
        );
    }

    /**
     * Avoid extra local-shuffle instances for a file TVF row-count scan with one
     * actual split assigned to this worker. The scan still feeds the upper COUNT
     * aggregation; this shortcut only reduces fragment-instance overhead.
     * Dynamic split sources and existing external-table COUNT paths retain their
     * original parallelism.
     */
    @Override
    protected int degreeOfParallelism(ScanSource scanSource, int maxParallel,
            boolean useLocalShuffleToAddParallel) {
        ScanNode scanNode = scanNodes.get(0);
        if (scanNode instanceof TVFScanNode && scanNode.getPushDownAggNoGroupingOp() == TPushAggOp.COUNT
                && scanSource instanceof DefaultScanSource) {
            ScanRanges scanRanges = ((DefaultScanSource) scanSource).scanNodeToScanRanges.get(scanNode);
            if (scanRanges.params.size() == 1) {
                TFileScanRange fileScanRange = scanRanges.params.get(0).getScanRange()
                        .getExtScanRange().getFileScanRange();
                if (!fileScanRange.isSetSplitSource() && fileScanRange.getRangesSize() == 1) {
                    return 1;
                }
            }
        }
        return super.degreeOfParallelism(scanSource, maxParallel, useLocalShuffleToAddParallel);
    }

    /**
     * If all file scan ranges have been pruned and the assigned job list is empty,
     * create a single empty instance on a random worker so the fragment can still
     * execute (returning an empty result) rather than failing.
     *
     * @param assignedJobs the list produced by {@link #insideMachineParallelization}
     * @param workerManager the worker manager to select a fallback worker from
     * @param inputJobs multimap from child exchange nodes to their assigned jobs
     * @return the original list if non-empty, otherwise a single empty instance
     */
    @Override
    protected List<AssignedJob> fillUpAssignedJobs(List<AssignedJob> assignedJobs,
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        if (assignedJobs.isEmpty()) {
            // the file scan have pruned, so no assignedJobs,
            // we should allocate an instance of it,
            assignedJobs = fillUpSingleEmptyInstance(workerManager);
        }
        return assignedJobs;
    }
}
