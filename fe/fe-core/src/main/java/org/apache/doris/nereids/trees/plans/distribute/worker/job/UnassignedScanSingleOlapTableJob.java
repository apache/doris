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

import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** UnassignedScanSingleOlapTableJob */
public class UnassignedScanSingleOlapTableJob extends AbstractUnassignedScanJob {
    private OlapScanNode olapScanNode;
    private final ScanWorkerSelector scanWorkerSelector;

    public UnassignedScanSingleOlapTableJob(
            PlanFragment fragment, OlapScanNode olapScanNode,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob,
            ScanWorkerSelector scanWorkerSelector) {
        super(fragment, ImmutableList.of(olapScanNode), exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(
                scanWorkerSelector, "scanWorkerSelector cat not be null");
        this.olapScanNode = olapScanNode;
    }

    @Override
    protected Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        // for every tablet, select its replica and worker.
        // for example:
        // {
        //    BackendWorker("172.0.0.1"):
        //          olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //    BackendWorker("172.0.0.2"):
        //          olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008, tablet_10009])
        // }
        return scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(olapScanNode);
    }

    @Override
    protected List<AssignedJob> insideMachineParallelization(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs, DistributedPlanWorkerManager workerManager) {
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
        return super.insideMachineParallelization(workerToScanRanges, inputJobs, workerManager);
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
