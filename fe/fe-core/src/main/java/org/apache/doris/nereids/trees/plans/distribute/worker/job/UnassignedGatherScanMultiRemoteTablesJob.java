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
import org.apache.doris.planner.DataGenScanNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/** UnassignedGatherScanMultiRemoteTablesJob */
public class UnassignedGatherScanMultiRemoteTablesJob extends AbstractUnassignedJob {

    public UnassignedGatherScanMultiRemoteTablesJob(PlanFragment fragment,
            List<ScanNode> scanNodes, ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(fragment, scanNodes, exchangeToChildJob);
    }

    /** canApply */
    public static boolean canApply(List<ScanNode> scanNodes) {
        if (scanNodes.size() <= 1) {
            return false;
        }
        for (ScanNode scanNode : scanNodes) {
            if (!(scanNode instanceof DataGenScanNode)) {
                return false;
            }
            DataGenScanNode dataGenScanNode = (DataGenScanNode) scanNode;
            if (dataGenScanNode.getScanRangeLocations(0).size() != 1) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        ConnectContext context = ConnectContext.get();
        Map<ScanNode, ScanRanges> scanNodeToScanRanges = Maps.newLinkedHashMap();
        for (ScanNode scanNode : scanNodes) {
            List<TScanRangeLocations> scanRangeLocations = scanNode.getScanRangeLocations(0);
            ScanRanges scanRanges = new ScanRanges();
            for (TScanRangeLocations scanRangeLocation : scanRangeLocations) {
                TScanRangeParams replica = ScanWorkerSelector.buildScanReplicaParams(
                        scanRangeLocation, scanRangeLocation.locations.get(0));
                scanRanges.addScanRange(replica, 0);
            }

            scanNodeToScanRanges.put(scanNode, scanRanges);
        }

        DistributedPlanWorker randomWorker = workerManager.randomAvailableWorker();
        return ImmutableList.of(
                assignWorkerAndDataSources(0, context.nextInstanceId(),
                        randomWorker, new DefaultScanSource(scanNodeToScanRanges)
                )
        );
    }
}
