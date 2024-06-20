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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.worker.Worker;
import org.apache.doris.nereids.worker.job.AssignedJob;
import org.apache.doris.nereids.worker.job.BucketScanSource;
import org.apache.doris.nereids.worker.job.DefaultScanSource;
import org.apache.doris.nereids.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.worker.job.ScanRanges;
import org.apache.doris.nereids.worker.job.ScanSource;
import org.apache.doris.nereids.worker.job.UnassignedJob;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/** NereidsCoordinator */
public class NereidsCoordinator extends Coordinator {
    private NereidsPlanner nereidsPlanner;
    private FragmentIdMapping<DistributedPlan> distributedPlans;

    public NereidsCoordinator(ConnectContext context, Analyzer analyzer,
            Planner planner, StatsErrorEstimator statsErrorEstimator, NereidsPlanner nereidsPlanner) {
        super(context, analyzer, planner, statsErrorEstimator);
        this.nereidsPlanner = Objects.requireNonNull(nereidsPlanner, "nereidsPlanner can not be null");
        this.distributedPlans = Objects.requireNonNull(
                nereidsPlanner.getDistributedPlans(), "distributedPlans can not be null"
        );
    }

    @Override
    protected void processFragmentAssignmentAndParams() throws Exception {
        // prepare information
        prepare();

        computeFragmentExecParams();
    }

    @Override
    protected void computeFragmentHosts() {
        // translate distributed plan to params
        for (DistributedPlan distributedPlan : distributedPlans.values()) {
            UnassignedJob fragmentJob = distributedPlan.getFragmentJob();
            PlanFragment fragment = fragmentJob.getFragment();
            FragmentExecParams fragmentExecParams = fragmentExecParamsMap.computeIfAbsent(
                    fragment.getFragmentId(), id -> new FragmentExecParams(fragment)
            );

            bucketShuffleJoinController
                    .isBucketShuffleJoin(fragment.getFragmentId().asInt(), fragment.getPlanRoot());

            for (ScanNode scanNode : distributedPlan.getFragmentJob().getScanNodes()) {
                if (scanNode instanceof FileQueryScanNode) {
                    fileScanRangeParamsMap.put(
                            scanNode.getId().asInt(),
                            ((FileQueryScanNode) scanNode).getFileScanRangeParams()
                    );
                }
            }

            List<AssignedJob> instanceJobs = ((PipelineDistributedPlan) distributedPlan).getInstanceJobs();
            boolean isShareScan = false;
            for (AssignedJob instanceJob : instanceJobs) {
                if (instanceJob instanceof LocalShuffleAssignedJob) {
                    isShareScan = true;
                    break;
                }
            }

            if (isShareScan) {
                fragmentExecParams.ignoreDataDistribution = true;
                fragmentExecParams.parallelTasksNum = 1;
            } else {
                fragmentExecParams.parallelTasksNum = instanceJobs.size();
            }

            for (AssignedJob instanceJob : instanceJobs) {
                Worker worker = instanceJob.getAssignedWorker();
                TNetworkAddress address = new TNetworkAddress(worker.host(), worker.port());
                FInstanceExecParam instanceExecParam = new FInstanceExecParam(
                        null, address, 0, fragmentExecParams);
                fragmentExecParams.instanceExecParams.add(instanceExecParam);
                addressToBackendID.put(address, worker.id());
                ScanSource scanSource = instanceJob.getScanSource();
                if (scanSource instanceof BucketScanSource) {
                    setForBucketScanSource(instanceExecParam, (BucketScanSource) scanSource, isShareScan);
                } else {
                    setForDefaultScanSource(instanceExecParam, (DefaultScanSource) scanSource, isShareScan);
                }
            }
        }
    }

    private void setForDefaultScanSource(
            FInstanceExecParam instanceExecParam, DefaultScanSource scanSource, boolean isShareScan) {
        for (Entry<ScanNode, ScanRanges> scanNodeIdToReplicaIds : scanSource.scanNodeToScanRanges.entrySet()) {
            ScanNode scanNode = scanNodeIdToReplicaIds.getKey();
            ScanRanges scanReplicas = scanNodeIdToReplicaIds.getValue();
            instanceExecParam.perNodeScanRanges.put(scanNode.getId().asInt(), scanReplicas.params);
            instanceExecParam.perNodeSharedScans.put(scanNode.getId().asInt(), isShareScan);
        }
    }

    private void setForBucketScanSource(FInstanceExecParam instanceExecParam,
            BucketScanSource bucketScanSource, boolean isShareScan) {
        for (Entry<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanTablets :
                bucketScanSource.bucketIndexToScanNodeToTablets.entrySet()) {
            Integer bucketIndex = bucketIndexToScanTablets.getKey();
            instanceExecParam.addBucketSeq(bucketIndex);
            Map<ScanNode, ScanRanges> scanNodeToRangeMap = bucketIndexToScanTablets.getValue();
            for (Entry<ScanNode, ScanRanges> scanNodeToRange : scanNodeToRangeMap.entrySet()) {
                ScanNode scanNode = scanNodeToRange.getKey();
                ScanRanges scanRanges = scanNodeToRange.getValue();
                List<TScanRangeParams> scanBucketTablets = instanceExecParam.perNodeScanRanges.computeIfAbsent(
                        scanNode.getId().asInt(), id -> Lists.newArrayList());
                scanBucketTablets.addAll(scanRanges.params);
                instanceExecParam.perNodeSharedScans.put(scanNode.getId().asInt(), isShareScan);

                if (scanNode instanceof OlapScanNode) {
                    OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                    if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
                        // In bucket shuffle join, we have 2 situation.
                        // 1. Only one partition: in this case, we use scanNode.getTotalTabletsNum()
                        //    to get the right bucket num because when table turn on dynamic partition,
                        //    the bucket number in default distribution info
                        //    is not correct.
                        // 2. Table is colocated: in this case, table could have more than one partition,
                        //    but all partition's bucket number must be same, so we use default bucket num is ok.
                        int bucketNum = 0;
                        if (olapScanNode.getOlapTable().isColocateTable()) {
                            bucketNum = olapScanNode.getOlapTable().getDefaultDistributionInfo()
                                    .getBucketNum();
                        } else {
                            bucketNum = (int) (olapScanNode.getTotalTabletsNum());
                        }
                        fragmentIdToSeqToAddressMap.put(olapScanNode.getFragmentId(), new HashMap<>());
                        bucketShuffleJoinController.fragmentIdBucketSeqToScanRangeMap
                                .put(scanNode.getFragmentId(), new BucketSeqToScanRange());
                        bucketShuffleJoinController.fragmentIdToBucketNumMap
                                .put(scanNode.getFragmentId(), bucketNum);
                        olapScanNode.getFragment().setBucketNum(bucketNum);
                    }
                } else if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
                    int bucketNum = 1;
                    fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashMap<>());
                    bucketShuffleJoinController.fragmentIdBucketSeqToScanRangeMap
                            .put(scanNode.getFragmentId(), new BucketSeqToScanRange());
                    bucketShuffleJoinController.fragmentIdToBucketNumMap
                            .put(scanNode.getFragmentId(), bucketNum);
                    scanNode.getFragment().setBucketNum(bucketNum);
                }

                BucketSeqToScanRange bucketSeqToScanRange = bucketShuffleJoinController
                        .fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

                Map<Integer, List<TScanRangeParams>> scanNodeIdToReplicas
                        = bucketSeqToScanRange.computeIfAbsent(bucketIndex, set -> Maps.newLinkedHashMap());
                List<TScanRangeParams> tablets = scanNodeIdToReplicas.computeIfAbsent(
                        scanNode.getId().asInt(), id -> new ArrayList<>());
                tablets.addAll(scanRanges.params);
            }
        }
    }
}
