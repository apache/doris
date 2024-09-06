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
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractUnassignedScanJob */
public abstract class AbstractUnassignedScanJob extends AbstractUnassignedJob {
    protected final AtomicInteger shareScanIdGenerator = new AtomicInteger();

    public AbstractUnassignedScanJob(PlanFragment fragment,
            List<ScanNode> scanNodes, ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(fragment, scanNodes, exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {

        Map<DistributedPlanWorker, UninstancedScanSource> workerToScanSource = multipleMachinesParallelization(
                workerManager, inputJobs);

        List<AssignedJob> assignedJobs = insideMachineParallelization(workerToScanSource, inputJobs, workerManager);

        return fillUpAssignedJobs(assignedJobs, workerManager, inputJobs);
    }

    protected List<AssignedJob> fillUpAssignedJobs(
            List<AssignedJob> assignedJobs,
            DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        return assignedJobs;
    }

    protected abstract Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs);

    protected List<AssignedJob> insideMachineParallelization(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs, DistributedPlanWorkerManager workerManager) {

        ConnectContext context = ConnectContext.get();
        boolean useLocalShuffleToAddParallel = useLocalShuffleToAddParallel(workerToScanRanges);
        int instanceIndexInFragment = 0;
        List<AssignedJob> instances = Lists.newArrayList();
        for (Entry<DistributedPlanWorker, UninstancedScanSource> entry : workerToScanRanges.entrySet()) {
            DistributedPlanWorker worker = entry.getKey();

            // the scanRanges which this worker should scan,
            // for example:
            // {
            //   scan tbl1: [tablet_10001, tablet_10002, tablet_10003, tablet_10004] // no instances
            // }
            ScanSource scanSource = entry.getValue().scanSource;

            // usually, its tablets num, or buckets num
            int scanSourceMaxParallel = scanSource.maxParallel(scanNodes);

            // now we should compute how many instances to process the data,
            // for example: two instances
            int instanceNum = degreeOfParallelism(scanSourceMaxParallel);

            List<ScanSource> instanceToScanRanges;
            if (useLocalShuffleToAddParallel) {
                // only generate one instance to scan all data, in this step
                instanceToScanRanges = scanSource.parallelize(
                        scanNodes, 1
                );

                // Some tablets too big, we need add parallel to process these tablets after scan,
                // for example, use one OlapScanNode to scan data, and use some local instances
                // to process Aggregation parallel. We call it `share scan`. Backend will know this
                // instances share the same ScanSource, and will not scan same data multiple times.
                //
                // +-------------------------------- same fragment in one host -------------------------------------+
                // |                instance1      instance2     instance3     instance4                            |
                // |                    \              \             /            /                                 |
                // |                                                                                                |
                // |                                     OlapScanNode                                               |
                // |(share scan node, and local shuffle data to other local instances to parallel compute this data)|
                // +------------------------------------------------------------------------------------------------+
                ScanSource shareScanSource = instanceToScanRanges.get(0);

                // one scan range generate multiple instances,
                // different instances reference the same scan source
                int shareScanId = shareScanIdGenerator.getAndIncrement();
                for (int i = 0; i < instanceNum; i++) {
                    LocalShuffleAssignedJob instance = new LocalShuffleAssignedJob(
                            instanceIndexInFragment++, shareScanId, context.nextInstanceId(),
                            this, worker, shareScanSource);
                    instances.add(instance);
                }
            } else {
                // split the scanRanges to some partitions, one partition for one instance
                // for example:
                //  [
                //     scan tbl1: [tablet_10001, tablet_10003], // instance 1
                //     scan tbl1: [tablet_10002, tablet_10004]  // instance 2
                //  ]
                instanceToScanRanges = scanSource.parallelize(
                        scanNodes, instanceNum
                );

                for (ScanSource instanceToScanRange : instanceToScanRanges) {
                    instances.add(
                            assignWorkerAndDataSources(
                                instanceIndexInFragment++, context.nextInstanceId(), worker, instanceToScanRange
                            )
                    );
                }
            }
        }

        return instances;
    }

    protected boolean useLocalShuffleToAddParallel(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isForceToLocalShuffle()) {
            return true;
        }
        return parallelTooLittle(workerToScanRanges);
    }

    protected boolean parallelTooLittle(Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges) {
        if (this instanceof UnassignedScanBucketOlapTableJob) {
            return scanRangesToLittle(workerToScanRanges) && bucketsTooLittle(workerToScanRanges);
        } else if (this instanceof UnassignedScanSingleOlapTableJob
                || this instanceof UnassignedScanSingleRemoteTableJob) {
            return scanRangesToLittle(workerToScanRanges);
        } else {
            return false;
        }
    }

    protected boolean scanRangesToLittle(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges) {
        ConnectContext context = ConnectContext.get();
        int backendNum = workerToScanRanges.size();
        for (ScanNode scanNode : scanNodes) {
            if (!scanNode.ignoreStorageDataDistribution(context, backendNum)) {
                return false;
            }
        }
        return true;
    }

    protected int degreeOfParallelism(int maxParallel) {
        Preconditions.checkArgument(maxParallel > 0, "maxParallel must be positive");
        if (!fragment.getDataPartition().isPartitioned()) {
            return 1;
        }
        if (scanNodes.size() == 1 && scanNodes.get(0) instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) scanNodes.get(0);
            // if the scan node have limit and no conjuncts, only need 1 instance to save cpu and mem resource,
            // e.g. select * from tbl limit 10
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null && olapScanNode.shouldUseOneInstance(connectContext)) {
                return 1;
            }
        }

        // the scan instance num should not larger than the tablets num
        return Math.min(maxParallel, Math.max(fragment.getParallelExecNum(), 1));
    }

    protected boolean bucketsTooLittle(Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges) {
        int parallelExecNum = fragment.getParallelExecNum();
        for (UninstancedScanSource uninstancedScanSource : workerToScanRanges.values()) {
            ScanSource scanSource = uninstancedScanSource.scanSource;
            if (scanSource instanceof BucketScanSource) {
                BucketScanSource bucketScanSource = (BucketScanSource) scanSource;
                int bucketNum = bucketScanSource.bucketIndexToScanNodeToTablets.size();
                if (bucketNum >= parallelExecNum) {
                    return false;
                }
            }
        }
        return true;
    }

    protected List<AssignedJob> fillUpSingleEmptyInstance(DistributedPlanWorkerManager workerManager) {
        return ImmutableList.of(
                assignWorkerAndDataSources(0,
                        ConnectContext.get().nextInstanceId(),
                        workerManager.randomAvailableWorker(),
                        DefaultScanSource.empty())
        );
    }
}
