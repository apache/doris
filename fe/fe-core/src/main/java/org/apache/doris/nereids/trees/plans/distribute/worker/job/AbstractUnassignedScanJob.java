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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
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

    public AbstractUnassignedScanJob(StatementContext statementContext, PlanFragment fragment,
            List<ScanNode> scanNodes, ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(statementContext, fragment, scanNodes, exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {

        Map<DistributedPlanWorker, UninstancedScanSource> workerToScanSource
                = multipleMachinesParallelization(distributeContext, inputJobs);

        List<AssignedJob> assignedJobs = insideMachineParallelization(workerToScanSource, inputJobs, distributeContext);

        return fillUpAssignedJobs(assignedJobs, distributeContext.workerManager, inputJobs);
    }

    protected List<AssignedJob> fillUpAssignedJobs(
            List<AssignedJob> assignedJobs,
            DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        return assignedJobs;
    }

    protected abstract Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs);

    protected List<AssignedJob> insideMachineParallelization(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs,
            DistributeContext distributeContext) {

        ConnectContext context = statementContext.getConnectContext();
        boolean useLocalShuffleToAddParallel = useLocalShuffleToAddParallel();
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

            if (useLocalShuffleToAddParallel) {
                assignLocalShuffleJobs(scanSource, instanceNum, instances, context, worker);
            } else {
                assignedDefaultJobs(scanSource, instanceNum, instances, context, worker);
            }
        }

        return instances;
    }

    protected boolean useLocalShuffleToAddParallel() {
        return fragment.useSerialSource(ConnectContext.get());
    }

    protected void assignedDefaultJobs(ScanSource scanSource, int instanceNum, List<AssignedJob> instances,
            ConnectContext context, DistributedPlanWorker worker) {
        // split the scanRanges to some partitions, one partition for one instance
        // for example:
        //  [
        //     scan tbl1: [tablet_10001, tablet_10003], // instance 1
        //     scan tbl1: [tablet_10002, tablet_10004]  // instance 2
        //  ]
        List<ScanSource> instanceToScanRanges = scanSource.parallelize(scanNodes, instanceNum);

        for (ScanSource instanceToScanRange : instanceToScanRanges) {
            instances.add(
                    assignWorkerAndDataSources(
                        instances.size(), context.nextInstanceId(), worker, instanceToScanRange
                    )
            );
        }
    }

    protected void assignLocalShuffleJobs(ScanSource scanSource, int instanceNum, List<AssignedJob> instances,
            ConnectContext context, DistributedPlanWorker worker) {
        // only generate one instance to scan all data, in this step
        List<ScanSource> instanceToScanRanges = scanSource.parallelize(scanNodes, 1);

        // when data not big, but aggregation too slow, we will use 1 instance to scan data,
        // and use more instances (to ***add parallel***) to process aggregate.
        // We call it `ignore data distribution` of `share scan`. Backend will know this instances
        // share the same ScanSource, and will not scan same data multiple times.
        //
        // +-------------------------------- same fragment in one host -------------------------------------+
        // |                instance1      instance2     instance3     instance4                            |
        // |                    \              \             /            /                                 |
        // |                                                                                                |
        // |                                     OlapScanNode                                               |
        // |(share scan node, instance1 will scan all data and local shuffle to other local instances       |
        // |                           to parallel compute this data)                                       |
        // +------------------------------------------------------------------------------------------------+
        ScanSource shareScanSource = instanceToScanRanges.get(0);

        // one scan range generate multiple instances,
        // different instances reference the same scan source
        int shareScanId = shareScanIdGenerator.getAndIncrement();
        ScanSource emptyShareScanSource = shareScanSource.newEmpty();
        for (int i = 0; i < instanceNum; i++) {
            LocalShuffleAssignedJob instance = new LocalShuffleAssignedJob(
                    instances.size(), shareScanId, i > 0,
                    context.nextInstanceId(), this, worker,
                    i == 0 ? shareScanSource : emptyShareScanSource
            );
            instances.add(instance);
        }
    }

    protected int degreeOfParallelism(int maxParallel) {
        Preconditions.checkArgument(maxParallel > 0, "maxParallel must be positive");
        if (!fragment.getDataPartition().isPartitioned()) {
            return 1;
        }
        if (fragment.queryCacheParam != null) {
            // backend need use one instance for one tablet to look up tablet query cache
            return maxParallel;
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

    protected List<AssignedJob> fillUpSingleEmptyInstance(DistributedPlanWorkerManager workerManager) {
        return ImmutableList.of(
                assignWorkerAndDataSources(0,
                        ConnectContext.get().nextInstanceId(),
                        workerManager.randomAvailableWorker(),
                        DefaultScanSource.empty())
        );
    }
}
