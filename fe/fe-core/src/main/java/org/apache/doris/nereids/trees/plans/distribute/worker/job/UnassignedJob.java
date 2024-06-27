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

import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ListMultimap;

import java.util.List;

/**
 * WorkerJob.
 * for example: a fragment job, which doesn't parallelization to some instance jobs and also no worker to invoke it
 */
public interface UnassignedJob extends TreeNode<UnassignedJob> {
    PlanFragment getFragment();

    List<ScanNode> getScanNodes();

    ListMultimap<ExchangeNode, UnassignedJob> getExchangeToChildJob();

    List<AssignedJob> computeAssignedJobs(
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs);

    // generate an instance job
    // e.g. build an instance job by a backends and the replica ids it contains
    default AssignedJob assignWorkerAndDataSources(
            int instanceIndexInFragment, TUniqueId instanceId, DistributedPlanWorker worker, ScanSource scanSource) {
        return new StaticAssignedJob(instanceIndexInFragment, instanceId, this, worker, scanSource);
    }
}
