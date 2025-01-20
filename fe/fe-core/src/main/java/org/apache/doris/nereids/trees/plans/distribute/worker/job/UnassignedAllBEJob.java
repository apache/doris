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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.List;

/** UnassignedAllBEJob */
public class UnassignedAllBEJob extends AbstractUnassignedJob {
    public UnassignedAllBEJob(StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToUpstreamJob) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToUpstreamJob);
    }

    // ExchangeNode -> upstreamFragment -> AssignedJob(instances of upstreamFragment)
    @Override
    public List<AssignedJob> computeAssignedJobs(DistributeContext distributeContext,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        ConnectContext connectContext = statementContext.getConnectContext();
        DistributedPlanWorkerManager workerManager = distributeContext.workerManager;

        // input jobs from upstream fragment - may have many instances.
        ExchangeNode exchange = inputJobs.keySet().iterator().next(); // random one - should be same for any exchange.
        int expectInstanceNum = exchange.getNumInstances();

        // for Coordinator to know the right parallelism of DictionarySink
        exchange.getFragment().setParallelExecNum(expectInstanceNum);

        List<Long> beIds = workerManager.getAllBackend(true);
        if (beIds.size() != expectInstanceNum) {
            // BE number changed when planning
            throw new IllegalArgumentException("BE number should be " + expectInstanceNum + ", but is " + beIds.size());
        }

        List<AssignedJob> assignedJobs = Lists.newArrayList();
        for (int i = 0; i < beIds.size(); ++i) {
            // every time one BE is selected
            DistributedPlanWorker worker = workerManager.getWorker(beIds.get(i));
            if (worker != null) {
                assignedJobs.add(assignWorkerAndDataSources(i, connectContext.nextInstanceId(), worker,
                        new DefaultScanSource(ImmutableMap.of())));
            } else {
                throw new IllegalArgumentException(
                        "worker " + Env.getCurrentSystemInfo().getBackend(beIds.get(i)).getAddress() + " not found");
            }
        }

        return assignedJobs;
    }
}
