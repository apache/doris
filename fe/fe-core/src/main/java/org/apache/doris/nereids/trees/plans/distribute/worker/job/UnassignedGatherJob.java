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
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.List;

/** UnassignedGatherJob */
public class UnassignedGatherJob extends AbstractUnassignedJob {
    private boolean useSerialSource;

    public UnassignedGatherJob(
            StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        useSerialSource = fragment.useSerialSource(
                distributeContext.isLoadJob ? null : statementContext.getConnectContext());

        ConnectContext connectContext = statementContext.getConnectContext();
        int expectInstanceNum = degreeOfParallelism();

        DistributedPlanWorker selectedWorker = distributeContext.selectedWorkers.tryToSelectRandomUsedWorker();
        if (useSerialSource) {
            // Using serial source means a serial source operator will be used in this fragment (e.g. data will be
            // shuffled to only 1 exchange operator) and then split by followed local exchanger
            ImmutableList.Builder<AssignedJob> instances = ImmutableList.builder();
            DefaultScanSource shareScan = new DefaultScanSource(ImmutableMap.of());
            LocalShuffleAssignedJob receiveDataFromRemote = new LocalShuffleAssignedJob(
                    0, 0,
                    connectContext.nextInstanceId(), this, selectedWorker, shareScan);

            instances.add(receiveDataFromRemote);
            for (int i = 1; i < expectInstanceNum; ++i) {
                LocalShuffleAssignedJob receiveDataFromLocal = new LocalShuffleAssignedJob(
                        i, 0, connectContext.nextInstanceId(), this, selectedWorker, shareScan);
                instances.add(receiveDataFromLocal);
            }
            return instances.build();
        } else {
            return ImmutableList.of(
                    assignWorkerAndDataSources(
                            0, connectContext.nextInstanceId(),
                            selectedWorker, new DefaultScanSource(ImmutableMap.of())
                    )
            );
        }
    }

    protected int degreeOfParallelism() {
        return useSerialSource ? Math.max(1, fragment.getParallelExecNum()) : 1;
    }
}
