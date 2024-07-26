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
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

/** UnassignedShuffleJob */
public class UnassignedShuffleJob extends AbstractUnassignedJob {
    public UnassignedShuffleJob(PlanFragment fragment, ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(fragment, ImmutableList.of(), exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        int expectInstanceNum = degreeOfParallelism();
        List<AssignedJob> biggestParallelChildFragment = getInstancesOfBiggestParallelChildFragment(inputJobs);

        if (expectInstanceNum > 0 && expectInstanceNum < biggestParallelChildFragment.size()) {
            // When group by cardinality is smaller than number of backend, only some backends always
            // process while other has no data to process.
            // So we shuffle instances to make different backends handle different queries.
            List<DistributedPlanWorker> shuffleWorkersInBiggestParallelChildFragment
                    = distinctShuffleWorkers(biggestParallelChildFragment);
            Function<Integer, DistributedPlanWorker> workerSelector = instanceIndex -> {
                int selectIndex = instanceIndex % shuffleWorkersInBiggestParallelChildFragment.size();
                return shuffleWorkersInBiggestParallelChildFragment.get(selectIndex);
            };
            return buildInstances(expectInstanceNum, workerSelector);
        } else {
            // keep same instance num like child fragment
            Function<Integer, DistributedPlanWorker> workerSelector = instanceIndex -> {
                int selectIndex = instanceIndex % biggestParallelChildFragment.size();
                return biggestParallelChildFragment.get(selectIndex).getAssignedWorker();
            };
            return buildInstances(biggestParallelChildFragment.size(), workerSelector);
        }
    }

    protected int degreeOfParallelism() {
        if (!fragment.getDataPartition().isPartitioned()) {
            return 1;
        }

        // TODO: check we use nested loop join do right outer / semi / anti join,
        //       we should add an exchange node with gather distribute under the nested loop join

        int expectInstanceNum = -1;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            expectInstanceNum = ConnectContext.get().getSessionVariable().getExchangeInstanceParallel();
        }
        return expectInstanceNum;
    }

    private List<AssignedJob> getInstancesOfBiggestParallelChildFragment(
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        int maxInstanceNum = -1;
        List<AssignedJob> biggestParallelChildFragment = ImmutableList.of();
        // skip broadcast exchange
        for (Entry<ExchangeNode, Collection<AssignedJob>> exchangeToChildInstances : inputJobs.asMap().entrySet()) {
            List<AssignedJob> instances = (List) exchangeToChildInstances.getValue();
            if (instances.size() > maxInstanceNum) {
                biggestParallelChildFragment = instances;
                maxInstanceNum = instances.size();
            }
        }
        return biggestParallelChildFragment;
    }

    private List<AssignedJob> buildInstances(int instanceNum, Function<Integer, DistributedPlanWorker> workerSelector) {
        ImmutableList.Builder<AssignedJob> instances = ImmutableList.builderWithExpectedSize(instanceNum);
        ConnectContext context = ConnectContext.get();
        for (int i = 0; i < instanceNum; i++) {
            DistributedPlanWorker selectedWorker = workerSelector.apply(i);
            AssignedJob assignedJob = assignWorkerAndDataSources(
                    i, context.nextInstanceId(), selectedWorker, new DefaultScanSource(ImmutableMap.of())
            );
            instances.add(assignedJob);
        }
        return instances.build();
    }

    private List<DistributedPlanWorker> distinctShuffleWorkers(List<AssignedJob> instances) {
        Set<DistributedPlanWorker> candidateWorkerSet = Sets.newLinkedHashSet();
        for (AssignedJob instance : instances) {
            candidateWorkerSet.add(instance.getAssignedWorker());
        }
        List<DistributedPlanWorker> candidateWorkers = Lists.newArrayList(candidateWorkerSet);
        Collections.shuffle(candidateWorkers);
        return candidateWorkers;
    }
}
