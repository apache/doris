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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

/** UnassignedShuffleJob */
public class UnassignedShuffleJob extends AbstractUnassignedJob {
    private boolean useLocalShuffleToAddParallel;

    public UnassignedShuffleJob(
            StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToChildJob);
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        useLocalShuffleToAddParallel = fragment.useSerialSource(statementContext.getConnectContext());

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
        // TODO: check we use nested loop join do right outer / semi / anti join,
        //       we should add an exchange node with gather distribute under the nested loop join

        int expectInstanceNum = -1;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
            expectInstanceNum = ConnectContext.get().getSessionVariable().getExchangeInstanceParallel();
        }

        // TODO: check nested loop join do right outer / semi / anti join
        PlanNode leftMostNode = findLeftmostNode(fragment.getPlanRoot()).second;
        // when we use nested loop join do right outer / semi / anti join, the instance must be 1.
        if (leftMostNode.getNumInstances() == 1) {
            expectInstanceNum = 1;
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

    private List<AssignedJob> buildInstances(
            int instanceNum, Function<Integer, DistributedPlanWorker> workerSelector) {
        ConnectContext connectContext = statementContext.getConnectContext();
        if (useLocalShuffleToAddParallel) {
            return buildInstancesWithLocalShuffle(instanceNum, workerSelector, connectContext);
        } else {
            return buildInstancesWithoutLocalShuffle(instanceNum, workerSelector, connectContext);
        }
    }

    private List<AssignedJob> buildInstancesWithoutLocalShuffle(
            int instanceNum, Function<Integer, DistributedPlanWorker> workerSelector, ConnectContext connectContext) {
        ImmutableList.Builder<AssignedJob> instances = ImmutableList.builderWithExpectedSize(instanceNum);
        for (int i = 0; i < instanceNum; i++) {
            DistributedPlanWorker selectedWorker = workerSelector.apply(i);
            AssignedJob assignedJob = assignWorkerAndDataSources(
                    i, connectContext.nextInstanceId(),
                    selectedWorker, new DefaultScanSource(ImmutableMap.of())
            );
            instances.add(assignedJob);
        }
        return instances.build();
    }

    private List<AssignedJob> buildInstancesWithLocalShuffle(
            int instanceNum, Function<Integer, DistributedPlanWorker> workerSelector, ConnectContext connectContext) {
        ImmutableList.Builder<AssignedJob> instances = ImmutableList.builderWithExpectedSize(instanceNum);
        Multimap<DistributedPlanWorker, Integer> workerToInstanceIds = ArrayListMultimap.create();
        for (int i = 0; i < instanceNum; i++) {
            DistributedPlanWorker selectedWorker = workerSelector.apply(i);
            workerToInstanceIds.put(selectedWorker, i);
        }

        int shareScanId = 0;
        for (Entry<DistributedPlanWorker, Collection<Integer>> kv : workerToInstanceIds.asMap().entrySet()) {
            DistributedPlanWorker worker = kv.getKey();
            Collection<Integer> indexesInFragment = kv.getValue();

            DefaultScanSource shareScanSource = new DefaultScanSource(ImmutableMap.of());

            boolean receiveDataFromLocal = false;
            for (Integer indexInFragment : indexesInFragment) {
                LocalShuffleAssignedJob instance = new LocalShuffleAssignedJob(
                        indexInFragment, shareScanId, receiveDataFromLocal, connectContext.nextInstanceId(),
                        this, worker, shareScanSource
                );
                instances.add(instance);
                receiveDataFromLocal = true;
            }
            shareScanId++;
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

    // Returns the id of the leftmost node of any of the gives types in 'plan_root',
    // or INVALID_PLAN_NODE_ID if no such node present.
    private Pair<PlanNode, PlanNode> findLeftmostNode(PlanNode plan) {
        PlanNode childPlan = plan;
        PlanNode fatherPlan = null;
        while (childPlan.getChildren().size() != 0 && !(childPlan instanceof ExchangeNode)) {
            fatherPlan = childPlan;
            childPlan = childPlan.getChild(0);
        }
        return Pair.of(fatherPlan, childPlan);
    }
}
