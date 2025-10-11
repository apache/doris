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

import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendDistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.RecursiveCteNode;
import org.apache.doris.planner.RecursiveCteScanNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRecCTENode;
import org.apache.doris.thrift.TRecCTEResetInfo;
import org.apache.doris.thrift.TRecCTETarget;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** AssignedJobBuilder */
public class AssignedJobBuilder {
    /** buildJobs */
    public static ListMultimap<PlanFragmentId, AssignedJob> buildJobs(
            Map<PlanFragmentId, UnassignedJob> unassignedJobs, BackendDistributedPlanWorkerManager workerManager,
            boolean isLoadJob) {
        DistributeContext distributeContext = new DistributeContext(workerManager, isLoadJob);
        ListMultimap<PlanFragmentId, AssignedJob> allAssignedJobs = ArrayListMultimap.create();
        Map<PlanFragmentId, TRecCTETarget> fragmentIdToRecCteTargetMap = new TreeMap<>();
        Map<PlanFragmentId, Set<TNetworkAddress>> fragmentIdToNetworkAddressMap = new TreeMap<>();
        for (Entry<PlanFragmentId, UnassignedJob> kv : unassignedJobs.entrySet()) {
            PlanFragmentId fragmentId = kv.getKey();
            UnassignedJob unassignedJob = kv.getValue();
            ListMultimap<ExchangeNode, AssignedJob> inputAssignedJobs
                    = getInputAssignedJobs(unassignedJob, allAssignedJobs);
            List<AssignedJob> fragmentAssignedJobs =
                    unassignedJob.computeAssignedJobs(distributeContext, inputAssignedJobs);
            for (AssignedJob assignedJob : fragmentAssignedJobs) {
                distributeContext.selectedWorkers.onCreateAssignedJob(assignedJob);
            }

            if (fragmentAssignedJobs.isEmpty()) {
                throw new IllegalStateException("Fragment has no instance, unassignedJob: " + unassignedJob
                        + ", fragment: " + unassignedJob.getFragment().getExplainString(TExplainLevel.VERBOSE));
            }
            allAssignedJobs.putAll(fragmentId, fragmentAssignedJobs);

            Set<TNetworkAddress> networkAddresses = new TreeSet<>();
            for (AssignedJob assignedJob : fragmentAssignedJobs) {
                DistributedPlanWorker distributedPlanWorker = assignedJob.getAssignedWorker();
                networkAddresses
                        .add(new TNetworkAddress(distributedPlanWorker.host(), distributedPlanWorker.brpcPort()));
            }
            fragmentIdToNetworkAddressMap.put(fragmentId, networkAddresses);

            PlanFragment planFragment = unassignedJob.getFragment();
            List<RecursiveCteScanNode> recursiveCteScanNodes = planFragment.getPlanRoot()
                    .collectInCurrentFragment(RecursiveCteScanNode.class::isInstance);
            if (!recursiveCteScanNodes.isEmpty()) {
                if (recursiveCteScanNodes.size() != 1) {
                    throw new IllegalStateException(
                            String.format("one fragment can only have 1 recursive cte scan node, but there is %d",
                                    recursiveCteScanNodes.size()));
                }
                if (fragmentAssignedJobs.size() != 1) {
                    throw new IllegalStateException(String.format(
                            "fragmentAssignedJobs's size must be 1 for recursive cte scan node, but it is %d",
                            fragmentAssignedJobs.size()));
                }
                TRecCTETarget tRecCTETarget = new TRecCTETarget();
                DistributedPlanWorker distributedPlanWorker = fragmentAssignedJobs.get(0).getAssignedWorker();
                tRecCTETarget
                        .setAddr(new TNetworkAddress(distributedPlanWorker.host(), distributedPlanWorker.brpcPort()));
                tRecCTETarget.setFragmentInstanceId(fragmentAssignedJobs.get(0).instanceId());
                tRecCTETarget.setNodeId(recursiveCteScanNodes.get(0).getId().asInt());
                fragmentIdToRecCteTargetMap.put(fragmentId, tRecCTETarget);
            }

            List<RecursiveCteNode> recursiveCteNodes = planFragment.getPlanRoot()
                    .collectInCurrentFragment(RecursiveCteNode.class::isInstance);
            if (!recursiveCteNodes.isEmpty()) {
                if (recursiveCteNodes.size() != 1) {
                    throw new IllegalStateException(
                            String.format("one fragment can only have 1 recursive cte node, but there is %d",
                                    recursiveCteNodes.size()));
                }

                List<TRecCTETarget> targets = new ArrayList<>();
                List<TRecCTEResetInfo> fragmentsToReset = new ArrayList<>();
                // PhysicalPlanTranslator will swap recursiveCteNodes's child fragment,
                // so we get recursive one by 1st child
                List<PlanFragment> childFragments = new ArrayList<>();
                planFragment.getChild(0).collectAll(PlanFragment.class::isInstance, childFragments);
                for (PlanFragment child : childFragments) {
                    PlanFragmentId childFragmentId = child.getFragmentId();
                    TRecCTETarget tRecCTETarget = fragmentIdToRecCteTargetMap.getOrDefault(childFragmentId, null);
                    if (tRecCTETarget != null) {
                        targets.add(tRecCTETarget);
                    }
                    Set<TNetworkAddress> tNetworkAddresses = fragmentIdToNetworkAddressMap.get(childFragmentId);
                    if (tNetworkAddresses == null) {
                        throw new IllegalStateException(
                                String.format("can't find TNetworkAddress for fragment %d", childFragmentId));
                    }
                    for (TNetworkAddress address : tNetworkAddresses) {
                        TRecCTEResetInfo tRecCTEResetInfo = new TRecCTEResetInfo();
                        tRecCTEResetInfo.setFragmentId(childFragmentId.asInt());
                        tRecCTEResetInfo.setAddr(address);
                        fragmentsToReset.add(tRecCTEResetInfo);
                    }
                }

                RecursiveCteNode recursiveCteNode = recursiveCteNodes.get(0);
                List<List<Expr>> materializedResultExprLists = recursiveCteNode.getMaterializedResultExprLists();
                List<List<TExpr>> texprLists = new ArrayList<>(materializedResultExprLists.size());
                for (List<Expr> exprList : materializedResultExprLists) {
                    texprLists.add(Expr.treesToThrift(exprList));
                }
                TRecCTENode tRecCTENode = new TRecCTENode();
                tRecCTENode.setIsUnionAll(recursiveCteNode.isUnionAll());
                tRecCTENode.setTargets(targets);
                tRecCTENode.setFragmentsToReset(fragmentsToReset);
                tRecCTENode.setResultExprLists(texprLists);
                recursiveCteNode.settRecCTENode(tRecCTENode);
            }
        }
        return allAssignedJobs;
    }

    private static ListMultimap<ExchangeNode, AssignedJob> getInputAssignedJobs(
            UnassignedJob unassignedJob, ListMultimap<PlanFragmentId, AssignedJob> assignedJobs) {
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        for (Entry<ExchangeNode, Collection<UnassignedJob>> exchangeNodeToChildJobs
                : unassignedJob.getExchangeToChildJob().asMap().entrySet()) {
            ExchangeNode exchangeNode = exchangeNodeToChildJobs.getKey();
            Collection<UnassignedJob> childJobs = exchangeNodeToChildJobs.getValue();
            for (UnassignedJob childJob : childJobs) {
                inputJobs.putAll(exchangeNode, assignedJobs.get(childJob.getFragment().getFragmentId()));
            }
        }
        return inputJobs;
    }
}
