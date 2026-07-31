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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPlan;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TRuntimeFilterDesc;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RuntimeFiltersThriftBuilderTest {
    @Test
    public void testSelectRuntimeFilterMergeWorkerFromTopMostWorkers() {
        BackendWorker worker1 = newBackendWorker(1);
        BackendWorker worker2 = newBackendWorker(2);
        BackendWorker worker3 = newBackendWorker(3);
        PipelineDistributedPlan scanPlan = newDistributedPlan(worker1, worker2);
        PipelineDistributedPlan joinPlan = newDistributedPlan(worker1, worker3);
        PipelineDistributedPlan topPlan = newDistributedPlan(worker1, worker3);

        List<PipelineDistributedPlan> distributedPlans = Arrays.asList(scanPlan, joinPlan, topPlan);
        List<BackendWorker> candidates = RuntimeFiltersThriftBuilder.collectMergeWorkerCandidates(distributedPlans);

        Assertions.assertEquals(Arrays.asList(worker1, worker3), candidates);
        Assertions.assertTrue(candidates.contains(RuntimeFiltersThriftBuilder.selectMergeWorker(distributedPlans)));
    }

    @Test
    public void testSelectBroadcastRuntimeFilterProducerWorkersPreferMergeWorker() {
        BackendWorker worker1 = newBackendWorker(1);
        BackendWorker worker2 = newBackendWorker(2);
        BackendWorker worker3 = newBackendWorker(3);

        List<BackendWorker> selectedWorkers = RuntimeFiltersThriftBuilder.selectBroadcastRuntimeFilterProducerWorkers(
                Arrays.asList(worker1, worker2, worker3), 1, worker2);

        Assertions.assertEquals(Collections.singletonList(worker2), selectedWorkers);
    }

    @Test
    public void testSelectBroadcastRuntimeFilterProducerWorkersIgnoreNonCandidatePreferredWorker() {
        BackendWorker worker1 = newBackendWorker(1);
        BackendWorker worker2 = newBackendWorker(2);
        BackendWorker worker3 = newBackendWorker(3);

        List<BackendWorker> selectedWorkers = RuntimeFiltersThriftBuilder.selectBroadcastRuntimeFilterProducerWorkers(
                Arrays.asList(worker1, worker2), 1, worker3);

        Assertions.assertEquals(1, selectedWorkers.size());
        Assertions.assertTrue(Arrays.asList(worker1, worker2).contains(selectedWorkers.get(0)));
        Assertions.assertFalse(selectedWorkers.contains(worker3));
    }

    @Test
    public void testPruneBroadcastRuntimeFilterProducers() {
        BackendWorker worker1 = newBackendWorker(1);
        BackendWorker worker2 = newBackendWorker(2);
        BackendWorker worker3 = newBackendWorker(3);

        IdGenerator<RuntimeFilterId> idGenerator = RuntimeFilterId.createGenerator();
        RuntimeFilterId broadcastRid1 = idGenerator.getNextId();
        RuntimeFilterId broadcastRid2 = idGenerator.getNextId();
        RuntimeFilterId localBroadcastRid = idGenerator.getNextId();
        RuntimeFilterId shuffleRid = idGenerator.getNextId();
        RuntimeFilter broadcastRf1 = newRuntimeFilter(broadcastRid1, 10, true, true);
        RuntimeFilter broadcastRf2 = newRuntimeFilter(broadcastRid2, 10, true, true);
        RuntimeFilter localBroadcastRf = newRuntimeFilter(localBroadcastRid, 10, true, false);
        RuntimeFilter shuffleRf = newRuntimeFilter(shuffleRid, 11, false, true);

        PlanFragment fragment = newFragment(0, broadcastRid1, broadcastRid2, localBroadcastRid, shuffleRid);
        PipelineDistributedPlan distributedPlan = newDistributedPlan(fragment, worker1, worker2, worker3);
        RuntimeFiltersThriftBuilder builder = RuntimeFiltersThriftBuilder.compute(
                Arrays.asList(broadcastRf1, broadcastRf2, localBroadcastRf, shuffleRf),
                Collections.singletonList(distributedPlan), 1);

        int selectedWorkerNum = 0;
        for (BackendWorker worker : Arrays.asList(worker1, worker2, worker3)) {
            TPlanFragment planFragment = newPlanFragment(
                    newPlanNode(10,
                            newRuntimeFilterDesc(broadcastRid1, true, true),
                            newRuntimeFilterDesc(broadcastRid2, true, true),
                            newRuntimeFilterDesc(localBroadcastRid, true, false)),
                    newPlanNode(11,
                            newRuntimeFilterDesc(shuffleRid, false, true)),
                    newPlanNode(20,
                            newRuntimeFilterDesc(broadcastRid1, true, true),
                            newRuntimeFilterDesc(broadcastRid2, true, true)));
            builder.pruneBroadcastRuntimeFilterProducers(planFragment, worker);

            List<Integer> builderFilterIds = planFragment.getPlan().getNodes().get(0).getRuntimeFilters()
                    .stream()
                    .map(desc -> desc.filter_id)
                    .collect(Collectors.toList());
            List<Integer> shuffleFilterIds = planFragment.getPlan().getNodes().get(1).getRuntimeFilters()
                    .stream()
                    .map(desc -> desc.filter_id)
                    .collect(Collectors.toList());
            List<Integer> targetFilterIds = planFragment.getPlan().getNodes().get(2).getRuntimeFilters()
                    .stream()
                    .map(desc -> desc.filter_id)
                    .collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(shuffleRid.asInt()), shuffleFilterIds);
            Assertions.assertEquals(new HashSet<>(Arrays.asList(
                    broadcastRid1.asInt(), broadcastRid2.asInt())), new HashSet<>(targetFilterIds));
            if (builderFilterIds.contains(broadcastRid1.asInt())) {
                selectedWorkerNum++;
                Assertions.assertEquals(new HashSet<>(Arrays.asList(
                        broadcastRid1.asInt(), broadcastRid2.asInt(), localBroadcastRid.asInt())),
                        new HashSet<>(builderFilterIds));
            } else {
                Assertions.assertFalse(builderFilterIds.contains(broadcastRid2.asInt()));
                Assertions.assertEquals(Collections.singletonList(localBroadcastRid.asInt()), builderFilterIds);
            }
        }
        Assertions.assertEquals(1, selectedWorkerNum);
    }

    private BackendWorker newBackendWorker(long id) {
        Backend backend = new Backend(id, "host" + id, (int) (9000 + id));
        backend.setBePort((int) (8000 + id));
        backend.setBrpcPort((int) (7000 + id));
        return new BackendWorker(0, backend);
    }

    private PipelineDistributedPlan newDistributedPlan(BackendWorker... workers) {
        return newDistributedPlan(null, workers);
    }

    private PipelineDistributedPlan newDistributedPlan(PlanFragment fragment, BackendWorker... workers) {
        TestUnassignedJob unassignedJob = new TestUnassignedJob();
        unassignedJob.fragment = fragment;
        List<AssignedJob> assignedJobs = Arrays.stream(workers)
                .map(worker -> unassignedJob.assignWorkerAndDataSources(
                        0, new TUniqueId(), worker, DefaultScanSource.empty()))
                .collect(Collectors.toList());
        return new PipelineDistributedPlan(unassignedJob, assignedJobs, ImmutableSetMultimap.of());
    }

    private RuntimeFilter newRuntimeFilter(RuntimeFilterId rid, int builderNodeId,
            boolean isBroadcast, boolean hasRemoteTargets) {
        PlanNode builderNode = Mockito.mock(PlanNode.class);
        Mockito.when(builderNode.getId()).thenReturn(new PlanNodeId(builderNodeId));

        RuntimeFilter runtimeFilter = Mockito.mock(RuntimeFilter.class);
        Mockito.when(runtimeFilter.getFilterId()).thenReturn(rid);
        Mockito.when(runtimeFilter.getBuilderNode()).thenReturn(builderNode);
        Mockito.when(runtimeFilter.isBroadcast()).thenReturn(isBroadcast);
        Mockito.when(runtimeFilter.hasRemoteTargets()).thenReturn(hasRemoteTargets);
        return runtimeFilter;
    }

    private TPlanFragment newPlanFragment(TPlanNode... nodes) {
        TPlan plan = new TPlan();
        plan.setNodes(Arrays.asList(nodes));
        TPlanFragment fragment = new TPlanFragment();
        fragment.setPlan(plan);
        return fragment;
    }

    private TPlanNode newPlanNode(int nodeId, TRuntimeFilterDesc... runtimeFilterDescs) {
        TPlanNode node = new TPlanNode();
        node.setNodeId(nodeId);
        node.setRuntimeFilters(Arrays.asList(runtimeFilterDescs));
        return node;
    }

    private TRuntimeFilterDesc newRuntimeFilterDesc(
            RuntimeFilterId rid, boolean isBroadcast, boolean hasRemoteTargets) {
        TRuntimeFilterDesc desc = new TRuntimeFilterDesc();
        desc.setFilterId(rid.asInt());
        desc.setIsBroadcastJoin(isBroadcast);
        desc.setHasRemoteTargets(hasRemoteTargets);
        return desc;
    }

    private PlanFragment newFragment(int fragmentId, RuntimeFilterId... builderRuntimeFilterIds) {
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(fragmentId));
        Set<RuntimeFilterId> builderIds = new HashSet<>(Arrays.asList(builderRuntimeFilterIds));
        Mockito.when(fragment.getBuilderRuntimeFilterIds()).thenReturn(builderIds);
        Mockito.when(fragment.getTargetRuntimeFilterIds()).thenReturn(Collections.emptySet());
        return fragment;
    }

    private static final class TestUnassignedJob extends AbstractTreeNode<UnassignedJob> implements UnassignedJob {
        private PlanFragment fragment;

        private TestUnassignedJob() {
            super(Collections.emptyList());
        }

        @Override
        public StatementContext getStatementContext() {
            return null;
        }

        @Override
        public PlanFragment getFragment() {
            return fragment;
        }

        @Override
        public List<ScanNode> getScanNodes() {
            return Collections.emptyList();
        }

        @Override
        public ListMultimap<ExchangeNode, UnassignedJob> getExchangeToChildJob() {
            return ArrayListMultimap.create();
        }

        @Override
        public List<AssignedJob> computeAssignedJobs(
                DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
            return Collections.emptyList();
        }

        @Override
        public UnassignedJob withChildren(List<UnassignedJob> children) {
            return this;
        }
    }
}
