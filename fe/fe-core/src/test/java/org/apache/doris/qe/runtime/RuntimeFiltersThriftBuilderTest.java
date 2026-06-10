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
import org.apache.doris.planner.ScanNode;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    private BackendWorker newBackendWorker(long id) {
        Backend backend = new Backend(id, "host" + id, (int) (9000 + id));
        backend.setBePort((int) (8000 + id));
        backend.setBrpcPort((int) (7000 + id));
        return new BackendWorker(0, backend);
    }

    private PipelineDistributedPlan newDistributedPlan(BackendWorker... workers) {
        TestUnassignedJob unassignedJob = new TestUnassignedJob();
        List<AssignedJob> assignedJobs = Arrays.stream(workers)
                .map(worker -> unassignedJob.assignWorkerAndDataSources(
                        0, new TUniqueId(), worker, DefaultScanSource.empty()))
                .collect(Collectors.toList());
        return new PipelineDistributedPlan(unassignedJob, assignedJobs, ImmutableSetMultimap.of());
    }

    private static final class TestUnassignedJob extends AbstractTreeNode<UnassignedJob> implements UnassignedJob {
        private TestUnassignedJob() {
            super(Collections.emptyList());
        }

        @Override
        public StatementContext getStatementContext() {
            return null;
        }

        @Override
        public PlanFragment getFragment() {
            return null;
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
