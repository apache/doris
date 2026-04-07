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
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.List;

/**
 * UnassignedLocalTVFSinkJob.
 * For INSERT INTO local(...) with a specific backend_id, the sink fragment must
 * execute on the designated backend so that data is written to the correct node's local disk.
 */
public class UnassignedLocalTVFSinkJob extends AbstractUnassignedJob {
    private final long backendId;

    public UnassignedLocalTVFSinkJob(
            StatementContext statementContext, PlanFragment fragment,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob,
            long backendId) {
        super(statementContext, fragment, ImmutableList.of(), exchangeToChildJob);
        this.backendId = backendId;
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        Backend targetBackend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (targetBackend == null || !targetBackend.isAlive()) {
            throw new IllegalStateException("Backend " + backendId
                    + " is not available for local TVF sink");
        }
        DistributedPlanWorker worker = new BackendWorker(
                Env.getCurrentInternalCatalog().getId(), targetBackend);
        return ImmutableList.of(
                assignWorkerAndDataSources(
                        0, statementContext.getConnectContext().nextInstanceId(),
                        worker, new DefaultScanSource(ImmutableMap.of())
                )
        );
    }
}
