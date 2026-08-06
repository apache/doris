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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.List;

/** UnassignedQueryConstantJob */
public class UnassignedQueryConstantJob extends AbstractUnassignedJob {
    public UnassignedQueryConstantJob(StatementContext statementContext, PlanFragment fragment) {
        super(statementContext, fragment, ImmutableList.of(), ArrayListMultimap.create());
    }

    /**
     * Compute a single assigned job on a randomly selected worker for constant queries
     * (e.g. SELECT 1, SELECT * FROM VALUES(...)). Such queries have no data scan,
     * so a single instance with an empty {@link DefaultScanSource} suffices.
     *
     * @param distributeContext the distribute context for random worker selection
     * @param inputJobs unused — constant queries have no child fragments
     * @return a list containing exactly one assigned job on a random worker
     */
    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        DistributedPlanWorker randomWorker = distributeContext.selectedWorkers.tryToSelectRandomUsedWorker();
        ConnectContext connectContext = statementContext.getConnectContext();
        return ImmutableList.of(
                new StaticAssignedJob(0, connectContext.nextInstanceId(), this,
                        randomWorker, new DefaultScanSource(ImmutableMap.of())
                )
        );
    }
}
