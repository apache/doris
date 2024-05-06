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

package org.apache.doris.nereids.worker.job;

import org.apache.doris.nereids.worker.Worker;
import org.apache.doris.nereids.worker.WorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.List;

/** UnassignedQueryConstantJob */
public class UnassignedQueryConstantJob extends AbstractUnassignedJob {
    public UnassignedQueryConstantJob(PlanFragment fragment) {
        super(fragment, ImmutableList.of(), ArrayListMultimap.create());
    }

    @Override
    public List<AssignedJob> computeAssignedJobs(WorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        Worker randomWorker = workerManager.randomAvailableWorker();
        return ImmutableList.of(
                new StaticAssignedJob(0, this, randomWorker,
                        new DefaultScanSource(ImmutableMap.of())
                )
        );
    }
}
