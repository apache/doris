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
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.SchemaScanNode;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** UnassignedScanMetadataJob */
public class UnassignedScanMetadataJob extends AbstractUnassignedScanJob {
    private final SchemaScanNode schemaScanNode;
    private final ScanWorkerSelector scanWorkerSelector;

    public UnassignedScanMetadataJob(PlanFragment fragment, SchemaScanNode schemaScanNode,
            ScanWorkerSelector scanWorkerSelector) {
        super(fragment, ImmutableList.of(schemaScanNode), ArrayListMultimap.create());
        this.scanWorkerSelector = Objects.requireNonNull(
                scanWorkerSelector, "scanWorkerSelector cat not be null");
        this.schemaScanNode = schemaScanNode;
    }

    @Override
    protected Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        return scanWorkerSelector.selectReplicaAndWorkerWithoutBucket(schemaScanNode);
    }

    @Override
    protected List<AssignedJob> fillUpAssignedJobs(List<AssignedJob> assignedJobs,
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        if (assignedJobs.isEmpty()) {
            return fillUpSingleEmptyInstance(workerManager);
        }
        return assignedJobs;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(tableName="
                + schemaScanNode.getSchemaCatalog()
                + "." + schemaScanNode.getSchemaDb()
                + "." + schemaScanNode.getTableName()
                + ")";
    }
}
