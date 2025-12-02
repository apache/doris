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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** SelectedWorkers */
public class SelectedWorkers {
    private final DistributedPlanWorkerManager workerManager;
    private final Map<Long, Map<TNetworkAddress, Long>> usedWorkersAddressToBackendID;
    private final Set<DistributedPlanWorker> usedWorkers;

    public SelectedWorkers(DistributedPlanWorkerManager workerManager) {
        this.workerManager = Objects.requireNonNull(workerManager, "workerManager can not be null");
        this.usedWorkersAddressToBackendID = Maps.newLinkedHashMap();
        this.usedWorkers = Sets.newLinkedHashSet();
    }

    /** onCreateAssignedJob */
    public void onCreateAssignedJob(AssignedJob assignedJob) {
        BackendWorker worker = (BackendWorker) assignedJob.getAssignedWorker();
        if (usedWorkers.add(worker)) {
            Backend backend = worker.getBackend();
            usedWorkersAddressToBackendID.computeIfAbsent(worker.getCatalogId(), k -> Maps.newLinkedHashMap());
            usedWorkersAddressToBackendID.get(worker.getCatalogId()).put(
                    new TNetworkAddress(backend.getHost(), backend.getBePort()), backend.getId()
            );
        }
    }

    /** tryToSelectRandomUsedWorker */
    public DistributedPlanWorker tryToSelectRandomUsedWorker() {
        long catalogId = Env.getCurrentInternalCatalog().getId();
        if (usedWorkers.isEmpty()) {
            return workerManager.randomAvailableWorker(catalogId);
        } else {
            Map<TNetworkAddress, Long> backendIDs;
            if (usedWorkersAddressToBackendID.containsKey(catalogId)) {
                backendIDs = usedWorkersAddressToBackendID.get(catalogId);
            } else {
                catalogId = usedWorkers.iterator().next().getCatalogId();
                backendIDs = usedWorkersAddressToBackendID.get(catalogId);
            }
            long id = workerManager.randomAvailableWorker(backendIDs);
            return workerManager.getWorker(catalogId, id);
        }
    }
}
