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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Reference;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import java.util.function.Supplier;

/** BackendWorkerManager */
public class BackendDistributedPlanWorkerManager implements DistributedPlanWorkerManager {
    private final Supplier<ImmutableMap<Long, Backend>> backends = Suppliers.memoize(() -> {
        try {
            return Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (Exception t) {
            throw new NereidsException("Can not get backends: " + t, t);
        }
    });

    @Override
    public DistributedPlanWorker getWorker(long backendId) {
        ImmutableMap<Long, Backend> backends = this.backends.get();
        Backend backend = backends.get(backendId);
        if (backend == null) {
            throw new IllegalStateException("Backend " + backendId + " is not exist");
        }
        return new BackendWorker(backend);
    }

    @Override
    public DistributedPlanWorker randomAvailableWorker() {
        try {
            Reference<Long> selectedBackendId = new Reference<>();
            ImmutableMap<Long, Backend> backends = this.backends.get();
            SimpleScheduler.getHost(backends, selectedBackendId);
            Backend selctedBackend = backends.get(selectedBackendId.getRef());
            return new BackendWorker(selctedBackend);
        } catch (Exception t) {
            throw new NereidsException("Can not get backends: " + t, t);
        }
    }
}
