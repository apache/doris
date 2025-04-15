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
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.Config;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.function.Supplier;

/** BackendWorkerManager */
public class BackendDistributedPlanWorkerManager implements DistributedPlanWorkerManager {
    private static final Logger LOG = LogManager.getLogger(BackendDistributedPlanWorkerManager.class);

    private static final Backend DUMMY_BACKEND;

    static {
        DUMMY_BACKEND = new Backend(-1, "dummy", -1);
        DUMMY_BACKEND.setAlive(true);
    }

    private final Supplier<ImmutableMap<Long, Backend>> allClusterBackends = Suppliers.memoize(() -> {
        try {
            return Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (Exception t) {
            throw new NereidsException("Can not get backends: " + t, t);
        }
    });

    private final ImmutableMap<Long, Backend> currentClusterBackends;

    public BackendDistributedPlanWorkerManager(
            ConnectContext context, boolean notNeedBackend, boolean isLoadJob) throws UserException {
        this.currentClusterBackends = checkAndInitClusterBackends(context, notNeedBackend, isLoadJob);
    }

    private ImmutableMap<Long, Backend> checkAndInitClusterBackends(
            ConnectContext context, boolean noNeedBackend, boolean isLoadJob) throws UserException {
        if (!Config.isCloudMode()) {
            return Env.getCurrentEnv().getClusterInfo().getBackendsByCurrentCluster();
        } else if (noNeedBackend) {
            // `select 1` will not need backend
            return ImmutableMap.of(-1L, DUMMY_BACKEND);
        }

        // if is load, the ConnectContext.get() would be null, then we can skip check cluster
        if (isLoadJob) {
            context = ConnectContext.get();
        }

        checkCluster(context);
        ImmutableMap<Long, Backend> clusterBackend
                = Env.getCurrentEnv().getClusterInfo().getBackendsByCurrentCluster();
        if (clusterBackend == null || clusterBackend.isEmpty()) {
            LOG.warn("no available backends, clusterBackend {}", clusterBackend);
            String clusterName = context != null ? context.getCloudCluster() : "ctx empty cant get clusterName";
            throw new UserException("no available backends, the cluster maybe not be set or been dropped clusterName = "
                    + clusterName);
        }
        return clusterBackend;
    }

    private void checkCluster(ConnectContext context) throws UserException {
        String cluster;
        if (context != null) {
            if (!Strings.isNullOrEmpty(context.getSessionVariable().getCloudCluster())) {
                cluster = context.getSessionVariable().getCloudCluster();
                try {
                    ((CloudEnv) Env.getCurrentEnv()).checkCloudClusterPriv(cluster);
                } catch (Exception e) {
                    LOG.warn("get cluster by session context exception", e);
                    throw new UserException("get cluster by session context exception", e);
                }
                LOG.debug("get cluster by session context cluster: {}", cluster);
            } else {
                cluster = context.getCloudCluster();
                LOG.debug("get cluster by context {}", cluster);
            }
        } else {
            LOG.warn("connect context is null in coordinator prepare");
            // may cant throw exception? maybe cant get context in some scenarios
            return;
        }

        if (Strings.isNullOrEmpty(cluster)) {
            LOG.warn("invalid clusterName: {}", cluster);
            throw new UserException("empty clusterName, please check cloud cluster privilege");
        }
    }

    public boolean isCurrentClusterBackend(long backendId) {
        return currentClusterBackends.containsKey(backendId);
    }

    @Override
    public DistributedPlanWorker getWorker(long backendId) {
        ImmutableMap<Long, Backend> backends = this.allClusterBackends.get();
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
            ImmutableMap<Long, Backend> backends = this.currentClusterBackends;
            SimpleScheduler.getHost(backends, selectedBackendId);
            Backend selctedBackend = backends.get(selectedBackendId.getRef());
            return new BackendWorker(selctedBackend);
        } catch (Exception t) {
            throw new NereidsException("Can not get backends: " + t, t);
        }
    }

    @Override
    public long randomAvailableWorker(Map<TNetworkAddress, Long> addressToBackendID) {
        TNetworkAddress backend = SimpleScheduler.getHostByCurrentBackend(addressToBackendID);
        return addressToBackendID.get(backend);
    }
}
