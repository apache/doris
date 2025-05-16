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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Reference;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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
        if (noNeedBackend) {
            // `select 1` will not need backend
            return ImmutableMap.of(-1L, DUMMY_BACKEND);
        } else if (!Config.isCloudMode()) {
            return Env.getCurrentEnv().getClusterInfo().getBackendsByCurrentCluster();
        }

        // if is load, the ConnectContext.get() would be null, then we can skip check cluster
        if (isLoadJob) {
            context = ConnectContext.get();
        }

        if (context == null) {
            throw new AnalysisException("connect context is null");
        }

        // 1 get compute group
        ComputeGroup cg = context.getComputeGroup();

        // 2 check priv
        try {
            ((CloudEnv) Env.getCurrentEnv()).checkCloudClusterPriv(cg.getName());
        } catch (Exception e) {
            LOG.warn("cluster priv check failed", e);
            throw new UserException(
                    "cluster priv check failed, user is " + ConnectContext.get().getCurrentUserIdentity().toString()
                            + ", cluster is " + cg.getName(), e);
        }

        // 3 return be list
        List<Backend> beList = cg.getBackendList();
        if (beList.isEmpty()) {
            LOG.warn("no available backends, compute group is {}", cg.toString());
            throw new UserException("no available backends, the cluster maybe not be set or been dropped clusterName = "
                    + cg.getName());
        }

        Map<Long, Backend> idToBeMap = Maps.newHashMap();
        for (Backend be : beList) {
            idToBeMap.put(be.getId(), be);
        }
        return ImmutableMap.copyOf(idToBeMap);
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
    public DistributedPlanWorker getWorker(Backend backend) {
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

    @Override
    public List<Backend> getAllBackends(boolean needAlive) {
        List<Backend> backends = null;
        if (needAlive) {
            backends = Lists.newArrayList();
            for (Map.Entry<Long, Backend> entry : this.allClusterBackends.get().entrySet()) {
                if (entry.getValue().isQueryAvailable()) {
                    backends.add(entry.getValue());
                }
            }
        } else {
            backends = Lists.newArrayList(this.allClusterBackends.get().values());
        }
        return backends;
    }
}
