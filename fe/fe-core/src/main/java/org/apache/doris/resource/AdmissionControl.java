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

package org.apache.doris.resource;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.proto.InternalService;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AdmissionControl extends MasterDaemon {

    public static final Logger LOG = LogManager.getLogger(AdmissionControl.class);

    private volatile boolean isAllBeMemoryEnough = true;

    private double currentMemoryLimit = 0;

    private SystemInfoService clusterInfoService;

    public AdmissionControl(SystemInfoService clusterInfoService) {
        super("get-be-resource-usage-thread", Config.get_be_resource_usage_interval_ms);
        this.clusterInfoService = clusterInfoService;
    }

    private ConcurrentLinkedQueue<QueueToken> queryWaitQueue = new ConcurrentLinkedQueue();

    public void addQueueToken(QueueToken queryQueue) {
        queryWaitQueue.offer(queryQueue);
    }

    @Override
    protected void runAfterCatalogReady() {
        getBeMemoryUsage();
        notifyWaitQuery();
    }

    public void getBeMemoryUsage() {
        if (Config.query_queue_by_be_used_memory < 0) {
            this.isAllBeMemoryEnough = true;
            return;
        }
        List<Backend> backends;
        try {
            backends = clusterInfoService.getAllBackendsByAllCluster().values().asList();
        } catch (AnalysisException e) {
            LOG.warn("get backends failed", e);
            throw new RuntimeException(e);
        }
        this.currentMemoryLimit = Config.query_queue_by_be_used_memory;
        boolean tmpIsAllBeMemoryEnough = true;
        List<Future<InternalService.PGetBeResourceResponse>> futureList = new ArrayList();
        for (Backend be : backends) {
            if (!be.isAlive()) {
                continue;
            }
            final InternalService.PGetBeResourceRequest request = InternalService.PGetBeResourceRequest.newBuilder()
                    .build();
            Future<InternalService.PGetBeResourceResponse> response = BackendServiceProxy.getInstance()
                    .getBeResourceAsync(be.getBrpcAddress(), 5, request);
            futureList.add(response);
        }

        for (Future<InternalService.PGetBeResourceResponse> future : futureList) {
            if (future == null) {
                continue;
            }
            try {
                InternalService.PGetBeResourceResponse response = future.get(5, TimeUnit.SECONDS);
                if (response.hasStatus()) {
                    Status status = new Status(response.getStatus());
                    if (status.getErrorCode() == TStatusCode.OK) {
                        InternalService.PGlobalResourceUsage globalUsage = response.getGlobalBeResourceUsage();
                        long memUsageL = globalUsage.getMemUsage();
                        long memLimitL = globalUsage.getMemLimit();
                        double memUsage = Double.valueOf(String.valueOf(memUsageL));
                        double memLimit = Double.valueOf(String.valueOf(memLimitL));
                        double memUsagePercent = memUsage / memLimit;
                        if (memUsagePercent > this.currentMemoryLimit) {
                            tmpIsAllBeMemoryEnough = false;
                            break;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.warn("wait get be resource response failed, ", t);
            }
        }

        this.isAllBeMemoryEnough = tmpIsAllBeMemoryEnough;
    }

    public void notifyWaitQuery() {
        if (!isAllBeMemoryEnough()) {
            return;
        }
        int waitQueryCountSnapshot = queryWaitQueue.size();
        Iterator<QueueToken> queueTokenIterator = queryWaitQueue.iterator();
        while (waitQueryCountSnapshot > 0 && queueTokenIterator.hasNext()) {
            QueueToken queueToken = queueTokenIterator.next();
            queueToken.notifyWaitQuery();
            waitQueryCountSnapshot--;
        }
    }

    public void removeQueueToken(QueueToken queueToken) {
        queryWaitQueue.remove(queueToken);
    }

    public boolean isAllBeMemoryEnough() {
        return isAllBeMemoryEnough;
    }

    //TODO(wb): add more resource type
    public boolean checkResourceAvailable(QueueToken queueToken) {
        if (isAllBeMemoryEnough()) {
            return true;
        } else {
            queueToken.setQueueMsg("WAIT_BE_MEMORY");
            return false;
        }
    }

}
