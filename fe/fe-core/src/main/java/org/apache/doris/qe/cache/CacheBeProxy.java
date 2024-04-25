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

package org.apache.doris.qe.cache;

import org.apache.doris.common.Status;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Encapsulates access to BE, including network and other exception handling
 */
public class CacheBeProxy extends CacheProxy {
    private static final Logger LOG = LogManager.getLogger(CacheBeProxy.class);

    public void updateCache(InternalService.PUpdateCacheRequest request, int timeoutMs, Status status) {
        Types.PUniqueId sqlKey = request.getSqlKey();
        Backend backend = CacheCoordinator.getInstance().findBackend(sqlKey);
        if (backend == null) {
            LOG.warn("update cache can't find backend, sqlKey {}", sqlKey);
            return;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            Future<InternalService.PCacheResponse> future = BackendServiceProxy.getInstance()
                    .updateCache(address, request);
            InternalService.PCacheResponse response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (response.getStatus() == InternalService.PCacheStatus.CACHE_OK) {
                status.updateStatus(TStatusCode.OK, "CACHE_OK");
            } else {
                status.updateStatus(TStatusCode.INTERNAL_ERROR, response.getStatus().toString());
            }
        } catch (Exception e) {
            LOG.warn("update cache exception, sqlKey {}", sqlKey, e);
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
        }
    }

    public InternalService.PFetchCacheResult fetchCache(InternalService.PFetchCacheRequest request,
                                                        int timeoutMs, Status status) {
        Types.PUniqueId sqlKey = request.getSqlKey();
        Backend backend = CacheCoordinator.getInstance().findBackend(sqlKey);
        if (backend == null) {
            return null;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            Future<InternalService.PFetchCacheResult> future = BackendServiceProxy.getInstance()
                    .fetchCache(address, request);
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (RpcException e) {
            LOG.warn("fetch catch rpc exception, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
            SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
        } catch (InterruptedException e) {
            LOG.warn("future get interrupted exception, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.updateStatus(TStatusCode.INTERNAL_ERROR, "interrupted exception");
        } catch (ExecutionException e) {
            LOG.warn("future get execution exception, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.updateStatus(TStatusCode.INTERNAL_ERROR, "execution exception");
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, sqlKey {}, backend {}", sqlKey, backend.getId(), e);
            status.updateStatus(TStatusCode.TIMEOUT, "query timeout");
        }
        return null;
    }

    public void clearCache(InternalService.PClearCacheRequest request) {
        this.clearCache(request, CacheCoordinator.getInstance().getBackendList());
    }

    public void clearCache(InternalService.PClearCacheRequest request, List<Backend> beList) {
        int retry;
        Status status = new Status();
        for (Backend backend : beList) {
            retry = 1;
            while (retry < 3 && !this.clearCache(request, backend, CLEAR_TIMEOUT, status)) {
                retry++;
                try {
                    Thread.sleep(1000); //sleep 1 second
                } catch (Exception e) {
                    // CHECKSTYLE IGNORE THIS LINE
                }
            }
            if (retry >= 3) {
                String errMsg = "clear cache timeout, backend " + backend.getId();
                LOG.warn(errMsg);
                SimpleScheduler.addToBlacklist(backend.getId(), errMsg);
            }
        }
    }

    protected boolean clearCache(InternalService.PClearCacheRequest request,
            Backend backend, int timeoutMs, Status status) {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            request = request.toBuilder().setClearType(InternalService.PClearType.CLEAR_ALL).build();
            LOG.info("clear all backend cache, backendId {}", backend.getId());
            Future<InternalService.PCacheResponse> future
                    = BackendServiceProxy.getInstance().clearCache(address, request);
            InternalService.PCacheResponse response = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            if (response.getStatus() == InternalService.PCacheStatus.CACHE_OK) {
                status.updateStatus(TStatusCode.OK, "CACHE_OK");
                return true;
            } else {
                status.updateStatus(TStatusCode.INTERNAL_ERROR, response.getStatus().toString());
                return false;
            }
        } catch (Exception e) {
            LOG.warn("clear cache exception, backendId {}", backend.getId(), e);
        }
        return false;
    }
}
