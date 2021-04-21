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

package org.apache.doris.rpc;

import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import java.util.Map;
import java.util.concurrent.Future;

public class BackendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(BackendServiceProxy.class);
    private static volatile BackendServiceProxy INSTANCE;
    private final Map<TNetworkAddress, BackendServiceClient> serviceMap;

    public BackendServiceProxy() {
        serviceMap = Maps.newHashMap();
    }

    public static BackendServiceProxy getInstance() {
        if (INSTANCE == null) {
            synchronized (BackendServiceProxy.class) {
                if (INSTANCE == null) {
                    INSTANCE = new BackendServiceProxy();
                }
            }
        }
        return INSTANCE;
    }

    private synchronized BackendServiceClient getProxy(TNetworkAddress address) {
        BackendServiceClient service = serviceMap.get(address);
        if (service != null) {
            return service;
        }
        service = new BackendServiceClient(address);
        serviceMap.put(address, service);
        return service;
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentAsync(
            TNetworkAddress address, TExecPlanFragmentParams tRequest)
            throws TException, RpcException {
        final InternalService.PExecPlanFragmentRequest pRequest = InternalService.PExecPlanFragmentRequest.newBuilder()
                .setRequest(ByteString.copyFrom(new TSerializer().serialize(tRequest))).build();
        try {
            final BackendServiceClient client = getProxy(address);
            return client.execPlanFragmentAsync(pRequest);
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            TNetworkAddress address, TUniqueId finstId, InternalService.PPlanFragmentCancelReason cancelReason)
            throws RpcException {
        final InternalService.PCancelPlanFragmentRequest pRequest = InternalService.PCancelPlanFragmentRequest
                .newBuilder()
                .setFinstId(
                        Types.PUniqueId.newBuilder().setHi(finstId.hi).setLo(finstId.lo).build())
                .setCancelReason(cancelReason).build();
        try {
            final BackendServiceClient client = getProxy(address);
            return client.cancelPlanFragmentAsync(pRequest);
        } catch (Throwable e) {
            LOG.warn("Cancel plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PFetchDataResult> fetchDataAsync(
            TNetworkAddress address, InternalService.PFetchDataRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.fetchDataAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public InternalService.PFetchDataResult fetchDataSync(
            TNetworkAddress address, InternalService.PFetchDataRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.fetchDataSync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PCacheResponse> updateCache(
            TNetworkAddress address, InternalService.PUpdateCacheRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.updateCache(request);
        } catch (Throwable e) {
            LOG.warn("update cache catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PFetchCacheResult> fetchCache(
            TNetworkAddress address, InternalService.PFetchCacheRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.fetchCache(request);
        } catch (Throwable e) {
            LOG.warn("fetch cache catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PCacheResponse> clearCache(
            TNetworkAddress address, InternalService.PClearCacheRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.clearCache(request);
        } catch (Throwable e) {
            LOG.warn("clear cache catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PTriggerProfileReportResult> triggerProfileReportAsync(
            TNetworkAddress address, InternalService.PTriggerProfileReportRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.triggerProfileReport(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PProxyResult> getInfo(
            TNetworkAddress address, InternalService.PProxyRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.getInfo(request);
        } catch (Throwable e) {
            LOG.warn("failed to get info, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }
}
