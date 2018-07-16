// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.rpc;

import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import com.baidu.palo.qe.SimpleScheduler;
import com.baidu.palo.thrift.TExecPlanFragmentParams;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TUniqueId;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.Future;

public class BackendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(BackendServiceProxy.class);

    private RpcClient rpcClient;
    // TODO(zc): use TNetworkAddress,
    private Map<TNetworkAddress, PInternalService> serviceMap;

    private static BackendServiceProxy INSTANCE;

    public BackendServiceProxy() {
        rpcClient = new RpcClient(new RpcClientOptions());
        serviceMap = Maps.newHashMap();
    }

    public static BackendServiceProxy getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new BackendServiceProxy();
        }
        return INSTANCE;
    }

    private synchronized PInternalService getProxy(TNetworkAddress address) {
        PInternalService service = serviceMap.get(address);
        if (service != null) {
            return service;
        }
        ProtobufRpcProxy<PInternalService> proxy = new ProtobufRpcProxy(rpcClient, PInternalService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        service = proxy.proxy();
        serviceMap.put(address, service);
        return service;
    }

    public Future<PExecPlanFragmentResult> execPlanFragmentAsync(
            TNetworkAddress address, TExecPlanFragmentParams tRequest)
            throws TException, RpcException {
        try {
            PExecPlanFragmentRequest pRequest = new PExecPlanFragmentRequest();
            pRequest.setRequest(tRequest);
            PInternalService service = getProxy(address);
            return service.execPlanFragmentAsync(pRequest);
        } catch (Throwable e) {
            LOG.warn("execute plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(e.getMessage());
        }
    }

    public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            TNetworkAddress address, TUniqueId finstId) throws RpcException {
        try {
            PCancelPlanFragmentRequest pRequest = new PCancelPlanFragmentRequest(new PUniqueId(finstId));
            PInternalService service = getProxy(address);
            return service.cancelPlanFragmentAsync(pRequest);
        } catch (Throwable e) {
            LOG.warn("cancel plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(e.getMessage());
        }
    }

    public Future<PFetchDataResult> fetchDataAsync(
            TNetworkAddress address, PFetchDataRequest request) throws RpcException {
        try {
            PInternalService service = getProxy(address);
            return service.fetchDataAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(e.getMessage());
        }
    }
}
