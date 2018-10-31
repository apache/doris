// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

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

package org.apache.doris.rpc;

import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

public class BackendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(BackendServiceProxy.class);

    private RpcClient rpcClient;
    // TODO(zc): use TNetworkAddress,
    private Map<TNetworkAddress, PInternalService> serviceMap;

    private static BackendServiceProxy INSTANCE;

    public BackendServiceProxy() {
        final RpcClientOptions rpcOptions = new RpcClientOptions();
        rpcOptions.setMaxWait(Config.brpc_idle_wait_max_time);
        rpcOptions.setThreadPoolSize(Config.brpc_number_of_concurrent_requests_processed);
        rpcClient = new RpcClient(rpcOptions);
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
        final PExecPlanFragmentRequest pRequest = new PExecPlanFragmentRequest();
        pRequest.setRequest(tRequest);
        try {
            final PInternalService service = getProxy(address);
            return service.execPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            try {
                // retry
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PInternalService service = getProxy(address);
                return service.execPlanFragmentAsync(pRequest);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Execute plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(e.getMessage());               
            }
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(e.getMessage());
        }
    }

    public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            TNetworkAddress address, TUniqueId finstId) throws RpcException {
        final PCancelPlanFragmentRequest pRequest = new PCancelPlanFragmentRequest(new PUniqueId(finstId));;
        try {
            final PInternalService service = getProxy(address);
            return service.cancelPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            // retry
            try {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PInternalService service = getProxy(address);
                return service.cancelPlanFragmentAsync(pRequest);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Cancel plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(e.getMessage());            
            }
        } catch (Throwable e) {
            LOG.warn("Cancel plan fragment catch a exception, address={}:{}",
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


    public Future<PFetchFragmentExecInfosResult> fetchFragmentExecInfosAsync(
            TNetworkAddress address, PFetchFragmentExecInfoRequest request) throws RpcException {
        try {
            final PInternalService service = getProxy(address);
            return service.fetchFragmentExecInfosAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(e.getMessage());
        }
    }
}
