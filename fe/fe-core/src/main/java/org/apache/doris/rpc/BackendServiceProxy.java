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

import com.baidu.bjf.remoting.protobuf.utils.JDKCompilerHelper;
import com.baidu.bjf.remoting.protobuf.utils.compiler.JdkCompiler;
import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import com.google.common.collect.Maps;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.JdkUtils;
import org.apache.doris.proto.PCacheResponse;
import org.apache.doris.proto.PCancelPlanFragmentRequest;
import org.apache.doris.proto.PCancelPlanFragmentResult;
import org.apache.doris.proto.PClearCacheRequest;
import org.apache.doris.proto.PCommitRequest;
import org.apache.doris.proto.PCommitResult;
import org.apache.doris.proto.PExecPlanFragmentResult;
import org.apache.doris.proto.PFetchCacheRequest;
import org.apache.doris.proto.PFetchCacheResult;
import org.apache.doris.proto.PFetchDataResult;
import org.apache.doris.proto.PPlanFragmentCancelReason;
import org.apache.doris.proto.PProxyRequest;
import org.apache.doris.proto.PProxyResult;
import org.apache.doris.proto.PRollbackRequest;
import org.apache.doris.proto.PRollbackResult;
import org.apache.doris.proto.PSendDataRequest;
import org.apache.doris.proto.PSendDataResult;
import org.apache.doris.proto.PTriggerProfileReportResult;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.proto.PUpdateCacheRequest;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BackendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(BackendServiceProxy.class);

    private RpcClient rpcClient;
    // TODO(zc): use TNetworkAddress,
    private Map<TNetworkAddress, PBackendService> serviceMap;

    private static BackendServiceProxy INSTANCE;

    static {
        int javaRuntimeVersion = JdkUtils.getJavaVersionAsInteger(System.getProperty("java.version"));
        JDKCompilerHelper.setCompiler(new JdkCompiler(JdkCompiler.class.getClassLoader(), String.valueOf(javaRuntimeVersion)));
    }

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

    private synchronized PBackendService getProxy(TNetworkAddress address) {
        PBackendService service = serviceMap.get(address);
        if (service != null) {
            return service;
        }
        ProtobufRpcProxy<PBackendService> proxy = new ProtobufRpcProxy(rpcClient, PBackendService.class);
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
            final PBackendService service = getProxy(address);
            return service.execPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            try {
                // retry
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendService service = getProxy(address);
                return service.execPlanFragmentAsync(pRequest);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Execute plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(address.hostname, e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(
            TNetworkAddress address, TUniqueId finstId, PPlanFragmentCancelReason cancelReason) throws RpcException {
        final PCancelPlanFragmentRequest pRequest = new PCancelPlanFragmentRequest();
        PUniqueId uid = new PUniqueId();
        uid.hi = finstId.hi;
        uid.lo = finstId.lo;
        pRequest.finst_id = uid;
        pRequest.cancel_reason = cancelReason;
        try {
            final PBackendService service = getProxy(address);
            return service.cancelPlanFragmentAsync(pRequest);
        } catch (NoSuchElementException e) {
            // retry
            try {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException interruptedException) {
                    // do nothing
                }
                final PBackendService service = getProxy(address);
                return service.cancelPlanFragmentAsync(pRequest);
            } catch (NoSuchElementException noSuchElementException) {
                LOG.warn("Cancel plan fragment retry failed, address={}:{}",
                        address.getHostname(), address.getPort(), noSuchElementException);
                throw new RpcException(address.hostname, e.getMessage());
            }
        } catch (Throwable e) {
            LOG.warn("Cancel plan fragment catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PFetchDataResult> fetchDataAsync(
            TNetworkAddress address, PFetchDataRequest request) throws RpcException {
        try {
            PBackendService service = getProxy(address);
            return service.fetchDataAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PCacheResponse> updateCache(
            TNetworkAddress address, PUpdateCacheRequest request) throws RpcException{
        try {
            PBackendService service = getProxy(address);
            return service.updateCache(request);
        } catch (Throwable e) {
            LOG.warn("update cache catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PFetchCacheResult> fetchCache(
            TNetworkAddress address, PFetchCacheRequest request) throws RpcException {
        try {
            PBackendService service = getProxy(address);
            return service.fetchCache(request);
        } catch (Throwable e) {
            LOG.warn("fetch cache catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PCacheResponse> clearCache(
            TNetworkAddress address, PClearCacheRequest request) throws RpcException {
        try {
            PBackendService service = getProxy(address);
            return service.clearCache(request);
        } catch (Throwable e) {
            LOG.warn("clear cache catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }


    public Future<PTriggerProfileReportResult> triggerProfileReportAsync(
            TNetworkAddress address, PTriggerProfileReportRequest request) throws RpcException {
        try {
            final PBackendService service = getProxy(address);
            return service.triggerProfileReport(request);
        } catch (Throwable e) {
            LOG.warn("fetch data catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PProxyResult> getInfo(
            TNetworkAddress address, PProxyRequest request) throws RpcException {
        try {
            final PBackendService service = getProxy(address);
            return service.getInfo(request);
        } catch (Throwable e) {
            LOG.warn("failed to get info, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PSendDataResult> sendData(
            TNetworkAddress address, PSendDataRequest request) throws RpcException {
        try {
            final PBackendService service = getProxy(address);
            return service.sendData(request);
        } catch (Throwable e) {
            LOG.warn("failed to send data, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public void insertForTxn(PUniqueId fragmentInstanceId, String data, Backend backend)
            throws TException, InterruptedException, ExecutionException, TimeoutException {
        PSendDataRequest request = new PSendDataRequest();
        request.fragment_instance_id = fragmentInstanceId;
        request.data = data;
        Future<PSendDataResult> future = execRemoteInsertAsync(backend, request);
        PSendDataResult result = future.get(5, TimeUnit.SECONDS);
        TStatusCode code = TStatusCode.findByValue(result.status.status_code);
        if (code != TStatusCode.OK) {
            throw new TException("failed to insert data: " + result.status.error_msgs);
        }
    }

    public Future<PSendDataResult> execRemoteInsertAsync(
            Backend backend, PSendDataRequest rpcParams) throws TException {
        TNetworkAddress brpcAddress = null;
        try {
            brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        try {
            return BackendServiceProxy.getInstance().sendData(brpcAddress, rpcParams);
        } catch (RpcException e) {
            LOG.warn("failed to sendData.", e);
            throw new TException("failed to sendData: " + e.getMessage());
        }
    }

    public Future<PRollbackResult> rollback(
            TNetworkAddress address, PRollbackRequest request) throws RpcException {
        try {
            final PBackendService service = getProxy(address);
            return service.rollback(request);
        } catch (Throwable e) {
            LOG.warn("failed to rollback, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public void rollbackForTxn(PUniqueId fragmentInstanceId, Backend backend)
            throws TException, InterruptedException, ExecutionException, TimeoutException {
        PRollbackRequest request = new PRollbackRequest();
        request.fragment_instance_id = fragmentInstanceId;
        Future<PRollbackResult> future = execRollbackAsync(backend, request);
        PRollbackResult result = future.get(5, TimeUnit.SECONDS);
        TStatusCode code = TStatusCode.findByValue(result.status.status_code);
        if (code != TStatusCode.OK) {
            throw new TException("failed to insert data: " + result.status.error_msgs);
        }
    }

    public Future<PRollbackResult> execRollbackAsync(
            Backend backend, PRollbackRequest rpcParams) throws TException {
        TNetworkAddress brpcAddress = null;
        try {
            brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        try {
            return rollback(brpcAddress, rpcParams);
        } catch (RpcException e) {
            LOG.warn("failed to sendData.", e);
            throw new TException("failed to sendData: " + e.getMessage());
        }
    }


    public Future<PCommitResult> commit(
            TNetworkAddress address, PCommitRequest request) throws RpcException {
        try {
            final PBackendService service = getProxy(address);
            return service.commit(request);
        } catch (Throwable e) {
            LOG.warn("failed to commit, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public void commitForTxn(PUniqueId fragmentInstanceId, Backend backend)
            throws TException, InterruptedException, ExecutionException, TimeoutException {
        PCommitRequest request = new PCommitRequest();
        request.fragment_instance_id = fragmentInstanceId;
        Future<PCommitResult> future = execCommitAsync(backend, request);
        PCommitResult result = future.get(5, TimeUnit.SECONDS);
        TStatusCode code = TStatusCode.findByValue(result.status.status_code);
        if (code != TStatusCode.OK) {
            throw new TException("failed to insert data: " + result.status.error_msgs);
        }
    }

    public Future<PCommitResult> execCommitAsync(
            Backend backend, PCommitRequest rpcParams) throws TException {
        TNetworkAddress brpcAddress = null;
        try {
            brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        try {
            return commit(brpcAddress, rpcParams);
        } catch (RpcException e) {
            LOG.warn("failed to sendData.", e);
            throw new TException("failed to sendData: " + e.getMessage());
        }
    }

}
