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

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PExecPlanFragmentStartRequest;
import org.apache.doris.proto.InternalService.PGroupCommitInsertRequest;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.proto.Types;
import org.apache.doris.thrift.TExecPlanFragmentParamsList;
import org.apache.doris.thrift.TFoldConstantParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class BackendServiceProxy {
    private static final Logger LOG = LogManager.getLogger(BackendServiceProxy.class);
    // use exclusive lock to make sure only one thread can add or remove client from serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private ReentrantLock lock = new ReentrantLock();

    private Executor grpcThreadPool = ThreadPoolManager.newDaemonCacheThreadPool(Config.grpc_threadmgr_threads_nums,
            "grpc_thread_pool", true);
    private final Map<TNetworkAddress, BackendServiceClientExtIp> serviceMap;

    public BackendServiceProxy() {
        serviceMap = Maps.newConcurrentMap();
    }

    private static class Holder {
        private static final int PROXY_NUM = Config.backend_proxy_num;
        private static BackendServiceProxy[] proxies = new BackendServiceProxy[PROXY_NUM];
        private static AtomicInteger count = new AtomicInteger();

        static {
            for (int i = 0; i < proxies.length; i++) {
                proxies[i] = new BackendServiceProxy();
            }
        }

        static BackendServiceProxy get() {
            return proxies[Math.abs(count.addAndGet(1) % PROXY_NUM)];
        }
    }

    public static BackendServiceProxy getInstance() {
        return Holder.get();
    }

    private class BackendServiceClientExtIp {
        private String realIp;
        private BackendServiceClient client;

        public BackendServiceClientExtIp(String realIp, BackendServiceClient client) {
            this.realIp = realIp;
            this.client = client;
        }

    }

    public void removeProxy(TNetworkAddress address) {
        LOG.warn("begin to remove proxy: {}", address);
        BackendServiceClientExtIp serviceClientExtIp;
        lock.lock();
        try {
            serviceClientExtIp = serviceMap.remove(address);
        } finally {
            lock.unlock();
        }

        if (serviceClientExtIp != null) {
            serviceClientExtIp.client.shutdown();
        }
    }

    private BackendServiceClient getProxy(TNetworkAddress address) throws UnknownHostException {
        String realIp = NetUtils.getIpByHost(address.getHostname());
        BackendServiceClientExtIp serviceClientExtIp = serviceMap.get(address);
        if (serviceClientExtIp != null && serviceClientExtIp.realIp.equals(realIp)
                && serviceClientExtIp.client.isNormalState()) {
            return serviceClientExtIp.client;
        }

        // not exist, create one and return.
        BackendServiceClient removedClient = null;
        lock.lock();
        try {
            serviceClientExtIp = serviceMap.get(address);
            if (serviceClientExtIp != null && !serviceClientExtIp.realIp.equals(realIp)) {
                LOG.warn("Cached ip changed ,before ip: {}, curIp: {}", serviceClientExtIp.realIp, realIp);
                serviceMap.remove(address);
                removedClient = serviceClientExtIp.client;
                serviceClientExtIp = null;
            }
            if (serviceClientExtIp != null && !serviceClientExtIp.client.isNormalState()) {
                // At this point we cannot judge the progress of reconnecting the underlying channel.
                // In the worst case, it may take two minutes. But we can't stand the connection refused
                // for two minutes, so rebuild the channel directly.
                serviceMap.remove(address);
                removedClient = serviceClientExtIp.client;
                serviceClientExtIp = null;
            }
            if (serviceClientExtIp == null) {
                BackendServiceClient client = new BackendServiceClient(address, grpcThreadPool);
                serviceMap.put(address, new BackendServiceClientExtIp(realIp, client));
            }
            return serviceMap.get(address).client;
        } finally {
            lock.unlock();
            if (removedClient != null) {
                removedClient.shutdown();
            }
        }
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentsAsync(TNetworkAddress address,
            TExecPlanFragmentParamsList paramsList, boolean twoPhaseExecution) throws TException, RpcException {
        InternalService.PExecPlanFragmentRequest.Builder builder =
                InternalService.PExecPlanFragmentRequest.newBuilder();
        if (Config.use_compact_thrift_rpc) {
            builder.setRequest(
                    ByteString.copyFrom(new TSerializer(new TCompactProtocol.Factory()).serialize(paramsList)));
            builder.setCompact(true);
        } else {
            builder.setRequest(ByteString.copyFrom(new TSerializer().serialize(paramsList))).build();
            builder.setCompact(false);
        }
        // VERSION 2 means we send TExecPlanFragmentParamsList, not single TExecPlanFragmentParams
        builder.setVersion(InternalService.PFragmentRequestVersion.VERSION_2);

        final InternalService.PExecPlanFragmentRequest pRequest = builder.build();
        MetricRepo.BE_COUNTER_QUERY_RPC_ALL.getOrAdd(address.hostname).increase(1L);
        MetricRepo.BE_COUNTER_QUERY_RPC_SIZE.getOrAdd(address.hostname).increase((long) pRequest.getSerializedSize());
        try {
            final BackendServiceClient client = getProxy(address);
            if (twoPhaseExecution) {
                return client.execPlanFragmentPrepareAsync(pRequest);
            } else {
                return client.execPlanFragmentAsync(pRequest);
            }
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}", address.getHostname(), address.getPort(),
                    e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentsAsync(TNetworkAddress address,
            TPipelineFragmentParamsList params, boolean twoPhaseExecution) throws TException, RpcException {
        InternalService.PExecPlanFragmentRequest.Builder builder =
                InternalService.PExecPlanFragmentRequest.newBuilder();
        if (Config.use_compact_thrift_rpc) {
            builder.setRequest(
                    ByteString.copyFrom(new TSerializer(new TCompactProtocol.Factory()).serialize(params)));
            builder.setCompact(true);
        } else {
            builder.setRequest(ByteString.copyFrom(new TSerializer().serialize(params))).build();
            builder.setCompact(false);
        }
        // VERSION 3 means we send TPipelineFragmentParamsList
        builder.setVersion(InternalService.PFragmentRequestVersion.VERSION_3);

        final InternalService.PExecPlanFragmentRequest pRequest = builder.build();
        MetricRepo.BE_COUNTER_QUERY_RPC_ALL.getOrAdd(address.hostname).increase(1L);
        MetricRepo.BE_COUNTER_QUERY_RPC_SIZE.getOrAdd(address.hostname).increase((long) pRequest.getSerializedSize());
        try {
            final BackendServiceClient client = getProxy(address);
            if (twoPhaseExecution) {
                return client.execPlanFragmentPrepareAsync(pRequest);
            } else {
                return client.execPlanFragmentAsync(pRequest);
            }
        } catch (Throwable e) {
            LOG.warn("Execute plan fragment catch a exception, address={}:{}", address.getHostname(), address.getPort(),
                    e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync(TNetworkAddress address,
            PExecPlanFragmentStartRequest request) throws TException, RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.execPlanFragmentStartAsync(request);
        } catch (Exception e) {
            throw new RpcException(address.hostname, e.getMessage(), e);
        }
    }

    public Future<InternalService.PCancelPlanFragmentResult> cancelPlanFragmentAsync(TNetworkAddress address,
            TUniqueId finstId, Types.PPlanFragmentCancelReason cancelReason) throws RpcException {
        final InternalService.PCancelPlanFragmentRequest pRequest =
                InternalService.PCancelPlanFragmentRequest.newBuilder()
                        .setFinstId(Types.PUniqueId.newBuilder().setHi(finstId.hi).setLo(finstId.lo).build())
                        .setCancelReason(cancelReason).build();
        try {
            final BackendServiceClient client = getProxy(address);
            return client.cancelPlanFragmentAsync(pRequest);
        } catch (Throwable e) {
            LOG.warn("Cancel plan fragment catch a exception, address={}:{}", address.getHostname(), address.getPort(),
                    e);
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

    public Future<InternalService.PTabletKeyLookupResponse> fetchTabletDataAsync(
            TNetworkAddress address, InternalService.PTabletKeyLookupRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.fetchTabletDataAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch tablet data catch a exception, address={}:{}",
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

    public Future<InternalService.PFetchArrowFlightSchemaResult> fetchArrowFlightSchema(
            TNetworkAddress address, InternalService.PFetchArrowFlightSchemaRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.fetchArrowFlightSchema(request);
        } catch (Throwable e) {
            LOG.warn("fetch arrow flight schema catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PFetchTableSchemaResult> fetchTableStructureAsync(
            TNetworkAddress address, InternalService.PFetchTableSchemaRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.fetchTableStructureAsync(request);
        } catch (Throwable e) {
            LOG.warn("fetch table structure catch a exception, address={}:{}",
                    address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PReportStreamLoadStatusResponse> reportStreamLoadStatus(
            TNetworkAddress address, InternalService.PReportStreamLoadStatusRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.reportStreamLoadStatus(request);
        } catch (Throwable e) {
            LOG.warn("report stream load status catch a exception, address={}:{}",
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

    public Future<InternalService.PSendDataResult> sendData(
            TNetworkAddress address, Types.PUniqueId fragmentInstanceId,
            Types.PUniqueId loadId, List<InternalService.PDataRow> data)
            throws RpcException {

        final InternalService.PSendDataRequest.Builder pRequest = InternalService.PSendDataRequest.newBuilder();
        pRequest.setFragmentInstanceId(fragmentInstanceId);
        pRequest.setLoadId(loadId);
        pRequest.addAllData(data);
        try {
            final BackendServiceClient client = getProxy(address);
            return client.sendData(pRequest.build());
        } catch (Throwable e) {
            LOG.warn("failed to send data, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PRollbackResult> rollback(TNetworkAddress address,
            Types.PUniqueId fragmentInstanceId, Types.PUniqueId loadId)
            throws RpcException {
        final InternalService.PRollbackRequest pRequest = InternalService.PRollbackRequest.newBuilder()
                .setFragmentInstanceId(fragmentInstanceId).setLoadId(loadId).build();
        try {
            final BackendServiceClient client = getProxy(address);
            return client.rollback(pRequest);
        } catch (Throwable e) {
            LOG.warn("failed to rollback, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PCommitResult> commit(TNetworkAddress address,
            Types.PUniqueId fragmentInstanceId, Types.PUniqueId loadId)
            throws RpcException {
        final InternalService.PCommitRequest pRequest = InternalService.PCommitRequest.newBuilder()
                .setFragmentInstanceId(fragmentInstanceId).setLoadId(loadId).build();
        try {
            final BackendServiceClient client = getProxy(address);
            return client.commit(pRequest);
        } catch (Throwable e) {
            LOG.warn("failed to commit, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PConstantExprResult> foldConstantExpr(
            TNetworkAddress address, TFoldConstantParams tParams) throws RpcException, TException {
        final InternalService.PConstantExprRequest pRequest = InternalService.PConstantExprRequest.newBuilder()
                .setRequest(ByteString.copyFrom(new TSerializer().serialize(tParams))).build();

        try {
            final BackendServiceClient client = getProxy(address);
            return client.foldConstantExpr(pRequest);
        } catch (Throwable e) {
            LOG.warn("failed to fold constant expr, address={}:{}", address.getHostname(), address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PFetchColIdsResponse> getColumnIdsByTabletIds(TNetworkAddress address,
            InternalService.PFetchColIdsRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.getColIdsByTabletIds(request);
        } catch (Throwable e) {
            LOG.warn("failed to fetch column id from address={}:{}", address.getHostname(), address.getPort());
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<InternalService.PGlobResponse> glob(TNetworkAddress address,
            InternalService.PGlobRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.glob(request);
        } catch (Throwable e) {
            LOG.warn("failed to glob dir from BE {}:{}, path: {}, error: ",
                    address.getHostname(), address.getPort(), request.getPattern());
            throw new RpcException(address.hostname, e.getMessage());
        }
    }

    public Future<PGroupCommitInsertResponse> groupCommitInsert(TNetworkAddress address,
            PGroupCommitInsertRequest request) throws RpcException {
        try {
            final BackendServiceClient client = getProxy(address);
            return client.groupCommitInsert(request);
        } catch (Throwable e) {
            LOG.warn("failed to group commit insert from address={}:{}", address.getHostname(),
                    address.getPort(), e);
            throw new RpcException(address.hostname, e.getMessage());
        }
    }
}
