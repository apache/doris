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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Maps;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class MetaServiceProxy {
    private static final Logger LOG = LogManager.getLogger(MetaServiceProxy.class);

    // use exclusive lock to make sure only one thread can add or remove client from
    // serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private ReentrantLock lock = new ReentrantLock();
    private final Map<String, MetaServiceClient> serviceMap;
    private Queue<Long> lastConnTimeMs = new LinkedList<>();

    static {
        if (Config.isCloudMode() && (Config.meta_service_endpoint == null || Config.meta_service_endpoint.isEmpty())) {
            throw new RuntimeException("in cloud mode, please configure meta_service_endpoint in fe.conf");
        }
    }

    public MetaServiceProxy() {
        this.serviceMap = Maps.newConcurrentMap();
        for (int i = 0; i < 3; ++i) {
            lastConnTimeMs.add(0L);
        }
    }

    private static class SingletonHolder {
        private static AtomicInteger count = new AtomicInteger();
        private static MetaServiceProxy[] proxies;

        static {
            if (Config.isCloudMode()) {
                int size = Config.meta_service_connection_pooled
                        ? Config.meta_service_connection_pool_size
                        : 1;
                proxies = new MetaServiceProxy[size];
                for (int i = 0; i < size; ++i) {
                    proxies[i] = new MetaServiceProxy();
                }
            }
        }

        static MetaServiceProxy get() {
            return proxies[Math.abs(count.addAndGet(1) % proxies.length)];
        }
    }

    public static MetaServiceProxy getInstance() {
        return MetaServiceProxy.SingletonHolder.get();
    }

    public boolean needReconn() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            return (now - lastConnTimeMs.element() > Config.meta_service_rpc_reconnect_interval_ms);
        } finally {
            lock.unlock();
        }
    }

    public Cloud.GetInstanceResponse getInstance(Cloud.GetInstanceRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getInstance(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public void removeProxy(String address) {
        LOG.warn("begin to remove proxy: {}", address);
        MetaServiceClient service;
        lock.lock();
        try {
            service = serviceMap.remove(address);
        } finally {
            lock.unlock();
        }

        if (service != null) {
            service.shutdown(false);
        }
    }

    private MetaServiceClient getProxy() {
        if (Config.enable_check_compatibility_mode) {
            LOG.error("Should not use RPC in check compatibility mode");
            throw new RuntimeException("use RPC in the check compatibility mode");
        }

        String address = Config.meta_service_endpoint;
        address = address.replaceAll("^[\"']|[\"']$", "");
        MetaServiceClient service = serviceMap.get(address);
        if (service != null && service.isNormalState() && !service.isConnectionAgeExpired()) {
            return service;
        }

        // not exist, create one and return.
        MetaServiceClient removedClient = null;
        lock.lock();
        try {
            service = serviceMap.get(address);
            if (service != null && !service.isNormalState()) {
                // At this point we cannot judge the progress of reconnecting the underlying
                // channel.
                // In the worst case, it may take two minutes. But we can't stand the connection
                // refused
                // for two minutes, so rebuild the channel directly.
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service != null && service.isConnectionAgeExpired()) {
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service == null) {
                service = new MetaServiceClient(address);
                serviceMap.put(address, service);
                lastConnTimeMs.add(System.currentTimeMillis());
                lastConnTimeMs.remove();
            }
            return service;
        } finally {
            lock.unlock();
            if (removedClient != null) {
                removedClient.shutdown(true);
            }
        }
    }

    public static class MetaServiceClientWrapper {
        private final MetaServiceProxy proxy;
        private Random random = new Random();

        public MetaServiceClientWrapper(MetaServiceProxy proxy) {
            this.proxy = proxy;
        }

        public <Response> Response executeRequest(Function<MetaServiceClient, Response> function) throws RpcException {
            long maxRetries = Config.meta_service_rpc_retry_cnt;
            for (long tried = 1; tried <= maxRetries; tried++) {
                MetaServiceClient client = null;
                try {
                    client = proxy.getProxy();
                    return function.apply(client);
                } catch (StatusRuntimeException sre) {
                    LOG.warn("failed to request meta service code {}, msg {}, trycnt {}", sre.getStatus().getCode(),
                            sre.getMessage(), tried);
                    boolean shouldRetry = false;
                    switch (sre.getStatus().getCode()) {
                        case UNAVAILABLE:
                        case UNKNOWN:
                            shouldRetry = true;
                            break;
                        case DEADLINE_EXCEEDED:
                            shouldRetry = tried <= Config.meta_service_rpc_timeout_retry_times;
                            break;
                        default:
                            shouldRetry = false;
                    }
                    if (!shouldRetry || tried >= maxRetries) {
                        throw new RpcException("", sre.getMessage(), sre);
                    }
                } catch (Exception e) {
                    LOG.warn("failed to request meta servive trycnt {}", tried, e);
                    if (tried >= maxRetries) {
                        throw new RpcException("", e.getMessage(), e);
                    }
                } finally {
                    if (proxy.needReconn() && client != null) {
                        client.shutdown(true);
                    }
                }

                int delay = 20 + random.nextInt(200 - 20 + 1);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RpcException("", interruptedException.getMessage(), interruptedException);
                }
            }
            // impossible and unreachable, just make the compiler happy
            throw new RpcException("", "All retries exhausted", null);
        }
    }

    private final MetaServiceClientWrapper w = new MetaServiceClientWrapper(this);

    public Future<Cloud.GetVersionResponse> getVisibleVersionAsync(Cloud.GetVersionRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getVisibleVersionAsync(request));
    }

    public Cloud.GetVersionResponse getVersion(Cloud.GetVersionRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getVersion(request));
    }

    public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) throws RpcException {
        return w.executeRequest((client) -> client.createTablets(request));
    }

    public Cloud.UpdateTabletResponse updateTablet(Cloud.UpdateTabletRequest request) throws RpcException {
        return w.executeRequest((client) -> client.updateTablet(request));
    }

    public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.beginTxn(request));
    }

    public Cloud.PrecommitTxnResponse precommitTxn(Cloud.PrecommitTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.precommitTxn(request));
    }

    public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.commitTxn(request));
    }

    public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.abortTxn(request));
    }

    public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getTxn(request));
    }

    public Cloud.GetTxnIdResponse getTxnId(Cloud.GetTxnIdRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getTxnId(request));
    }

    public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getCurrentMaxTxnId(request));
    }

    public Cloud.BeginSubTxnResponse beginSubTxn(Cloud.BeginSubTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.beginSubTxn(request));
    }

    public Cloud.AbortSubTxnResponse abortSubTxn(Cloud.AbortSubTxnRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.abortSubTxn(request));
    }

    public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.checkTxnConflict(request));
    }

    public Cloud.CleanTxnLabelResponse cleanTxnLabel(Cloud.CleanTxnLabelRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.cleanTxnLabel(request));
    }

    public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getCluster(request));
    }

    public Cloud.IndexResponse prepareIndex(Cloud.IndexRequest request) throws RpcException {
        return w.executeRequest((client) -> client.prepareIndex(request));
    }

    public Cloud.IndexResponse commitIndex(Cloud.IndexRequest request) throws RpcException {
        return w.executeRequest((client) -> client.commitIndex(request));
    }

    public Cloud.CheckKVResponse checkKv(Cloud.CheckKVRequest request) throws RpcException {
        return w.executeRequest((client) -> client.checkKv(request));
    }

    public Cloud.IndexResponse dropIndex(Cloud.IndexRequest request) throws RpcException {
        return w.executeRequest((client) -> client.dropIndex(request));
    }

    public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.preparePartition(request));
    }

    public Cloud.PartitionResponse commitPartition(Cloud.PartitionRequest request) throws RpcException {
        return w.executeRequest((client) -> client.commitPartition(request));
    }

    public Cloud.PartitionResponse dropPartition(Cloud.PartitionRequest request) throws RpcException {
        return w.executeRequest((client) -> client.dropPartition(request));
    }

    public Cloud.GetTabletStatsResponse getTabletStats(Cloud.GetTabletStatsRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getTabletStats(request));
    }

    public Cloud.CreateStageResponse createStage(Cloud.CreateStageRequest request) throws RpcException {
        return w.executeRequest((client) -> client.createStage(request));
    }

    public Cloud.GetStageResponse getStage(Cloud.GetStageRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getStage(request));
    }

    public Cloud.DropStageResponse dropStage(Cloud.DropStageRequest request) throws RpcException {
        return w.executeRequest((client) -> client.dropStage(request));
    }

    public Cloud.GetIamResponse getIam(Cloud.GetIamRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getIam(request));
    }

    public Cloud.BeginCopyResponse beginCopy(Cloud.BeginCopyRequest request) throws RpcException {
        return w.executeRequest((client) -> client.beginCopy(request));
    }

    public Cloud.FinishCopyResponse finishCopy(Cloud.FinishCopyRequest request) throws RpcException {
        return w.executeRequest((client) -> client.finishCopy(request));
    }

    public Cloud.GetCopyJobResponse getCopyJob(Cloud.GetCopyJobRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getCopyJob(request));
    }

    public Cloud.GetCopyFilesResponse getCopyFiles(Cloud.GetCopyFilesRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getCopyFiles(request));
    }

    public Cloud.FilterCopyFilesResponse filterCopyFiles(Cloud.FilterCopyFilesRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.filterCopyFiles(request));
    }

    public Cloud.AlterClusterResponse alterCluster(Cloud.AlterClusterRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.alterCluster(request));
    }

    public Cloud.GetDeleteBitmapUpdateLockResponse getDeleteBitmapUpdateLock(
            Cloud.GetDeleteBitmapUpdateLockRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getDeleteBitmapUpdateLock(request));
    }

    public Cloud.RemoveDeleteBitmapUpdateLockResponse removeDeleteBitmapUpdateLock(
            Cloud.RemoveDeleteBitmapUpdateLockRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.removeDeleteBitmapUpdateLock(request));
    }

    /**
     * This method is deprecated, there is no code to call it.
     */
    @Deprecated
    public Cloud.AlterObjStoreInfoResponse alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.alterObjStoreInfo(request));
    }

    public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.alterStorageVault(request));
    }

    public Cloud.FinishTabletJobResponse finishTabletJob(Cloud.FinishTabletJobRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.finishTabletJob(request));
    }

    public Cloud.GetRLTaskCommitAttachResponse
            getRLTaskCommitAttach(Cloud.GetRLTaskCommitAttachRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.getRLTaskCommitAttach(request));
    }

    public Cloud.ResetRLProgressResponse resetRLProgress(Cloud.ResetRLProgressRequest request)
            throws RpcException {
        return w.executeRequest((client) -> client.resetRLProgress(request));
    }

    public Cloud.GetObjStoreInfoResponse
            getObjStoreInfo(Cloud.GetObjStoreInfoRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getObjStoreInfo(request));
    }

    public Cloud.AbortTxnWithCoordinatorResponse
            abortTxnWithCoordinator(Cloud.AbortTxnWithCoordinatorRequest request) throws RpcException {
        return w.executeRequest((client) -> client.abortTxnWithCoordinator(request));
    }

    public Cloud.CreateInstanceResponse createInstance(Cloud.CreateInstanceRequest request) throws RpcException {
        return w.executeRequest((client) -> client.createInstance(request));
    }

    public Cloud.GetStreamingTaskCommitAttachResponse getStreamingTaskCommitAttach(
            Cloud.GetStreamingTaskCommitAttachRequest request) throws RpcException {
        return w.executeRequest((client) -> client.getStreamingTaskCommitAttach(request));
    }
}
