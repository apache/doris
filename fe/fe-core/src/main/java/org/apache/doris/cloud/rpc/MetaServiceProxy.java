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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MetaServiceProxy {
    private static final Logger LOG = LogManager.getLogger(MetaServiceProxy.class);

    // use exclusive lock to make sure only one thread can add or remove client from
    // serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private ReentrantLock lock = new ReentrantLock();
    private final Map<String, MetaServiceClient> serviceMap;

    static {
        if (Config.isCloudMode() && (Config.meta_service_endpoint == null || Config.meta_service_endpoint.isEmpty())) {
            throw new RuntimeException("in cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
    }

    public MetaServiceProxy() {
        this.serviceMap = Maps.newConcurrentMap();
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
        MetaServiceClient service = serviceMap.get(address);
        if (service != null && service.isNormalState()) {
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
            if (service != null && !service.isConnectionAgeExpired()) {
                serviceMap.remove(address);
                removedClient = service;
                service = null;
            }
            if (service == null) {
                service = new MetaServiceClient(address);
                serviceMap.put(address, service);
            }
            return service;
        } finally {
            lock.unlock();
            if (removedClient != null) {
                removedClient.shutdown(true);
            }
        }
    }

    public Future<Cloud.GetVersionResponse> getVisibleVersionAsync(Cloud.GetVersionRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getVisibleVersionAsync(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetVersionResponse getVersion(Cloud.GetVersionRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getVersion(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.CreateTabletsResponse createTablets(Cloud.CreateTabletsRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.createTablets(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.UpdateTabletResponse updateTablet(Cloud.UpdateTabletRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.updateTablet(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.BeginTxnResponse beginTxn(Cloud.BeginTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.beginTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.PrecommitTxnResponse precommitTxn(Cloud.PrecommitTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.precommitTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.CommitTxnResponse commitTxn(Cloud.CommitTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.commitTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.AbortTxnResponse abortTxn(Cloud.AbortTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.abortTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetTxnResponse getTxn(Cloud.GetTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetTxnIdResponse getTxnId(Cloud.GetTxnIdRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getTxnId(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetCurrentMaxTxnResponse getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getCurrentMaxTxnId(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.BeginSubTxnResponse beginSubTxn(Cloud.BeginSubTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.beginSubTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.AbortSubTxnResponse abortSubTxn(Cloud.AbortSubTxnRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.abortSubTxn(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.CheckTxnConflictResponse checkTxnConflict(Cloud.CheckTxnConflictRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.checkTxnConflict(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.CleanTxnLabelResponse cleanTxnLabel(Cloud.CleanTxnLabelRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.cleanTxnLabel(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetClusterResponse getCluster(Cloud.GetClusterRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getCluster(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.IndexResponse prepareIndex(Cloud.IndexRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.prepareIndex(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.IndexResponse commitIndex(Cloud.IndexRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.commitIndex(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.CheckKVResponse checkKv(Cloud.CheckKVRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.checkKv(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.IndexResponse dropIndex(Cloud.IndexRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.dropIndex(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.preparePartition(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.PartitionResponse commitPartition(Cloud.PartitionRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.commitPartition(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.PartitionResponse dropPartition(Cloud.PartitionRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.dropPartition(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetTabletStatsResponse getTabletStats(Cloud.GetTabletStatsRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getTabletStats(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.CreateStageResponse createStage(Cloud.CreateStageRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.createStage(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetStageResponse getStage(Cloud.GetStageRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getStage(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.DropStageResponse dropStage(Cloud.DropStageRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.dropStage(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetIamResponse getIam(Cloud.GetIamRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getIam(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.BeginCopyResponse beginCopy(Cloud.BeginCopyRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.beginCopy(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.FinishCopyResponse finishCopy(Cloud.FinishCopyRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.finishCopy(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetCopyJobResponse getCopyJob(Cloud.GetCopyJobRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getCopyJob(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetCopyFilesResponse getCopyFiles(Cloud.GetCopyFilesRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getCopyFiles(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.FilterCopyFilesResponse filterCopyFiles(Cloud.FilterCopyFilesRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.filterCopyFiles(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.AlterClusterResponse alterCluster(Cloud.AlterClusterRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.alterCluster(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetDeleteBitmapUpdateLockResponse getDeleteBitmapUpdateLock(
            Cloud.GetDeleteBitmapUpdateLockRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getDeleteBitmapUpdateLock(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.AlterObjStoreInfoResponse alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.alterObjStoreInfo(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetRLTaskCommitAttachResponse
            getRLTaskCommitAttach(Cloud.GetRLTaskCommitAttachRequest request)
            throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getRLTaskCommitAttach(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }

    public Cloud.GetObjStoreInfoResponse
            getObjStoreInfo(Cloud.GetObjStoreInfoRequest request) throws RpcException {
        try {
            final MetaServiceClient client = getProxy();
            return client.getObjStoreInfo(request);
        } catch (Exception e) {
            throw new RpcException("", e.getMessage(), e);
        }
    }
}
