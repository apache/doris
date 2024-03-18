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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MetaServiceProxy {
    private static final Logger LOG = LogManager.getLogger(MetaServiceProxy.class);
    // use exclusive lock to make sure only one thread can add or remove client from serviceMap.
    // use concurrent map to allow access serviceMap in multi thread.
    private static Pair<String, Integer> metaServiceHostPort = null;

    static {
        if (Config.isCloudMode()) {
            try {
                metaServiceHostPort = SystemInfoService.validateHostAndPort(Config.meta_service_endpoint);
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ReentrantLock lock = new ReentrantLock();

    private final Map<TNetworkAddress, MetaServiceClient> serviceMap;

    public MetaServiceProxy() {
        this.serviceMap = Maps.newConcurrentMap();
    }

    private static class SingletonHolder {
        private static AtomicInteger count = new AtomicInteger();
        private static MetaServiceProxy[] proxies;

        static {
            if (Config.isCloudMode()) {
                int size = Config.meta_service_connection_pooled
                        ? Config.meta_service_connection_pool_size : 1;
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
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getInstance(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public void removeProxy(TNetworkAddress address) {
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

    private MetaServiceClient getProxy(TNetworkAddress address) {
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
                // At this point we cannot judge the progress of reconnecting the underlying channel.
                // In the worst case, it may take two minutes. But we can't stand the connection refused
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

    public Future<Cloud.GetVersionResponse>
            getVisibleVersionAsync(Cloud.GetVersionRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getVisibleVersionAsync(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetVersionResponse
            getVersion(Cloud.GetVersionRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getVersion(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.CreateTabletsResponse
            createTablets(Cloud.CreateTabletsRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.createTablets(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.UpdateTabletResponse
            updateTablet(Cloud.UpdateTabletRequest request) throws RpcException {
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.updateTablet(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.BeginTxnResponse
            beginTxn(Cloud.BeginTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.beginTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.PrecommitTxnResponse
            precommitTxn(Cloud.PrecommitTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.precommitTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.CommitTxnResponse
            commitTxn(Cloud.CommitTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.commitTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.AbortTxnResponse
            abortTxn(Cloud.AbortTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.abortTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetTxnResponse
            getTxn(Cloud.GetTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getTxn(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetCurrentMaxTxnResponse
            getCurrentMaxTxnId(Cloud.GetCurrentMaxTxnRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCurrentMaxTxnId(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.CheckTxnConflictResponse
            checkTxnConflict(Cloud.CheckTxnConflictRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.checkTxnConflict(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.CleanTxnLabelResponse
            cleanTxnLabel(Cloud.CleanTxnLabelRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.cleanTxnLabel(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetClusterResponse
            getCluster(Cloud.GetClusterRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCluster(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.IndexResponse
            prepareIndex(Cloud.IndexRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.prepareIndex(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.IndexResponse
            commitIndex(Cloud.IndexRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.commitIndex(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.IndexResponse
            dropIndex(Cloud.IndexRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.dropIndex(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.PartitionResponse preparePartition(Cloud.PartitionRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.preparePartition(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.PartitionResponse
            commitPartition(Cloud.PartitionRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.commitPartition(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.PartitionResponse
            dropPartition(Cloud.PartitionRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.dropPartition(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetTabletStatsResponse
            getTabletStats(Cloud.GetTabletStatsRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getTabletStats(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.CreateStageResponse createStage(Cloud.CreateStageRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.createStage(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetStageResponse getStage(Cloud.GetStageRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getStage(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.DropStageResponse dropStage(Cloud.DropStageRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.dropStage(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetIamResponse getIam(Cloud.GetIamRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getIam(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.BeginCopyResponse beginCopy(Cloud.BeginCopyRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.beginCopy(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.FinishCopyResponse finishCopy(Cloud.FinishCopyRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.finishCopy(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetCopyJobResponse getCopyJob(Cloud.GetCopyJobRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCopyJob(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetCopyFilesResponse getCopyFiles(Cloud.GetCopyFilesRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getCopyFiles(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.FilterCopyFilesResponse filterCopyFiles(Cloud.FilterCopyFilesRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.filterCopyFiles(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.AlterClusterResponse alterCluster(Cloud.AlterClusterRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.alterCluster(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.GetDeleteBitmapUpdateLockResponse
            getDeleteBitmapUpdateLock(Cloud.GetDeleteBitmapUpdateLockRequest request)
            throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.getDeleteBitmapUpdateLock(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }

    public Cloud.AlterObjStoreInfoResponse
            alterObjStoreInfo(Cloud.AlterObjStoreInfoRequest request) throws RpcException {
        if (metaServiceHostPort == null) {
            throw new RpcException("", "cloud mode, please configure cloud_unique_id and meta_service_endpoint");
        }
        TNetworkAddress metaAddress = new TNetworkAddress(metaServiceHostPort.first, metaServiceHostPort.second);
        try {
            final MetaServiceClient client = getProxy(metaAddress);
            return client.alterObjStoreInfo(request);
        } catch (Exception e) {
            throw new RpcException(metaAddress.hostname, e.getMessage(), e);
        }
    }
}
