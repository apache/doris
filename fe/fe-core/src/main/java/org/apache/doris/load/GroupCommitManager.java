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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.SlidingWindowCounter;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.proto.InternalService.PGetWalQueueSizeRequest;
import org.apache.doris.proto.InternalService.PGetWalQueueSizeResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class GroupCommitManager {

    private static final Logger LOG = LogManager.getLogger(GroupCommitManager.class);

    private Set<Long> blockedTableIds = new HashSet<>();

    // Encoded <Cluster and Table id> to BE id map. Only for group commit.
    private final Map<String, Long> tableToBeMap = new ConcurrentHashMap<>();
    // Table id to pressure map. Only for group commit.
    private final Map<Long, SlidingWindowCounter> tableToPressureMap = new ConcurrentHashMap<>();

    public boolean isBlock(long tableId) {
        return blockedTableIds.contains(tableId);
    }

    public void blockTable(long tableId) {
        LOG.info("block group commit for table={} when schema change", tableId);
        blockedTableIds.add(tableId);
    }

    public void unblockTable(long tableId) {
        blockedTableIds.remove(tableId);
        LOG.info("unblock group commit for table={} when schema change", tableId);
    }

    /**
     * Waiting All WAL files to be deleted.
     */
    public void waitWalFinished(long tableId) {
        List<Long> aliveBeIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        long expireTime = System.currentTimeMillis() + Config.check_wal_queue_timeout_threshold;
        while (true) {
            LOG.info("wait for wal queue size to be empty");
            boolean walFinished = Env.getCurrentEnv().getGroupCommitManager()
                    .isPreviousWalFinished(tableId, aliveBeIds);
            if (walFinished) {
                LOG.info("all wal is finished for table={}", tableId);
                break;
            } else if (System.currentTimeMillis() > expireTime) {
                LOG.warn("waitWalFinished time out for table={}", tableId);
                break;
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.warn("failed to wait for wal for table={} when schema change", tableId, ie);
                }
            }
        }
    }

    /**
     * Check the wal before the endTransactionId is finished or not.
     */
    private boolean isPreviousWalFinished(long tableId, List<Long> aliveBeIds) {
        boolean empty = true;
        for (int i = 0; i < aliveBeIds.size(); i++) {
            Backend backend = Env.getCurrentSystemInfo().getBackend(aliveBeIds.get(i));
            // in ut port is -1, skip checking
            if (backend.getBrpcPort() < 0) {
                return true;
            }
            PGetWalQueueSizeRequest request = PGetWalQueueSizeRequest.newBuilder()
                    .setTableId(tableId)
                    .build();
            long size = getWalQueueSize(backend, request);
            if (size > 0) {
                LOG.info("backend id:" + backend.getId() + ",wal size:" + size);
                empty = false;
            }
        }
        return empty;
    }

    public long getAllWalQueueSize(Backend backend) {
        PGetWalQueueSizeRequest request = PGetWalQueueSizeRequest.newBuilder()
                .setTableId(-1)
                .build();
        long size = getWalQueueSize(backend, request);
        if (size > 0) {
            LOG.info("backend id:" + backend.getId() + ",all wal size:" + size);
        }
        return size;
    }

    private long getWalQueueSize(Backend backend, PGetWalQueueSizeRequest request) {
        PGetWalQueueSizeResponse response = null;
        long expireTime = System.currentTimeMillis() + Config.check_wal_queue_timeout_threshold;
        long size = 0;
        while (System.currentTimeMillis() <= expireTime) {
            if (!backend.isAlive()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("group commit manager sleep wait InterruptedException: ", ie);
                }
                continue;
            }
            try {
                Future<PGetWalQueueSizeResponse> future = BackendServiceProxy.getInstance()
                        .getWalQueueSize(new TNetworkAddress(backend.getHost(), backend.getBrpcPort()), request);
                response = future.get();
            } catch (Exception e) {
                LOG.warn("encounter exception while getting wal queue size on backend id: " + backend.getId()
                        + ",exception:" + e);
                String msg = e.getMessage();
                if (msg.contains("Method") && msg.contains("unimplemented")) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("group commit manager sleep wait InterruptedException: ", ie);
                }
                continue;
            }
            TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                String msg = "get all queue size fail,backend id: " + backend.getId() + ", status: "
                        + response.getStatus();
                LOG.warn(msg);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    LOG.info("group commit manager sleep wait InterruptedException: ", ie);
                }
                continue;
            }
            size = response.getSize();
            break;
        }
        return size;
    }

    public Backend selectBackendForGroupCommit(long tableId, ConnectContext context)
            throws LoadException, DdlException {
        // If a group commit request is sent to the follower FE, we will send this request to the master FE. master FE
        // can select a BE and return this BE id to follower FE.
        if (!Env.getCurrentEnv().isMaster()) {
            try {
                long backendId = new MasterOpExecutor(context)
                        .getGroupCommitLoadBeId(tableId, context.getCloudCluster());
                return Env.getCurrentSystemInfo().getBackend(backendId);
            } catch (Exception e) {
                throw new LoadException(e.getMessage());
            }
        } else {
            // Master FE will select BE by itself.
            return Env.getCurrentSystemInfo()
                    .getBackend(selectBackendForGroupCommitInternal(tableId, context.getCloudCluster()));
        }
    }

    public long selectBackendForGroupCommitInternal(long tableId, String cluster)
            throws LoadException, DdlException {
        // Understanding Group Commit and Backend Selection Logic
        //
        // Group commit is a server-side technique used for batching data imports.
        // The primary purpose of group commit is to enhance import performance by
        // reducing the number of versions created for high-frequency, small-batch imports.
        // Without batching, each import operation creates a separate version, similar to a rowset in an LSM Tree,
        // which can consume significant compaction resources and degrade system performance.
        // By batching data, fewer versions are generated from the same amount of data,
        // thus minimizing compaction and improving performance. For detailed usage,
        // you can refer to the Group Commit Manual
        // (https://doris.incubator.apache.org/docs/data-operate/import/group-commit-manual/) .
        //
        // The specific backend (BE) selection logic for group commits aims to
        // direct data belonging to the same table to the same BE for batching.
        // This is because group commit batches data imported to the same table
        // on the same BE into a single version, which is then flushed periodically.
        // For example, if data for the same table is distributed across three BEs,
        // it will result in three versions.
        // Conversely, if data for four different tables is directed to the same BE,
        // it will create four versions. However,
        // directing all data for the same table to a single BE will only produce one version.
        //
        // To optimize performance and avoid overloading a single BE, the strategy for selecting a BE works as follows:
        //
        // If a BE is already handling imports for table A and is not under significant load,
        // the data is sent to this BE.
        // If the BE is overloaded or if there is no existing record of a BE handling imports for table A,
        // a BE is chosen at random. This BE is then recorded along with the mapping of table A and its load level.
        // This approach ensures that group commits can effectively batch data together
        // while managing the load on each BE efficiently.
        return Config.isCloudMode() ? selectBackendForCloudGroupCommitInternal(tableId, cluster)
                : selectBackendForLocalGroupCommitInternal(tableId);
    }

    private long selectBackendForCloudGroupCommitInternal(long tableId, String cluster)
            throws DdlException, LoadException {
        LOG.debug("cloud group commit select be info, tableToBeMap {}, tablePressureMap {}",
                tableToBeMap.toString(), tableToPressureMap.toString());
        if (Strings.isNullOrEmpty(cluster)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_CLUSTER_ERROR);
        }

        Long cachedBackendId = getCachedBackend(cluster, tableId);
        if (cachedBackendId != null) {
            return cachedBackendId;
        }

        List<Backend> backends = new ArrayList<>(
                ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudIdToBackend(cluster)
                        .values());
        if (backends.isEmpty()) {
            throw new LoadException("No alive backend");
        }
        // If the cached backend is not active or decommissioned, select a random new backend.
        Long randomBackendId = getRandomBackend(cluster, tableId, backends);
        if (randomBackendId != null) {
            return randomBackendId;
        }
        List<String> backendsInfo = backends.stream()
                .map(be -> "{ beId=" + be.getId() + ", alive=" + be.isAlive() + ", active=" + be.isActive()
                        + ", decommission=" + be.isDecommissioned() + " }")
                .collect(Collectors.toList());
        throw new LoadException("No suitable backend for cloud cluster=" + cluster + ", backends = " + backendsInfo);
    }

    private long selectBackendForLocalGroupCommitInternal(long tableId) throws LoadException {
        LOG.debug("group commit select be info, tableToBeMap {}, tablePressureMap {}", tableToBeMap.toString(),
                tableToPressureMap.toString());
        Long cachedBackendId = getCachedBackend(null, tableId);
        if (cachedBackendId != null) {
            return cachedBackendId;
        }

        List<Backend> backends = new ArrayList<>();
        try {
            backends = new ArrayList<>(Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values().asList());
        } catch (AnalysisException e) {
            LOG.warn("failed to get backends by all cluster", e);
            throw new LoadException(e.getMessage());
        }

        if (backends.isEmpty()) {
            throw new LoadException("No alive backend");
        }

        // If the cached backend is not active or decommissioned, select a random new backend.
        Long randomBackendId = getRandomBackend(null, tableId, backends);
        if (randomBackendId != null) {
            return randomBackendId;
        }
        List<String> backendsInfo = backends.stream()
                .map(be -> "{ beId=" + be.getId() + ", alive=" + be.isAlive() + ", active=" + be.isActive()
                        + ", decommission=" + be.isDecommissioned() + " }")
                .collect(Collectors.toList());
        throw new LoadException("No suitable backend " + ", backends = " + backendsInfo);
    }

    @Nullable
    private Long getCachedBackend(String cluster, long tableId) {
        OlapTable table = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getTableByTableId(tableId);
        if (tableToBeMap.containsKey(encode(cluster, tableId))) {
            if (tableToPressureMap.get(tableId).get() < table.getGroupCommitDataBytes()) {
                // There are multiple threads getting cached backends for the same table.
                // Maybe one thread removes the tableId from the tableToBeMap.
                // Another thread gets the same tableId but can not find this tableId.
                // So another thread needs to get the random backend.
                Long backendId = tableToBeMap.get(encode(cluster, tableId));
                Backend backend;
                if (backendId != null) {
                    backend = Env.getCurrentSystemInfo().getBackend(backendId);
                } else {
                    return null;
                }
                if (backend.isActive() && !backend.isDecommissioned()) {
                    return backend.getId();
                } else {
                    tableToBeMap.remove(encode(cluster, tableId));
                }
            } else {
                tableToBeMap.remove(encode(cluster, tableId));
            }
        }
        return null;
    }

    @Nullable
    private Long getRandomBackend(String cluster, long tableId, List<Backend> backends) {
        OlapTable table = (OlapTable) Env.getCurrentEnv().getInternalCatalog().getTableByTableId(tableId);
        Collections.shuffle(backends);
        for (Backend backend : backends) {
            if (backend.isActive() && !backend.isDecommissioned()) {
                tableToBeMap.put(encode(cluster, tableId), backend.getId());
                tableToPressureMap.put(tableId,
                        new SlidingWindowCounter(table.getGroupCommitIntervalMs() / 1000 + 1));
                return backend.getId();
            }
        }
        return null;
    }

    private String encode(String cluster, long tableId) {
        if (cluster == null) {
            return String.valueOf(tableId);
        } else {
            return cluster + tableId;
        }
    }

    public void updateLoadData(long tableId, long receiveData) {
        if (tableId == -1) {
            LOG.warn("invalid table id: " + tableId);
        }
        if (!Env.getCurrentEnv().isMaster()) {
            ConnectContext ctx = new ConnectContext();
            ctx.setEnv(Env.getCurrentEnv());
            ctx.setThreadLocalInfo();
            // set user to ADMIN_USER, so that we can get the proper resource tag
            ctx.setQualifiedUser(Auth.ADMIN_USER);
            ctx.setThreadLocalInfo();
            try {
                new MasterOpExecutor(ctx).updateLoadData(tableId, receiveData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            updateLoadDataInternal(tableId, receiveData);
        }
    }

    private void updateLoadDataInternal(long tableId, long receiveData) {
        if (tableToPressureMap.containsKey(tableId)) {
            tableToPressureMap.get(tableId).add(receiveData);
            LOG.info("Update load data for table{}, receiveData {}, tablePressureMap {}", tableId, receiveData,
                    tableToPressureMap.toString());
        } else {
            LOG.warn("can not find backend id: {}", tableId);
        }
    }
}
