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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TCloudVersionInfo;
import org.apache.doris.thrift.TFrontendSyncCloudVersionRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CloudFEVersionSynchronizer {
    private static final Logger LOG = LogManager.getLogger(CloudFEVersionSynchronizer.class);

    private static final ExecutorService SYNC_VERSION_THREAD_POOL = Executors.newFixedThreadPool(
            Config.cloud_sync_version_task_threads_num,
            new ThreadFactoryBuilder().setNameFormat("sync-version-%d").setDaemon(true).build());

    public CloudFEVersionSynchronizer() {
    }

    // master FE send sync version rpc to other FEs
    public void pushVersionAsync(long dbId, OlapTable table, long version) {
        if (!Config.cloud_enable_version_syncer) {
            return;
        }
        pushVersionAsync(dbId, Collections.singletonList(Pair.of(table, version)), Collections.emptyMap());
    }

    public void pushVersionAsync(long dbId, List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> partitionVersionMap) {
        if (!Config.cloud_enable_version_syncer) {
            return;
        }
        if (tableVersions.isEmpty() && partitionVersionMap.isEmpty()) {
            return;
        }
        pushVersion(dbId, tableVersions, partitionVersionMap);
    }

    private void pushVersion(long dbId, List<Pair<OlapTable, Long>> tableVersions,
            Map<CloudPartition, Pair<Long, Long>> partitionVersionMap) {
        List<Frontend> frontends = getFrontends();
        if (frontends == null || frontends.isEmpty()) {
            return;
        }

        List<TCloudVersionInfo> tableVersionInfos = new ArrayList<>(tableVersions.size());
        tableVersions.forEach(pair -> {
            TCloudVersionInfo tableVersion = new TCloudVersionInfo();
            tableVersion.setTableId(pair.first.getId());
            tableVersion.setVersion(pair.second);
            tableVersionInfos.add(tableVersion);
        });
        List<TCloudVersionInfo> partitionVersionInfos = new ArrayList<>(partitionVersionMap.size());
        partitionVersionMap.forEach((partition, versionPair) -> {
            TCloudVersionInfo partitionVersion = new TCloudVersionInfo();
            partitionVersion.setTableId(partition.getTableId());
            partitionVersion.setPartitionId(partition.getId());
            partitionVersion.setVersion(versionPair.first);
            partitionVersion.setVersionUpdateTime(versionPair.second);
            partitionVersionInfos.add(partitionVersion);
        });
        TFrontendSyncCloudVersionRequest request = new TFrontendSyncCloudVersionRequest();
        request.setDbId(dbId);
        request.setTableVersionInfos(tableVersionInfos);
        request.setPartitionVersionInfos(partitionVersionInfos);
        for (Frontend fe : frontends) {
            SYNC_VERSION_THREAD_POOL.submit(() -> {
                try {
                    pushVersionToFe(request, fe);
                } catch (Exception e) {
                    LOG.warn("push cloud version error", e);
                }
            });
        }
    }

    private List<Frontend> getFrontends() {
        HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        return Env.getCurrentEnv().getFrontends(null).stream()
                .filter(fe -> fe.isAlive() && !(fe.getHost().equals(selfNode.getHost())
                        && fe.getEditLogPort() == selfNode.getPort())).collect(
                        Collectors.toList());
    }

    private void pushVersionToFe(TFrontendSyncCloudVersionRequest request, Frontend fe) {
        FrontendService.Client client = null;
        TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
        boolean ok = false;
        try {
            client = ClientPool.frontendVersionPool.borrowObject(addr);
            TStatus status = client.syncCloudVersion(request);
            ok = true;
            if (status.getStatusCode() != TStatusCode.OK) {
                LOG.warn("failed to push cloud version to frontend {}:{}, err: {}", fe.getHost(), fe.getRpcPort(),
                        status.getErrorMsgs());
            }
        } catch (Exception e) {
            LOG.warn("failed to push cloud version to frontend {}:{}", fe.getHost(), fe.getRpcPort(), e);
        } finally {
            if (ok) {
                ClientPool.frontendVersionPool.returnObject(addr, client);
            } else {
                ClientPool.frontendVersionPool.invalidateObject(addr, client);
            }
        }
    }

    // follower and observer FE receive sync version rpc from master FE
    public void syncVersionAsync(TFrontendSyncCloudVersionRequest request) {
        Database db = Env.getCurrentInternalCatalog().getDbNullable(request.getDbId());
        if (db == null) {
            return;
        }
        // only update table version
        if (request.getPartitionVersionInfos().isEmpty()) {
            request.getTableVersionInfos().forEach(tableVersionInfo -> {
                Table table = db.getTableNullable(tableVersionInfo.getTableId());
                if (table == null || !table.isManagedTable()) {
                    return;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.setCachedTableVersion(tableVersionInfo.getVersion());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update tableId: {}, version: {}", olapTable.getId(), tableVersionInfo.getVersion());
                }
            });
            return;
        }
        // update partition and table version
        SYNC_VERSION_THREAD_POOL.submit(() -> {
            syncVersion(db, request);
        });
    }

    private void syncVersion(Database db, TFrontendSyncCloudVersionRequest request) {
        List<Pair<OlapTable, Long>> tableVersions = new ArrayList<>(request.getTableVersionInfos().size());
        for (TCloudVersionInfo tableVersionInfo : request.getTableVersionInfos()) {
            Table table = db.getTableNullable(tableVersionInfo.getTableId());
            if (table == null || !table.isManagedTable()) {
                continue;
            }
            tableVersions.add(Pair.of((OlapTable) table, tableVersionInfo.getVersion()));
        }
        Collections.sort(tableVersions, Comparator.comparingLong(o -> o.first.getId()));
        for (Pair<OlapTable, Long> tableVersion : tableVersions) {
            tableVersion.first.versionWriteLock();
        }
        try {
            for (TCloudVersionInfo partitionVersionInfo : request.getPartitionVersionInfos()) {
                Table table = db.getTableNullable(partitionVersionInfo.getTableId());
                if (table == null || !table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                Partition partition = olapTable.getPartition(partitionVersionInfo.getPartitionId());
                if (partition == null) {
                    continue;
                }
                CloudPartition cloudPartition = (CloudPartition) partition;
                cloudPartition.setCachedVisibleVersion(partitionVersionInfo.getVersion(),
                        partitionVersionInfo.getVersionUpdateTime());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update tableId: {}, partitionId: {}, version: {}, updateTime: {}",
                            partitionVersionInfo.getTableId(), partition.getId(), partitionVersionInfo.getVersion(),
                            partitionVersionInfo.getVersionUpdateTime());
                }
            }
            for (Pair<OlapTable, Long> tableVersion : tableVersions) {
                tableVersion.first.setCachedTableVersion(tableVersion.second);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update tableId: {}, version: {}", tableVersion.first.getId(), tableVersion.second);
                }
            }
        } finally {
            for (int i = tableVersions.size() - 1; i >= 0; i--) {
                tableVersions.get(i).first.versionWriteUnlock();
            }
        }
    }
}
