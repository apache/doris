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

package org.apache.doris.binlog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.alter.CloudSchemaChangeHandler;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.BinlogGcTask;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.thrift.TTabletMetaInfo;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BinlogGcer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(BinlogGcer.class);
    private static final long GC_DURATION_MS = 15 * 1000L; // 15s

    public BinlogGcer() {
        super("binlog-gcer", GC_DURATION_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("start binlog syncer jobs.");
        }
        try {
            syncRowBinlogTtlReferenceTso();
            List<BinlogTombstone> tombstones = Env.getCurrentEnv().getBinlogManager().gc();
            if (tombstones != null && !tombstones.isEmpty()) {
                LOG.info("tombstones size: {}", tombstones.size());
            } else {
                LOG.info("no gc binlog");
                return;
            }

            try {
                sendGcInfoToBe(tombstones);
            } catch (Throwable e) {
                // TODO(Drogon): retry
                // if send gc info to be failed, next gc depend on gc duration
                LOG.warn("Failed to send gc info to be", e);
            }

            for (BinlogTombstone tombstone : tombstones) {
                tombstone.clearTableVersionMap();
            }
            BinlogGcInfo info = new BinlogGcInfo(tombstones);
            Env.getCurrentEnv().getEditLog().logGcBinlog(info);
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of BinlogGcer", e);
        }
    }

    void syncRowBinlogTtlReferenceTso() {
        if (!Config.enable_feature_binlog) {
            return;
        }
        List<OlapTable> ttlTables = Env.getCurrentInternalCatalog().getDbs().stream()
                .flatMap(db -> db.getTables().stream())
                .filter(OlapTable.class::isInstance)
                .map(OlapTable.class::cast)
                .filter(OlapTable::hasRowBinlogTtl)
                .collect(Collectors.toList());
        if (ttlTables.isEmpty()) {
            return;
        }
        long referenceTso;
        try {
            referenceTso = Env.getCurrentTSOService().getTSO();
        } catch (RuntimeException e) {
            LOG.warn("Failed to get row binlog TTL reference TSO; keep the previous GC boundary", e);
            return;
        }
        if (referenceTso <= 0) {
            LOG.warn("Ignore non-positive row binlog TTL reference TSO: {}", referenceTso);
            return;
        }

        for (OlapTable table : ttlTables) {
            syncTableRowBinlogTtl(table, referenceTso);
        }
    }

    private void syncTableRowBinlogTtl(OlapTable table, long referenceTso) {
        Map<Long, List<TTabletMetaInfo>> backendTabletInfos = Maps.newHashMap();
        List<Long> cloudTabletIds = new ArrayList<>();
        table.readLock();
        try {
            for (Partition partition : table.getPartitions()) {
                for (MaterializedIndex index :
                        partition.getMaterializedIndices(IndexExtState.VISIBLE, true)) {
                    if (!index.isRowBinlog()) {
                        continue;
                    }
                    for (Tablet tablet : index.getTablets()) {
                        if (Config.isCloudMode()) {
                            cloudTabletIds.add(tablet.getId());
                            continue;
                        }
                        for (Replica replica : tablet.getReplicas()) {
                            backendTabletInfos.computeIfAbsent(replica.getBackendIdWithoutException(),
                                    ignored -> new ArrayList<>()).add(new TTabletMetaInfo()
                                            .setTabletId(tablet.getId())
                                            .setRowBinlogTtlReferenceTso(referenceTso));
                        }
                    }
                }
            }
        } finally {
            table.readUnlock();
        }

        if (Config.isCloudMode()) {
            syncCloudRowBinlogTtl(table.getName(), cloudTabletIds, referenceTso);
            return;
        }
        if (!FeConstants.runningUnitTest) {
            AgentBatchTask batchTask = new AgentBatchTask();
            backendTabletInfos.forEach((backendId, infos) ->
                    batchTask.addTask(new UpdateTabletMetaInfoTask(backendId, infos)));
            if (batchTask.getTaskNum() > 0) {
                AgentTaskExecutor.submit(batchTask);
            }
        }
    }

    private void syncCloudRowBinlogTtl(String tableName, List<Long> tabletIds, long referenceTso) {
        for (int index = 0; index < tabletIds.size();) {
            int nextIndex = Math.min(index + Config.cloud_txn_tablet_batch_size, tabletIds.size());
            Cloud.UpdateTabletRequest.Builder request = Cloud.UpdateTabletRequest.newBuilder()
                    .setRequestIp(FrontendOptions.getLocalHostAddressCached());
            while (index < nextIndex) {
                request.addTabletMetaInfos(Cloud.TabletMetaInfoPB.newBuilder()
                        .setTabletId(tabletIds.get(index++))
                        .setRowBinlogTtlReferenceTso(referenceTso));
            }
            try {
                Cloud.UpdateTabletResponse response = MetaServiceProxy.getInstance().updateTablet(request.build());
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                    LOG.warn("Failed to update row binlog TTL reference TSO for table {}: {}",
                            tableName, response.getStatus().getMsg());
                    continue;
                }
                CloudSchemaChangeHandler.notifyBackendsToSyncTabletMeta(tableName,
                        request.getTabletMetaInfosList().stream()
                                .map(Cloud.TabletMetaInfoPB::getTabletId)
                                .collect(Collectors.toList()));
            } catch (Exception e) {
                LOG.warn("Failed to update row binlog TTL reference TSO for table {}", tableName, e);
            }
        }
    }

    private void sendGcInfoToBe(List<BinlogTombstone> tombstones) {
        if (tombstones == null || tombstones.isEmpty()) {
            return;
        }

        Map<Long, BinlogGcTask> beBinlogGcTaskMap = Maps.newHashMap();
        for (BinlogTombstone tombstone : tombstones) {
            sendDbGcInfoToBe(beBinlogGcTaskMap, tombstone);
        }

        if (beBinlogGcTaskMap.isEmpty()) {
            return;
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        for (BinlogGcTask task : beBinlogGcTaskMap.values()) {
            batchTask.addTask(task);
        }
        AgentTaskExecutor.submit(batchTask);
    }

    private void sendDbGcInfoToBe(Map<Long, BinlogGcTask> beBinlogGcTaskMap, BinlogTombstone tombstone) {
        long dbId = tombstone.getDbId();
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            LOG.warn("db {} does not exist", dbId);
            return;
        }

        Map<Long, UpsertRecord.TableRecord> tableVersionMap = tombstone.getTableVersionMap();
        for (Map.Entry<Long, UpsertRecord.TableRecord> entry : tableVersionMap.entrySet()) {
            long tableId = entry.getKey();

            OlapTable table = null;
            try {
                Table tbl = db.getTableOrMetaException(tableId);
                if (tbl == null) {
                    LOG.warn("fail to get table. db: {}, table id: {}", db.getFullName(), tableId);
                    continue;
                }
                if (!(tbl instanceof OlapTable)) {
                    LOG.warn("table is not olap table. db: {}, table id: {}", db.getFullName(), tableId);
                    continue;
                }
                table = (OlapTable) tbl;
            } catch (Exception e) {
                LOG.warn("fail to get table. db: {}, table id: {}", db.getFullName(), tableId);
                continue;
            }

            UpsertRecord.TableRecord record = entry.getValue();
            sendTableGcInfoToBe(beBinlogGcTaskMap, table, record);
        }
    }

    private void sendTableGcInfoToBe(Map<Long, BinlogGcTask> beBinlogGcTaskMap, OlapTable olapTable,
            UpsertRecord.TableRecord tableRecord) {

        olapTable.readLock();
        try {
            for (UpsertRecord.TableRecord.PartitionRecord partitionRecord : tableRecord.getPartitionRecords()) {
                long partitionId = partitionRecord.partitionId;
                Partition partition = olapTable.getPartition(partitionId);
                if (partition == null) {
                    LOG.warn("fail to get partition. table: {}, partition id: {}", olapTable.getName(), partitionId);
                    continue;
                }

                long version = partitionRecord.version;

                List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.VISIBLE);
                for (MaterializedIndex index : indexes) {
                    List<Tablet> tablets = index.getTablets();
                    for (Tablet tablet : tablets) {
                        List<Replica> replicas = tablet.getReplicas();
                        for (Replica replica : replicas) {
                            long beId = replica.getBackendIdWithoutException();
                            long signature = -1;
                            BinlogGcTask binlogGcTask = null;
                            if (beBinlogGcTaskMap.containsKey(beId)) {
                                binlogGcTask = beBinlogGcTaskMap.get(beId);
                            } else {
                                binlogGcTask = new BinlogGcTask(beId, signature);
                                beBinlogGcTaskMap.put(beId, binlogGcTask);
                            }

                            binlogGcTask.addTask(tablet.getId(), version);
                        }
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
        }
    }
}
