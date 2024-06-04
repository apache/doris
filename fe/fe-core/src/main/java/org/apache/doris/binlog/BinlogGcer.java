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
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.BinlogGcTask;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class BinlogGcer extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(BinlogGcer.class);
    private static final long GC_DURATION_MS = 15 * 1000L; // 15s

    // TODO(Drogon): use this to control gc frequency by real gc time waste sample
    private long lastGcTime = 0L;

    public BinlogGcer() {
        super("binlog-gcer", GC_DURATION_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("start binlog syncer jobs.");
        }
        try {
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
                            long beId = replica.getBackendId();
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
