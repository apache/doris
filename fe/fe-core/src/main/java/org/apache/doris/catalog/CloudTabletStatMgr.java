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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsRequest;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TabletIndexPB;
import org.apache.doris.cloud.proto.Cloud.TabletStatsPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * CloudTabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class CloudTabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletStatMgr.class);

    // <(dbId, tableId) -> OlapTable.Statistics>
    private volatile Map<Pair<Long, Long>, OlapTable.Statistics> cloudTableStatsMap = new HashMap<>();

    public CloudTabletStatMgr() {
        super("cloud tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet stat begin");
        long start = System.currentTimeMillis();

        List<GetTabletStatsRequest> reqList = new ArrayList<GetTabletStatsRequest>();
        GetTabletStatsRequest.Builder builder = GetTabletStatsRequest.newBuilder();
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }

                table.readLock();
                try {
                    OlapTable tbl = (OlapTable) table;
                    for (Partition partition : tbl.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                Tablet tablet = index.getTablet(tabletId);
                                TabletIndexPB.Builder tabletBuilder = TabletIndexPB.newBuilder();
                                tabletBuilder.setDbId(dbId);
                                tabletBuilder.setTableId(table.getId());
                                tabletBuilder.setIndexId(index.getId());
                                tabletBuilder.setPartitionId(partition.getId());
                                tabletBuilder.setTabletId(tablet.getId());
                                builder.addTabletIdx(tabletBuilder);

                                if (builder.getTabletIdxCount() >= Config.get_tablet_stat_batch_size) {
                                    reqList.add(builder.build());
                                    builder = GetTabletStatsRequest.newBuilder();
                                }
                            }
                        }
                    } // partitions
                } finally {
                    table.readUnlock();
                }
            } // tables
        } // end for dbs

        if (builder.getTabletIdxCount() > 0) {
            reqList.add(builder.build());
        }

        for (GetTabletStatsRequest req : reqList) {
            GetTabletStatsResponse resp;
            try {
                resp = getTabletStats(req);
            } catch (RpcException e) {
                LOG.info("get tablet stats exception:", e);
                continue;
            }

            if (resp.getStatus().getCode() != MetaServiceCode.OK) {
                continue;
            }

            if (LOG.isDebugEnabled()) {
                int i = 0;
                for (TabletIndexPB idx : req.getTabletIdxList()) {
                    LOG.debug("db_id: {} table_id: {} index_id: {} tablet_id: {} size: {}",
                            idx.getDbId(), idx.getTableId(), idx.getIndexId(), idx.getTabletId(),
                            resp.getTabletStats(i++).getDataSize());
                }
            }
            updateTabletStat(resp);
        }

        LOG.info("finished to get tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();

        Map<Pair<Long, Long>, OlapTable.Statistics> newCloudTableStatsMap = new HashMap<>();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;

                Long tableDataSize = 0L;
                Long tableTotalReplicaDataSize = 0L;

                Long tableReplicaCount = 0L;

                Long tableRowCount = 0L;
                Long tableRowsetCount = 0L;
                Long tableSegmentCount = 0L;

                if (!table.readLockIfExist()) {
                    continue;
                }
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletDataSize = 0L;

                                long tabletRowsetCount = 0L;
                                long tabletSegmentCount = 0L;
                                long tabletRowCount = 0L;

                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.getDataSize() > tabletDataSize) {
                                        tabletDataSize = replica.getDataSize();
                                        tableTotalReplicaDataSize += replica.getDataSize();
                                    }

                                    if (replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }

                                    if (replica.getRowsetCount() > tabletRowsetCount) {
                                        tabletRowsetCount = replica.getRowsetCount();
                                    }

                                    if (replica.getSegmentCount() > tabletSegmentCount) {
                                        tabletSegmentCount = replica.getSegmentCount();
                                    }

                                    tableReplicaCount++;
                                }

                                tableDataSize += tabletDataSize;

                                tableRowCount += tabletRowCount;
                                indexRowCount += tabletRowCount;

                                tableRowsetCount += tabletRowsetCount;
                                tableSegmentCount += tabletSegmentCount;
                            } // end for tablets
                            index.setRowCountReported(true);
                            index.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions

                    //  this is only one thread to update table statistics, readLock is enough
                    olapTable.setStatistics(new OlapTable.Statistics(db.getName(),
                            table.getName(), tableDataSize, tableTotalReplicaDataSize, 0L,
                            tableReplicaCount, tableRowCount, tableRowsetCount, tableSegmentCount));
                    LOG.debug("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                } finally {
                    table.readUnlock();
                }

                newCloudTableStatsMap.put(Pair.of(dbId, table.getId()), new OlapTable.Statistics(db.getName(),
                        table.getName(), tableDataSize, tableTotalReplicaDataSize, 0L,
                        tableReplicaCount, tableRowCount, tableRowsetCount, tableSegmentCount));
            }
        }
        this.cloudTableStatsMap = newCloudTableStatsMap;
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateTabletStat(GetTabletStatsResponse response) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (TabletStatsPB stat : response.getTabletStatsList()) {
            if (invertedIndex.getTabletMeta(stat.getIdx().getTabletId()) != null) {
                List<Replica> replicas = invertedIndex.getReplicasByTabletId(stat.getIdx().getTabletId());
                if (replicas == null || replicas.isEmpty() || replicas.get(0) == null) {
                    continue;
                }
                Replica replica = replicas.get(0);
                replica.setDataSize(stat.getDataSize());
                replica.setRowsetCount(stat.getNumRowsets());
                replica.setSegmentCount(stat.getNumSegments());
                replica.setRowCount(stat.getNumRows());
            }
        }
    }

    private GetTabletStatsResponse getTabletStats(GetTabletStatsRequest request)
            throws RpcException {
        GetTabletStatsResponse response;
        try {
            response = MetaServiceProxy.getInstance().getTabletStats(request);
        } catch (RpcException e) {
            LOG.info("get tablet stat get exception:", e);
            throw e;
        }
        return response;
    }

    public Map<Pair<Long, Long>, OlapTable.Statistics> getCloudTableStatsMap() {
        return this.cloudTableStatsMap;
    }
}
