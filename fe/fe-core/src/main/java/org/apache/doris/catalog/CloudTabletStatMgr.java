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
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/*
 * CloudTabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class CloudTabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletStatMgr.class);

    private volatile List<OlapTable.Statistics> cloudTableStatsList = new ArrayList<>();

    private static final ExecutorService GET_TABLET_STATS_THREAD_POOL = Executors.newFixedThreadPool(
            Config.max_get_tablet_stat_task_threads_num);

    public CloudTabletStatMgr() {
        super("cloud tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("cloud tablet stat begin");
        List<Long> dbIds = getAllTabletStats();
        updateStatInfo(dbIds);
    }

    private List<Long> getAllTabletStats() {
        long start = System.currentTimeMillis();
        List<Future<Void>> futures = new ArrayList<>();
        GetTabletStatsRequest.Builder builder =
                GetTabletStatsRequest.newBuilder().setRequestIp(FrontendOptions.getLocalHostAddressCached());
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
                                TabletIndexPB.Builder tabletBuilder = TabletIndexPB.newBuilder();
                                tabletBuilder.setDbId(dbId);
                                tabletBuilder.setTableId(table.getId());
                                tabletBuilder.setIndexId(index.getId());
                                tabletBuilder.setPartitionId(partition.getId());
                                tabletBuilder.setTabletId(tabletId);
                                builder.addTabletIdx(tabletBuilder);

                                if (builder.getTabletIdxCount() >= Config.get_tablet_stat_batch_size) {
                                    futures.add(submitGetTabletStatsTask(builder.build()));
                                    builder = GetTabletStatsRequest.newBuilder()
                                            .setRequestIp(FrontendOptions.getLocalHostAddressCached());
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
            futures.add(submitGetTabletStatsTask(builder.build()));
        }

        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get tablet stats tasks to complete", e);
        }

        LOG.info("finished to get tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));
        return dbIds;
    }

    private Future<Void> submitGetTabletStatsTask(GetTabletStatsRequest req) {
        return GET_TABLET_STATS_THREAD_POOL.submit(() -> {
            GetTabletStatsResponse resp;
            try {
                resp = getTabletStats(req);
            } catch (RpcException e) {
                LOG.warn("get tablet stats exception:", e);
                return null;
            }
            if (resp.getStatus().getCode() != MetaServiceCode.OK) {
                LOG.warn("get tablet stats return failed.");
                return null;
            }
            if (LOG.isDebugEnabled()) {
                int i = 0;
                for (TabletIndexPB idx : req.getTabletIdxList()) {
                    LOG.debug("db_id: {} table_id: {} index_id: {} tablet_id: {} size: {}",
                            idx.getDbId(), idx.getTableId(), idx.getIndexId(),
                            idx.getTabletId(), resp.getTabletStats(i++).getDataSize());
                }
            }
            updateTabletStat(resp);
            return null;
        });
    }

    private void updateStatInfo(List<Long> dbIds) {
        // after update replica in all backends, update index row num
        long start = System.currentTimeMillis();
        Pair<String, Long> maxTabletSize = Pair.of(/* tablet id= */null, /* byte size= */0L);
        Pair<String, Long> maxPartitionSize = Pair.of(/* partition id= */null, /* byte size= */0L);
        Pair<String, Long> maxTableSize = Pair.of(/* table id= */null, /* byte size= */0L);
        Pair<String, Long> minTabletSize = Pair.of(/* tablet id= */null, /* byte size= */Long.MAX_VALUE);
        Pair<String, Long> minPartitionSize = Pair.of(/* partition id= */null, /* byte size= */Long.MAX_VALUE);
        Pair<String, Long> minTableSize = Pair.of(/* tablet id= */null, /* byte size= */Long.MAX_VALUE);
        long totalTableSize = 0L;
        long tabletCount = 0L;
        long partitionCount = 0L;
        long tableCount = 0L;
        List<OlapTable.Statistics> newCloudTableStatsList = new ArrayList<>();
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
                tableCount++;
                OlapTable olapTable = (OlapTable) table;

                long tableDataSize = 0L;
                long tableTotalReplicaDataSize = 0L;
                long tableTotalLocalIndexSize = 0L;
                long tableTotalLocalSegmentSize = 0L;

                long tableReplicaCount = 0L;

                long tableRowCount = 0L;
                long tableRowsetCount = 0L;
                long tableSegmentCount = 0L;

                if (!table.readLockIfExist()) {
                    continue;
                }
                OlapTable.Statistics tableStats;
                try {
                    List<Partition> allPartitions = olapTable.getAllPartitions();
                    partitionCount += allPartitions.size();
                    for (Partition partition : allPartitions) {
                        long partitionDataSize = 0L;
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            List<Tablet> tablets = index.getTablets();
                            tabletCount += tablets.size();
                            for (Tablet tablet : tablets) {
                                long tabletDataSize = 0L;

                                long tabletRowsetCount = 0L;
                                long tabletSegmentCount = 0L;
                                long tabletRowCount = 0L;
                                long tabletIndexSize = 0L;
                                long tabletSegmentSize = 0L;

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

                                    if (replica.getLocalInvertedIndexSize() > tabletIndexSize) {
                                        tabletIndexSize = replica.getLocalInvertedIndexSize();
                                    }
                                    if (replica.getLocalSegmentSize() > tabletSegmentSize) {
                                        tabletSegmentSize = replica.getLocalSegmentSize();
                                    }

                                    tableReplicaCount++;
                                }

                                tableDataSize += tabletDataSize;
                                partitionDataSize += tabletDataSize;
                                if (maxTabletSize.second <= tabletDataSize) {
                                    maxTabletSize = Pair.of("" + tablet.getId(), tabletDataSize);
                                }
                                if (minTabletSize.second >= tabletDataSize) {
                                    minTabletSize = Pair.of("" + tablet.getId(), tabletDataSize);
                                }

                                tableRowCount += tabletRowCount;
                                indexRowCount += tabletRowCount;

                                tableRowsetCount += tabletRowsetCount;
                                tableSegmentCount += tabletSegmentCount;
                                tableTotalLocalIndexSize += tabletIndexSize;
                                tableTotalLocalSegmentSize += tabletSegmentSize;
                            } // end for tablets
                            index.setRowCountReported(true);
                            index.setRowCount(indexRowCount);
                        } // end for indices
                        if (maxPartitionSize.second <= partitionDataSize) {
                            maxPartitionSize = Pair.of("" + partition.getId(), partitionDataSize);
                        }
                        if (minPartitionSize.second >= partitionDataSize) {
                            minPartitionSize = Pair.of("" + partition.getId(), partitionDataSize);
                        }
                    } // end for partitions
                    if (maxTableSize.second <= tableDataSize) {
                        maxTableSize = Pair.of("" + table.getId(), tableDataSize);
                    }
                    if (minTableSize.second >= tableDataSize) {
                        minTableSize = Pair.of("" + table.getId(), tableDataSize);
                    }

                    //  this is only one thread to update table statistics, readLock is enough
                    tableStats = new OlapTable.Statistics(db.getName(),
                            table.getName(), tableDataSize, tableTotalReplicaDataSize, 0L,
                            tableReplicaCount, tableRowCount, tableRowsetCount, tableSegmentCount,
                            tableTotalLocalIndexSize, tableTotalLocalSegmentSize, 0L, 0L);
                    olapTable.setStatistics(tableStats);
                    LOG.debug("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                } finally {
                    table.readUnlock();
                }
                totalTableSize += tableDataSize;
                newCloudTableStatsList.add(tableStats);
            }
        }
        this.cloudTableStatsList = newCloudTableStatsList;

        if (MetricRepo.isInit) {
            MetricRepo.GAUGE_MAX_TABLE_SIZE_BYTES.setValue(maxTableSize.second);
            MetricRepo.GAUGE_MAX_PARTITION_SIZE_BYTES.setValue(maxPartitionSize.second);
            MetricRepo.GAUGE_MAX_TABLET_SIZE_BYTES.setValue(maxTabletSize.second);
            long minTableSizeTmp = minTableSize.second == Long.MAX_VALUE ? 0 : minTableSize.second;
            MetricRepo.GAUGE_MIN_TABLE_SIZE_BYTES.setValue(minTableSizeTmp);
            long minPartitionSizeTmp = minPartitionSize.second == Long.MAX_VALUE ? 0 : minPartitionSize.second;
            MetricRepo.GAUGE_MIN_PARTITION_SIZE_BYTES.setValue(minPartitionSizeTmp);
            long minTabletSizeTmp = minTabletSize.second == Long.MAX_VALUE ? 0 : minTabletSize.second;
            MetricRepo.GAUGE_MIN_TABLET_SIZE_BYTES.setValue(minTabletSizeTmp);
            // avoid ArithmeticException: / by zero
            long avgTableSize = totalTableSize / Math.max(1, tableCount);
            MetricRepo.GAUGE_AVG_TABLE_SIZE_BYTES.setValue(avgTableSize);
            // avoid ArithmeticException: / by zero
            long avgPartitionSize = totalTableSize / Math.max(1, partitionCount);
            MetricRepo.GAUGE_AVG_PARTITION_SIZE_BYTES.setValue(avgPartitionSize);
            // avoid ArithmeticException: / by zero
            long avgTabletSize = totalTableSize / Math.max(1, tabletCount);
            MetricRepo.GAUGE_AVG_TABLET_SIZE_BYTES.setValue(avgTabletSize);

            LOG.info("OlapTable num=" + tableCount
                    + ", partition num=" + partitionCount + ", tablet num=" + tabletCount
                    + ", max tablet byte size=" + maxTabletSize.second
                    + "(tablet_id=" + maxTabletSize.first + ")"
                    + ", min tablet byte size=" + minTabletSizeTmp
                    + "(tablet_id=" + minTabletSize.first + ")"
                    + ", avg tablet byte size=" + avgTabletSize
                    + ", max partition byte size=" + maxPartitionSize.second
                    + "(partition_id=" + maxPartitionSize.first + ")"
                    + ", min partition byte size=" + minPartitionSizeTmp
                    + "(partition_id=" + minPartitionSize.first + ")"
                    + ", avg partition byte size=" + avgPartitionSize
                    + ", max table byte size=" + maxTableSize.second + "(table_id=" + maxTableSize.first + ")"
                    + ", min table byte size=" + minTableSizeTmp + "(table_id=" + minTableSize.first + ")"
                    + ", avg table byte size=" + avgTableSize);
        }

        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateTabletStat(GetTabletStatsResponse response) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (TabletStatsPB stat : response.getTabletStatsList()) {
            List<Replica> replicas = invertedIndex.getReplicasByTabletId(stat.getIdx().getTabletId());
            if (replicas == null || replicas.isEmpty() || replicas.get(0) == null) {
                continue;
            }
            Replica replica = replicas.get(0);
            replica.setDataSize(stat.getDataSize());
            replica.setRowsetCount(stat.getNumRowsets());
            replica.setSegmentCount(stat.getNumSegments());
            replica.setRowCount(stat.getNumRows());
            replica.setLocalInvertedIndexSize(stat.getIndexSize());
            replica.setLocalSegmentSize(stat.getSegmentSize());
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

    public List<OlapTable.Statistics> getCloudTableStats() {
        return this.cloudTableStatsList;
    }
}
