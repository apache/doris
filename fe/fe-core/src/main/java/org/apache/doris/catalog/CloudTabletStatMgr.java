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
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsRequest;
import org.apache.doris.cloud.proto.Cloud.GetTabletStatsResponse;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TabletIndexPB;
import org.apache.doris.cloud.proto.Cloud.TabletStatsPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TSyncCloudTabletStatsRequest;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * CloudTabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Config.cloud_get_tablet_stats_version:
 * 1: Each FE will collect by itself.
 * 2: Master FE collects active tablet stats and pushes to followers and observers,
 *    and each FE will collect tablet stats by interval ladder.
 */
public class CloudTabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletStatMgr.class);

    private volatile long totalTableSize = 0;
    // keep Config.prom_output_table_metrics_limit tables with the largest data size, used for prometheus output
    private volatile List<OlapTable.Statistics> cloudTableStatsList = new ArrayList<>();

    private static final ExecutorService GET_TABLET_STATS_THREAD_POOL = Executors.newFixedThreadPool(
            Config.max_get_tablet_stat_task_threads_num,
            new ThreadFactoryBuilder().setNameFormat("get-tablet-stats-%d").setDaemon(true).build());
    // Master: send tablet stats to followers and observers
    // Follower and observer: receive tablet stats from master
    private static final ExecutorService SYNC_TABLET_STATS_THREAD_POOL = Executors.newFixedThreadPool(
            Config.cloud_sync_tablet_stats_task_threads_num,
            new ThreadFactoryBuilder().setNameFormat("sync-tablet-stats-%d").setDaemon(true).build());
    private Set<Long> activeTablets = ConcurrentHashMap.newKeySet();

    /**
     * Interval ladder in milliseconds: 1m, 5m, 10m, 30m, 2h, 6h, 12h, 3d, infinite.
     * Tablets with changing stats stay at lower intervals; stable tablets move to higher intervals.
     */
    private static final long[] DEFAULT_INTERVAL_LADDER_MS = {
            TimeUnit.MINUTES.toMillis(1),    // 1 minute
            TimeUnit.MINUTES.toMillis(5),    // 5 minutes
            TimeUnit.MINUTES.toMillis(10),   // 10 minutes
            TimeUnit.MINUTES.toMillis(30),   // 30 minutes
            TimeUnit.HOURS.toMillis(2),      // 2 hours
            TimeUnit.HOURS.toMillis(6),      // 6 hours
            TimeUnit.HOURS.toMillis(12),     // 12 hours
            TimeUnit.DAYS.toMillis(3),       // 3 days
            Long.MAX_VALUE                   // infinite (never auto-fetch)
    };

    public CloudTabletStatMgr() {
        super("cloud tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (cloudTableStatsList.isEmpty()) {
            // use tablet stats loaded from image to update table stats when fe start
            // avoid that the table stats is empty for a long time since getAllTabletStats may consume a long time
            LOG.info("cloud tablet stat is empty, will update stat info of all tables");
            updateStatInfo(Env.getCurrentInternalCatalog().getDbIds());
        }

        int version = Config.cloud_get_tablet_stats_version;
        LOG.info("cloud tablet stat begin with version: {}", version);

        // version1: get all tablet stats
        if (version == 1) {
            this.activeTablets.clear();
            List<Long> dbIds = getAllTabletStats(null);
            updateStatInfo(dbIds);
            return;
        }

        // version2: get stats for active tablets
        Set<Long> copiedTablets = new HashSet<>(activeTablets);
        activeTablets.removeAll(copiedTablets);
        getActiveTabletStats(copiedTablets);

        // get stats by interval
        List<Long> dbIds = getAllTabletStats(cloudTablet -> {
            if (copiedTablets.contains(cloudTablet.getId())) {
                return false;
            }
            List<Replica> replicas = Env.getCurrentInvertedIndex().getReplicas(cloudTablet.getId());
            if (replicas == null || replicas.isEmpty()) {
                return false;
            }
            CloudReplica cloudReplica = (CloudReplica) replicas.get(0);
            int index = cloudReplica.getStatsIntervalIndex();
            if (index >= DEFAULT_INTERVAL_LADDER_MS.length) {
                LOG.warn("get tablet stats interval index out of range, tabletId: {}, index: {}",
                        cloudTablet.getId(), index);
                index = DEFAULT_INTERVAL_LADDER_MS.length - 1;
            }
            long interval = DEFAULT_INTERVAL_LADDER_MS[index];
            if (interval == Long.MAX_VALUE
                    || System.currentTimeMillis() - cloudReplica.getLastGetTabletStatsTime() < interval) {
                return false;
            }
            return true;
        });
        updateStatInfo(dbIds);
    }

    private List<Long> getAllTabletStats(Function<CloudTablet, Boolean> filter) {
        long getStatsTabletNum = 0;
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
                            for (Tablet tablet : index.getTablets()) {
                                if (filter != null && !filter.apply((CloudTablet) tablet)) {
                                    continue;
                                }
                                getStatsTabletNum++;
                                TabletIndexPB.Builder tabletBuilder = TabletIndexPB.newBuilder();
                                tabletBuilder.setDbId(dbId);
                                tabletBuilder.setTableId(table.getId());
                                tabletBuilder.setIndexId(index.getId());
                                tabletBuilder.setPartitionId(partition.getId());
                                tabletBuilder.setTabletId(tablet.getId());
                                builder.addTabletIdx(tabletBuilder);

                                if (builder.getTabletIdxCount() >= Config.get_tablet_stat_batch_size) {
                                    futures.add(submitGetTabletStatsTask(builder.build(), filter == null));
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
            futures.add(submitGetTabletStatsTask(builder.build(), filter == null));
        }

        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get tablet stats tasks to complete", e);
        }

        LOG.info("finished to get tablet stats. getStatsTabletNum: {}, cost: {} ms",
                getStatsTabletNum, (System.currentTimeMillis() - start));
        return dbIds;
    }

    private void getActiveTabletStats(Set<Long> tablets) {
        List<Long> tabletIds = new ArrayList<>(tablets);
        Collections.sort(tabletIds);
        List<TabletMeta> tabletMetas = Env.getCurrentInvertedIndex().getTabletMetaList(tabletIds);
        long start = System.currentTimeMillis();
        List<Future<Void>> futures = new ArrayList<>();
        GetTabletStatsRequest.Builder builder =
                GetTabletStatsRequest.newBuilder().setRequestIp(FrontendOptions.getLocalHostAddressCached());
        long activeTabletNum = 0;
        for (int i = 0; i < tabletIds.size(); i++) {
            TabletIndexPB tabletIndexPB = getTabletIndexPB(tabletIds.get(i), tabletMetas.get(i));
            if (tabletIndexPB == null) {
                continue;
            }
            activeTabletNum++;
            builder.addTabletIdx(tabletIndexPB);
            if (builder.getTabletIdxCount() >= Config.get_tablet_stat_batch_size) {
                futures.add(submitGetTabletStatsTask(builder.build(), true));
                builder = GetTabletStatsRequest.newBuilder()
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
            }
        }
        if (builder.getTabletIdxCount() > 0) {
            futures.add(submitGetTabletStatsTask(builder.build(), true));
        }

        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error waiting for get tablet stats tasks to complete", e);
        }
        LOG.info("finished to get {} active tablets stats, cost {}ms", activeTabletNum,
                System.currentTimeMillis() - start);
    }

    private TabletIndexPB getTabletIndexPB(long tabletId, TabletMeta tabletMeta) {
        if (tabletMeta == null || tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
            return null;
        }
        return TabletIndexPB.newBuilder().setDbId(tabletMeta.getDbId()).setTableId(tabletMeta.getTableId())
                .setIndexId(tabletMeta.getIndexId()).setPartitionId(tabletMeta.getPartitionId()).setTabletId(tabletId)
                .build();
    }

    private Future<Void> submitGetTabletStatsTask(GetTabletStatsRequest req, boolean activeUpdate) {
        return GET_TABLET_STATS_THREAD_POOL.submit(() -> {
            GetTabletStatsResponse resp;
            try {
                resp = getTabletStatsFromMs(req);
            } catch (RpcException e) {
                LOG.warn("get tablet stats exception:", e);
                return null;
            }
            if (resp.getStatus().getCode() != MetaServiceCode.OK) {
                LOG.warn("get tablet stats return failed.");
                return null;
            }
            updateTabletStat(resp, activeUpdate);
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
        long autoPartitionNearLimitCount = 0L;
        long dynamicPartitionNearLimitCount = 0L;
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

                if (!table.readLockIfExist()) {
                    continue;
                }
                OlapTable.Statistics tableStats;
                try {
                    List<Partition> allPartitions = olapTable.getAllPartitions();
                    // Use getPartitionNum() (excludes temp partitions) for limit check,
                    // consistent with how partition limits are enforced elsewhere.
                    int nonTempPartitionNum = olapTable.getPartitionNum();
                    partitionCount += allPartitions.size();
                    // Check if this table's partition count is near the limit (>80%)
                    if (olapTable.getPartitionInfo().enableAutomaticPartition()) {
                        int limit = Config.max_auto_partition_num;
                        if (nonTempPartitionNum > limit * 8L / 10) {
                            autoPartitionNearLimitCount++;
                        }
                    }
                    if (olapTable.dynamicPartitionExists()
                            && olapTable.getTableProperty().getDynamicPartitionProperty().getEnable()) {
                        int limit = Config.max_dynamic_partition_num;
                        if (nonTempPartitionNum > limit * 8L / 10) {
                            dynamicPartitionNearLimitCount++;
                        }
                    }
                    for (Partition partition : allPartitions) {
                        long partitionDataSize = 0L;
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            List<Tablet> tablets = index.getTablets();
                            tabletCount += tablets.size();
                            for (Tablet tablet : tablets) {
                                long tabletDataSize = 0L;

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
                            tableReplicaCount, tableRowCount, 0L, 0L,
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
        filterTopTableStatsByDataSize(newCloudTableStatsList);
        this.totalTableSize = totalTableSize;

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

            MetricRepo.GAUGE_AUTO_PARTITION_NEAR_LIMIT.setValue(autoPartitionNearLimitCount);
            MetricRepo.GAUGE_DYNAMIC_PARTITION_NEAR_LIMIT.setValue(dynamicPartitionNearLimitCount);

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

    private void updateTabletStat(GetTabletStatsResponse response, boolean activeUpdate) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        for (TabletStatsPB stat : response.getTabletStatsList()) {
            List<Replica> replicas = invertedIndex.getReplicasByTabletId(stat.getIdx().getTabletId());
            if (replicas == null || replicas.isEmpty() || replicas.get(0) == null) {
                continue;
            }
            Replica replica = replicas.get(0);
            boolean statsChanged = replica.getDataSize() != stat.getDataSize()
                    || replica.getRowCount() != stat.getNumRows()
                    || replica.getLocalInvertedIndexSize() != stat.getIndexSize()
                    || replica.getLocalSegmentSize() != stat.getSegmentSize();
            replica.setDataSize(stat.getDataSize());
            replica.setRowCount(stat.getNumRows());
            replica.setLocalInvertedIndexSize(stat.getIndexSize());
            replica.setLocalSegmentSize(stat.getSegmentSize());

            CloudReplica cloudReplica = (CloudReplica) replica;
            cloudReplica.setLastGetTabletStatsTime(System.currentTimeMillis());
            int statsIntervalIndex = cloudReplica.getStatsIntervalIndex();
            if (activeUpdate || statsChanged) {
                statsIntervalIndex = 0;
                if (!activeUpdate && statsChanged && LOG.isDebugEnabled()) {
                    LOG.debug("tablet stats changed, reset interval index to 0, dbId: {}, tableId: {}, "
                                    + "indexId: {}, partitionId: {}, tabletId: {}, dataSize: {}, rowCount: {}, "
                                    + "rowsetCount: {}, segmentCount: {}, indexSize: {}, segmentSize: {}. lastIdx: {}",
                            stat.getIdx().getDbId(), stat.getIdx().getTableId(), stat.getIdx().getIndexId(),
                            stat.getIdx().getPartitionId(), stat.getIdx().getTabletId(), stat.getDataSize(),
                            stat.getNumRows(), stat.getNumRowsets(), stat.getNumSegments(), stat.getIndexSize(),
                            stat.getSegmentSize(), cloudReplica.getStatsIntervalIndex());
                }
            } else {
                statsIntervalIndex = Math.min(statsIntervalIndex + 1, DEFAULT_INTERVAL_LADDER_MS.length - 1);
            }
            cloudReplica.setStatsIntervalIndex(statsIntervalIndex);
        }
        // push tablet stats to other fes
        if (Config.cloud_get_tablet_stats_version == 2 && activeUpdate && Env.getCurrentEnv().isMaster()) {
            pushTabletStats(response);
        }
    }

    private GetTabletStatsResponse getTabletStatsFromMs(GetTabletStatsRequest request)
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

    public long getTotalTableSize() {
        return this.totalTableSize;
    }

    private void filterTopTableStatsByDataSize(List<OlapTable.Statistics> newCloudTableStatsList) {
        int limit = Config.prom_output_table_metrics_limit;
        if (limit <= 0 || newCloudTableStatsList.size() <= limit) {
            this.cloudTableStatsList = newCloudTableStatsList;
            return;
        }
        // only copy elements if number of tables > prom_output_table_metrics_limit
        PriorityQueue<OlapTable.Statistics> topStats = new PriorityQueue<>(limit,
                Comparator.comparingLong(OlapTable.Statistics::getDataSize));
        for (OlapTable.Statistics stats : newCloudTableStatsList) {
            if (topStats.size() < limit) {
                topStats.offer(stats);
            } else if (!topStats.isEmpty() && stats.getDataSize() > topStats.peek().getDataSize()) {
                topStats.poll();
                topStats.offer(stats);
            }
        }
        this.cloudTableStatsList = new ArrayList<>(topStats);
    }

    public void addActiveTablets(List<Long> tabletIds) {
        if (Config.cloud_get_tablet_stats_version == 1 || tabletIds == null || tabletIds.isEmpty()) {
            return;
        }
        List<String> ignoreTablets = Arrays.asList(DebugPointUtil.getDebugParamOrDefault(
                "FE.CloudTabletStatMgr.addActiveTablets.ignore.tablets", "").split(","));
        if (!ignoreTablets.isEmpty() && tabletIds.stream().anyMatch(id -> ignoreTablets.contains(String.valueOf(id)))) {
            LOG.info("ignore adding active tablets: {}, debug param: {}", tabletIds, ignoreTablets);
            return;
        }
        activeTablets.addAll(tabletIds);
    }

    // master FE send update tablet stats rpc to other FEs
    private void pushTabletStats(GetTabletStatsResponse response) {
        List<Frontend> frontends = getFrontends();
        if (frontends == null || frontends.isEmpty()) {
            return;
        }
        TSyncCloudTabletStatsRequest request = new TSyncCloudTabletStatsRequest();
        request.setTabletStatsPb(ByteBuffer.wrap(response.toByteArray()));
        for (Frontend fe : frontends) {
            SYNC_TABLET_STATS_THREAD_POOL.submit(() -> {
                try {
                    pushTabletStatsToFe(request, fe);
                } catch (Exception e) {
                    LOG.warn("push tablet stats to frontend {}:{} error", fe.getHost(), fe.getRpcPort(), e);
                }
            });
        }
    }

    private void pushTabletStatsToFe(TSyncCloudTabletStatsRequest request, Frontend fe) {
        FrontendService.Client client = null;
        TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
        boolean ok = false;
        try {
            client = ClientPool.frontendStatsPool.borrowObject(addr);
            TStatus status = client.syncCloudTabletStats(request);
            ok = true;
            if (status.getStatusCode() != TStatusCode.OK) {
                LOG.warn("failed to push cloud tablet stats to frontend {}:{}, err: {}", fe.getHost(),
                        fe.getRpcPort(), status.getErrorMsgs());
            }
        } catch (Exception e) {
            LOG.warn("failed to push update cloud tablet stats to frontend {}:{}", fe.getHost(), fe.getRpcPort(), e);
        } finally {
            if (ok) {
                ClientPool.frontendStatsPool.returnObject(addr, client);
            } else {
                ClientPool.frontendStatsPool.invalidateObject(addr, client);
            }
        }
    }

    // follower and observer FE receive sync tablet stats rpc from master FE
    public void syncTabletStats(GetTabletStatsResponse response) {
        if (Config.cloud_get_tablet_stats_version == 1 || response.getTabletStatsList().isEmpty()) {
            return;
        }
        SYNC_TABLET_STATS_THREAD_POOL.submit(() -> {
            updateTabletStat(response, true);
        });
    }

    private List<Frontend> getFrontends() {
        if (!Env.getCurrentEnv().isMaster()) {
            return Collections.emptyList();
        }
        HostInfo selfNode = Env.getCurrentEnv().getSelfNode();
        return Env.getCurrentEnv().getFrontends(null).stream()
                .filter(fe -> fe.isAlive() && !(fe.getHost().equals(selfNode.getHost())
                        && fe.getEditLogPort() == selfNode.getPort())).collect(
                        Collectors.toList());
    }

    public static CloudTabletStatMgr getInstance() {
        return (CloudTabletStatMgr) Env.getCurrentEnv().getTabletStatMgr();
    }
}
