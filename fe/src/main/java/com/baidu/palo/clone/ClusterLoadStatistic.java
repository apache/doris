// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.clone;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.TabletMeta;
import com.baidu.palo.clone.BalanceStatus.ErrCode;
import com.baidu.palo.common.util.DebugUtil;
import com.baidu.palo.system.Backend;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/*
 * save all load statistic of backends.
 * Statistics will be re-calculated at a fix interval.
 */
public class ClusterLoadStatistic {
    private static final Logger LOG = LogManager.getLogger(ClusterLoadStatistic.class);

    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private long totalCapacityB = 1;
    private long totalUsedCapacityB = 0;
    private long totalReplicaNum = 0;
    private long backendNum = 0;

    private double avgUsedCapacityPercent = 0.0;
    private double avgReplicaNumPercent = 0.0;

    private List<BackendLoadStatistic> beLoadStatistics = Lists.newArrayList();

    public ClusterLoadStatistic(Catalog catalog, SystemInfoService infoService, TabletInvertedIndex invertedIndex) {
        this.catalog = catalog;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public synchronized void init(String clusterName) {
        ImmutableMap<Long, Backend> backends = infoService.getBackendsInCluster(clusterName);
        for (Backend backend : backends.values()) {
            BackendLoadStatistic beStatistic = new BackendLoadStatistic(backend.getId(), infoService, invertedIndex);
            try {
                beStatistic.init();
            } catch (LoadBalanceException e) {
                LOG.info(e.getMessage());
                continue;
            }

            totalCapacityB += beStatistic.getTotalCapacityB();
            totalUsedCapacityB += beStatistic.getTotalUsedCapacityB();
            totalReplicaNum += beStatistic.getReplicaNum();
            backendNum++;
            beLoadStatistics.add(beStatistic);
        }
        
        avgUsedCapacityPercent = totalUsedCapacityB / (double) totalCapacityB;
        avgReplicaNumPercent = totalReplicaNum / (double) backendNum;

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            beStatistic.calcScore(avgUsedCapacityPercent, avgReplicaNumPercent);
        }

        // sort the list
        Collections.sort(beLoadStatistics);
    }

    public synchronized List<List<String>> getCLusterStatistic() {
        List<List<String>> statistics = Lists.newArrayList();

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            List<String> beStat = Lists.newArrayList();
            beStat.add(String.valueOf(beStatistic.getBeId()));
            beStat.add(String.valueOf(beStatistic.getTotalUsedCapacityB()));
            beStat.add(String.valueOf(beStatistic.getTotalCapacityB()));
            beStat.add(String.valueOf(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(beStatistic.getTotalUsedCapacityB() * 100
                    / (double) beStatistic.getTotalCapacityB())));
            beStat.add(String.valueOf(beStatistic.getReplicaNum()));
            beStat.add(String.valueOf(beStatistic.getLoadScore()));
            statistics.add(beStat);
        }

        return statistics;
    }

    public synchronized List<List<String>> getBackendStatistic(long beId) {
        List<List<String>> statistics = Lists.newArrayList();

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            if (beStatistic.getBeId() != beId) {
                continue;
            }

            for (RootPathLoadStatistic pathStatistic : beStatistic.getPathStatistics()) {
                List<String> pathStat = Lists.newArrayList();
                pathStat.add(pathStatistic.getPath());
                pathStat.add(String.valueOf(pathStatistic.getUsedCapacityB()));
                pathStat.add(String.valueOf(pathStatistic.getCapacityB()));
                pathStat.add(String.valueOf(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(pathStatistic.getUsedCapacityB() * 100
                        / (double) pathStatistic.getCapacityB())));
                statistics.add(pathStat);
            }
            break;
        }
        return statistics;
    }

    public synchronized BackendLoadStatistic getBackendLoadStatistic(long beId) {
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            if (backendLoadStatistic.getBeId() == beId) {
                return backendLoadStatistic;
            }
        }
        return null;
    }

    /*
     * Try to choose 2 backend root paths as source and destination for the specified tablet to recovery.
     */
    public BalanceStatus chooseSrcAndDestBackendForTablet(long tabletId,
            List<RootPathLoadStatistic> resultRootPath) {

        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            return new BalanceStatus(ErrCode.COMMON_ERROR, "tablet " + tabletId + " does not exist");
        }
        
        Database db = catalog.getDb(tabletMeta.getDbId());
        if (db == null) {
            return new BalanceStatus(ErrCode.COMMON_ERROR, "db " + tabletMeta.getDbId() + " does not exist");
        }
        db.writeLock();
        try {
            Table tbl = db.getTable(tabletMeta.getTableId());
            if (tbl == null) {
                return new BalanceStatus(ErrCode.COMMON_ERROR, "table " + tabletMeta.getTableId() + " does not exist");
            }
            OlapTable olapTbl = (OlapTable) tbl;

            Partition partition = olapTbl.getPartition(tabletMeta.getPartitionId());
            if (partition == null) {
                return new BalanceStatus(ErrCode.COMMON_ERROR,
                        "part " + tabletMeta.getPartitionId() + " does not exist");
            }

            short expectedRepNum = olapTbl.getPartitionInfo().getReplicationNum(tabletMeta.getPartitionId());
            MaterializedIndex idx = partition.getIndex(tabletMeta.getIndexId());
            if (idx == null) {
                return new BalanceStatus(ErrCode.COMMON_ERROR, "idx " + tabletMeta.getIndexId() + " does not exist");
            }

            Tablet tablet = idx.getTablet(tabletId);
            List<Replica> replicas = tablet.getReplicas();

            // TODO(cmy)

        } finally {
            db.writeUnlock();
        }

        List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
        if (replicas == null) {
            return new BalanceStatus(ErrCode.COMMON_ERROR, "tablet " + tabletId + " does not exist");
        }
        List<Long> excludedBackends = replicas.stream().map(n -> n.getBackendId()).collect(Collectors.toList());
        
        // use max replica size as this tablet's size
        long tabletSize = replicas.stream().max((a, b) -> a.getDataSize() > b.getDataSize() ? 1
                : 0).get().getDataSize();
        
        // try choosing backend from first to end
        BalanceStatus status = new BalanceStatus(ErrCode.COMMON_ERROR, "");
        for (int i = 0; i < beLoadStatistics.size(); i++) {
            BackendLoadStatistic beStatistic = beLoadStatistics.get(i);
            if (excludedBackends.contains(beStatistic.getBeId())) {
                continue;
            }

            resultRootPath.clear();
            BalanceStatus bStatus = beStatistic.isFit(tabletSize, resultRootPath, true /* is recovery */);
            if (!bStatus.ok()) {
                status.addErrMsgs(bStatus.getErrMsgs());
                continue;
            }

            break;
        }

        if (resultRootPath.isEmpty()) {
            return status;
        }

        Preconditions.checkState(resultRootPath.size() == 1);
        return BalanceStatus.OK;
    }
}
