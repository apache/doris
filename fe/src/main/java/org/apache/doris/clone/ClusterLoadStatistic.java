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

package org.apache.doris.clone;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

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
            BackendLoadStatistic beStatistic = new BackendLoadStatistic(backend.getId(),
                    backend.getOwnerClusterName(),
                    infoService, invertedIndex);
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

    public synchronized boolean moreBalanced(TabletInfo tablet, long srcBeId, long destBeId) {
        return false;
    }

    public synchronized List<List<String>> getCLusterStatistic() {
        List<List<String>> statistics = Lists.newArrayList();

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            List<String> beStat = Lists.newArrayList();
            beStat.add(String.valueOf(beStatistic.getBeId()));
            beStat.add(beStatistic.getClusterName());
            beStat.add(String.valueOf(beStatistic.isAvailable()));
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
                pathStat.add(String.valueOf(pathStatistic.getPathHash()));
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

    public List<BackendLoadStatistic> getBeLoadStatistics() {
        return beLoadStatistics;
    }

    public synchronized String getBrief() {
        StringBuilder sb = new StringBuilder();
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            sb.append("    ").append(backendLoadStatistic.getBrief()).append("\n");
        }
        return sb.toString();
    }
}
