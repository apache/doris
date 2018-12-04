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
import org.apache.doris.clone.BackendLoadStatistic.Classification;
import org.apache.doris.clone.BackendLoadStatistic.LoadScore;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/*
 * Load statistics of a cluster
 */
public class ClusterLoadStatistic {
    private static final Logger LOG = LogManager.getLogger(ClusterLoadStatistic.class);

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private String clusterName;

    private long totalCapacityB = 1;
    private long totalUsedCapacityB = 0;
    private long totalReplicaNum = 0;
    private long backendNum = 0;

    private double avgUsedCapacityPercent = 0.0;
    private double avgReplicaNumPercent = 0.0;

    private double avgLoadScore = 0.0;

    private List<BackendLoadStatistic> beLoadStatistics = Lists.newArrayList();

    public ClusterLoadStatistic(String clusterName, Catalog catalog, SystemInfoService infoService,
            TabletInvertedIndex invertedIndex) {
        this.clusterName = clusterName;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public void init() {
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

        // classify all backends
        classifyBackendByLoad();

        // sort the list
        Collections.sort(beLoadStatistics);
    }

    /*
     * classify backends into 'low', 'mid' and 'high', by load
     */
    private void classifyBackendByLoad() {
        double totalLoadScore = 0.0;
        for (BackendLoadStatistic beStat : beLoadStatistics) {
            totalLoadScore += beStat.getLoadScore();
        }
        avgLoadScore = totalLoadScore / beLoadStatistics.size();

        int lowCounter = 0;
        int midCounter = 0;
        int highCounter = 0;
        for (BackendLoadStatistic beStat : beLoadStatistics) {
            if (Math.abs(beStat.getLoadScore() - avgLoadScore) / avgLoadScore > Config.balance_load_score_threshold) {
                if (beStat.getLoadScore() > avgLoadScore) {
                    beStat.setClazz(Classification.HIGH);
                    highCounter++;
                } else if (beStat.getLoadScore() < avgLoadScore) {
                    beStat.setClazz(Classification.LOW);
                    lowCounter++;
                }
            } else {
                beStat.setClazz(Classification.MID);
                midCounter++;
            }
        }

        LOG.info("classify backend by load. avg load score: {}. low/mid/high: {}/{}/{}",
                avgLoadScore, lowCounter, midCounter, highCounter);
    }

    /*
     * Check whether the cluster can be more balance if we migrate a tablet with size 'tabletSize' from
     * `srcBeId` to 'destBeId'
     * 1. re calculate the load core of src and dest be after migrate the tablet.
     * 2. if the summary of the diff between the new score and average score becomes smaller, we consider it
     *    as more balance.
     */
    public boolean isMoreBalanced(long srcBeId, long destBeId, long tabletId, long tabletSize) {
        double currentSrcBeScore;
        double currentDestBeScore;
        
        BackendLoadStatistic srcBeStat = null;
        Optional<BackendLoadStatistic> optSrcBeStat = beLoadStatistics.stream().filter(
                t -> t.getBeId() == srcBeId).findFirst();
        if (optSrcBeStat.isPresent()) {
            srcBeStat = optSrcBeStat.get();
        } else {
            return false;
        }
        
        BackendLoadStatistic destBeStat = null;
        Optional<BackendLoadStatistic> optDestBeStat = beLoadStatistics.stream().filter(
                t -> t.getBeId() == destBeId).findFirst();
        if (optDestBeStat.isPresent()) {
            destBeStat = optDestBeStat.get();
        } else {
            return false;
        }

        currentSrcBeScore = srcBeStat.getLoadScore();
        currentDestBeScore = destBeStat.getLoadScore();

        LoadScore newSrcBeScore = BackendLoadStatistic.calcSore(srcBeStat.getTotalUsedCapacityB() - tabletSize,
                srcBeStat.getTotalCapacityB(), srcBeStat.getReplicaNum() - 1,
                avgUsedCapacityPercent, avgReplicaNumPercent);

        LoadScore newDestBeScore = BackendLoadStatistic.calcSore(destBeStat.getTotalUsedCapacityB() + tabletSize,
                destBeStat.getTotalCapacityB(), destBeStat.getReplicaNum() + 1,
                avgUsedCapacityPercent, avgReplicaNumPercent);

        double currentDiff = Math.abs(currentSrcBeScore - avgLoadScore) + Math.abs(currentDestBeScore - avgLoadScore);
        double newDiff = Math.abs(newSrcBeScore.score - avgLoadScore) + Math.abs(newDestBeScore.score - avgLoadScore);

        LOG.debug("after migrate {}(size: {}) from {} to {}, the load score changed."
                + "src: {} -> {}, dest: {}->{}, average score: {}. current diff: {}, new diff: {}",
                tabletId, tabletSize, srcBeId, destBeId, currentSrcBeScore, newSrcBeScore.score,
                currentDestBeScore, newDestBeScore.score, avgLoadScore, currentDiff, newDiff);

        return newDiff < currentDiff;
    }

    public List<List<String>> getClusterStatistic() {
        List<List<String>> statistics = Lists.newArrayList();

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            List<String> beStat = beStatistic.getInfo();
            statistics.add(beStat);
        }

        return statistics;
    }

    public List<List<String>> getBackendStatistic(long beId) {
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

    public BackendLoadStatistic getBackendLoadStatistic(long beId) {
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            if (backendLoadStatistic.getBeId() == beId) {
                return backendLoadStatistic;
            }
        }
        return null;
    }

    /*
     * If cluster is balance, all Backends will be in 'mid', and 'high' and 'low' is empty
     * If both 'high' and 'low' has Backends, just return
     * If no 'high' Backends, 'mid' Backends will be treated as 'high'
     * If no 'low' Backends, 'mid' Backends will be treated as 'low'
     */
    public void getBackendStatisticByClass(
            List<BackendLoadStatistic> low,
            List<BackendLoadStatistic> mid,
            List<BackendLoadStatistic> high) {

        for (BackendLoadStatistic beStat : beLoadStatistics) {
            if (beStat.getClazz() == Classification.LOW) {
                low.add(beStat);
            } else if (beStat.getClazz() == Classification.HIGH) {
                high.add(beStat);
            } else {
                mid.add(beStat);
            }
        }

        if (low.isEmpty() && high.isEmpty()) {
            return;
        }

        // If there is no 'low' or 'high' backends, we treat 'mid' as 'low' or 'high'
        if (low.isEmpty()) {
            low.addAll(mid);
            mid.clear();
        } else if (high.isEmpty()) {
            high.addAll(mid);
            mid.clear();
        }

        Collections.sort(low);
        Collections.sort(mid);
        Collections.sort(high);

        LOG.debug("after adjust, cluster {} backend classification low/mid/high: {}/{}/{}",
                clusterName, low.size(), mid.size(), high.size());
    }

    public List<BackendLoadStatistic> getBeLoadStatistics() {
        return beLoadStatistics;
    }

    public String getBrief() {
        StringBuilder sb = new StringBuilder();
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            sb.append("    ").append(backendLoadStatistic.getBrief()).append("\n");
        }
        return sb.toString();
    }
}
