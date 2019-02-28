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

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.BalanceStatus.ErrCode;
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
import java.util.Set;

public class BackendLoadStatistic implements Comparable<BackendLoadStatistic> {
    private static final Logger LOG = LogManager.getLogger(BackendLoadStatistic.class);

    public enum Classification {
        INIT,
        LOW, // load score is Config.balance_load_score_threshold lower than average load score of cluster
        MID, // between LOW and HIGH
        HIGH // load score is Config.balance_load_score_threshold higher than average load score of cluster
    }

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private long beId;
    private String clusterName;

    private boolean isAvailable;

    private long totalCapacityB = 1; // init as 1 to avoid dividing zero error
    private long totalUsedCapacityB = 0;
    private long totalReplicaNum = 0;

    public static class LoadScore {
        public double replicaNumCoefficient = 0.5;
        public double capacityCoefficient = 0.5;
        public double score = 0.0;
    }

    private LoadScore loadScore;

    private Classification clazz = Classification.INIT;

    private List<RootPathLoadStatistic> pathStatistics = Lists.newArrayList();

    public BackendLoadStatistic(long beId, String clusterName, SystemInfoService infoService,
            TabletInvertedIndex invertedIndex) {
        this.beId = beId;
        this.clusterName = clusterName;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public long getBeId() {
        return beId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public long getTotalUsedCapacityB() {
        return totalUsedCapacityB;
    }

    public long getReplicaNum() {
        return totalReplicaNum;
    }

    public double getLoadScore() {
        return loadScore.score;
    }

    public void setClazz(Classification clazz) {
        this.clazz = clazz;
    }

    public Classification getClazz() {
        return clazz;
    }

    public void init() throws LoadBalanceException {
        Backend be = infoService.getBackend(beId);
        if (be == null) {
            throw new LoadBalanceException("backend " + beId + " does not exist");
        }

        if (!be.isAvailable()) {
            isAvailable = false;
        } else {
            isAvailable = true;
        }

        ImmutableMap<String, DiskInfo> disks = be.getDisks();
        for (DiskInfo diskInfo : disks.values()) {
            if (diskInfo.getState() == DiskState.ONLINE) {
                // we only collect online disk's capacity
                totalCapacityB += diskInfo.getTotalCapacityB();
                totalUsedCapacityB += diskInfo.getDataUsedCapacityB();
            }

            RootPathLoadStatistic pathStatistic = new RootPathLoadStatistic(beId, diskInfo.getRootPath(),
                    diskInfo.getPathHash(), diskInfo.getStorageMedium(),
                    diskInfo.getTotalCapacityB(), diskInfo.getDataUsedCapacityB(), diskInfo.getState());
            pathStatistics.add(pathStatistic);
        }

        totalReplicaNum = invertedIndex.getTabletNumByBackendId(beId);

        classifyPathByLoad();

        // sort the list
        Collections.sort(pathStatistics);
    }

    private void classifyPathByLoad() {
        long totalCapacity = 0;
        long totalUsedCapacity = 0;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            totalCapacity += pathStat.getCapacityB();
            totalUsedCapacity += pathStat.getUsedCapacityB();
        }
        double avgUsedPercent = totalCapacity == 0 ? 0.0 : totalUsedCapacity / (double) totalCapacity;

        int lowCounter = 0;
        int midCounter = 0;
        int highCounter = 0;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (Math.abs(pathStat.getUsedPercent() - avgUsedPercent)
                    / avgUsedPercent > Config.balance_load_score_threshold) {
                if (pathStat.getUsedPercent() > avgUsedPercent) {
                    pathStat.setClazz(Classification.HIGH);
                    highCounter++;
                } else if (pathStat.getUsedPercent() < avgUsedPercent) {
                    pathStat.setClazz(Classification.LOW);
                    lowCounter++;
                }
            } else {
                pathStat.setClazz(Classification.MID);
                midCounter++;
            }
        }

        LOG.info("classify path by load. avg used percent: {}. low/mid/high: {}/{}/{}",
                avgUsedPercent, lowCounter, midCounter, highCounter);
    }

    public void calcScore(double avgClusterUsedCapacityPercent, double avgClusterReplicaNumPerBackend) {
        loadScore = calcSore(totalUsedCapacityB, totalCapacityB, totalReplicaNum, avgClusterUsedCapacityPercent,
                avgClusterReplicaNumPerBackend);
        
        LOG.debug("backend {}, capacity coefficient: {}, replica coefficient: {}, load score: {}",
                beId, loadScore.capacityCoefficient, loadScore.replicaNumCoefficient, loadScore.score);
    }

    public static LoadScore calcSore(long beUsedCapacityB, long beTotalCapacity, long beTotalReplicaNum,
            double avgClusterUsedCapacityPercent, double avgClusterReplicaNumPerBackend) {
        
        double usedCapacityPercent = (beUsedCapacityB / (double) beTotalCapacity);
        double capacityProportion = avgClusterUsedCapacityPercent <= 0 ? 0.0
                : usedCapacityPercent / avgClusterUsedCapacityPercent;
        double replicaNumProportion = avgClusterReplicaNumPerBackend <= 0 ? 0.0
                : beTotalReplicaNum / avgClusterReplicaNumPerBackend;
        
        LoadScore loadScore = new LoadScore();

        // If this backend's capacity used percent < 50%, set capacityCoefficient to 0.5.
        // Else if capacity used percent > 75%, set capacityCoefficient to 1.
        // Else, capacityCoefficient changed smoothly from 0.5 to 1 with used capacity increasing
        // Function: (2 * usedCapacityPercent - 0.5)
        loadScore.capacityCoefficient = usedCapacityPercent < 0.5 ? 0.5
                : (usedCapacityPercent > Config.capacity_used_percent_high_water ? 1.0
                        : (2 * usedCapacityPercent - 0.5));
        loadScore.replicaNumCoefficient = 1 - loadScore.capacityCoefficient;
        loadScore.score = capacityProportion * loadScore.capacityCoefficient
                + replicaNumProportion * loadScore.replicaNumCoefficient;
        
        return loadScore;
    }

    public BalanceStatus isFit(long tabletSize, List<RootPathLoadStatistic> result, boolean isSupplement) {
        BalanceStatus status = new BalanceStatus(ErrCode.COMMON_ERROR);
        // try choosing path from first to end
        for (int i = 0; i < pathStatistics.size(); i++) {
            RootPathLoadStatistic pathStatistic = pathStatistics.get(i);
            BalanceStatus bStatus = pathStatistic.isFit(tabletSize, isSupplement);
            if (!bStatus.ok()) {
                status.addErrMsgs(bStatus.getErrMsgs());
                continue;
            }

            result.add(pathStatistic);
            return BalanceStatus.OK;
        }
        return status;
    }

    public boolean hasAvailDisk() {
        for (RootPathLoadStatistic rootPathLoadStatistic : pathStatistics) {
            if (rootPathLoadStatistic.getDiskState() == DiskState.ONLINE) {
                return true;
            }
        }
        return false;
    }

    /*
     * Classify the paths into 'low', 'mid' and 'high',
     * and skip offline path
     */
    public void getPathStatisticByClass(
            Set<Long> low,
            Set<Long> mid,
            Set<Long> high) {

        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getDiskState() == DiskState.OFFLINE) {
                continue;
            }

            if (pathStat.getClazz() == Classification.LOW) {
                low.add(pathStat.getPathHash());
            } else if (pathStat.getClazz() == Classification.HIGH) {
                high.add(pathStat.getPathHash());
            } else {
                mid.add(pathStat.getPathHash());
            }
        }

        LOG.debug("after adjust, backend {} path classification low/mid/high: {}/{}/{}",
                beId, low.size(), mid.size(), high.size());
    }

    public List<RootPathLoadStatistic> getPathStatistics() {
        return pathStatistics;
    }

    public long getAvailPathNum() {
        return pathStatistics.stream().filter(p -> p.getDiskState() == DiskState.ONLINE).count();
    }

    public String getBrief() {
        StringBuilder sb = new StringBuilder();
        sb.append(beId).append(": replica: ").append(totalReplicaNum);
        sb.append(" used: ").append(totalUsedCapacityB);
        sb.append(" total: ").append(totalCapacityB);
        sb.append(" score: ").append(loadScore);
        return sb.toString();
    }

    public List<String> getInfo() {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(beId));
        info.add(clusterName);
        info.add(String.valueOf(isAvailable));
        info.add(String.valueOf(totalUsedCapacityB));
        info.add(String.valueOf(totalCapacityB));
        info.add(String.valueOf(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalUsedCapacityB * 100
                / (double) totalCapacityB)));
        info.add(String.valueOf(totalReplicaNum));
        info.add(String.valueOf(loadScore.capacityCoefficient));
        info.add(String.valueOf(loadScore.replicaNumCoefficient));
        info.add(String.valueOf(loadScore.score));
        info.add(clazz.name());
        return info;
    }

    // ascend order by load score
    @Override
    public int compareTo(BackendLoadStatistic o) {
        if (getLoadScore() > o.getLoadScore()) {
            return 1;
        } else if (getLoadScore() == o.getLoadScore()) {
            return 0;
        } else {
            return -1;
        }
    }
}
