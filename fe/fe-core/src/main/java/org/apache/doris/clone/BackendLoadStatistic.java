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
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BackendLoadStatistic {
    private static final Logger LOG = LogManager.getLogger(BackendLoadStatistic.class);

    // comparator based on load score and storage medium, smaller load score first
    public static class BeStatComparator implements Comparator<BackendLoadStatistic> {
        private TStorageMedium medium;

        public BeStatComparator(TStorageMedium medium) {
            this.medium = medium;
        }

        @Override
        public int compare(BackendLoadStatistic o1, BackendLoadStatistic o2) {
            double score1 = o1.getLoadScore(medium);
            double score2 = o2.getLoadScore(medium);
            return Double.compare(score1, score2);
        }
    }

    public static class BeStatMixComparator implements Comparator<BackendLoadStatistic> {
        @Override
        public int compare(BackendLoadStatistic o1, BackendLoadStatistic o2) {
            Double score1 = o1.getMixLoadScore();
            Double score2 = o2.getMixLoadScore();
            return score1.compareTo(score2);
        }
    }

    public static class BePathLoadStatPair {
        private BackendLoadStatistic beLoadStatistic;
        private RootPathLoadStatistic pathLoadStatistic;

        BePathLoadStatPair(BackendLoadStatistic beLoadStatistic, RootPathLoadStatistic pathLoadStatistic) {
            this.beLoadStatistic = beLoadStatistic;
            this.pathLoadStatistic = pathLoadStatistic;
        }

        BackendLoadStatistic getBackendLoadStatistic() {
            return beLoadStatistic;
        }

        RootPathLoadStatistic getPathLoadStatistic() {
            return pathLoadStatistic;
        }

        @Override
        public String toString() {
            return "{ beId: " + beLoadStatistic.getBeId() + ", be score: "
                    + beLoadStatistic.getLoadScore(pathLoadStatistic.getStorageMedium())
                    + ", path: " + pathLoadStatistic.getPath()
                    + ", path used percent: " + pathLoadStatistic.getUsedPercent()
                    + " }";
        }
    }

    public static class BePathLoadStatPairComparator implements Comparator<BePathLoadStatPair> {
        private double avgBackendLoadScore;
        private double avgPathUsedPercent;

        BePathLoadStatPairComparator(List<BePathLoadStatPair> loadStats) {
            avgBackendLoadScore = 0.0;
            avgPathUsedPercent = 0.0;
            for (BePathLoadStatPair loadStat : loadStats) {
                RootPathLoadStatistic pathStat = loadStat.getPathLoadStatistic();
                avgBackendLoadScore += loadStat.getBackendLoadStatistic().getLoadScore(pathStat.getStorageMedium());
                avgPathUsedPercent += pathStat.getUsedPercent();
            }
            if (!loadStats.isEmpty()) {
                avgPathUsedPercent /= loadStats.size();
                avgBackendLoadScore /= loadStats.size();
            }
            if (avgBackendLoadScore == 0.0) {
                avgBackendLoadScore = 1.0;
            }
            if (avgPathUsedPercent == 0.0) {
                avgPathUsedPercent = 1.0;
            }
        }

        @Override
        public int compare(BePathLoadStatPair o1, BePathLoadStatPair o2) {
            return Double.compare(getCompareValue(o1), getCompareValue(o2));
        }

        private double getCompareValue(BePathLoadStatPair loadStat) {
            BackendLoadStatistic beStat = loadStat.getBackendLoadStatistic();
            RootPathLoadStatistic pathStat = loadStat.getPathLoadStatistic();
            return 0.5 * beStat.getLoadScore(pathStat.getStorageMedium()) / avgBackendLoadScore
                    + 0.5 * pathStat.getUsedPercent() / avgPathUsedPercent;
        }
    }

    public static final BeStatComparator HDD_COMPARATOR = new BeStatComparator(TStorageMedium.HDD);
    public static final BeStatComparator SSD_COMPARATOR = new BeStatComparator(TStorageMedium.SSD);
    public static final BeStatMixComparator MIX_COMPARATOR = new BeStatMixComparator();

    public enum Classification {
        INIT,
        LOW, // load score is Config.balance_load_score_threshold lower than average load score of cluster
        MID, // between LOW and HIGH
        HIGH // load score is Config.balance_load_score_threshold higher than average load score of cluster
    }

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private long beId;
    private boolean isAvailable;
    private Tag tag;

    public static class LoadScore {
        public double capacityCoefficient = 0.5;
        public double score = 0.0;

        public static final LoadScore DUMMY = new LoadScore();

        public double getReplicaNumCoefficient() {
            return 1.0 - capacityCoefficient;
        }
    }

    private Map<TStorageMedium, Long> totalCapacityMap = Maps.newHashMap();
    private Map<TStorageMedium, Long> totalUsedCapacityMap = Maps.newHashMap();
    private Map<TStorageMedium, Long> totalReplicaNumMap = Maps.newHashMap();
    private Map<TStorageMedium, LoadScore> loadScoreMap = Maps.newHashMap();
    private Map<TStorageMedium, Classification> clazzMap = Maps.newHashMap();
    private List<RootPathLoadStatistic> pathStatistics = Lists.newArrayList();

    public BackendLoadStatistic(long beId, Tag tag, SystemInfoService infoService,
            TabletInvertedIndex invertedIndex) {
        this.beId = beId;
        this.tag = tag;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public long getBeId() {
        return beId;
    }

    public Tag getTag() {
        return tag;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    public long getTotalCapacityB(TStorageMedium medium) {
        return totalCapacityMap.getOrDefault(medium, 0L);
    }

    public long getTotalUsedCapacityB(TStorageMedium medium) {
        return totalUsedCapacityMap.getOrDefault(medium, 0L);
    }

    public long getReplicaNum(TStorageMedium medium) {
        return totalReplicaNumMap.getOrDefault(medium, 0L);
    }

    public double getLoadScore(TStorageMedium medium) {
        if (loadScoreMap.containsKey(medium)) {
            return loadScoreMap.get(medium).score;
        }
        return 0.0;
    }

    public double getMixLoadScore() {
        int mediumCount = 0;
        double totalLoadScore = 0.0;
        for (TStorageMedium medium : TStorageMedium.values()) {
            if (hasMedium(medium)) {
                mediumCount++;
                totalLoadScore += getLoadScore(medium);
            }
        }
        return totalLoadScore / (mediumCount == 0 ? 1 : mediumCount);
    }

    public void setClazz(TStorageMedium medium, Classification clazz) {
        this.clazzMap.put(medium, clazz);
    }

    public Classification getClazz(TStorageMedium medium) {
        return clazzMap.getOrDefault(medium, Classification.INIT);
    }

    public void init() throws LoadBalanceException {
        Backend be = infoService.getBackend(beId);
        if (be == null) {
            throw new LoadBalanceException("backend " + beId + " does not exist");
        }

        isAvailable = be.isScheduleAvailable() && be.isLoadAvailable() && be.isQueryAvailable();

        ImmutableMap<String, DiskInfo> disks = be.getDisks();
        for (DiskInfo diskInfo : disks.values()) {
            TStorageMedium medium = diskInfo.getStorageMedium();
            if (diskInfo.getState() == DiskState.ONLINE) {
                // we only collect online disk's capacity
                totalCapacityMap.put(medium, totalCapacityMap.getOrDefault(medium, 0L)
                        + diskInfo.getTotalCapacityB());
                totalUsedCapacityMap.put(medium, totalUsedCapacityMap.getOrDefault(medium, 0L)
                        + (diskInfo.getTotalCapacityB() - diskInfo.getAvailableCapacityB()));
            }

            RootPathLoadStatistic pathStatistic = new RootPathLoadStatistic(beId, diskInfo.getRootPath(),
                    diskInfo.getPathHash(), diskInfo.getStorageMedium(),
                    diskInfo.getTotalCapacityB(), diskInfo.getDiskUsedCapacityB(), diskInfo.getState());
            pathStatistics.add(pathStatistic);
        }

        totalReplicaNumMap = invertedIndex.getReplicaNumByBeIdAndStorageMedium(beId);
        // This is very tricky. because the number of replica on specified medium we get
        // from getReplicaNumByBeIdAndStorageMedium() is defined by table properties,
        // but in fact there may not has SSD disk on this backend. So if we found that no SSD disk on this
        // backend, set the replica number to 0, otherwise, the average replica number on specified medium
        // will be incorrect.
        for (TStorageMedium medium : TStorageMedium.values()) {
            if (!hasMedium(medium)) {
                totalReplicaNumMap.put(medium, 0L);
            }
        }

        for (TStorageMedium storageMedium : TStorageMedium.values()) {
            classifyPathByLoad(storageMedium);
        }

        // sort the list
        Collections.sort(pathStatistics);
    }

    private void classifyPathByLoad(TStorageMedium medium) {
        long totalCapacity = 0;
        long totalUsedCapacity = 0;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getStorageMedium() == medium) {
                totalCapacity += pathStat.getCapacityB();
                totalUsedCapacity += pathStat.getUsedCapacityB();
            }
        }
        double avgUsedPercent = totalCapacity == 0 ? 0.0 : totalUsedCapacity / (double) totalCapacity;

        int lowCounter = 0;
        int midCounter = 0;
        int highCounter = 0;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getStorageMedium() != medium) {
                continue;
            }

            // ensure: HIGH - LOW >= 2.5% * 2 = 5%
            if (Math.abs(pathStat.getUsedPercent() - avgUsedPercent)
                    > Math.max(avgUsedPercent * Config.balance_load_score_threshold, 0.025)) {
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

        LOG.debug("classify path by load. be id: {} storage: {} avg used percent: {}. low/mid/high: {}/{}/{}",
                beId, medium, avgUsedPercent, lowCounter, midCounter, highCounter);
    }

    public void calcScore(Map<TStorageMedium, Double> avgClusterUsedCapacityPercentMap,
            Map<TStorageMedium, Double> maxUsedPercentDiffMap,
            Map<TStorageMedium, Double> avgClusterReplicaNumPerBackendMap) {

        for (TStorageMedium medium : TStorageMedium.values()) {
            LoadScore loadScore = calcScore(totalUsedCapacityMap.getOrDefault(medium, 0L),
                    totalCapacityMap.getOrDefault(medium, 1L),
                    totalReplicaNumMap.getOrDefault(medium, 0L),
                    avgClusterUsedCapacityPercentMap.getOrDefault(medium, 0.0),
                    maxUsedPercentDiffMap.getOrDefault(medium, 0.0),
                    avgClusterReplicaNumPerBackendMap.getOrDefault(medium, 0.0));

            loadScoreMap.put(medium, loadScore);

            LOG.debug("backend {}, medium: {}, capacity coefficient: {}, replica coefficient: {}, load score: {}",
                    beId, medium, loadScore.capacityCoefficient, loadScore.getReplicaNumCoefficient(),
                    loadScore.score);
        }
    }

    public static LoadScore calcScore(long beUsedCapacityB, long beTotalCapacityB, long beTotalReplicaNum,
            double avgClusterUsedCapacityPercent, double maxUsedPercentDiff, double avgClusterReplicaNumPerBackend) {

        double usedCapacityPercent = (beUsedCapacityB / (double) beTotalCapacityB);
        double capacityProportion = avgClusterUsedCapacityPercent <= 0 ? 0.0
                : usedCapacityPercent / avgClusterUsedCapacityPercent;
        double replicaNumProportion = avgClusterReplicaNumPerBackend <= 0 ? 0.0
                : beTotalReplicaNum / avgClusterReplicaNumPerBackend;

        LoadScore loadScore = new LoadScore();

        if (Config.backend_load_capacity_coeficient >= 0.0 && Config.backend_load_capacity_coeficient <= 1.0) {
            loadScore.capacityCoefficient = Config.backend_load_capacity_coeficient;
        } else if (!Config.be_rebalancer_fuzzy_test) {
            // Emphasize disk usage for calculating load score.
            //
            // thisBeCapacityCoefficient:
            //   If this be has a high used capacity percent, we should increase its load score.
            //   So we increase capacity coefficient with this be's used capacity percent.
            //
            // Also we emphasize the difference of disk usage between all the BEs.
            // For example, if the tablets have a big difference in data size.
            // Then for below two BEs, their load score maybe the same:
            // BE A:  disk usage = 60%,  replica number = 2000  (it contains the big tablets)
            // BE B:  disk usage = 30%,  replica number = 4000  (it contains the small tablets)
            //
            // But what we want is: firstly move some big tablets from A to B, after their disk usages are close,
            // then move some small tablets from B to A, finally both of their disk usages and replica number
            // are close.
            //
            // allBeCapacityCoefficient is used to achieve this, when the max difference between all BE's
            // disk usages >= 30%,  we set the capacity cofficient to 1.0 and avoid the affect of replica num.
            // After the disk usage difference decrease, then decrease the capacity cofficient.
            //
            double thisBeCapacityCoefficient = getSmoothCofficient(usedCapacityPercent, 0.5,
                    Config.capacity_used_percent_high_water);
            double allBeCapacityCoefficient = getSmoothCofficient(maxUsedPercentDiff, 0.05,
                    Config.used_capacity_percent_max_diff);

            loadScore.capacityCoefficient = Math.max(thisBeCapacityCoefficient, allBeCapacityCoefficient);
        }
        loadScore.score = capacityProportion * loadScore.capacityCoefficient
                + replicaNumProportion * loadScore.getReplicaNumCoefficient();

        return loadScore;
    }

    // the return cofficient:
    // 1. percent <= percentLowWatermark: cofficient = 0.5;
    // 2. percentLowWatermark < percent < percentHighWatermark: cofficient linear increase from 0.5 to 1.0;
    // 3. percent >= percentHighWatermark: cofficient = 1.0;
    public static double getSmoothCofficient(double percent, double percentLowWatermark,
            double percentHighWatermark) {
        final double lowCofficient = 0.5;
        final double highCofficient = 1.0;

        // low watermark and high watermark equal, then return 0.75
        if (Math.abs(percentHighWatermark - percentLowWatermark) < 1e-6) {
            return (lowCofficient + highCofficient) / 2;
        }
        if (percentLowWatermark > percentHighWatermark) {
            double tmp = percentLowWatermark;
            percentLowWatermark = percentHighWatermark;
            percentHighWatermark = tmp;
        }
        if (percent <= percentLowWatermark) {
            return lowCofficient;
        }
        if (percent >= percentHighWatermark) {
            return highCofficient;
        }

        return lowCofficient + (highCofficient - lowCofficient)
            * (percent - percentLowWatermark) / (percentHighWatermark - percentLowWatermark);
    }

    public BalanceStatus isFit(long tabletSize, TStorageMedium medium,
            List<RootPathLoadStatistic> result, boolean isSupplement) {
        BalanceStatus status = new BalanceStatus(ErrCode.COMMON_ERROR);
        // try choosing path from first to end (low usage to high usage)
        for (int i = 0; i < pathStatistics.size(); i++) {
            RootPathLoadStatistic pathStatistic = pathStatistics.get(i);
            // if this is a supplement task, ignore the storage medium
            if (!isSupplement && pathStatistic.getStorageMedium() != medium) {
                LOG.debug("backend {} path {}'s storage medium {} is not {} storage medium, actual: {}",
                        beId, pathStatistic.getPath(), pathStatistic.getStorageMedium(), medium);
                continue;
            }

            BalanceStatus bStatus = pathStatistic.isFit(tabletSize, isSupplement);
            if (!bStatus.ok()) {
                status.addErrMsgs(bStatus.getErrMsgs());
                continue;
            }

            result.add(pathStatistic);
        }

        return result.isEmpty() ? status : BalanceStatus.OK;
    }

    /**
     * Check whether the backend can be more balance if we migrate a tablet with size 'tabletSize' from
     * `srcPath` to 'destPath'
     * 1. recalculate the load score of src and dest path after migrate the tablet.
     * 2. if the summary of the diff between the new score and average score becomes smaller, we consider it
     *    as more balance.
     */
    public boolean isMoreBalanced(long srcPath, long destPath, long tabletId, long tabletSize,
                                  TStorageMedium medium) {
        long totalCapacity = 0;
        long totalUsedCapacity = 0;
        RootPathLoadStatistic srcPathStat = null;
        RootPathLoadStatistic destPathStat = null;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getStorageMedium() == medium) {
                totalCapacity += pathStat.getCapacityB();
                totalUsedCapacity += pathStat.getUsedCapacityB();
                if (pathStat.getPathHash() == srcPath) {
                    srcPathStat = pathStat;
                } else if (pathStat.getPathHash() == destPath) {
                    destPathStat = pathStat;
                }
            }
        }
        if (srcPathStat == null || destPathStat == null) {
            LOG.info("migrate {}(size: {}) from {} to {} failed, medium: {}, src or dest path stat does not exist.",
                    tabletId, tabletSize, srcPath, destPath, medium);
            return false;
        }
        double avgUsedPercent = totalCapacity == 0 ? 0.0 : totalUsedCapacity / (double) totalCapacity;
        double currentSrcPathScore = srcPathStat.getCapacityB() == 0
                ? 0.0 : srcPathStat.getUsedCapacityB() / (double) srcPathStat.getCapacityB();
        double currentDestPathScore = destPathStat.getCapacityB() == 0
                ? 0.0 : destPathStat.getUsedCapacityB() / (double) destPathStat.getCapacityB();

        double newSrcPathScore = srcPathStat.getCapacityB() == 0
                ? 0.0 : (srcPathStat.getUsedCapacityB() - tabletSize) / (double) srcPathStat.getCapacityB();
        double newDestPathScore = destPathStat.getCapacityB() == 0
                ? 0.0 : (destPathStat.getUsedCapacityB() + tabletSize) / (double) destPathStat.getCapacityB();

        double currentDiff = Math.abs(currentSrcPathScore - avgUsedPercent)
                + Math.abs(currentDestPathScore - avgUsedPercent);
        double newDiff = Math.abs(newSrcPathScore - avgUsedPercent) + Math.abs(newDestPathScore - avgUsedPercent);

        LOG.debug("after migrate {}(size: {}) from {} to {}, medium: {}, the load score changed."
                        + " src: {} -> {}, dest: {}->{}, average score: {}. current diff: {}, new diff: {},"
                        + " more balanced: {}",
                tabletId, tabletSize, srcPath, destPath, medium, currentSrcPathScore, newSrcPathScore,
                currentDestPathScore, newDestPathScore, avgUsedPercent, currentDiff, newDiff,
                (newDiff < currentDiff));

        return newDiff < currentDiff;
    }

    public boolean hasAvailDisk() {
        for (RootPathLoadStatistic rootPathLoadStatistic : pathStatistics) {
            if (rootPathLoadStatistic.getDiskState() == DiskState.ONLINE) {
                return true;
            }
        }
        return false;
    }

    /**
     * Classify the paths into 'low', 'mid' and 'high',
     * and skip offline path, and path with different storage medium
     */
    public void getPathStatisticByClass(
            Set<Long> low, Set<Long> mid, Set<Long> high, TStorageMedium storageMedium) {

        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getDiskState() == DiskState.OFFLINE
                    || (storageMedium != null && pathStat.getStorageMedium() != storageMedium)) {
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

    public void getPathStatisticByClass(List<RootPathLoadStatistic> low,
            List<RootPathLoadStatistic> mid, List<RootPathLoadStatistic> high, TStorageMedium storageMedium) {
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getDiskState() == DiskState.OFFLINE
                    || (storageMedium != null && pathStat.getStorageMedium() != storageMedium)) {
                continue;
            }

            if (pathStat.getClazz() == Classification.LOW) {
                low.add(pathStat);
            } else if (pathStat.getClazz() == Classification.HIGH) {
                high.add(pathStat);
            } else {
                mid.add(pathStat);
            }
        }

        LOG.debug("after adjust, backend {} path classification low/mid/high: {}/{}/{}",
                beId, low.size(), mid.size(), high.size());
    }

    public void incrPathsCopingSize(Map<Long, Long> pathsCopingSize) {
        boolean updated = false;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            Long copingSize = pathsCopingSize.get(pathStat.getPathHash());
            if (copingSize != null && copingSize > 0) {
                pathStat.incrCopingSizeB(copingSize);
                updated = true;
            }
        }
        if (updated) {
            Collections.sort(pathStatistics);
        }
    }

    public void incrPathCopingSize(long pathHash, long copingSize) {
        RootPathLoadStatistic pathStat = pathStatistics.stream().filter(
                p -> p.getPathHash() == pathHash).findFirst().orElse(null);
        if (pathStat != null) {
            pathStat.incrCopingSizeB(copingSize);
            Collections.sort(pathStatistics);
        }
    }

    public List<RootPathLoadStatistic> getPathStatistics() {
        return pathStatistics;
    }

    public long getAvailPathNum(TStorageMedium medium) {
        return pathStatistics.stream().filter(
                p -> p.getDiskState() == DiskState.ONLINE && p.getStorageMedium() == medium).count();
    }

    public boolean hasMedium(TStorageMedium medium) {
        for (RootPathLoadStatistic rootPathLoadStatistic : pathStatistics) {
            if (rootPathLoadStatistic.getStorageMedium() == medium) {
                return true;
            }
        }
        return false;
    }

    public String getBrief() {
        StringBuilder sb = new StringBuilder();
        sb.append(beId);
        for (TStorageMedium medium : TStorageMedium.values()) {
            sb.append(", ").append(medium).append(": replica: ").append(totalReplicaNumMap.get(medium));
            sb.append(" used: ").append(totalUsedCapacityMap.getOrDefault(medium, 0L));
            sb.append(" total: ").append(totalCapacityMap.getOrDefault(medium, 0L));
            sb.append(" score: ").append(loadScoreMap.getOrDefault(medium, LoadScore.DUMMY).score);
        }
        return sb.toString();
    }

    public List<String> getInfo(TStorageMedium medium) {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(beId));
        info.add(String.valueOf(isAvailable));
        long used = totalUsedCapacityMap.getOrDefault(medium, 0L);
        long total = totalCapacityMap.getOrDefault(medium, 0L);
        info.add(String.valueOf(used));
        info.add(String.valueOf(total));
        info.add(String.valueOf(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(used * 100
                / (double) total)));
        info.add(String.valueOf(totalReplicaNumMap.getOrDefault(medium, 0L)));
        LoadScore loadScore = loadScoreMap.getOrDefault(medium, new LoadScore());
        info.add(String.valueOf(loadScore.capacityCoefficient));
        info.add(String.valueOf(loadScore.getReplicaNumCoefficient()));
        info.add(String.valueOf(loadScore.score));
        info.add(clazzMap.getOrDefault(medium, Classification.INIT).name());
        return info;
    }

    public boolean canFitInColocate(long totalReplicaSize) {
        long afterUsedCap = totalUsedCapacityMap.values().stream().reduce(0L, Long::sum) + totalReplicaSize;
        long totalCap = totalCapacityMap.values().stream().reduce(0L, Long::sum);
        double afterRatio = (double) afterUsedCap / totalCap;
        return afterRatio < 0.9;
    }
}
