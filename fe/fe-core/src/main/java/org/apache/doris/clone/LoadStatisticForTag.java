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

import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.BackendLoadStatistic.Classification;
import org.apache.doris.clone.BackendLoadStatistic.LoadScore;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/*
 * Load statistics with specified tag
 */
public class LoadStatisticForTag {
    private static final Logger LOG = LogManager.getLogger(LoadStatisticForTag.class);

    private final SystemInfoService infoService;
    private final TabletInvertedIndex invertedIndex;

    private final Tag tag;

    private final Map<TStorageMedium, Long> totalCapacityMap = Maps.newHashMap();
    private final Map<TStorageMedium, Long> totalUsedCapacityMap = Maps.newHashMap();
    private final Map<TStorageMedium, Long> totalReplicaNumMap = Maps.newHashMap();
    private final Map<TStorageMedium, Double> maxUsedPercentDiffMap = Maps.newHashMap();
    private final Map<TStorageMedium, Double> avgUsedCapacityPercentMap = Maps.newHashMap();
    private final Map<TStorageMedium, Double> avgReplicaNumPercentMap = Maps.newHashMap();
    private final Map<TStorageMedium, Double> avgLoadScoreMap = Maps.newHashMap();
    // storage medium -> number of backend which has this kind of medium
    private final Map<TStorageMedium, Integer> backendNumMap = Maps.newHashMap();
    private final List<BackendLoadStatistic> beLoadStatistics = Lists.newArrayList();
    private final Map<TStorageMedium, TreeMultimap<Long, Long>> beByTotalReplicaCountMaps = Maps.newHashMap();
    private Map<TStorageMedium, TreeMultimap<Long, TabletInvertedIndex.PartitionBalanceInfo>> skewMaps
            = Maps.newHashMap();

    public LoadStatisticForTag(Tag tag, SystemInfoService infoService,
            TabletInvertedIndex invertedIndex) {
        this.tag = tag;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public Tag getTag() {
        return tag;
    }

    public void init() {
        List<Backend> backends = infoService.getBackendsByTag(tag);
        for (Backend backend : backends) {
            if (backend.isDecommissioned()) {
                // Skip the decommission backend.
                // Otherwise, before the decommission is done, these BE will always to treated
                // as LOW Backends. But the balancer will not handle BE in decommission state.
                // So balance will be blocked.
                continue;
            }
            // only mix node have tablet statistic
            if (!backend.isMixNode()) {
                continue;
            }
            BackendLoadStatistic beStatistic = new BackendLoadStatistic(backend.getId(),
                    backend.getLocationTag(), infoService, invertedIndex);
            try {
                beStatistic.init();
            } catch (LoadBalanceException e) {
                LOG.info(e.getMessage());
                continue;
            }

            for (TStorageMedium medium : TStorageMedium.values()) {
                totalCapacityMap.put(medium, totalCapacityMap.getOrDefault(medium, 0L)
                        + beStatistic.getTotalCapacityB(medium));
                totalUsedCapacityMap.put(medium, totalUsedCapacityMap.getOrDefault(medium, 0L)
                        + beStatistic.getTotalUsedCapacityB(medium));
                totalReplicaNumMap.put(medium, totalReplicaNumMap.getOrDefault(medium, 0L)
                        + beStatistic.getReplicaNum(medium));
                if (beStatistic.hasMedium(medium)) {
                    backendNumMap.put(medium, backendNumMap.getOrDefault(medium, 0) + 1);
                }
            }
            beLoadStatistics.add(beStatistic);
        }

        for (TStorageMedium medium : TStorageMedium.values()) {
            avgUsedCapacityPercentMap.put(medium, totalUsedCapacityMap.getOrDefault(medium, 0L)
                    / (double) totalCapacityMap.getOrDefault(medium, 1L));
            avgReplicaNumPercentMap.put(medium, totalReplicaNumMap.getOrDefault(medium, 0L)
                    / (double) backendNumMap.getOrDefault(medium, 1));

            double maxUsedPercent = -1.0;
            double minUsedPercent = -1.0;
            for (BackendLoadStatistic beStatistic : beLoadStatistics) {
                long beTotalCapacityB = beStatistic.getTotalCapacityB(medium);
                long beTotalUsedCapacityB = beStatistic.getTotalUsedCapacityB(medium);
                if (beTotalCapacityB > 0) {
                    double beUsedPercent = ((double) beTotalUsedCapacityB) / beTotalCapacityB;
                    if (maxUsedPercent < 0.0 || beUsedPercent > maxUsedPercent) {
                        maxUsedPercent = beUsedPercent;
                    }
                    if (minUsedPercent < 0.0 || beUsedPercent < minUsedPercent) {
                        minUsedPercent = beUsedPercent;
                    }
                }
            }
            maxUsedPercentDiffMap.put(medium, maxUsedPercent - minUsedPercent);
        }

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            beStatistic.calcScore(avgUsedCapacityPercentMap, maxUsedPercentDiffMap, avgReplicaNumPercentMap);
        }

        // classify all backends
        for (TStorageMedium medium : TStorageMedium.values()) {
            classifyBackendByLoad(medium);
        }

        // sort be stats by mix load score
        Collections.sort(beLoadStatistics, BackendLoadStatistic.MIX_COMPARATOR);

        // <medium -> Multimap<totalReplicaCount -> beId>>
        // Only count the available be
        for (TStorageMedium medium : TStorageMedium.values()) {
            TreeMultimap<Long, Long> beByTotalReplicaCount = TreeMultimap.create();
            beLoadStatistics.stream().filter(BackendLoadStatistic::isAvailable).forEach(beStat ->
                    beByTotalReplicaCount.put(beStat.getReplicaNum(medium), beStat.getBeId()));
            beByTotalReplicaCountMaps.put(medium, beByTotalReplicaCount);
        }

        if (Config.tablet_rebalancer_type.equalsIgnoreCase("Partition")) {
            // Actually the partition is [partition_id, index_id], aka pid.
            // Multimap<skew -> PartitionBalanceInfo>
            //                  PartitionBalanceInfo: <pid -> <partitionReplicaCount, beId>>
            // Only count available bes here, aligned with the beByTotalReplicaCountMaps.
            skewMaps = invertedIndex.buildPartitionInfoBySkew(beLoadStatistics.stream()
                    .filter(BackendLoadStatistic::isAvailable)
                    .map(BackendLoadStatistic::getBeId)
                    .collect(Collectors.toList()));
        }
    }

    /*
     * classify backends into 'low', 'mid' and 'high', by load
     */
    private void classifyBackendByLoad(TStorageMedium medium) {
        if (backendNumMap.getOrDefault(medium, 0) == 0) {
            return;
        }
        double totalLoadScore = 0.0;
        for (BackendLoadStatistic beStat : beLoadStatistics) {
            totalLoadScore += beStat.getLoadScore(medium);
        }
        double avgLoadScore = totalLoadScore / backendNumMap.get(medium);
        avgLoadScoreMap.put(medium, avgLoadScore);

        int lowCounter = 0;
        int midCounter = 0;
        int highCounter = 0;

        long debugHighBeId = DebugPointUtil.getDebugParamOrDefault("FE.HIGH_LOAD_BE_ID", -1L);
        if (debugHighBeId > 0) {
            final long targetBeId = debugHighBeId; // debugHighBeId can not put in lambda cause it's updated later
            if (!beLoadStatistics.stream().anyMatch(it -> it.getBeId() == targetBeId)) {
                debugHighBeId = -1L;
            }
        }

        for (BackendLoadStatistic beStat : beLoadStatistics) {
            if (!beStat.hasMedium(medium)) {
                continue;
            }

            Classification clazz = Classification.MID;
            if (debugHighBeId > 0) {
                if (beStat.getBeId() == debugHighBeId) {
                    clazz = Classification.HIGH;
                } else {
                    clazz = Classification.LOW;
                }
            } else if (Config.be_rebalancer_fuzzy_test) {
                if (beStat.getLoadScore(medium) > avgLoadScore) {
                    clazz = Classification.HIGH;
                } else if (beStat.getLoadScore(medium) < avgLoadScore) {
                    clazz = Classification.LOW;
                }
            } else {
                if (Math.abs(beStat.getLoadScore(medium) - avgLoadScore) / avgLoadScore
                        > Config.balance_load_score_threshold) {
                    if (beStat.getLoadScore(medium) > avgLoadScore) {
                        clazz = Classification.HIGH;
                    } else if (beStat.getLoadScore(medium) < avgLoadScore) {
                        clazz = Classification.LOW;
                    }
                }
            }

            beStat.setClazz(medium, clazz);
            switch (clazz) {
                case HIGH:
                    highCounter++;
                    break;
                case LOW:
                    lowCounter++;
                    break;
                case MID:
                    midCounter++;
                    break;
                default:
                    break;
            }
        }

        LOG.debug("classify backend by load. medium: {} avg load score: {}. low/mid/high: {}/{}/{}",
                medium, avgLoadScore, lowCounter, midCounter, highCounter);
    }

    private static void sortBeStats(List<BackendLoadStatistic> beStats, TStorageMedium medium) {
        if (medium == null) {
            Collections.sort(beStats, BackendLoadStatistic.MIX_COMPARATOR);
        } else if (medium == TStorageMedium.HDD) {
            Collections.sort(beStats, BackendLoadStatistic.HDD_COMPARATOR);
        } else {
            Collections.sort(beStats, BackendLoadStatistic.SSD_COMPARATOR);
        }
    }

    /*
     * Check whether the cluster can be more balance if we migrate a tablet with size 'tabletSize' from
     * `srcBeId` to 'destBeId'
     * 1. recalculate the load score of src and dest be after migrate the tablet.
     * 2. if the summary of the diff between the new score and average score becomes smaller, we consider it
     *    as more balance.
     */
    public boolean isMoreBalanced(long srcBeId, long destBeId, long tabletId, long tabletSize,
                                  TStorageMedium medium) {
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

        if (!srcBeStat.hasMedium(medium) || !destBeStat.hasMedium(medium)) {
            return false;
        }

        long debugHighBeId = DebugPointUtil.getDebugParamOrDefault("FE.HIGH_LOAD_BE_ID", -1L);
        if (srcBeStat.getBeId() == debugHighBeId) {
            return true;
        }

        currentSrcBeScore = srcBeStat.getLoadScore(medium);
        currentDestBeScore = destBeStat.getLoadScore(medium);

        LoadScore newSrcBeScore = BackendLoadStatistic.calcScore(srcBeStat.getTotalUsedCapacityB(medium) - tabletSize,
                srcBeStat.getTotalCapacityB(medium), srcBeStat.getReplicaNum(medium) - 1,
                avgUsedCapacityPercentMap.get(medium), maxUsedPercentDiffMap.get(medium),
                avgReplicaNumPercentMap.get(medium));

        LoadScore newDestBeScore = BackendLoadStatistic.calcScore(destBeStat.getTotalUsedCapacityB(medium) + tabletSize,
                destBeStat.getTotalCapacityB(medium), destBeStat.getReplicaNum(medium) + 1,
                avgUsedCapacityPercentMap.get(medium), maxUsedPercentDiffMap.get(medium),
                avgReplicaNumPercentMap.get(medium));

        double currentDiff = Math.abs(currentSrcBeScore - avgLoadScoreMap.get(medium))
                + Math.abs(currentDestBeScore - avgLoadScoreMap.get(medium));
        double newDiff = Math.abs(newSrcBeScore.score - avgLoadScoreMap.get(medium))
                + Math.abs(newDestBeScore.score - avgLoadScoreMap.get(medium));

        LOG.debug("after migrate {}(size: {}) from {} to {}, medium: {}, the load score changed."
                        + " src: {} -> {}, dest: {}->{}, average score: {}. current diff: {}, new diff: {},"
                        + " more balanced: {}",
                tabletId, tabletSize, srcBeId, destBeId, medium, currentSrcBeScore, newSrcBeScore.score,
                currentDestBeScore, newDestBeScore.score, avgLoadScoreMap.get(medium), currentDiff, newDiff,
                (newDiff < currentDiff));

        return newDiff < currentDiff;
    }

    public List<List<String>> getStatistic(TStorageMedium medium) {
        List<List<String>> statistics = Lists.newArrayList();

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            if (!beStatistic.hasMedium(medium)) {
                continue;
            }
            List<String> beStat = beStatistic.getInfo(medium);
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
                pathStat.add(pathStatistic.getStorageMedium().name());
                pathStat.add(String.valueOf(pathStatistic.getUsedCapacityB()));
                pathStat.add(String.valueOf(pathStatistic.getCapacityB()));
                pathStat.add(String.valueOf(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(
                        pathStatistic.getUsedCapacityB() * 100 / (double) pathStatistic.getCapacityB())));
                pathStat.add(pathStatistic.getClazz().name());
                pathStat.add(pathStatistic.getDiskState().name());
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

    public List<BackendLoadStatistic> getBackendLoadStatistics() {
        return beLoadStatistics;
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
            List<BackendLoadStatistic> high,
            TStorageMedium medium) {

        for (BackendLoadStatistic beStat : beLoadStatistics) {
            Classification clazz = beStat.getClazz(medium);
            switch (clazz) {
                case LOW:
                    low.add(beStat);
                    break;
                case MID:
                    mid.add(beStat);
                    break;
                case HIGH:
                    high.add(beStat);
                    break;
                default:
                    break;
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

        sortBeStats(low, medium);
        sortBeStats(mid, medium);
        sortBeStats(high, medium);

        LOG.debug("after adjust, backends' classification low/mid/high: {}/{}/{}, medium: {}",
                low.size(), mid.size(), high.size(), medium);
    }

    public List<BackendLoadStatistic> getSortedBeLoadStats(TStorageMedium medium) {
        if (medium != null) {
            List<BackendLoadStatistic> beStatsWithMedium = beLoadStatistics.stream().filter(
                    b -> b.hasMedium(medium)).collect(Collectors.toList());
            sortBeStats(beStatsWithMedium, medium);
            return beStatsWithMedium;
        } else {
            // be stats are already sorted by mix load score in init()
            return beLoadStatistics;
        }
    }

    public String getBrief() {
        StringBuilder sb = new StringBuilder();
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            sb.append("    ").append(backendLoadStatistic.getBrief()).append("\n");
        }
        return sb.toString();
    }

    public TreeMultimap<Long, Long> getBeByTotalReplicaMap(TStorageMedium medium) {
        return beByTotalReplicaCountMaps.get(medium);
    }

    public TreeMultimap<Long, TabletInvertedIndex.PartitionBalanceInfo> getSkewMap(TStorageMedium medium) {
        return skewMaps.get(medium);
    }
}
