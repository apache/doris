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
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.BalanceStatus.ErrCode;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class BackendLoadStatistic implements Comparable<BackendLoadStatistic> {
    private static final Logger LOG = LogManager.getLogger(BackendLoadStatistic.class);

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private long beId;
    private String clusterName;

    private long totalCapacityB = 1; // init as 1 to avoid dividing zero error
    private long totalUsedCapacityB = 0;
    private long totalReplicaNum = 0;

    private double replicaNumCoefficient = 0.5;
    private double capacityCoefficient = 0.5;
    private double loadScore = 0.0;

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
        return loadScore;
    }

    public void init() throws LoadBalanceException {
        Backend be = infoService.getBackend(beId);
        if (be == null) {
            throw new LoadBalanceException("backend " + beId + " does not exist");
        }

        if (!be.isAlive() || be.isDecommissioned()) {
            throw new LoadBalanceException("backend " + beId + " is unavailable. alive: " + be.isAlive()
                    + ", decommission: " + be.isDecommissioned());
        }

        ImmutableMap<String, DiskInfo> disks = be.getDisks();
        for (DiskInfo diskInfo : disks.values()) {
            totalCapacityB += diskInfo.getTotalCapacityB();
            totalUsedCapacityB += diskInfo.getDataUsedCapacityB();
            RootPathLoadStatistic pathStatistic = new RootPathLoadStatistic(beId, diskInfo.getRootPath(),
                    diskInfo.getPathHash(), diskInfo.getStorageMedium(),
                    diskInfo.getTotalCapacityB(), diskInfo.getDataUsedCapacityB());
            pathStatistics.add(pathStatistic);
        }

        totalReplicaNum = invertedIndex.getTabletNumByBackendId(beId);

        // sort the list
        Collections.sort(pathStatistics);
    }

    public void calcScore(double avgClusterUsedCapacityPercent, double avgClusterReplicaNumPerBackend) {
        double usedCapacityPercent = (totalUsedCapacityB / (double) totalCapacityB);
        double capacityProportion = usedCapacityPercent / avgClusterUsedCapacityPercent;
        double replicaNumProportion = totalReplicaNum / avgClusterReplicaNumPerBackend;
        
        // If this backend's capacity used percent < 50%, set capacityCoefficient to 0.5.
        // Else if capacity used percent > 75%, set capacityCoefficient to 1.
        // Else, capacityCoefficient changed smoothly from 0.5 to 1 with used capacity increasing
        // Function: (2 * usedCapacityPercent - 0.5)
        capacityCoefficient = usedCapacityPercent < 0.5 ? 0.5
                : (usedCapacityPercent > Config.capacity_used_percent_high_water ? 1.0
                        : (2 * usedCapacityPercent - 0.5));
        replicaNumCoefficient = 1 - capacityCoefficient;

        loadScore = capacityProportion * capacityCoefficient + replicaNumProportion * replicaNumCoefficient;
        LOG.debug("backend {}, used capacity percent: {}, capacity proportion: {}, replica proportion: {},"
                + " capacity coefficient: {}, replica coefficient: {}, load score: {}",
                  beId, usedCapacityPercent, capacityProportion, replicaNumProportion,
                  capacityCoefficient, replicaNumCoefficient, loadScore);
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

    public List<RootPathLoadStatistic> getPathStatistics() {
        return pathStatistics;
    }

    public String getBrief() {
        StringBuilder sb = new StringBuilder();
        sb.append(beId).append(": replica: ").append(totalReplicaNum);
        sb.append(" used: ").append(totalUsedCapacityB);
        sb.append(" total: ").append(totalCapacityB);
        sb.append(" score: ").append(loadScore);
        return sb.toString();
    }

    // ascend order
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
