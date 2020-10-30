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
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*
 * Rebalancer is responsible for
 * 1. selectAlternativeTablets: selecting alternative tablets by one rebalance strategy,
 * and return them to tablet scheduler(maybe contains the concrete moves, or maybe not).
 * 2. createBalanceTask: given a tablet, try to create a clone task for this tablet.
 * 3. getSrcReplicaId: if the rebalance strategy wants to delete the src replica,
 * override this func.
 * NOTICE:
 * It may have a long interval between selectAlternativeTablets() & createBalanceTask(). So the concrete moves may be
 * invalid when we generating a clone task in createBalanceTask(), you should check the moves' validation.
 */
public abstract class Rebalancer {
    // When Rebalancer init, the statisticMap is usually empty. So it's no need to be an arg.
    // Only use updateLoadStatistic() to load stats.
    protected Map<String, ClusterLoadStatistic> statisticMap = new HashMap<>();
    protected TabletInvertedIndex invertedIndex;
    protected SystemInfoService infoService;

    public Rebalancer(SystemInfoService infoService, TabletInvertedIndex invertedIndex) {
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public List<TabletSchedCtx> selectAlternativeTablets() {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        for (Map.Entry<String, ClusterLoadStatistic> entry : statisticMap.entrySet()) {
            for (TStorageMedium medium : TStorageMedium.values()) {
                alternativeTablets.addAll(selectAlternativeTabletsForCluster(entry.getKey(),
                        entry.getValue(), medium));
            }
        }
        return alternativeTablets;
    }

    protected abstract List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            String clusterName, ClusterLoadStatistic clusterStat, TStorageMedium medium);

    public abstract void createBalanceTask(TabletSchedCtx tabletCtx, Map<Long, PathSlot> backendsWorkingSlots,
                                           AgentBatchTask batchTask) throws SchedException;

    public Long getSrcReplicaId(Long tabletId) {
        return -1L;
    }

    public void updateLoadStatistic(Map<String, ClusterLoadStatistic> statisticMap) {
        this.statisticMap = statisticMap;
    }
}
