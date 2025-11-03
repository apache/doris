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

import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * Rebalancer is responsible for
 * 1. selectAlternativeTablets: selecting alternative tablets by one rebalance strategy,
 * and return them to tablet scheduler(maybe contains the concrete moves, or maybe not).
 * 2. createBalanceTask: given a tablet, try to create a clone task for this tablet.
 * 3. getToDeleteReplicaId: if the rebalance strategy wants to delete the specified replica,
 * override this func to let TabletScheduler know in handling redundant replica.
 * NOTICE:
 * 1. Adding the selected tablets by TabletScheduler may not succeed at all.
 *  And the move may be failed in some other places. So the thing you need to know is,
 *  Rebalancer cannot know when the move is failed.
 * 2. If you want to make sure the move is succeed, you can assume that it's succeed when getToDeleteReplicaId called.
 */
public abstract class Rebalancer {
    private static final Logger LOG = LogManager.getLogger(Rebalancer.class);
    // When Rebalancer init, the statisticMap is usually empty. So it's no need to be an arg.
    // Only use updateLoadStatistic() to load stats.
    protected Map<Tag, LoadStatisticForTag> statisticMap = Maps.newHashMap();
    protected Map<Long, PathSlot> backendsWorkingSlots;
    protected TabletInvertedIndex invertedIndex;
    protected SystemInfoService infoService;
    // be id -> end time of prio
    protected Map<Long, Long> prioBackends = Maps.newConcurrentMap();

    protected boolean canBalanceColocateTable = false;
    private Set<Long> alterTableIds = Sets.newHashSet();

    // tag -> (medium, timestamp)
    private Table<Tag, TStorageMedium, Long> lastPickTimeTable = HashBasedTable.create();

    // for ut
    public Table<Tag, TStorageMedium, Long> getLastPickTimeTable() {
        return lastPickTimeTable;
    }

    public Rebalancer(SystemInfoService infoService, TabletInvertedIndex invertedIndex,
            Map<Long, PathSlot> backendsWorkingSlots) {
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
        this.backendsWorkingSlots = backendsWorkingSlots;
    }

    public List<TabletSchedCtx> selectAlternativeTablets() {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        for (Map.Entry<Tag, LoadStatisticForTag> entry : statisticMap.entrySet()) {
            for (TStorageMedium medium : TStorageMedium.values()) {
                List<TabletSchedCtx> candidates =
                        selectAlternativeTabletsForCluster(entry.getValue(), medium);
                alternativeTablets.addAll(candidates);
                if (!candidates.isEmpty()) {
                    lastPickTimeTable.put(entry.getKey(), medium, System.currentTimeMillis());
                }
            }
        }
        return alternativeTablets;
    }

    // The returned TabletSchedCtx should have the tablet id at least. {srcReplica, destBe} can be complete here or
    // later(when createBalanceTask called).
    protected abstract List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            LoadStatisticForTag clusterStat, TStorageMedium medium);

    // 5mins
    protected boolean unPickOverLongTime(Tag tag, TStorageMedium medium) {
        Long lastPickTime = lastPickTimeTable.get(tag, medium);
        Long now = System.currentTimeMillis();
        if (LOG.isDebugEnabled()) {
            LOG.debug("tag={}, medium={}, lastPickTime={}, now={}", tag, medium, lastPickTime, now);
        }
        return lastPickTime == null || now - lastPickTime >= Config.be_rebalancer_idle_seconds * 1000L;
    }

    protected boolean canBalanceTablet(TabletMeta tabletMeta) {
        // Clone ut mocked env, but CatalogRecycleBin is not mockable (it extends from Thread)
        // so in clone ut recycleBin need to set to null.
        ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
        CatalogRecycleBin recycleBin = null;
        if (!FeConstants.runningUnitTest) {
            recycleBin = Env.getCurrentRecycleBin();
        }
        return tabletMeta != null
                && !alterTableIds.contains(tabletMeta.getTableId())
                && (canBalanceColocateTable || !colocateTableIndex.isColocateTable(tabletMeta.getTableId()))
                && (recycleBin == null || !recycleBin.isRecyclePartition(tabletMeta.getDbId(),
                        tabletMeta.getTableId(), tabletMeta.getPartitionId()));
    }

    public AgentTask createBalanceTask(TabletSchedCtx tabletCtx)
            throws SchedException {
        completeSchedCtx(tabletCtx);
        if (tabletCtx.getBalanceType() == TabletSchedCtx.BalanceType.BE_BALANCE) {
            return tabletCtx.createCloneReplicaAndTask();
        } else {
            return tabletCtx.createStorageMediaMigrationTask();
        }
    }

    // Before createCloneReplicaAndTask, we need to complete the TabletSchedCtx.
    // 1. If you generate {tabletId, srcReplica, destBe} in selectAlternativeTablets(), it may be invalid at
    // this point(it may have a long interval between selectAlternativeTablets & createBalanceTask).
    // You should check the moves' validation.
    // 2. If you want to generate {srcReplica, destBe} here, just do it.
    // 3. You should check the path slots of src & dest.
    protected abstract void completeSchedCtx(TabletSchedCtx tabletCtx)
            throws SchedException;

    public Long getToDeleteReplicaId(TabletSchedCtx tabletCtx) {
        return -1L;
    }

    public void invalidateToDeleteReplicaId(TabletSchedCtx tabletCtx) {
    }

    public void onTabletFailed(TabletSchedCtx tabletCtx) {
    }

    public void updateLoadStatistic(Map<Tag, LoadStatisticForTag> statisticMap) {
        this.statisticMap = statisticMap;
    }

    public void updateAlterTableIds(Set<Long> alterTableIds) {
        this.alterTableIds = alterTableIds;
    }

    public void addPrioBackends(List<Backend> backends, long timeoutS) {
        long currentTimeMillis = System.currentTimeMillis();
        for (Backend backend : backends) {
            prioBackends.put(backend.getId(), currentTimeMillis + timeoutS);
        }
    }

    public void removePrioBackends(List<Backend> backends) {
        for (Backend backend : backends) {
            prioBackends.remove(backend.getId());
        }
    }

    public boolean hasPrioBackends() {
        return !prioBackends.isEmpty();
    }
}
