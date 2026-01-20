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

import org.apache.doris.clone.PartitionRebalancer.TabletMove;
import org.apache.doris.common.Pair;
import org.apache.doris.cooldown.CooldownConf;
import org.apache.doris.master.PartitionInfoCollector.PartitionCollectInfo;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletMetaInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

/*
 * this class stores a inverted index
 * key is tablet id. value is the related ids of this tablet
 * Checkpoint thread is no need to modify this inverted index, because this inverted index will not be wrote
 * into images, all meta data are in catalog, and the inverted index will be rebuild when FE restart.
 */
public abstract class TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

    public static final TabletMeta NOT_EXIST_TABLET_META = new TabletMeta(NOT_EXIST_VALUE, NOT_EXIST_VALUE,
            NOT_EXIST_VALUE, NOT_EXIST_VALUE, NOT_EXIST_VALUE, TStorageMedium.HDD);

    private StampedLock lock = new StampedLock();

    // tablet id -> tablet meta
    protected Map<Long, TabletMeta> tabletMetaMap = Maps.newHashMap();

    public TabletInvertedIndex() {
    }

    protected long readLock() {
        return this.lock.readLock();
    }

    protected void readUnlock(long stamp) {
        this.lock.unlockRead(stamp);
    }

    protected long writeLock() {
        return this.lock.writeLock();
    }

    protected void writeUnlock(long stamp) {
        this.lock.unlockWrite(stamp);
    }

    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             Map<Long, Long> backendPartitionsVersion,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> tabletFoundInMeta,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap,
                             Map<Long, Long> partitionVersionSyncMap,
                             Map<Long, SetMultimap<Long, TPartitionVersionInfo>> transactionsToPublish,
                             SetMultimap<Long, Long> transactionsToClear,
                             ListMultimap<Long, Long> tabletRecoveryMap,
                             List<TTabletMetaInfo> tabletToUpdate,
                             List<CooldownConf> cooldownConfToPush,
                             List<CooldownConf> cooldownConfToUpdate) {
        throw new UnsupportedOperationException("tabletReport is not supported in TabletInvertedIndex");
    }

    public TabletMeta getTabletMeta(long tabletId) {
        long stamp = readLock();
        try {
            return tabletMetaMap.get(tabletId);
        } finally {
            readUnlock(stamp);
        }
    }

    public List<TabletMeta> getTabletMetaList(List<Long> tabletIdList) {
        List<TabletMeta> tabletMetaList = new ArrayList<>(tabletIdList.size());
        long stamp = readLock();
        try {
            for (Long tabletId : tabletIdList) {
                tabletMetaList.add(tabletMetaMap.getOrDefault(tabletId, NOT_EXIST_TABLET_META));
            }
            return tabletMetaList;
        } finally {
            readUnlock(stamp);
        }
    }

    public abstract List<Replica> getReplicas(Long tabletId);

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        long stamp = writeLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return;
            }
            tabletMetaMap.put(tabletId, tabletMeta);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add tablet: {}", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    public abstract void deleteTablet(long tabletId);

    public abstract void addReplica(long tabletId, Replica replica);

    public abstract void deleteReplica(long tabletId, long backendId);

    public abstract Replica getReplica(long tabletId, long backendId);

    public abstract List<Replica> getReplicasByTabletId(long tabletId);

    public Long getTabletSizeByBackendId(long backendId) {
        throw new UnsupportedOperationException(
                "getTabletSizeByBackendId is not supported in TabletInvertedIndex");
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        throw new UnsupportedOperationException(
                "getTabletIdsByBackendId is not supported in TabletInvertedIndex");
    }

    public List<Pair<Long, Long>> getTabletSizeByBackendIdAndStorageMedium(long backendId,
            TStorageMedium storageMedium) {
        throw new UnsupportedOperationException(
                "getTabletSizeByBackendIdAndStorageMedium is not supported in TabletInvertedIndex");
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId,
            TStorageMedium storageMedium) {
        return getTabletSizeByBackendIdAndStorageMedium(backendId, storageMedium).stream()
                .map(Pair::key).collect(Collectors.toList());
    }

    public int getTabletNumByBackendId(long backendId) {
        throw new UnsupportedOperationException(
                "getTabletNumByBackendId is not supported in TabletInvertedIndex");
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        throw new UnsupportedOperationException(
                "getReplicaNumByBeIdAndStorageMedium is not supported in TabletInvertedIndex");
    }

    protected abstract void innerClear();

    // just for test
    public void clear() {
        long stamp = writeLock();
        try {
            tabletMetaMap.clear();
            innerClear();
        } finally {
            writeUnlock(stamp);
        }
    }

    public void setPartitionCollectInfoMap(ImmutableMap<Long, PartitionCollectInfo> partitionCollectInfoMap) {
        throw new UnsupportedOperationException(
                "setPartitionCollectInfoMap is not supported in TabletInvertedIndex");
    }

    public Map<TStorageMedium, TreeMultimap<Long, PartitionBalanceInfo>> buildPartitionInfoBySkew(
            List<Long> availableBeIds, Map<Long, Pair<TabletMove, Long>> movesInProgress) {
        throw new UnsupportedOperationException("buildPartitionInfoBySkew is not supported in TabletInvertedIndex");
    }

    public static class PartitionBalanceInfo {
        public Long partitionId;
        public Long indexId;
        // Natural ordering
        public TreeMultimap<Long, Long> beByReplicaCount = TreeMultimap.create();

        public PartitionBalanceInfo(Long partitionId, Long indexId) {
            this.partitionId = partitionId;
            this.indexId = indexId;
        }

        public PartitionBalanceInfo(PartitionBalanceInfo info) {
            this.partitionId = info.partitionId;
            this.indexId = info.indexId;
            this.beByReplicaCount = TreeMultimap.create(info.beByReplicaCount);
        }

        @Override
        public String toString() {
            return "[partition=" + partitionId + ", index=" + indexId + ", replicaNum2BeId=" + beByReplicaCount + "]";
        }
    }

    // just for ut
    public Table<Long, Long, Replica> getReplicaMetaTable() {
        throw new UnsupportedOperationException("getReplicaMetaTable is not supported in TabletInvertedIndex");
    }

    // just for ut
    public Table<Long, Long, Replica> getBackingReplicaMetaTable() {
        throw new UnsupportedOperationException("getBackingReplicaMetaTable is not supported in TabletInvertedIndex");
    }

    // just for ut
    public Map<Long, TabletMeta> getTabletMetaMap() {
        long stamp = readLock();
        try {
            return new HashMap(tabletMetaMap);
        } finally {
            readUnlock(stamp);
        }
    }

}
