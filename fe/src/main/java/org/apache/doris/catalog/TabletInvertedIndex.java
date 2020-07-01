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

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/*
 * this class stores a inverted index
 * key is tablet id. value is the related ids of this tablet
 * Checkpoint thread is no need to modify this inverted index, because this inverted index will not be wrote
 * into images, all meta data are in catalog, and the inverted index will be rebuild when FE restart.
 */
public class TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

    public static final TabletMeta NOT_EXIST_TABLET_META = new TabletMeta(NOT_EXIST_VALUE, NOT_EXIST_VALUE,
            NOT_EXIST_VALUE, NOT_EXIST_VALUE, NOT_EXIST_VALUE, TStorageMedium.HDD);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // tablet id -> tablet meta
    private Map<Long, TabletMeta> tabletMetaMap = Maps.newHashMap();
    
    // replica id -> tablet id
    private Map<Long, Long> replicaToTabletMap = Maps.newHashMap();
    
    /*
     *  we use this to save memory.
     *  we do not need create TabletMeta instance for each tablet,
     *  cause tablets in one (Partition-MaterializedIndex) has same parent info
     *      (dbId, tableId, partitionId, indexId, schemaHash)
     *  we use 'tabletMetaTable' to do the update things
     *      (eg. update schema hash in TabletMeta)
     *  partition id -> (index id -> tablet meta)
     */
    private Table<Long, Long, TabletMeta> tabletMetaTable = HashBasedTable.create();
    
    // tablet id -> (backend id -> replica)
    private Table<Long, Long, Replica> replicaMetaTable = HashBasedTable.create();
    // backing replica table, for visiting backend replicas faster.
    // backend id -> (tablet id -> replica)
    private Table<Long, Long, Replica> backingReplicaMetaTable = HashBasedTable.create();

    public TabletInvertedIndex() {
    }

    private void readLock() {
        this.lock.readLock().lock();
    }

    private void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> foundTabletsWithValidSchema,
                             Map<Long, TTabletInfo> foundTabletsWithInvalidSchema,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap, 
                             Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish,
                             ListMultimap<Long, Long> transactionsToClear,
                             ListMultimap<Long, Long> tabletRecoveryMap,
                             Set<Pair<Long, Integer>> tabletWithoutPartitionId) {

        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetPartition_id() || tabletInfo.getPartition_id() < 1) {
                    tabletWithoutPartitionId.add(new Pair<>(tabletInfo.getTablet_id(), tabletInfo.getSchema_hash()));
                }
            }
        }

        readLock();
        long start = System.currentTimeMillis();
        try {
            LOG.info("begin to do tablet diff with backend[{}]. num: {}", backendId, backendTablets.size());
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                // traverse replicas in meta with this backend
                for (Map.Entry<Long, Replica> entry : replicaMetaWithBackend.entrySet()) {
                    long tabletId = entry.getKey();
                    Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
                    TabletMeta tabletMeta = tabletMetaMap.get(tabletId);

                    if (backendTablets.containsKey(tabletId)) {
                        TTablet backendTablet = backendTablets.get(tabletId);
                        Replica replica = entry.getValue();
                        for (TTabletInfo backendTabletInfo : backendTablet.getTablet_infos()) {
                            if (tabletMeta.containsSchemaHash(backendTabletInfo.getSchema_hash())) {
                                foundTabletsWithValidSchema.add(tabletId);
                                // 1. (intersection)
                                if (needSync(replica, backendTabletInfo)) {
                                    // need sync
                                    tabletSyncMap.put(tabletMeta.getDbId(), tabletId);
                                }
                                
                                // check and set path
                                // path info of replica is only saved in Master FE
                                if (backendTabletInfo.isSetPath_hash() &&
                                        replica.getPathHash() != backendTabletInfo.getPath_hash()) {
                                    replica.setPathHash(backendTabletInfo.getPath_hash());
                                }

                                if (backendTabletInfo.isSetSchema_hash() && replica.getState() == ReplicaState.NORMAL
                                        && replica.getSchemaHash() != backendTabletInfo.getSchema_hash()) {
                                    // update the schema hash only when replica is normal
                                    replica.setSchemaHash(backendTabletInfo.getSchema_hash());
                                }

                                if (needRecover(replica, tabletMeta.getOldSchemaHash(), backendTabletInfo)) {
                                    LOG.warn("replica {} of tablet {} on backend {} need recovery. "
                                            + "replica in FE: {}, report version {}-{}, report schema hash: {},"
                                            + " is bad: {}, is version missing: {}",
                                            replica.getId(), tabletId, backendId, replica,
                                            backendTabletInfo.getVersion(),
                                            backendTabletInfo.getVersion_hash(),
                                            backendTabletInfo.getSchema_hash(),
                                            backendTabletInfo.isSetUsed() ? backendTabletInfo.isUsed() : "unknown",
                                            backendTabletInfo.isSetVersion_miss() ? backendTabletInfo.isVersion_miss() : "unset");
                                    tabletRecoveryMap.put(tabletMeta.getDbId(), tabletId);
                                }

                                // check if need migration
                                long partitionId = tabletMeta.getPartitionId();
                                TStorageMedium storageMedium = storageMediumMap.get(partitionId);
                                if (storageMedium != null && backendTabletInfo.isSetStorage_medium()) {
                                    if (storageMedium != backendTabletInfo.getStorage_medium()) {
                                        tabletMigrationMap.put(storageMedium, tabletId);
                                    }
                                    if (storageMedium != tabletMeta.getStorageMedium()) {
                                        tabletMeta.setStorageMedium(storageMedium);
                                    }
                                }
                                // check if should clear transactions
                                if (backendTabletInfo.isSetTransaction_ids()) {
                                    List<Long> transactionIds = backendTabletInfo.getTransaction_ids();
                                    GlobalTransactionMgr transactionMgr = Catalog.getCurrentGlobalTransactionMgr();
                                    for (Long transactionId : transactionIds) {
                                        TransactionState transactionState = transactionMgr.getTransactionState(tabletMeta.getDbId(), transactionId);
                                        if (transactionState == null || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
                                            transactionsToClear.put(transactionId, tabletMeta.getPartitionId());
                                            LOG.debug("transaction id [{}] is not valid any more, " 
                                                    + "clear it from backend [{}]", transactionId, backendId);
                                        } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                                            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfo(tabletMeta.getTableId());
                                            PartitionCommitInfo partitionCommitInfo = tableCommitInfo.getPartitionCommitInfo(partitionId);
                                            if (partitionCommitInfo == null) {
                                                /*
                                                 * This may happen as follows:
                                                 * 1. txn is committed on BE, and report commit info to FE
                                                 * 2. FE received report and begin to assemble partitionCommitInfos.
                                                 * 3. At the same time, some of partitions have been dropped, so partitionCommitInfos does not contain these partitions.
                                                 * 4. So we will not able to get partitionCommitInfo here.
                                                 * 
                                                 * Just print a log to observe
                                                 */
                                                LOG.info("failed to find partition commit info. table: {}, partition: {}, tablet: {}, txn id: {}",
                                                        tabletMeta.getTableId(), partitionId, tabletId, transactionState.getTransactionId());
                                            } else {
                                                TPartitionVersionInfo versionInfo = new TPartitionVersionInfo(tabletMeta.getPartitionId(), 
                                                        partitionCommitInfo.getVersion(),
                                                        partitionCommitInfo.getVersionHash());
                                                ListMultimap<Long, TPartitionVersionInfo> map = transactionsToPublish.get(transactionState.getDbId());
                                                if (map == null) {
                                                    map = ArrayListMultimap.create();
                                                    transactionsToPublish.put(transactionState.getDbId(), map);
                                                }
                                                map.put(transactionId, versionInfo);
                                            }
                                        }
                                    }
                                } // end for txn id

                                // update replicas's version count
                                // no need to write log, and no need to get db lock.
                                if (backendTabletInfo.isSetVersion_count()) {
                                    replica.setVersionCount(backendTabletInfo.getVersion_count());
                                }
                            } else {
                                // tablet with invalid schemahash
                                foundTabletsWithInvalidSchema.put(tabletId, backendTabletInfo);
                            } // end for be tablet info
                        }
                    }  else {
                        // 2. (meta - be)
                        // may need delete from meta
                        LOG.debug("backend[{}] does not report tablet[{}-{}]", backendId, tabletId, tabletMeta);
                        tabletDeleteFromMeta.put(tabletMeta.getDbId(), tabletId);
                    }
                } // end for replicaMetaWithBackend
            }
        } finally {
            readUnlock();
        }

        long end = System.currentTimeMillis();
        LOG.info("finished to do tablet diff with backend[{}]. sync: {}. metaDel: {}. foundValid: {}. foundInvalid: {}."
                         + " migration: {}. found invalid transactions {}. found republish transactions {} " 
                         + " cost: {} ms", backendId, tabletSyncMap.size(),
                 tabletDeleteFromMeta.size(), foundTabletsWithValidSchema.size(), foundTabletsWithInvalidSchema.size(),
                 tabletMigrationMap.size(), transactionsToClear.size(), transactionsToPublish.size(), (end - start));
    }

    public Long getTabletIdByReplica(long replicaId) {
        readLock();
        try {
            return replicaToTabletMap.get(replicaId);
        } finally {
            readUnlock();
        }
    }

    public TabletMeta getTabletMeta(long tabletId) {
        readLock();
        try {
            return tabletMetaMap.get(tabletId);
        } finally {
            readUnlock();
        }
    }

    public List<TabletMeta> getTabletMetaList(List<Long> tabletIdList) {
        List<TabletMeta> tabletMetaList = new ArrayList<>(tabletIdList.size());
        readLock();
        try {
            for (Long tabletId : tabletIdList) {
                tabletMetaList.add(tabletMetaMap.getOrDefault(tabletId, NOT_EXIST_TABLET_META));
            }
            return tabletMetaList;
        } finally {
            readUnlock();
        }
    }

    private boolean needSync(Replica replicaInFe, TTabletInfo backendTabletInfo) {
        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            // tablet is bad, do not sync
            // it will be handled in needRecovery()
            return false;
        }

        if (replicaInFe.getState() == ReplicaState.ALTER) {
            // ignore the replica is ALTER state. its version will be taken care by load process and alter table process
            return false;
        }

        long versionInFe = replicaInFe.getVersion();
        long versionHashInFe = replicaInFe.getVersionHash();
        
        if (backendTabletInfo.getVersion() > versionInFe) {
            // backend replica's version is larger or newer than replica in FE, sync it.
            return true;
        } else if (versionInFe == backendTabletInfo.getVersion() && versionHashInFe == backendTabletInfo.getVersion_hash()
                && replicaInFe.isBad()) {
            // backend replica's version is equal to replica in FE, but replica in FE is bad, while backend replica is good, sync it
            return true;
        }
        
        return false;
    }
    
    /**
     * Be will set `used' to false for bad replicas and `version_miss' to true for replicas with hole
     * in their version chain. In either case, those replicas need to be fixed by TabletScheduler.
     */
    private boolean needRecover(Replica replicaInFe, int schemaHashInFe, TTabletInfo backendTabletInfo) {
        if (replicaInFe.getState() != ReplicaState.NORMAL) {
            // only normal replica need recover
            // case:
            // the replica's state is CLONE, which means this a newly created replica in clone process.
            // and an old out-of-date replica reports here, and this report should not mark this replica as
            // 'need recovery'.
            // Other state such as ROLLUP/SCHEMA_CHANGE, the replica behavior is unknown, so for safety reason,
            // also not mark this replica as 'need recovery'.
            return false;
        }

        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            // tablet is bad
            return true;
        }

        if (schemaHashInFe != backendTabletInfo.getSchema_hash()
                || backendTabletInfo.getVersion() == -1 && backendTabletInfo.getVersion_hash() == 0) {
            // no data file exist on BE, maybe this is a newly created schema change tablet. no need to recovery
            return false;
        }

        if (replicaInFe.getVersionHash() == 0 && backendTabletInfo.getVersion() == replicaInFe.getVersion() - 1) {
            /*
             * This is very tricky:
             * 1. Assume that we want to create a replica with version (X, Y), the init version of replica in FE
             *      is (X, Y), and BE will create a replica with version (X+1, 0).
             * 2. BE will report version (X+1, 0), and FE will sync with this version, change to (X+1, 0), too.
             * 3. When restore, BE will restore the replica with version (X, Y) (which is the visible version of partition)
             * 4. BE report the version (X-Y), and than we fall into here
             * 
             * Actually, the version (X+1, 0) is a 'virtual' version, so here we ignore this kind of report
             */
            return false;
        }

        if (backendTabletInfo.isSetVersion_miss() && backendTabletInfo.isVersion_miss()) {
            // even if backend version is less than fe's version, but if version_miss is false,
            // which means this may be a stale report.
            // so we only return true if version_miss is true.
            return true;
        }
        return false;
    }

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return;
            }
            tabletMetaMap.put(tabletId, tabletMeta);
            if (!tabletMetaTable.contains(tabletMeta.getPartitionId(), tabletMeta.getIndexId())) {
                tabletMetaTable.put(tabletMeta.getPartitionId(), tabletMeta.getIndexId(), tabletMeta);
                LOG.debug("add tablet meta: {}", tabletId);
            }

            LOG.debug("add tablet: {}", tabletId);
        } finally {
            writeUnlock();
        }
    }

    public void deleteTablet(long tabletId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Map<Long, Replica> replicas = replicaMetaTable.rowMap().remove(tabletId);
            if (replicas != null) {
                for (Replica replica : replicas.values()) {
                    replicaToTabletMap.remove(replica.getId());
                }

                for (long backendId : replicas.keySet()) {
                    backingReplicaMetaTable.remove(backendId, tabletId);
                }
            }
            TabletMeta tabletMeta = tabletMetaMap.remove(tabletId);
            if (tabletMeta != null) {
                tabletMetaTable.remove(tabletMeta.getPartitionId(), tabletMeta.getIndexId());
                LOG.debug("delete tablet meta: {}", tabletId);
            }

            LOG.debug("delete tablet: {}", tabletId);
        } finally {
            writeUnlock();
        }
    }

    public void addReplica(long tabletId, Replica replica) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            replicaMetaTable.put(tabletId, replica.getBackendId(), replica);
            replicaToTabletMap.put(replica.getId(), tabletId);
            backingReplicaMetaTable.put(replica.getBackendId(), tabletId, replica);
            LOG.debug("add replica {} of tablet {} in backend {}",
                    replica.getId(), tabletId, replica.getBackendId());
        } finally {
            writeUnlock();
        }
    }

    public void deleteReplica(long tabletId, long backendId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            if (replicaMetaTable.containsRow(tabletId)) {
                Replica replica = replicaMetaTable.remove(tabletId, backendId);
                replicaToTabletMap.remove(replica.getId());
                replicaMetaTable.remove(tabletId, backendId);
                backingReplicaMetaTable.remove(backendId, tabletId);
                LOG.debug("delete replica {} of tablet {} in backend {}",
                        replica.getId(), tabletId, backendId);
            } else {
                // this may happen when fe restart after tablet is empty(bug cause)
                // add log instead of assertion to observe
                LOG.error("tablet[{}] contains no replica in inverted index", tabletId);
            }
        } finally {
            writeUnlock();
        }
    }
    
    public Replica getReplica(long tabletId, long backendId) {
        readLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), tabletId);
            return replicaMetaTable.get(tabletId, backendId);
        } finally {
            readUnlock();
        }
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        readLock();
        try {
            if (replicaMetaTable.containsRow(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.row(tabletId).values());
            }
            return Lists.newArrayList();
        } finally {
            readUnlock();
        }
    }

    public void setNewSchemaHash(long partitionId, long indexId, int newSchemaHash) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaTable.contains(partitionId, indexId));
            tabletMetaTable.get(partitionId, indexId).setNewSchemaHash(newSchemaHash);
        } finally {
            writeUnlock();
        }
    }

    public void updateToNewSchemaHash(long partitionId, long indexId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaTable.contains(partitionId, indexId));
            tabletMetaTable.get(partitionId, indexId).updateToNewSchemaHash();
        } finally {
            writeUnlock();
        }
    }

    public void deleteNewSchemaHash(long partitionId, long indexId) {
        if (Catalog.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            TabletMeta tabletMeta = tabletMetaTable.get(partitionId, indexId);
            if (tabletMeta != null) {
                tabletMeta.deleteNewSchemaHash();
            }
        } finally {
            writeUnlock();
        }
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        List<Long> tabletIds = Lists.newArrayList();
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                tabletIds.addAll(replicaMetaWithBackend.keySet());
            }
        } finally {
            readUnlock();
        }
        return tabletIds;
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        List<Long> tabletIds = Lists.newArrayList();
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                tabletIds = replicaMetaWithBackend.keySet().stream().filter(
                        id -> tabletMetaMap.get(id).getStorageMedium() == storageMedium).collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
        return tabletIds;
    }

    public int getTabletNumByBackendId(long backendId) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                return replicaMetaWithBackend.size();
            }
        } finally {
            readUnlock();
        }
        return 0;
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                for (long tabletId : replicaMetaWithBackend.keySet()) {
                    if (tabletMetaMap.get(tabletId).getStorageMedium() == TStorageMedium.HDD) {
                        hddNum++;
                    } else {
                        ssdNum++;
                    }
                }
            }
        } finally {
            readUnlock();
        }
        replicaNumMap.put(TStorageMedium.HDD, hddNum);
        replicaNumMap.put(TStorageMedium.SSD, ssdNum);
        return replicaNumMap;
    }

    // just for test
    public void clear() {
        writeLock();
        try {
            tabletMetaMap.clear();
            replicaToTabletMap.clear();
            tabletMetaTable.clear();
            replicaMetaTable.clear();
            backingReplicaMetaTable.clear();
        } finally {
            writeUnlock();
        }
    }

    public Map<Long, Long> getReplicaToTabletMap() {
        return replicaToTabletMap;
    }
}

