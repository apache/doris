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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * this class stores a inverted index
 * key is tablet id. value is the related ids of this tablet
 * Checkpoint thread is no need to modify this inverted index, because this inverted index will no be wrote
 * into images, all meta data are in catalog, and the inverted index will be rebuild when FE restart.
 */
public class TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

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

    private final void readLock() {
        this.lock.readLock().lock();
    }

    private final void readUnlock() {
        this.lock.readLock().unlock();
    }

    private final void writeLock() {
        this.lock.writeLock().lock();
    }

    private final void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> foundTabletsWithValidSchema,
                             Map<Long, TTabletInfo> foundTabletsWithInvalidSchema,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap, 
                             ListMultimap<Long, TPartitionVersionInfo> transactionsToPublish, 
                             ListMultimap<Long, Long> transactionsToClear, 
                             ListMultimap<Long, Long> tabletRecoveryMap) {
        long start = 0L;
        readLock();
        try {
            LOG.info("begin to do tablet diff with backend[{}]. num: {}", backendId, backendTablets.size());
            start = System.currentTimeMillis();
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

                                if (needRecover(replica, tabletMeta.getOldSchemaHash(), backendTabletInfo)) {
                                    LOG.warn("replica {} of tablet {} on backend {} need recovery. "
                                            + "replica in FE: {}, report version {}-{}, report schema hash: {},"
                                            + " is bad: {}",
                                            replica.getId(), tabletId, backendId, replica,
                                            backendTabletInfo.getVersion(),
                                            backendTabletInfo.getVersion_hash(),
                                            backendTabletInfo.getSchema_hash(),
                                            backendTabletInfo.isSetUsed() ? backendTabletInfo.isUsed() : "unknown");
                                    tabletRecoveryMap.put(tabletMeta.getDbId(), tabletId);
                                }

                                // check if need migration
                                long partitionId = tabletMeta.getPartitionId();
                                TStorageMedium storageMedium = storageMediumMap.get(partitionId);
                                if (storageMedium != null && backendTabletInfo.isSetStorage_medium()) {
                                    if (storageMedium != backendTabletInfo.getStorage_medium()) {
                                        tabletMigrationMap.put(storageMedium, tabletId);
                                    }
                                }
                                // check if should clear transactions
                                if (backendTabletInfo.isSetTransaction_ids()) {
                                    List<Long> transactionIds = backendTabletInfo.getTransaction_ids();
                                    GlobalTransactionMgr transactionMgr = Catalog.getCurrentGlobalTransactionMgr();
                                    for (Long transactionId : transactionIds) {
                                        TransactionState transactionState = transactionMgr.getTransactionState(transactionId);
                                        if (transactionState == null || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
                                            transactionsToClear.put(transactionId, tabletMeta.getPartitionId());
                                            LOG.debug("transaction id [{}] is not valid any more, " 
                                                    + "clear it from backend [{}]", transactionId, backendId);
                                        } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                                            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfo(tabletMeta.getTableId());
                                            PartitionCommitInfo partitionCommitInfo = tableCommitInfo.getPartitionCommitInfo(partitionId);
                                            TPartitionVersionInfo versionInfo = new TPartitionVersionInfo(tabletMeta.getPartitionId(), 
                                                    partitionCommitInfo.getVersion(),
                                                    partitionCommitInfo.getVersionHash());
                                            transactionsToPublish.put(transactionId, versionInfo);
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

    public long getDbId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getDbId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public long getTableId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getTableId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }
    
    public TabletMeta getTabletMetaByReplica(long replicaId) {
        readLock();
        try {
            Long tabletId = replicaToTabletMap.get(replicaId);
            if (tabletId == null) {
                return null;
            }
            TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
            return tabletMeta;
        } finally {
            readUnlock();
        }
    }
    
    public Long getTabletIdByReplica(long replicaId) {
        readLock();
        try {
            Long tabletId = replicaToTabletMap.get(replicaId);
            return tabletId;
        } finally {
            readUnlock();
        }
    }

    public long getPartitionId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getPartitionId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public long getIndexId(long tabletId) {
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getIndexId();
            }
            return NOT_EXIST_VALUE;
        } finally {
            readUnlock();
        }
    }

    public int getEffectiveSchemaHash(long tabletId) {
        // always get old schema hash(as effective one)
        readLock();
        try {
            if (tabletMetaMap.containsKey(tabletId)) {
                return tabletMetaMap.get(tabletId).getOldSchemaHash();
            }
            return NOT_EXIST_VALUE;
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
    
    public Set<Long> getTabletBackends(long tabletId) {
        Map<Long, Replica> backendIdToReplica = replicaMetaTable.row(tabletId);
        return backendIdToReplica.keySet();
    }

    private boolean needSync(Replica replicaInFe, TTabletInfo backendTabletInfo) {
        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            // tablet is bad, do not sync
            // it will be handled in needRecovery()
            return false;
        }
        long versionInFe = replicaInFe.getVersion();
        long versionHashInFe = replicaInFe.getVersionHash();
        if (backendTabletInfo.getVersion() > versionInFe
                || (versionInFe == backendTabletInfo.getVersion()
                        && versionHashInFe != backendTabletInfo.getVersion_hash())) {
            return true;
        }
        return false;
    }
    
    /**
     * if be's report version < fe's meta version, or tablet is unused, it means some version is missing in BE
     * because of some unrecoverable failure.
     */
    private boolean needRecover(Replica replicaInFe, int schemaHashInFe, TTabletInfo backendTabletInfo) {
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

        if (backendTabletInfo.getVersion() < replicaInFe.getVersion()
                && backendTabletInfo.isSetVersion_miss() && backendTabletInfo.isVersion_miss()) {
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
            // Preconditions.checkState(replicaMetaTable.containsRow(tabletId));
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

