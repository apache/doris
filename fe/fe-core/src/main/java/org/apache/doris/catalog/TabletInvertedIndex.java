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
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.cooldown.CooldownConf;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTabletMetaInfo;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.StampedLock;
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

    private StampedLock lock = new StampedLock();

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

    private volatile ImmutableSet<Long> partitionIdInMemorySet = ImmutableSet.of();

    private ForkJoinPool taskPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    public TabletInvertedIndex() {
    }

    private long readLock() {
        return this.lock.readLock();
    }

    private void readUnlock(long stamp) {
        this.lock.unlockRead(stamp);
    }

    private long writeLock() {
        return this.lock.writeLock();
    }

    private void writeUnlock(long stamp) {
        this.lock.unlockWrite(stamp);
    }

    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> tabletFoundInMeta,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap,
                             Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish,
                             ListMultimap<Long, Long> transactionsToClear,
                             ListMultimap<Long, Long> tabletRecoveryMap,
                             List<TTabletMetaInfo> tabletToUpdate,
                             List<CooldownConf> cooldownConfToPush,
                             List<CooldownConf> cooldownConfToUpdate) {
        List<Pair<TabletMeta, TTabletInfo>> cooldownTablets = new ArrayList<>();
        long stamp = readLock();
        long start = System.currentTimeMillis();
        try {
            LOG.debug("begin to do tablet diff with backend[{}]. num: {}", backendId, backendTablets.size());
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                taskPool.submit(() -> {
                    // traverse replicas in meta with this backend
                    replicaMetaWithBackend.entrySet().parallelStream().forEach(entry -> {
                        long tabletId = entry.getKey();
                        Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
                        TabletMeta tabletMeta = tabletMetaMap.get(tabletId);

                        if (backendTablets.containsKey(tabletId)) {
                            TTablet backendTablet = backendTablets.get(tabletId);
                            Replica replica = entry.getValue();
                            tabletFoundInMeta.add(tabletId);
                            TTabletInfo backendTabletInfo = backendTablet.getTabletInfos().get(0);
                            TTabletMetaInfo tabletMetaInfo = null;
                            if (backendTabletInfo.getReplicaId() != replica.getId()
                                    && replica.getState() != ReplicaState.CLONE) {
                                // Need to update replica id in BE
                                tabletMetaInfo = new TTabletMetaInfo();
                                tabletMetaInfo.setReplicaId(replica.getId());
                            }
                            if (partitionIdInMemorySet.contains(
                                    backendTabletInfo.getPartitionId()) != backendTabletInfo.isIsInMemory()) {
                                if (tabletMetaInfo == null) {
                                    tabletMetaInfo = new TTabletMetaInfo();
                                    tabletMetaInfo.setIsInMemory(!backendTabletInfo.isIsInMemory());
                                }
                            }
                            // 1. (intersection)
                            if (needSync(replica, backendTabletInfo)) {
                                // need sync
                                synchronized (tabletSyncMap) {
                                    tabletSyncMap.put(tabletMeta.getDbId(), tabletId);
                                }
                            }

                            // check and set path
                            // path info of replica is only saved in Master FE
                            if (backendTabletInfo.isSetPathHash()
                                    && replica.getPathHash() != backendTabletInfo.getPathHash()) {
                                replica.setPathHash(backendTabletInfo.getPathHash());
                            }

                            if (backendTabletInfo.isSetSchemaHash() && replica.getState() == ReplicaState.NORMAL
                                    && replica.getSchemaHash() != backendTabletInfo.getSchemaHash()) {
                                // update the schema hash only when replica is normal
                                replica.setSchemaHash(backendTabletInfo.getSchemaHash());
                            }

                            if (needRecover(replica, tabletMeta.getOldSchemaHash(), backendTabletInfo)) {
                                LOG.warn("replica {} of tablet {} on backend {} need recovery. "
                                                + "replica in FE: {}, report version {}, report schema hash: {},"
                                                + " is bad: {}, is version missing: {}",
                                        replica.getId(), tabletId, backendId, replica,
                                        backendTabletInfo.getVersion(),
                                        backendTabletInfo.getSchemaHash(),
                                        backendTabletInfo.isSetUsed() ? !backendTabletInfo.isUsed() : "false",
                                        backendTabletInfo.isSetVersionMiss() ? backendTabletInfo.isVersionMiss() :
                                                "unset");
                                synchronized (tabletRecoveryMap) {
                                    tabletRecoveryMap.put(tabletMeta.getDbId(), tabletId);
                                }
                            }

                            if (Config.enable_storage_policy && backendTabletInfo.isSetCooldownTerm()) {
                                // Place tablet info in a container and process it outside of read lock to avoid
                                // deadlock with OlapTable lock
                                synchronized (cooldownTablets) {
                                    cooldownTablets.add(Pair.of(tabletMeta, backendTabletInfo));
                                }
                                replica.setCooldownMetaId(backendTabletInfo.getCooldownMetaId());
                                replica.setCooldownTerm(backendTabletInfo.getCooldownTerm());
                            }

                            long partitionId = tabletMeta.getPartitionId();
                            if (!Config.disable_storage_medium_check) {
                                // check if need migration
                                TStorageMedium storageMedium = storageMediumMap.get(partitionId);
                                if (storageMedium != null && backendTabletInfo.isSetStorageMedium()
                                        && isLocal(storageMedium) && isLocal(backendTabletInfo.getStorageMedium())
                                        && isLocal(tabletMeta.getStorageMedium())) {
                                    if (storageMedium != backendTabletInfo.getStorageMedium()) {
                                        synchronized (tabletMigrationMap) {
                                            tabletMigrationMap.put(storageMedium, tabletId);
                                        }
                                    }
                                    if (storageMedium != tabletMeta.getStorageMedium()) {
                                        tabletMeta.setStorageMedium(storageMedium);
                                    }
                                }
                            }

                            // check if should clear transactions
                            if (backendTabletInfo.isSetTransactionIds()) {
                                List<Long> transactionIds = backendTabletInfo.getTransactionIds();
                                GlobalTransactionMgr transactionMgr = Env.getCurrentGlobalTransactionMgr();
                                for (Long transactionId : transactionIds) {
                                    TransactionState transactionState
                                            = transactionMgr.getTransactionState(tabletMeta.getDbId(), transactionId);
                                    if (transactionState == null
                                            || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
                                        synchronized (transactionsToClear) {
                                            transactionsToClear.put(transactionId, tabletMeta.getPartitionId());
                                        }
                                        LOG.debug("transaction id [{}] is not valid any more, "
                                                + "clear it from backend [{}]", transactionId, backendId);
                                    } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                                        TableCommitInfo tableCommitInfo
                                                = transactionState.getTableCommitInfo(tabletMeta.getTableId());
                                        PartitionCommitInfo partitionCommitInfo = tableCommitInfo == null
                                                ? null : tableCommitInfo.getPartitionCommitInfo(partitionId);
                                        if (partitionCommitInfo != null) {
                                            TPartitionVersionInfo versionInfo
                                                    = new TPartitionVersionInfo(tabletMeta.getPartitionId(),
                                                    partitionCommitInfo.getVersion(), 0);
                                            synchronized (transactionsToPublish) {
                                                ListMultimap<Long, TPartitionVersionInfo> map
                                                        = transactionsToPublish.get(transactionState.getDbId());
                                                if (map == null) {
                                                    map = ArrayListMultimap.create();
                                                    transactionsToPublish.put(transactionState.getDbId(), map);
                                                }
                                                map.put(transactionId, versionInfo);
                                            }
                                        }
                                    } else if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                                        // for some reasons, transaction pushlish succeed replica num less than quorum,
                                        // this transaction's status can not to be VISIBLE, and this publish task of
                                        // this replica of this tablet on this backend need retry publish success to
                                        // make transaction VISIBLE when last publish failed.
                                        Map<Long, PublishVersionTask> publishVersionTask =
                                                        transactionState.getPublishVersionTasks();
                                        PublishVersionTask task = publishVersionTask.get(backendId);
                                        if (task != null && task.isFinished()) {
                                            List<Long> errorTablets = task.getErrorTablets();
                                            if (errorTablets != null) {
                                                for (int i = 0; i < errorTablets.size(); i++) {
                                                    if (tabletId == errorTablets.get(i)) {
                                                        TableCommitInfo tableCommitInfo
                                                                = transactionState.getTableCommitInfo(
                                                                        tabletMeta.getTableId());
                                                        PartitionCommitInfo partitionCommitInfo =
                                                                tableCommitInfo == null ? null :
                                                                tableCommitInfo.getPartitionCommitInfo(partitionId);
                                                        if (partitionCommitInfo != null) {
                                                            TPartitionVersionInfo versionInfo
                                                                    = new TPartitionVersionInfo(
                                                                        tabletMeta.getPartitionId(),
                                                                        partitionCommitInfo.getVersion(), 0);
                                                            synchronized (transactionsToPublish) {
                                                                ListMultimap<Long, TPartitionVersionInfo> map
                                                                        = transactionsToPublish.get(
                                                                        transactionState.getDbId());
                                                                if (map == null) {
                                                                    map = ArrayListMultimap.create();
                                                                    transactionsToPublish.put(
                                                                            transactionState.getDbId(), map);
                                                                }
                                                                map.put(transactionId, versionInfo);
                                                            }
                                                        }
                                                        break;
                                                    }
                                                }
                                            }
                                        }

                                    }
                                }
                            } // end for txn id

                            // update replicase's version count
                            // no need to write log, and no need to get db lock.
                            if (backendTabletInfo.isSetVersionCount()) {
                                replica.setVersionCount(backendTabletInfo.getVersionCount());
                            }
                            if (tabletMetaInfo != null) {
                                tabletMetaInfo.setTabletId(tabletId);
                                synchronized (tabletToUpdate) {
                                    tabletToUpdate.add(tabletMetaInfo);
                                }
                            }
                        } else {
                            // 2. (meta - be)
                            // may need delete from meta
                            LOG.debug("backend[{}] does not report tablet[{}-{}]", backendId, tabletId, tabletMeta);
                            synchronized (tabletDeleteFromMeta) {
                                tabletDeleteFromMeta.put(tabletMeta.getDbId(), tabletId);
                            }
                        }
                    });
                }).join();
            }
        } finally {
            readUnlock(stamp);
        }
        cooldownTablets.forEach(p -> handleCooldownConf(p.first, p.second, cooldownConfToPush, cooldownConfToUpdate));

        long end = System.currentTimeMillis();
        LOG.info("finished to do tablet diff with backend[{}]. sync: {}."
                        + " metaDel: {}. foundInMeta: {}. migration: {}. "
                        + "found invalid transactions {}. found republish transactions {}. tabletToUpdate: {}."
                        + " need recovery: {}. cost: {} ms", backendId, tabletSyncMap.size(),
                tabletDeleteFromMeta.size(), tabletFoundInMeta.size(), tabletMigrationMap.size(),
                transactionsToClear.size(), transactionsToPublish.size(), tabletToUpdate.size(),
                tabletRecoveryMap.size(), (end - start));
    }

    public Long getTabletIdByReplica(long replicaId) {
        long stamp = readLock();
        try {
            return replicaToTabletMap.get(replicaId);
        } finally {
            readUnlock(stamp);
        }
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

        if (backendTabletInfo.getVersion() > versionInFe) {
            // backend replica's version is larger or newer than replica in FE, sync it.
            return true;
        } else if (versionInFe == backendTabletInfo.getVersion()) {
            // backend replica's version is equal to replica in FE, but replica in FE is bad,
            // while backend replica is good, sync it
            if (replicaInFe.isBad()) {
                return true;
            }

            // FE' s replica last failed version > partition's committed version
            // this can be occur when be report miss version, fe will set last failed version = visible version + 1
            // then last failed version may greater than partition's committed version
            //
            // But here cannot got variable partition, we just check lastFailedVersion = version + 1,
            // In ReportHandler.sync, we will check if last failed version > partition's committed version again.
            if (replicaInFe.getLastFailedVersion() == versionInFe + 1) {
                return true;
            }
        }

        return false;
    }

    private void handleCooldownConf(TabletMeta tabletMeta, TTabletInfo beTabletInfo,
            List<CooldownConf> cooldownConfToPush, List<CooldownConf> cooldownConfToUpdate) {
        Tablet tablet;
        try {
            OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbNullable(tabletMeta.getDbId())
                    .getTable(tabletMeta.getTableId())
                    .get();
            table.readLock();
            try {
                tablet = table.getPartition(tabletMeta.getPartitionId()).getIndex(tabletMeta.getIndexId())
                        .getTablet(beTabletInfo.tablet_id);
            } finally {
                table.readUnlock();
            }
        } catch (RuntimeException e) {
            LOG.warn("failed to get tablet. tabletId={}", beTabletInfo.tablet_id);
            return;
        }
        Pair<Long, Long> cooldownConf = tablet.getCooldownConf();
        if (beTabletInfo.getCooldownTerm() > cooldownConf.second) { // should not be here
            LOG.warn("report cooldownTerm({}) > cooldownTerm in TabletMeta({}), tabletId={}",
                    beTabletInfo.getCooldownTerm(), cooldownConf.second, beTabletInfo.tablet_id);
            return;
        }

        if (cooldownConf.first <= 0) { // invalid cooldownReplicaId
            CooldownConf conf = new CooldownConf(tabletMeta.getDbId(), tabletMeta.getTableId(),
                    tabletMeta.getPartitionId(), tabletMeta.getIndexId(), beTabletInfo.tablet_id, cooldownConf.second);
            cooldownConfToUpdate.add(conf);
            return;
        }

        // check cooldown replica is alive
        Map<Long, Replica> replicaMap = replicaMetaTable.row(beTabletInfo.getTabletId());
        if (replicaMap.isEmpty()) {
            return;
        }
        boolean replicaAlive = false;
        for (Replica replica : replicaMap.values()) {
            if (replica.getId() == cooldownConf.first) {
                if (replica.isAlive()) {
                    replicaAlive = true;
                }
                break;
            }
        }
        if (!replicaAlive) {
            CooldownConf conf = new CooldownConf(tabletMeta.getDbId(), tabletMeta.getTableId(),
                    tabletMeta.getPartitionId(), tabletMeta.getIndexId(), beTabletInfo.tablet_id, cooldownConf.second);
            cooldownConfToUpdate.add(conf);
            return;
        }

        if (beTabletInfo.getCooldownTerm() < cooldownConf.second) {
            CooldownConf conf = new CooldownConf(beTabletInfo.tablet_id, cooldownConf.first, cooldownConf.second);
            cooldownConfToPush.add(conf);
            return;
        }
    }

    public List<Replica> getReplicas(Long tabletId) {
        long stamp = readLock();
        try {
            Map<Long, Replica> replicaMap = replicaMetaTable.row(tabletId);
            return replicaMap.values().stream().collect(Collectors.toList());
        } finally {
            readUnlock(stamp);
        }
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

        if (schemaHashInFe != backendTabletInfo.getSchemaHash() || backendTabletInfo.getVersion() == -1) {
            // no data file exist on BE, maybe this is a newly created schema change tablet. no need to recovery
            return false;
        }

        if (backendTabletInfo.isSetVersionMiss() && backendTabletInfo.isVersionMiss()) {
            // even if backend version is less than fe's version, but if version_miss is false,
            // which means this may be a stale report.
            // so we only return true if version_miss is true.
            return true;
        }

        // backend versions regressive due to bugs
        if (replicaInFe.checkVersionRegressive(backendTabletInfo.getVersion())) {
            return true;
        }

        return false;
    }

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        long stamp = writeLock();
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
            writeUnlock(stamp);
        }
    }

    public void deleteTablet(long tabletId) {
        long stamp = writeLock();
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
            writeUnlock(stamp);
        }
    }

    public void addReplica(long tabletId, Replica replica) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            replicaMetaTable.put(tabletId, replica.getBackendId(), replica);
            replicaToTabletMap.put(replica.getId(), tabletId);
            backingReplicaMetaTable.put(replica.getBackendId(), tabletId, replica);
            LOG.debug("add replica {} of tablet {} in backend {}",
                    replica.getId(), tabletId, replica.getBackendId());
        } finally {
            writeUnlock(stamp);
        }
    }

    public void deleteReplica(long tabletId, long backendId) {
        long stamp = writeLock();
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
            writeUnlock(stamp);
        }
    }

    public Replica getReplica(long tabletId, long backendId) {
        long stamp = readLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), tabletId);
            return replicaMetaTable.get(tabletId, backendId);
        } finally {
            readUnlock(stamp);
        }
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        long stamp = readLock();
        try {
            if (replicaMetaTable.containsRow(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.row(tabletId).values());
            }
            return Lists.newArrayList();
        } finally {
            readUnlock(stamp);
        }
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        List<Long> tabletIds = Lists.newArrayList();
        long stamp = readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                tabletIds.addAll(replicaMetaWithBackend.keySet());
            }
        } finally {
            readUnlock(stamp);
        }
        return tabletIds;
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        List<Long> tabletIds = Lists.newArrayList();
        long stamp = readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                tabletIds = replicaMetaWithBackend.keySet().stream().filter(
                        id -> tabletMetaMap.get(id).getStorageMedium() == storageMedium).collect(Collectors.toList());
            }
        } finally {
            readUnlock(stamp);
        }
        return tabletIds;
    }

    public int getTabletNumByBackendId(long backendId) {
        long stamp = readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                return replicaMetaWithBackend.size();
            }
        } finally {
            readUnlock(stamp);
        }
        return 0;
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;
        long stamp = readLock();
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
            readUnlock(stamp);
        }
        replicaNumMap.put(TStorageMedium.HDD, hddNum);
        replicaNumMap.put(TStorageMedium.SSD, ssdNum);
        return replicaNumMap;
    }

    // just for test
    public void clear() {
        long stamp = writeLock();
        try {
            tabletMetaMap.clear();
            replicaToTabletMap.clear();
            tabletMetaTable.clear();
            replicaMetaTable.clear();
            backingReplicaMetaTable.clear();
        } finally {
            writeUnlock(stamp);
        }
    }

    public void setPartitionIdInMemorySet(ImmutableSet<Long> partitionIdInMemorySet) {
        this.partitionIdInMemorySet = partitionIdInMemorySet;
    }

    public Map<Long, Long> getReplicaToTabletMap() {
        return replicaToTabletMap;
    }

    // Only build from available bes, exclude colocate tables
    public Map<TStorageMedium, TreeMultimap<Long, PartitionBalanceInfo>> buildPartitionInfoBySkew(
            List<Long> availableBeIds) {
        Set<Long> dbIds = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();
        Env.getCurrentRecycleBin().getRecycleIds(dbIds, tableIds, partitionIds);
        long stamp = readLock();

        // 1. gen <partitionId-indexId, <beId, replicaCount>>
        // for each replica(all tablets):
        //      find beId, then replicaCount++
        Map<TStorageMedium, Table<Long, Long, Map<Long, Long>>> partitionReplicasInfoMaps = Maps.newHashMap();
        for (TStorageMedium medium : TStorageMedium.values()) {
            partitionReplicasInfoMaps.put(medium, HashBasedTable.create());
        }
        try {
            // Changes to the returned set will update the underlying table
            // tablet id -> (backend id -> replica)
            Set<Table.Cell<Long, Long, Replica>> cells = replicaMetaTable.cellSet();
            for (Table.Cell<Long, Long, Replica> cell : cells) {
                Long tabletId = cell.getRowKey();
                Long beId = cell.getColumnKey();
                try {
                    Preconditions.checkState(availableBeIds.contains(beId), "dead be " + beId);
                    TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
                    if (dbIds.contains(tabletMeta.getDbId()) || tableIds.contains(tabletMeta.getTableId())
                            || partitionIds.contains(tabletMeta.getPartitionId())) {
                        continue;
                    }
                    Preconditions.checkNotNull(tabletMeta, "invalid tablet " + tabletId);
                    Preconditions.checkState(
                            !Env.getCurrentColocateIndex().isColocateTable(tabletMeta.getTableId()),
                            "should not be the colocate table");

                    TStorageMedium medium = tabletMeta.getStorageMedium();
                    Table<Long, Long, Map<Long, Long>> partitionReplicasInfo = partitionReplicasInfoMaps.get(medium);
                    Map<Long, Long> countMap = partitionReplicasInfo.get(
                            tabletMeta.getPartitionId(), tabletMeta.getIndexId());
                    if (countMap == null) {
                        // If one be doesn't have any replica of one partition, it should be counted too.
                        countMap = availableBeIds.stream().collect(Collectors.toMap(i -> i, i -> 0L));
                    }

                    Long count = countMap.get(beId);
                    countMap.put(beId, count + 1L);
                    partitionReplicasInfo.put(tabletMeta.getPartitionId(), tabletMeta.getIndexId(), countMap);
                    partitionReplicasInfoMaps.put(medium, partitionReplicasInfo);
                } catch (IllegalStateException | NullPointerException e) {
                    // If the tablet or be has some problem, don't count in
                    LOG.debug(e.getMessage());
                }
            }
        } finally {
            readUnlock(stamp);
        }

        // 2. Populate ClusterBalanceInfo::table_info_by_skew
        // for each PartitionId-MaterializedIndex:
        //      for each beId: record max_count, min_count(replicaCount)
        //      put <max_count-min_count, TableBalanceInfo> to table_info_by_skew
        Map<TStorageMedium, TreeMultimap<Long, PartitionBalanceInfo>> skewMaps = Maps.newHashMap();
        for (TStorageMedium medium : TStorageMedium.values()) {
            TreeMultimap<Long, PartitionBalanceInfo> partitionInfoBySkew
                    = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
            Set<Table.Cell<Long, Long, Map<Long, Long>>> mapCells
                    = partitionReplicasInfoMaps.getOrDefault(medium, HashBasedTable.create()).cellSet();
            for (Table.Cell<Long, Long, Map<Long, Long>> cell : mapCells) {
                Map<Long, Long> countMap = cell.getValue();
                Preconditions.checkNotNull(countMap);
                PartitionBalanceInfo pbi = new PartitionBalanceInfo(cell.getRowKey(), cell.getColumnKey());
                for (Map.Entry<Long, Long> entry : countMap.entrySet()) {
                    Long beID = entry.getKey();
                    Long replicaCount = entry.getValue();
                    pbi.beByReplicaCount.put(replicaCount, beID);
                }
                // beByReplicaCount values are natural ordering
                long minCount = pbi.beByReplicaCount.keySet().first();
                long maxCount = pbi.beByReplicaCount.keySet().last();
                partitionInfoBySkew.put(maxCount - minCount, pbi);
            }
            skewMaps.put(medium, partitionInfoBySkew);
        }
        return skewMaps;
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
    }

    // just for ut
    public Table<Long, Long, Replica> getReplicaMetaTable() {
        return replicaMetaTable;
    }

    // just for ut
    public Table<Long, Long, Replica> getBackingReplicaMetaTable() {
        return backingReplicaMetaTable;
    }

    // just for ut
    public Table<Long, Long, TabletMeta> getTabletMetaTable() {
        return tabletMetaTable;
    }

    // just for ut
    public Map<Long, TabletMeta> getTabletMetaMap() {
        return tabletMetaMap;
    }

    private boolean isLocal(TStorageMedium storageMedium) {
        return storageMedium == TStorageMedium.HDD || storageMedium == TStorageMedium.SSD;
    }

}
