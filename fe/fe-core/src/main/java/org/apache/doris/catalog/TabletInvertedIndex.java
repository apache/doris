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
import org.apache.doris.clone.PartitionRebalancer.TabletMove;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.cooldown.CooldownConf;
import org.apache.doris.master.PartitionInfoCollector.PartitionCollectInfo;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTabletMetaInfo;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
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
    // for cloud mode, no need to known the replica's backend, so use backend id = -1 in cloud mode.
    private Table<Long, Long, Replica> replicaMetaTable = HashBasedTable.create();

    // backing replica table, for visiting backend replicas faster.
    // backend id -> (tablet id -> replica)
    private Table<Long, Long, Replica> backingReplicaMetaTable = HashBasedTable.create();

    // partition id -> partition info.
    // notice partition info update every Config.partition_info_update_interval_secs seconds,
    // so it may be stale.
    // Notice only none-cloud use it for be reporting tablets. This map is empty in cloud mode.
    private volatile ImmutableMap<Long, PartitionCollectInfo> partitionCollectInfoMap = ImmutableMap.of();

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
                             Map<Long, Long> backendPartitionsVersion,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> tabletFoundInMeta,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap,
                             Map<Long, Long> partitionVersionSyncMap,
                             Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish,
                             ListMultimap<Long, Long> transactionsToClear,
                             ListMultimap<Long, Long> tabletRecoveryMap,
                             List<TTabletMetaInfo> tabletToUpdate,
                             List<CooldownConf> cooldownConfToPush,
                             List<CooldownConf> cooldownConfToUpdate) {
        List<Pair<TabletMeta, TTabletInfo>> cooldownTablets = new ArrayList<>();
        long feTabletNum = 0;
        long stamp = readLock();
        long start = System.currentTimeMillis();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("begin to do tablet diff with backend[{}]. num: {}", backendId, backendTablets.size());
            }
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                feTabletNum = replicaMetaWithBackend.size();
                taskPool.submit(() -> {
                    // traverse replicas in meta with this backend
                    replicaMetaWithBackend.entrySet().parallelStream().forEach(entry -> {
                        long tabletId = entry.getKey();
                        Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                                "tablet " + tabletId + " not exists, backend " + backendId);
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
                            PartitionCollectInfo partitionCollectInfo =
                                    partitionCollectInfoMap.get(backendTabletInfo.getPartitionId());
                            boolean isInMemory = partitionCollectInfo != null && partitionCollectInfo.isInMemory();
                            if (isInMemory != backendTabletInfo.isIsInMemory()) {
                                if (tabletMetaInfo == null) {
                                    tabletMetaInfo = new TTabletMetaInfo();
                                    tabletMetaInfo.setIsInMemory(isInMemory);
                                }
                            }
                            if (Config.fix_tablet_partition_id_eq_0
                                    && tabletMeta.getPartitionId() > 0
                                    && backendTabletInfo.getPartitionId() == 0) {
                                LOG.warn("be report tablet partition id not eq fe, in be {} but in fe {}",
                                        backendTabletInfo, tabletMeta);
                                // Need to update partition id in BE
                                tabletMetaInfo = new TTabletMetaInfo();
                                tabletMetaInfo.setPartitionId(tabletMeta.getPartitionId());
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
                                handleBackendTransactions(backendId, backendTabletInfo.getTransactionIds(), tabletId,
                                        tabletMeta, transactionsToPublish, transactionsToClear);
                            } // end for txn id

                            // update replicase's version count
                            // no need to write log, and no need to get db lock.
                            if (backendTabletInfo.isSetTotalVersionCount()) {
                                replica.setTotalVersionCount(backendTabletInfo.getTotalVersionCount());
                                replica.setVisibleVersionCount(backendTabletInfo.isSetVisibleVersionCount()
                                        ? backendTabletInfo.getVisibleVersionCount()
                                                : backendTabletInfo.getTotalVersionCount());
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
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("backend[{}] does not report tablet[{}-{}]", backendId, tabletId, tabletMeta);
                            }
                            synchronized (tabletDeleteFromMeta) {
                                tabletDeleteFromMeta.put(tabletMeta.getDbId(), tabletId);
                            }
                        }
                    });

                    backendPartitionsVersion.entrySet().parallelStream().forEach(entry -> {
                        long partitionId = entry.getKey();
                        long backendVersion = entry.getValue();
                        PartitionCollectInfo partitionInfo = partitionCollectInfoMap.get(partitionId);
                        if (partitionInfo != null && partitionInfo.getVisibleVersion() > backendVersion) {
                            partitionVersionSyncMap.put(partitionId, partitionInfo.getVisibleVersion());
                        }
                    });
                }).join();
            }
        } finally {
            readUnlock(stamp);
        }
        cooldownTablets.forEach(p -> handleCooldownConf(p.first, p.second, cooldownConfToPush, cooldownConfToUpdate));

        long end = System.currentTimeMillis();
        LOG.info("finished to do tablet diff with backend[{}]. fe tablet num: {}, backend tablet num: {}. sync: {}."
                        + " metaDel: {}. foundInMeta: {}. migration: {}. backend partition num: {}, backend need "
                        + "update: {}. found invalid transactions {}. found republish "
                        + "transactions {}. tabletToUpdate: {}. need recovery: {}. cost: {} ms",
                backendId, feTabletNum, backendTablets.size(), tabletSyncMap.size(),
                tabletDeleteFromMeta.size(), tabletFoundInMeta.size(), tabletMigrationMap.size(),
                backendPartitionsVersion.size(), partitionVersionSyncMap.size(),
                transactionsToClear.size(), transactionsToPublish.size(), tabletToUpdate.size(),
                tabletRecoveryMap.size(), (end - start));
    }

    private void handleBackendTransactions(long backendId, List<Long> transactionIds, long tabletId,
            TabletMeta tabletMeta, Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish,
            ListMultimap<Long, Long> transactionsToClear) {
        GlobalTransactionMgrIface transactionMgr = Env.getCurrentGlobalTransactionMgr();
        long partitionId = tabletMeta.getPartitionId();
        for (Long transactionId : transactionIds) {
            TransactionState transactionState = transactionMgr.getTransactionState(tabletMeta.getDbId(), transactionId);
            if (transactionState == null || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
                synchronized (transactionsToClear) {
                    transactionsToClear.put(transactionId, tabletMeta.getPartitionId());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("transaction id [{}] is not valid any more, clear it from backend [{}]",
                            transactionId, backendId);
                }
            } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                publishPartition(transactionState, transactionId, tabletMeta, partitionId, transactionsToPublish);
            } else if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                // for some reasons, transaction pushlish succeed replica num less than quorum,
                // this transaction's status can not to be VISIBLE, and this publish task of
                // this replica of this tablet on this backend need retry publish success to
                // make transaction VISIBLE when last publish failed.
                Map<Long, List<PublishVersionTask>> publishVersionTask = transactionState.getPublishVersionTasks();
                List<PublishVersionTask> tasks = publishVersionTask.get(backendId);
                if (tasks == null) {
                    continue;
                }
                for (PublishVersionTask task : tasks) {
                    if (task != null && task.isFinished()) {
                        List<Long> errorTablets = task.getErrorTablets();
                        if (errorTablets != null) {
                            for (int i = 0; i < errorTablets.size(); i++) {
                                if (tabletId == errorTablets.get(i)) {
                                    publishPartition(transactionState, transactionId, tabletMeta, partitionId,
                                            transactionsToPublish);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // the transactionId may be sub transaction id or transaction id
    private TPartitionVersionInfo generatePartitionVersionInfoWhenReport(TransactionState transactionState,
            long transactionId, TabletMeta tabletMeta, long partitionId) {
        TableCommitInfo tableCommitInfo;
        if (transactionState.getSubTxnIds() == null) {
            tableCommitInfo = transactionState.getTableCommitInfo(tabletMeta.getTableId());
        } else {
            tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(transactionId);
        }
        if (tableCommitInfo != null && tableCommitInfo.getPartitionCommitInfo(partitionId) != null) {
            PartitionCommitInfo partitionCommitInfo = tableCommitInfo.getPartitionCommitInfo(partitionId);
            return new TPartitionVersionInfo(tabletMeta.getPartitionId(),
                    partitionCommitInfo.getVersion(), 0);
        }
        return null;
    }

    private void publishPartition(TransactionState transactionState, long transactionId, TabletMeta tabletMeta,
            long partitionId, Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish) {
        TPartitionVersionInfo versionInfo = generatePartitionVersionInfoWhenReport(transactionState,
                transactionId, tabletMeta, partitionId);
        if (versionInfo != null) {
            synchronized (transactionsToPublish) {
                ListMultimap<Long, TPartitionVersionInfo> map = transactionsToPublish.get(
                        transactionState.getDbId());
                if (map == null) {
                    map = ArrayListMultimap.create();
                    transactionsToPublish.put(transactionState.getDbId(), map);
                }
                map.put(transactionId, versionInfo);
            }
        }
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
            if (!Env.getCurrentRecycleBin().isRecyclePartition(tabletMeta.getDbId(),
                    tabletMeta.getTableId(), tabletMeta.getPartitionId())) {
                LOG.warn("failed to get tablet. tabletId={}", beTabletInfo.tablet_id);
            }
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("add tablet meta: {}", tabletId);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("add tablet: {}", tabletId);
            }
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete tablet meta: {}", tabletId);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("delete tablet: {}", tabletId);
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    public void addReplica(long tabletId, Replica replica) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, replica " + replica.getId()
                    + ", backend " + replica.getBackendId());
            // cloud mode, create table not need backendId, represent with -1.
            long backendId = Config.isCloudMode() ? -1 : replica.getBackendId();
            replicaMetaTable.put(tabletId, backendId, replica);
            replicaToTabletMap.put(replica.getId(), tabletId);
            backingReplicaMetaTable.put(backendId, tabletId, replica);
            if (LOG.isDebugEnabled()) {
                LOG.debug("add replica {} of tablet {} in backend {}",
                        replica.getId(), tabletId, replica.getBackendId());
            }
        } finally {
            writeUnlock(stamp);
        }
    }

    public void deleteReplica(long tabletId, long backendId) {
        long stamp = writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, backend " + backendId);
            if (Config.isCloudMode()) {
                backendId = -1;
            }
            if (replicaMetaTable.containsRow(tabletId)) {
                Replica replica = replicaMetaTable.remove(tabletId, backendId);

                // sometimes, replicas may have same replica id in different backend
                // we need to cover this situation to avoid some "replica not found" issue
                if (replicaMetaTable.containsRow(tabletId)) {
                    long replicaNum = replicaMetaTable.row(tabletId).values().stream()
                            .filter(c -> c.getId() == replica.getId()).count();
                    if (replicaNum == 0) {
                        replicaToTabletMap.remove(replica.getId());
                    }
                } else {
                    replicaToTabletMap.remove(replica.getId());
                }

                backingReplicaMetaTable.remove(backendId, tabletId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete replica {} of tablet {} in backend {}",
                            replica.getId(), tabletId, backendId);
                }
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
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId),
                    "tablet " + tabletId + " not exists, backend " + backendId);
            if (Config.isCloudMode()) {
                backendId = -1;
            }
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

    public List<Pair<Long, Long>> getTabletSizeByBackendIdAndStorageMedium(long backendId,
            TStorageMedium storageMedium) {
        List<Pair<Long, Long>> tabletIdSizes = Lists.newArrayList();
        long stamp = readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            if (replicaMetaWithBackend != null) {
                tabletIdSizes = replicaMetaWithBackend.entrySet().stream()
                        .filter(entry -> tabletMetaMap.get(entry.getKey()).getStorageMedium() == storageMedium)
                        .map(entry -> Pair.of(entry.getKey(), entry.getValue().getDataSize()))
                        .collect(Collectors.toList());
            }
        } finally {
            readUnlock(stamp);
        }
        return tabletIdSizes;
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId,
            TStorageMedium storageMedium) {
        return getTabletSizeByBackendIdAndStorageMedium(backendId, storageMedium).stream()
                .map(Pair::key).collect(Collectors.toList());
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

    public void setPartitionCollectInfoMap(ImmutableMap<Long, PartitionCollectInfo> partitionCollectInfoMap) {
        this.partitionCollectInfoMap = partitionCollectInfoMap;
    }

    public Map<Long, Long> getReplicaToTabletMap() {
        return replicaToTabletMap;
    }

    // Only build from available bes, exclude colocate tables
    public Map<TStorageMedium, TreeMultimap<Long, PartitionBalanceInfo>> buildPartitionInfoBySkew(
            List<Long> availableBeIds, Map<Long, Pair<TabletMove, Long>> movesInProgress) {
        Set<Long> dbIds = Sets.newHashSet();
        Set<Long> tableIds = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();
        // Clone ut mocked env, but CatalogRecycleBin is not mockable (it extends from Thread)
        if (!FeConstants.runningUnitTest) {
            Env.getCurrentRecycleBin().getRecycleIds(dbIds, tableIds, partitionIds);
        }
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
                Pair<TabletMove, Long> movePair = movesInProgress.get(tabletId);
                TabletMove move = movePair != null ? movePair.first : null;
                // there exists move from fromBe to toBe
                if (move != null && beId == move.fromBe
                        && availableBeIds.contains(move.toBe)) {

                    // if movePair.second == -1, it means toBe hadn't added this tablet but it will add later;
                    // otherwise it means toBe had added this tablet
                    boolean toBeHadReplica = movePair.second != -1L;
                    if (toBeHadReplica) {
                        // toBe had add this tablet, fromBe just ignore this tablet
                        continue;
                    }

                    // later fromBe will delete this replica
                    // and toBe will add a replica
                    // so this replica should belong to toBe
                    beId = move.toBe;
                }

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
                            "table " + tabletMeta.getTableId() + " should not be the colocate table");

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
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(e.getMessage());
                    }
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

        @Override
        public String toString() {
            return "[partition=" + partitionId + ", index=" + indexId + ", replicaNum2BeId=" + beByReplicaCount + "]";
        }
    }

    // just for ut
    public Table<Long, Long, Replica> getReplicaMetaTable() {
        long stamp = readLock();
        try {
            return HashBasedTable.create(replicaMetaTable);
        } finally {
            readUnlock(stamp);
        }
    }

    // just for ut
    public Table<Long, Long, Replica> getBackingReplicaMetaTable() {
        long stamp = readLock();
        try {
            return HashBasedTable.create(backingReplicaMetaTable);
        } finally {
            readUnlock(stamp);
        }
    }

    // just for ut
    public Table<Long, Long, TabletMeta> getTabletMetaTable() {
        long stamp = readLock();
        try {
            return HashBasedTable.create(tabletMetaTable);
        } finally {
            readUnlock(stamp);
        }
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

    private boolean isLocal(TStorageMedium storageMedium) {
        return storageMedium == TStorageMedium.HDD || storageMedium == TStorageMedium.SSD;
    }

}
