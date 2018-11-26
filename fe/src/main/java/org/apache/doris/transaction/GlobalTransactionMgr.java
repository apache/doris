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

package org.apache.doris.transaction;

import org.apache.doris.alter.RollupJob;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.Load;
import org.apache.doris.persist.EditLog;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Transaction Manager
 * 1. begin
 * 2. commit
 * 3. abort
 * 
 * Attention: all api in txn manager should get db lock or load lock first, then get txn manager's lock, or there will be dead lock
 */
public class GlobalTransactionMgr {
    private static final Logger LOG = LogManager.getLogger(GlobalTransactionMgr.class);

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock(true);
    private EditLog editLog;

    // transactionId -> TransactionState
    private Map<Long, TransactionState> idToTransactionState;
    // db id -> (label -> txn id)
    private com.google.common.collect.Table<Long, String, Long> dbIdToTxnLabels; 
    private Map<Long, Integer> runningTxnNums;
    private TransactionIdGenerator idGenerator;
    
    private Catalog catalog;
    
    public GlobalTransactionMgr(Catalog catalog) {
        idToTransactionState = new HashMap<>();
        dbIdToTxnLabels = HashBasedTable.create();  
        runningTxnNums = Maps.newHashMap();
        this.catalog = catalog;
        this.idGenerator = new TransactionIdGenerator();
    }

    /**
     * the app could specify the transaction id
     *
     * @param coordinator
     * @throws BeginTransactionException
     * @throws IllegalTransactionParameterException
     */
    public long beginTransaction(long dbId, String label, String coordinator, LoadJobSourceType sourceType)
            throws AnalysisException, LabelAlreadyExistsException, BeginTransactionException {

        if (Config.disable_load_job) {
            throw new BeginTransactionException("disable_load_job is set to true, all load jobs are prevented");
        }
        
        writeLock();
        try {
            Preconditions.checkNotNull(coordinator);
            Preconditions.checkNotNull(label);
            FeNameFormat.checkLabel(label);
            Map<String, Long> txnLabels = dbIdToTxnLabels.row(dbId);
            if (txnLabels != null && txnLabels.containsKey(label)) {
                throw new LabelAlreadyExistsException("label already exists, label=" + label);
            }
            if (runningTxnNums.get(dbId) != null 
                    && runningTxnNums.get(dbId) > Config.max_running_txn_num_per_db) {
                throw new BeginTransactionException("current running txns on db " + dbId + " is " 
                    + runningTxnNums.get(dbId) + ", larger than limit " + Config.max_running_txn_num_per_db);
            }
            long tid = idGenerator.getNextTransactionId();
            LOG.debug("beginTransaction: tid {} with label {} from coordinator {}", tid, label, coordinator);
            TransactionState transactionState = new TransactionState(dbId, tid, label, sourceType, coordinator);
            transactionState.setPrepareTime(System.currentTimeMillis());
            unprotectUpsertTransactionState(transactionState);
            return tid;
        } finally {
            writeUnlock();
        }
    }

    public TransactionStatus getLabelState(long dbId, String label) {
        readLock();
        try {
            Map<String, Long> txnLabels = dbIdToTxnLabels.row(dbId);
            if (txnLabels == null) {
                return TransactionStatus.UNKNOWN;
            }
            Long transactionId = txnLabels.get(label);
            if (transactionId == null) {
                return TransactionStatus.UNKNOWN;
            }
            return idToTransactionState.get(transactionId).getTransactionStatus();
        } finally {
            readUnlock();
        }
    }

    public void deleteTransaction(long transactionId) {
        writeLock();
        try {
            TransactionState state = idToTransactionState.get(transactionId);
            if (state == null) {
                return;
            }
            editLog.logDeleteTransactionState(state);
            replayDeleteTransactionState(state);
        } finally {
            writeUnlock();
        }
    }

    /**
     * commit transaction process as follows：
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. update transaction state version
     * 5. persistent transactionState
     * 6. update nextVersion because of the failure of persistent transaction resulting in error version
     *
     * @param transactionId
     * @param tabletCommitInfos
     * @return
     * @throws MetaNotFoundException
     * @throws TransactionCommitFailedException
     * @note it is necessary to optimize the `lock` mechanism and `lock` scope resulting from wait lock long time
     * @note callers should get db.write lock before call this api
     */
    public void commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws MetaNotFoundException, TransactionCommitFailedException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }
        
        LOG.debug("try to commit transaction: {}", transactionId);
        if (tabletCommitInfos == null || tabletCommitInfos.isEmpty()) {
            throw new TransactionCommitFailedException("all partitions have no load data");
        }

        // 1. check status
        // the caller method already own db lock, we not obtain db lock here
        Database db = catalog.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }
        TransactionState transactionState = idToTransactionState.get(transactionId);
        if (transactionState == null 
                || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException("Transaction has already been cancelled");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            return;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            return;
        }
        
        TabletInvertedIndex tabletInvertedIndex = catalog.getTabletInvertedIndex();
        Map<Long, Set<Long>> tabletToBackends = new HashMap<>();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();
        // 2. validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if table or partition is dropped during load, the job is fail
        // if index is dropped, it does not matter
        for (TabletCommitInfo tabletCommitInfo : tabletCommitInfos) {
            long tabletId = tabletCommitInfo.getTabletId();
            long tableId = tabletInvertedIndex.getTableId(tabletId);
            if (TabletInvertedIndex.NOT_EXIST_VALUE == tableId) {
                throw new MetaNotFoundException("could not find table for tablet [" + tabletId + "]");
            }
            
            long partitionId = tabletInvertedIndex.getPartitionId(tabletId);
            if (TabletInvertedIndex.NOT_EXIST_VALUE == partitionId) {
                throw new MetaNotFoundException("could not find partition for tablet [" + tabletId + "]");
            }
            
            if (!tableToPartition.containsKey(tableId)) {
                tableToPartition.put(tableId, new HashSet<>());
            }
            tableToPartition.get(tableId).add(partitionId);
            if (!tabletToBackends.containsKey(tabletId)) {
                tabletToBackends.put(tabletId, new HashSet<>());
            }
            tabletToBackends.get(tabletId).add(tabletCommitInfo.getBackendId());
        }

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTable(tableId);
            for (Partition partition : table.getPartitions()) {
                if (!tableToPartition.get(tableId).contains(partition.getId())) {
                    continue;
                }
                List<MaterializedIndex> allIndices = new ArrayList<>();
                allIndices.addAll(partition.getMaterializedIndices());
                MaterializedIndex rollingUpIndex = null;
                RollupJob rollupJob = null;
                if (table.getState() == OlapTableState.ROLLUP) {
                    rollupJob = (RollupJob) catalog.getRollupHandler().getAlterJob(tableId);
                    rollingUpIndex = rollupJob.getRollupIndex(partition.getId());
                }
                // the rolling up index should also be taken care
                // if the rollup index failed during load, then set its last failed version
                // if rollup task finished, it should compare version and last failed version,
                // if version < last failed version, then the replica is failed
                if (rollingUpIndex != null) {
                    allIndices.add(rollingUpIndex);
                }
                MaterializedIndex baseIndex = partition.getBaseIndex();
                int quorumReplicaNum = table.getPartitionInfo().getReplicationNum(partition.getId()) / 2 + 1;
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        int successReplicaNum = 0;
                        long tabletId = tablet.getId();
                        Set<Long> tabletBackends = tablet.getBackendIds();
                        totalInvolvedBackends.addAll(tabletBackends);
                        Set<Long> commitBackends = tabletToBackends.get(tabletId);
                        for (long tabletBackend : tabletBackends) {
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, tabletBackend);
                            if (replica == null) {
                                throw new TransactionCommitFailedException("could not find replica for tablet [" 
                                                                           + tabletId + "], backend [" 
                                                                           + tabletBackend + "]");
                            }
                            // if the tablet have no replica's to commit or the tablet is a rolling up tablet, the commit backends maybe null
                            // if the commit backends is null, set all replicas as error replicas
                            if (commitBackends != null && commitBackends.contains(tabletBackend)) {
                                // if the backend load success but the backend has some errors previously, then it is not a normal replica
                                // ignore it but not log it
                                // for example, a replica is in clone state
                                if (replica.getLastFailedVersion() < 0) {
                                    ++successReplicaNum;
                                } else {
                                    // if this error replica is a base replica and it is under rollup
                                    // then remove the rollup task and rollup job will remove the rollup replica automatically
                                    // should remove here, because the error replicas not contains this base replica,
                                    // but it has errors in the past
                                    if (index.getId() == baseIndex.getId() && rollupJob != null) {
                                        LOG.info("the base replica [{}] has error, remove the related rollup replica from rollupjob [{}]", 
                                                replica, rollupJob);
                                        rollupJob.removeReplicaRelatedTask(partition.getId(), 
                                                tabletId, replica.getId(), replica.getBackendId());
                                    }
                                }
                            } else {
                                errorReplicaIds.add(replica.getId());
                                // not remove rollup task here, because the commit maybe failed
                                // remove rollup task when commit successfully
                            }
                        }
                        if (index.getState() != IndexState.ROLLUP && successReplicaNum < quorumReplicaNum) {
                            // not throw exception here, wait the upper application retry
                            LOG.info("Index [{}] success replica num is {} < quorum replica num {}", 
                                    index, successReplicaNum, quorumReplicaNum);
                            return;
                        }
                    }
                }
            }
        }

        writeLock();
        try {
            // transaction state is modified during check if the transaction could committed
            if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
                return;
            }
            // 4. update transaction state version
            transactionState.setCommitTime(System.currentTimeMillis());
            transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
            transactionState.setErrorReplicas(errorReplicaIds);
            for (long tableId : tableToPartition.keySet()) {
                TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
                for (long partitionId : tableToPartition.get(tableId)) {
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    Partition partition = table.getPartition(partitionId);
                    PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId, 
                                                                                      partition.getNextVersion(), 
                                                                                      partition.getNextVersionHash());
                    tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
                }
                transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
            }
            // 5. persistent transactionState
            unprotectUpsertTransactionState(transactionState);

            // add publish version tasks. set task to null as a placeholder.
            // tasks will be created when publishing version.
            for (long backendId : totalInvolvedBackends) {
                transactionState.addPublishVersionTask(backendId, null);
            }
        } finally {
            writeUnlock();
        }

        // 6. update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    public boolean commitAndPublishTransaction(Database db, long transactionId,
                                            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws MetaNotFoundException, TransactionCommitFailedException {
        db.writeLock();
        try {
            commitTransaction(db.getId(), transactionId, tabletCommitInfos);
        } finally {
            db.writeUnlock();
        }

        TransactionState transactionState = idToTransactionState.get(transactionId);
        switch (transactionState.getTransactionStatus()) {
            case COMMITTED:
            case VISIBLE:
                break;
            default:
                LOG.warn("transaction commit failed, db={}, txn={}", db.getFullName(), transactionId);
                throw new TransactionCommitFailedException("transaction commit failed");
        }

        long currentTimeMillis = System.currentTimeMillis();
        long timeoutTimeMillis = currentTimeMillis + timeoutMillis;
        while (currentTimeMillis < timeoutTimeMillis &&
                transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            try {
                transactionState.waitTransactionVisible(timeoutMillis);
            } catch (InterruptedException e) {
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        return transactionState.getTransactionStatus() == TransactionStatus.VISIBLE;
    }
    
    public void abortTransaction(long transactionId, String reason) throws UserException {
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job, ignore abort operation", transactionId);
            return;
        }
        writeLock();
        try {
            unprotectAbortTransaction(transactionId, reason);
        } catch (Exception exception) {
            LOG.info("transaction:[{}] reason:[{}] abort failure exception:{}", transactionId, reason, exception);
            throw exception;
        } finally {
            writeUnlock();
        }
        return;
    }
    
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        Preconditions.checkNotNull(label);
        writeLock();
        try {
            Map<String, Long> dbTxns = dbIdToTxnLabels.row(dbId);
            if (dbTxns == null) {
                throw new UserException("transaction not found, label=" + label);
            }
            Long transactionId = dbTxns.get(label);
            if (transactionId == null) {
                throw new UserException("transaction not found, label=" + label);
            }
            unprotectAbortTransaction(transactionId, reason);
        } finally {
            writeUnlock();
        }
    }

    /*
     * get all txns which is ready to publish
     * a ready-to-publish txn's partition's visible version should be ONE less than txn's commit version.
     */
    public List<TransactionState> getReadyToPublishTransactions() {
        List<TransactionState> readyPublishTransactionState = new ArrayList<>();
        List<TransactionState> allCommittedTransactionState = null;
        writeLock();
        try {
            // only send task to committed transaction
            allCommittedTransactionState = idToTransactionState.values().stream()
                .filter(transactionState -> (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED))
                .collect(Collectors.toList());
            for (TransactionState transactionState : allCommittedTransactionState) {
                long dbId = transactionState.getDbId();
                Database db = catalog.getDb(dbId);
                if (null == db) {
                    transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                    unprotectUpsertTransactionState(transactionState);
                    continue;
                }
            }
        } finally {
            writeUnlock();
        }

        for (TransactionState transactionState : allCommittedTransactionState) {
            boolean meetPublishPredicate = true;
            long dbId = transactionState.getDbId();
            Database db = catalog.getDb(dbId);
            if (null == db) {
                continue;
            }
            db.readLock();
            try {
                readLock();
                try {
                    for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
                        OlapTable table = (OlapTable) db.getTable(tableCommitInfo.getTableId());
                        if (null == table) {
                            LOG.warn("table {} is dropped after commit, ignore this table",
                                     tableCommitInfo.getTableId());
                            continue;
                        }
                        for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                            Partition partition = table.getPartition(partitionCommitInfo.getPartitionId());
                            if (null == partition) {
                                LOG.warn("partition {} is dropped after commit, ignore this partition",
                                         partitionCommitInfo.getPartitionId());
                                continue;
                            }
                            if (partitionCommitInfo.getVersion() != partition.getVisibleVersion() + 1) {
                                meetPublishPredicate = false;
                                break;
                            }
                        }
                        if (!meetPublishPredicate) {
                            break;
                        }
                    }
                    if (meetPublishPredicate) {
                        LOG.debug("transaction [{}] is ready to publish", transactionState);
                        readyPublishTransactionState.add(transactionState);
                    }
                } finally {
                    readUnlock();
                }
            } finally {
                db.readUnlock();
            }
        }
        return readyPublishTransactionState;
    }

    /**
     * if the table is deleted between commit and publish version, then should ignore the partition
     *
     * @param transactionId
     * @param errorReplicaIds
     * @return
     */
    public void finishTransaction(long transactionId, Set<Long> errorReplicaIds) {
        TransactionState transactionState = idToTransactionState.get(transactionId);
        // add all commit errors and publish errors to a single set
        if (errorReplicaIds == null) {
            errorReplicaIds = Sets.newHashSet();
        }
        Set<Long> originalErrorReplicas = transactionState.getErrorReplicas();
        if (originalErrorReplicas != null) {
            errorReplicaIds.addAll(originalErrorReplicas);
        }
         
        Database db = catalog.getDb(transactionState.getDbId());
        if (db == null) {
            writeLock();
            try {
                transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                transactionState.setReason("db is dropped");
                LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                unprotectUpsertTransactionState(transactionState);
                return;
            } finally {
                writeUnlock();
            }
        }
        db.writeLock();
        try {
            boolean hasError = false;
            for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
                long tableId = tableCommitInfo.getTableId();
                OlapTable table = (OlapTable) db.getTable(tableId);
                // table maybe dropped between commit and publish, ignore this error
                if (table == null) {
                    transactionState.removeTable(tableId);
                    LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}", 
                            tableId, 
                            transactionState);
                    continue;
                }
                PartitionInfo partitionInfo = table.getPartitionInfo();
                for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    // partition maybe dropped between commit and publish version, ignore this error
                    if (partition == null) {
                        tableCommitInfo.removePartition(partitionId);
                        LOG.warn("partition {} is dropped, skip version check and remove it from transaction state {}", 
                                partitionId, 
                                transactionState);
                        continue;
                    }
                    int quorumReplicaNum = partitionInfo.getReplicationNum(partitionId) / 2 + 1;
                    MaterializedIndex baseIndex = partition.getBaseIndex();
                    MaterializedIndex rollingUpIndex = null;
                    RollupJob rollupJob = null;
                    if (table.getState() == OlapTableState.ROLLUP) {
                        rollupJob = (RollupJob) catalog.getRollupHandler().getAlterJob(tableId);
                        rollingUpIndex = rollupJob.getRollupIndex(partitionId);
                    }
                    List<MaterializedIndex> allInices = new ArrayList<>();
                    allInices.addAll(partition.getMaterializedIndices());
                    if (rollingUpIndex != null) {
                        allInices.add(rollingUpIndex);
                    }
                    for (MaterializedIndex index : allInices) {
                        for (Tablet tablet : index.getTablets()) {
                            int healthReplicaNum = 0;
                            for (Replica replica : tablet.getReplicas()) {
                                if (!errorReplicaIds.contains(replica.getId()) 
                                        && replica.getLastFailedVersion() < 0) {
                                    // this means the replica is a healthy replica,
                                    // it is healthy in the past and does not have error in current load

                                    if (replica.checkVersionCatchUp(partition.getVisibleVersion(),
                                                                    partition.getVisibleVersionHash())) {
                                        // during rollup, the rollup replica's last failed version < 0,
                                        // it may be treated as a normal replica.
                                        // the replica is not failed during commit or publish
                                        // during upgrade, one replica's last version maybe invalid,
                                        // has to compare version hash.

                                        // Here we still update the replica's info even if we failed to publish
                                        // this txn, for the following case:
                                        // replica A,B,C is successfully committed, but only A is successfully
                                        // published,
                                        // B and C is crashed, now we need a Clone task to repair this tablet.
                                        // So, here we update A's version info, so that clone task will clone
                                        // the latest version of data.
                                        replica.updateVersionInfo(partitionCommitInfo.getVersion(),
                                                                  partitionCommitInfo.getVersionHash(),
                                                                  replica.getDataSize(), replica.getRowCount());
                                        ++healthReplicaNum;
                                    } else {
                                        // this means the replica has error in the past, but we did not observe it
                                        // during upgrade, one job maybe in quorum finished state, for example, A,B,C 3 replica
                                        // A,B 's version is 10, C's version is 10 but C' 10 is abnormal should be rollback
                                        // then we will detect this and set C's last failed version to 10 and last success version to 11
                                        // this logic has to be replayed in checkpoint thread
                                        replica.updateVersionInfo(replica.getVersion(), replica.getVersionHash(), 
                                                partition.getVisibleVersion(), partition.getVisibleVersionHash(), 
                                                partitionCommitInfo.getVersion(), partitionCommitInfo.getVersionHash());
                                        LOG.warn("transaction state {} has error, the replica [{}] not appeared in error replica list " 
                                                + " and its version not equal to partition commit version or commit version - 1" 
                                                + " if its not a upgrate stage, its a fatal error. ");
                                    }
                                } else if (replica.getVersion() == partitionCommitInfo.getVersion()
                                        && replica.getVersionHash() == partitionCommitInfo.getVersionHash()) {
                                    // the replica's version and version hash is equal to current transaction partition's version and version hash
                                    // the replica is normal, then remove it from error replica ids
                                    errorReplicaIds.remove(replica.getId());
                                    ++healthReplicaNum;
                                }
                                if (replica.getLastFailedVersion() > 0) {
                                    // if this error replica is a base replica and it is under rollup
                                    // then remove the rollup task and rollup job will remove the rollup replica automatically
                                    if (index.getId() == baseIndex.getId() && rollupJob != null) {
                                        LOG.info("base replica [{}] has errors during load, remove rollup task on related replica", replica);
                                        rollupJob.removeReplicaRelatedTask(partition.getId(), 
                                                tablet.getId(), replica.getId(), replica.getBackendId());
                                    }
                                }
                            }
                            if (index.getState() != IndexState.ROLLUP && healthReplicaNum < quorumReplicaNum) {
                                LOG.info("publish version failed for transaction {} on tablet {},  with only {} replicas less than quorum {}", 
                                        transactionState, tablet, healthReplicaNum, quorumReplicaNum);
                                hasError = true;
                            }
                        }
                    }
                }
            }
            if (hasError) {
                return;
            }
            writeLock();
            try {
                transactionState.setErrorReplicas(errorReplicaIds);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                unprotectUpsertTransactionState(transactionState);
            } finally {
                writeUnlock();
            }
            updateCatalogAfterVisible(transactionState, db);
        } finally {
            db.writeUnlock();
        }
        LOG.info("finish transaction {} successfully", transactionState);
        return;
    }

    // check if there exists a load job before the endTransactionId have all finished
    // load job maybe started but could not know the affected tableid, so that we not check by table
    public boolean hasPreviousTransactionsFinished(long endTransactionId, long dbId) {
        readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToTransactionState.entrySet()) {
                if (entry.getValue().getDbId() != dbId || !entry.getValue().isRunning()) {
                    continue;
                }
                if (entry.getKey() <= endTransactionId) {
                    return false;
                }
            }
        } finally {
            readUnlock();
        }
        return true;
    }

    /**
     * in this method should get db lock or load lock first then get txn manager lock , or there will be dead lock
     */
    public void removeOldTransactions() {
        long currentMillis = System.currentTimeMillis();

        // to avoid dead lock (transaction lock and load lock), we do this in 3 phases
        // 1. get all related db ids of txn in idToTransactionState
        Set<Long> dbIds = Sets.newHashSet();
        readLock();
        try {
            for (TransactionState transactionState : idToTransactionState.values()) {
                if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED
                        || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                    if ((currentMillis - transactionState.getFinishTime()) / 1000 > Config.label_keep_max_second) {
                        dbIds.add(transactionState.getDbId());
                    }
                } else {
                    // check if job is also deleted
                    // streaming insert stmt not add to fe load job, should use this method to
                    // recycle the timeout insert stmt load job
                    if (transactionState.getTransactionStatus() == TransactionStatus.PREPARE
                            && (currentMillis - transactionState.getPrepareTime())
                                    / 1000 > Config.stream_load_default_timeout_second) {
                        dbIds.add(transactionState.getDbId());
                    }
                }
            }
        } finally {
            readUnlock();
        }

        // 2. get all load jobs' txn id of these databases
        Map<Long, Set<Long>> dbIdToTxnIds = Maps.newHashMap();
        Load loadInstance = Catalog.getCurrentCatalog().getLoadInstance();
        for (Long dbId : dbIds) {
            Set<Long> txnIds = loadInstance.getTxnIdsByDb(dbId);
            dbIdToTxnIds.put(dbId, txnIds);
        }

        // 3. use dbIdToTxnIds to remove old transactions, without holding load locks again
        writeLock();
        try {
            List<Long> transactionsToDelete = Lists.newArrayList();
            for (TransactionState transactionState : idToTransactionState.values()) {
                if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED 
                        || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                    if ((currentMillis - transactionState.getFinishTime()) / 1000 > Config.label_keep_max_second) {
                        // if this txn is not from front end then delete it immediately
                        // if this txn is from front end but could not find in job list, then delete it immediately
                        if (transactionState.getSourceType() != LoadJobSourceType.FRONTEND
                                || !checkTxnHasRelatedJob(transactionState, dbIdToTxnIds)) {
                            transactionsToDelete.add(transactionState.getTransactionId());
                        }
                    }
                } else {
                    // check if job is also deleted
                    // streaming insert stmt not add to fe load job, should use this method to
                    // recycle the timeout insert stmt load job
                    if (transactionState.getTransactionStatus() == TransactionStatus.PREPARE
                            && (currentMillis - transactionState.getPrepareTime()) / 1000 > Config.stream_load_default_timeout_second) {
                        if (transactionState.getSourceType() != LoadJobSourceType.FRONTEND
                                || !checkTxnHasRelatedJob(transactionState, dbIdToTxnIds)) {
                            transactionState.setFinishTime(System.currentTimeMillis());
                            transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                            transactionState.setReason("transaction is timeout and is cancelled automatically");
                            unprotectUpsertTransactionState(transactionState);
                        }
                    }
                }
            }

            for (Long transId : transactionsToDelete) {
                deleteTransaction(transId);
                LOG.info("transaction [" + transId + "] is expired, remove it from transaction table");
            }
        } finally {
            writeUnlock();
        }
    }
    
    private boolean checkTxnHasRelatedJob(TransactionState txnState, Map<Long, Set<Long>> dbIdToTxnIds) {
        Set<Long> txnIds = dbIdToTxnIds.get(txnState.getDbId());
        if (txnIds == null) {
            // We can't find the related load job of this database.
            // But dbIdToTxnIds is not a up-to-date results.
            // So we return true to assume that we find a related load job, to avoid mistaken delete
            return true;
        }

        return txnIds.contains(txnState.getTransactionId());
    }

    public TransactionState getTransactionState(long transactionId) {
        readLock();
        try {
            return idToTransactionState.get(transactionId);
        } finally {
            readUnlock();
        }
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
        this.idGenerator.setEditLog(editLog);
    }

    private void readLock() {
        this.transactionLock.readLock().lock();
    }

    private void readUnlock() {
        this.transactionLock.readLock().unlock();
    }

    private void writeLock() {
        this.transactionLock.writeLock().lock();
    }

    private void writeUnlock() {
        this.transactionLock.writeLock().unlock();
    }

    // for add/update/delete TransactionState
    private void unprotectUpsertTransactionState(TransactionState transactionState) {
        editLog.logInsertTransactionState(transactionState);
        idToTransactionState.put(transactionState.getTransactionId(), transactionState);
        updateTxnLabels(transactionState);
        updateDBRunningTxnNum(transactionState.getPreStatus(), transactionState);
    }
    
    private void unprotectAbortTransaction(long transactionId, String reason) throws UserException {
        TransactionState transactionState = idToTransactionState.get(transactionId);
        if (transactionState == null) {
            throw new UserException("transaction not found");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            return;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            throw new UserException("transaction's state is already committed or visible, could not abort");
        }
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.setReason(reason);
        transactionState.setTransactionStatus(TransactionStatus.ABORTED);
        unprotectUpsertTransactionState(transactionState);
        for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
        }
    }
    
    // for replay idToTransactionState
    // check point also run transaction cleaner, the cleaner maybe concurrently modify id to 
    public void replayUpsertTransactionState(TransactionState transactionState) {
        writeLock();
        try {
            Database db = catalog.getDb(transactionState.getDbId());
            if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                LOG.debug("replay a committed transaction {}", transactionState);
                updateCatalogAfterCommitted(transactionState, db);
            } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.debug("replay a visible transaction {}", transactionState);
                updateCatalogAfterVisible(transactionState, db);
            }
            TransactionState preTxnState = idToTransactionState.get(transactionState.getTransactionId());
            idToTransactionState.put(transactionState.getTransactionId(), transactionState);
            updateTxnLabels(transactionState);
            updateDBRunningTxnNum(preTxnState == null ? null : preTxnState.getTransactionStatus(), 
                    transactionState);
        } finally {
            writeUnlock();
        }
    }

    public void replayDeleteTransactionState(TransactionState transactionState) {
        writeLock();
        try {
            idToTransactionState.remove(transactionState.getTransactionId());
            dbIdToTxnLabels.remove(transactionState.getDbId(), transactionState.getLabel());
        } finally {
            writeUnlock();
        }
    }
    
    private void updateCatalogAfterCommitted(TransactionState transactionState, Database db) {
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTable(tableId);
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                List<MaterializedIndex> allIndices = new ArrayList<>();
                allIndices.addAll(partition.getMaterializedIndices());
                MaterializedIndex baseIndex = partition.getBaseIndex();
                MaterializedIndex rollingUpIndex = null;
                RollupJob rollupJob = null;
                if (table.getState() == OlapTableState.ROLLUP) {
                    rollupJob = (RollupJob) catalog.getRollupHandler().getAlterJob(tableId);
                    rollingUpIndex = rollupJob.getRollupIndex(partition.getId());
                }
                if (rollingUpIndex != null) {
                    allIndices.add(rollingUpIndex);
                }
                for (MaterializedIndex index : allIndices) {
                    List<Tablet> tablets = index.getTablets();
                    for (Tablet tablet : tablets) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (errorReplicaIds.contains(replica.getId())) {
                                // should not use partition.getNextVersion and partition.getNextVersionHash because partition's next version hash is generated locally
                                // should get from transaction state
                                replica.updateLastFailedVersion(partitionCommitInfo.getVersion(), 
                                        partitionCommitInfo.getVersionHash());
                                // if this error replica is a base replica and it is under rollup
                                // then remove the rollup task and rollup job will remove the rollup replica automatically
                                if (index.getId() == baseIndex.getId() && rollupJob != null) {
                                    LOG.debug("the base replica [{}] has error, remove the related rollup replica from rollupjob [{}]", 
                                            replica, rollupJob);
                                    rollupJob.removeReplicaRelatedTask(partition.getId(), 
                                            tablet.getId(), replica.getId(), replica.getBackendId());
                                }
                            }
                        }
                    }
                }
                partition.setNextVersion(partition.getNextVersion() + 1);
                // Although committed version(hash) is not visible to user,
                // but they need to be synchronized among Frontends.
                // because we use committed version(hash) to create clone task, if the first Master FE
                // send clone task with committed version hash X, and than Master changed, the new Master FE
                // received the clone task report with version hash X, which not equals to it own committed
                // version hash, than the clone task is failed.
                partition.setNextVersionHash(Util.generateVersionHash() /* next version hash */,
                                             partitionCommitInfo.getVersionHash() /* committed version hash*/);
            }
        }
    }
    
    private boolean updateCatalogAfterVisible(TransactionState transactionState, Database db) {
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTable(tableId);
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                long newCommitVersion = partitionCommitInfo.getVersion();
                long newCommitVersionHash = partitionCommitInfo.getVersionHash();
                Partition partition = table.getPartition(partitionId);
                MaterializedIndex baseIndex = partition.getBaseIndex();
                MaterializedIndex rollingUpIndex = null;
                RollupJob rollupJob = null;
                if (table.getState() == OlapTableState.ROLLUP) {
                    rollupJob = (RollupJob) catalog.getRollupHandler().getAlterJob(tableId);
                    rollingUpIndex = rollupJob.getRollupIndex(partitionId);
                }
                List<MaterializedIndex> allIndices = new ArrayList<>();
                allIndices.addAll(partition.getMaterializedIndices());
                if (rollingUpIndex != null) {
                    allIndices.add(rollingUpIndex);
                }
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            long lastFailedVersion = replica.getLastFailedVersion();
                            long lastFailedVersionHash = replica.getLastFailedVersionHash();
                            long newVersion = newCommitVersion;
                            long newVersionHash = newCommitVersionHash;
                            long lastSucessVersion = replica.getLastSuccessVersion();
                            long lastSuccessVersionHash = replica.getLastSuccessVersionHash();
                            if (!errorReplicaIds.contains(replica.getId())) {
                                if (replica.getLastFailedVersion() > 0) {
                                    // if the replica is a failed replica, then not changing version and version hash
                                    newVersion = replica.getVersion();
                                    newVersionHash = replica.getVersionHash();
                                } else if (!replica.checkVersionCatchUp(partition.getVisibleVersion(),
                                                                        partition.getVisibleVersionHash())) {
                                    // this means the replica has error in the past, but we did not observe it
                                    // during upgrade, one job maybe in quorum finished state, for example, A,B,C 3 replica
                                    // A,B 's version is 10, C's version is 10 but C' 10 is abnormal should be rollback
                                    // then we will detect this and set C's last failed version to 10 and last success version to 11
                                    // this logic has to be replayed in checkpoint thread
                                    lastFailedVersion = partition.getVisibleVersion();
                                    lastFailedVersionHash = partition.getVisibleVersionHash();
                                    newVersion = replica.getVersion();
                                    newVersionHash = replica.getVersionHash();
                                }

                                // success version always move forward
                                lastSucessVersion = newCommitVersion;
                                lastSuccessVersionHash = newCommitVersionHash;
                            } else {
                                // for example, A,B,C 3 replicas, B,C failed during publish version, then B C will be set abnormal
                                // all loading will failed, B,C will have to recovery by clone, it is very inefficient and maybe lost data
                                // Using this method, B,C will publish failed, and fe will publish again, not update their last failed version
                                // if B is publish successfully in next turn, then B is normal and C will be set abnormal so that quorum is maintained
                                // and loading will go on.
                                newVersion = replica.getVersion();
                                newVersionHash = replica.getVersionHash();
                                if (newCommitVersion > lastFailedVersion) {
                                    lastFailedVersion = newCommitVersion;
                                    lastFailedVersionHash = newCommitVersionHash;
                                }
                            }
                            replica.updateVersionInfo(newVersion, newVersionHash, lastFailedVersion, lastFailedVersionHash, lastSucessVersion, lastSuccessVersionHash);
                            // if this error replica is a base replica and it is under rollup
                            // then remove the rollup task and rollup job will remove the rollup replica automatically
                            if (index.getId() == baseIndex.getId() 
                                    && replica.getLastFailedVersion() > 0 
                                    && rollupJob != null) {
                                LOG.debug("base replica [{}] has errors during load, remove rollup task on related replica", replica);
                                rollupJob.removeReplicaRelatedTask(partition.getId(), 
                                        tablet.getId(), replica.getId(), replica.getBackendId());
                            }
                        }
                    }
                } // end for indices
                long version = partitionCommitInfo.getVersion();
                long versionHash = partitionCommitInfo.getVersionHash();
                partition.updateVisibleVersionAndVersionHash(version, versionHash);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("transaction state {} set partition's version to [{}] and version hash to [{}]", 
                            transactionState, version, versionHash);
                }
            }
        }
        return true;
    }
    
    private void updateTxnLabels(TransactionState transactionState) {
        // if the transaction is aborted, then its label could be reused
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            dbIdToTxnLabels.remove(transactionState.getDbId(), transactionState.getLabel());
        } else {
            dbIdToTxnLabels.put(transactionState.getDbId(), transactionState.getLabel(), 
                    transactionState.getTransactionId());
        }
    }
    
    private void updateDBRunningTxnNum(TransactionStatus preStatus, TransactionState curTxnState) {
        int dbRunningTxnNum = 0;
        if (runningTxnNums.get(curTxnState.getDbId()) != null) {
            dbRunningTxnNum = runningTxnNums.get(curTxnState.getDbId());
        }
        if (preStatus == null 
                && (curTxnState.getTransactionStatus() == TransactionStatus.PREPARE
                    || curTxnState.getTransactionStatus() == TransactionStatus.COMMITTED)) {
            ++dbRunningTxnNum;
            runningTxnNums.put(curTxnState.getDbId(), dbRunningTxnNum);
        } else if (preStatus != null
                && (preStatus == TransactionStatus.PREPARE
                    || preStatus == TransactionStatus.COMMITTED)
                && (curTxnState.getTransactionStatus() == TransactionStatus.VISIBLE
                    || curTxnState.getTransactionStatus() == TransactionStatus.ABORTED)) {
            --dbRunningTxnNum;
            if (dbRunningTxnNum < 1) {
                runningTxnNums.remove(curTxnState.getDbId());
            } else {
                runningTxnNums.put(curTxnState.getDbId(), dbRunningTxnNum);
            }
        }
    }

    public List<List<Comparable>> getDbInfo() {
        List<List<Comparable>> infos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            Set<Long> dbIds = new HashSet<>();
            for (TransactionState transactionState : idToTransactionState.values()) {
                dbIds.add(transactionState.getDbId());
            }
            for (long dbId : dbIds) {
                List<Comparable> info = new ArrayList<Comparable>();
                info.add(dbId);
                Database db = Catalog.getInstance().getDb(dbId);
                if (db == null) {
                    continue;
                }
                info.add(db.getFullName());
                infos.add(info);
            }
        } finally {
            readUnlock();
        }
        return infos;
    }

    public List<List<Comparable>> getDbTransInfo(long dbId) throws AnalysisException {
        List<List<Comparable>> infos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                throw new AnalysisException("Database[" + dbId + "] does not exist");
            }
            idToTransactionState.values().stream()
                    .filter(t -> t.getDbId() == dbId)
                    .forEach(t -> {
                        List<Comparable> info = new ArrayList<Comparable>();
                        info.add(t.getTransactionId());
                        info.add(t.getLabel());
                        info.add(t.getCoordinator());
                        info.add(t.getTransactionStatus());
                        info.add(t.getSourceType());
                        info.add(TimeUtils.longToTimeString(t.getPrepareTime()));
                        info.add(TimeUtils.longToTimeString(t.getCommitTime()));
                        info.add(TimeUtils.longToTimeString(t.getFinishTime()));
                        info.add(t.getReason());
                        info.add(t.getErrorReplicas().size());
                        infos.add(info);
                    });
        } finally {
            readUnlock();
        }
        return infos;
    }

    public List<List<Comparable>> getTableTransInfo(long tid, Database db) throws AnalysisException {
        List<List<Comparable>> tableInfos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            TransactionState transactionState = idToTransactionState.get(tid);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + tid + "] does not exist.");
            }
            db.readLock();
            try {
                for (long tableId : transactionState.getIdToTableCommitInfos().keySet()) {
                    List<Comparable> tableInfo = new ArrayList<Comparable>();
                    Table table = db.getTable(tableId);
                    if (null == table) {
                        throw new AnalysisException("Table[" + tableId + "] does not exist.");
                    }
                    int partitionNum = 1;
                    if (table.getType() == Table.TableType.OLAP) {
                        OlapTable olapTable = (OlapTable) table;
                        tableInfo.add(table.getId());
                        tableInfo.add(table.getName());
                        tableInfo.add(partitionNum);
                        tableInfo.add(olapTable.getState());
                        tableInfos.add(tableInfo);
                    }
                }
            } finally {
                db.readUnlock();
            }
        } finally {
            readUnlock();
        }
        return tableInfos;
    }

    public List<List<Comparable>> getPartitionTransInfo(long tid, Database db, OlapTable olapTable)
            throws AnalysisException {
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            TransactionState transactionState = idToTransactionState.get(tid);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + tid + "] does not exist.");
            }
            TableCommitInfo tableCommitInfo = transactionState.getIdToTableCommitInfos().get(olapTable.getId());
            db.readLock();
            try {
                Map<Long, PartitionCommitInfo> idToPartitionCommitInfo = tableCommitInfo.getIdToPartitionCommitInfo();
                for (long partitionId : idToPartitionCommitInfo.keySet()) {
                    Partition partition = olapTable.getPartition(partitionId);
                    List<Comparable> partitionInfo = new ArrayList<Comparable>();
                    String partitionName = partition.getName();
                    partitionInfo.add(partitionId);
                    partitionInfo.add(partitionName);
                    PartitionCommitInfo partitionCommitInfo = idToPartitionCommitInfo.get(partitionId);
                    partitionInfo.add(partitionCommitInfo.getVersion());
                    partitionInfo.add(partitionCommitInfo.getVersionHash());
                    partitionInfo.add(partition.getState());
                    partitionInfos.add(partitionInfo);
                }
            } finally {
                db.readUnlock();
            }
        } finally {
            readUnlock();
        }
        return partitionInfos;
    }
    
    public int getTransactionNum() {
        return this.idToTransactionState.size();
    }
    
    public TransactionIdGenerator getTransactionIDGenerator() {
        return this.idGenerator;
    }
    
    // this two function used to read snapshot or write snapshot
    public void write(DataOutput out) throws IOException {
        int numTransactions = idToTransactionState.size();
        out.writeInt(numTransactions);
        for (Map.Entry<Long, TransactionState> entry : idToTransactionState.entrySet()) {
            entry.getValue().write(out);
        }
        idGenerator.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        int numTransactions = in.readInt();
        for (int i = 0; i < numTransactions; ++i) {
            TransactionState transactionState = new TransactionState();
            transactionState.readFields(in);
            TransactionState preTxnState = idToTransactionState.get(transactionState.getTransactionId());
            idToTransactionState.put(transactionState.getTransactionId(), transactionState);
            updateTxnLabels(transactionState);
            updateDBRunningTxnNum(preTxnState == null ? null : preTxnState.getTransactionStatus(), 
                    transactionState);
        }
        idGenerator.readFields(in);
    }
}
