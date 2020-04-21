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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transaction Manager
 * 1. begin
 * 2. commit
 * 3. abort
 * Attention: all api in txn manager should get db lock or load lock first, then get txn manager's lock, or there will be dead lock
 */
public class GlobalTransactionMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(GlobalTransactionMgr.class);

    private Map<Long, DatabaseTransactionMgr> dbIdToDatabaseTransactionMgrs = Maps.newConcurrentMap();

    private EditLog editLog;

    private TransactionIdGenerator idGenerator = new TransactionIdGenerator();
    private TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();
    
    private Catalog catalog;

    public GlobalTransactionMgr(Catalog catalog) {
        this.catalog = catalog;
    }
    
    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    public DatabaseTransactionMgr getDatabaseTransactioMgr(long dbId) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTransactionMgr == null) {
            throw new NullPointerException("databaseTransactionMgr[" + dbId + "] does not exist");
        }
        return dbTransactionMgr;
    }

    public void addDatabaseTransactionMgr(Long dbId) {
        dbIdToDatabaseTransactionMgrs.put(dbId, new DatabaseTransactionMgr(editLog));
    }

    public void removeDatabaseTransactionMgr(Long dbId) {
        dbIdToDatabaseTransactionMgrs.remove(dbId);
    }

    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator, LoadJobSourceType sourceType,
            long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }
    
    /**
     * the app could specify the transaction id
     * 
     * requestId is used to judge that whether the request is a internal retry request
     * if label already exist, and requestId are equal, we return the exist tid, and consider this 'begin'
     * as success.
     * requestId == null is for compatibility
     *
     * @param coordinator
     * @throws BeginTransactionException
     * @throws DuplicatedRequestException
     * @throws IllegalTransactionParameterException
     */
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
                                 TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException {

        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }
        
        if (timeoutSecond > Config.max_load_timeout_second ||
                timeoutSecond < Config.min_load_timeout_second) {
            throw new AnalysisException("Invalid timeout. Timeout should between "
                    + Config.min_load_timeout_second + " and " + Config.max_load_timeout_second
                    + " seconds");
        }

        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);

        dbTransactionMgr.writeLock();
        try {
            Preconditions.checkNotNull(coordinator);
            Preconditions.checkNotNull(label);
            FeNameFormat.checkLabel(label);

            /*
             * Check if label already used, by following steps
             * 1. get all existing transactions
             * 2. if there is a PREPARE transaction, check if this is a retry request. If yes, return the
             *    existing txn id.
             * 3. if there is a non-aborted transaction, throw label already used exception.
             */
            Set<Long> existingTxnIds = dbTransactionMgr.getTxnIdsByLabel(label);
            if (existingTxnIds != null && !existingTxnIds.isEmpty()) {
                List<TransactionState> notAbortedTxns = Lists.newArrayList();
                for (long txnId : existingTxnIds) {
                    TransactionState txn = dbTransactionMgr.getTransactionState(txnId);
                    Preconditions.checkNotNull(txn);
                    if (txn.getTransactionStatus() != TransactionStatus.ABORTED) {
                        notAbortedTxns.add(txn);
                    }
                }
                // there should be at most 1 txn in PREPARE/COMMITTED/VISIBLE status
                Preconditions.checkState(notAbortedTxns.size() <= 1, notAbortedTxns);
                if (!notAbortedTxns.isEmpty()) {
                    TransactionState notAbortedTxn = notAbortedTxns.get(0);
                    if (requestId != null && notAbortedTxn.getTransactionStatus() == TransactionStatus.PREPARE
                            && notAbortedTxn.getRequsetId() != null && notAbortedTxn.getRequsetId().equals(requestId)) {
                        // this may be a retry request for same job, just return existing txn id.
                        throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                                notAbortedTxn.getTransactionId(), "");
                    }
                    throw new LabelAlreadyUsedException(label, notAbortedTxn.getTransactionStatus());
                }
            }

            checkRunningTxnExceedLimit(dbTransactionMgr, dbId, sourceType);
          
            long tid = idGenerator.getNextTransactionId();
            LOG.info("begin transaction: txn id {} with label {} from coordinator {}", tid, label, coordinator);
            TransactionState transactionState = new TransactionState(dbId, tableIdList, tid, label, requestId, sourceType,
                    coordinator, listenerId, timeoutSecond * 1000);
            transactionState.setPrepareTime(System.currentTimeMillis());
            dbTransactionMgr.unprotectUpsertTransactionState(transactionState, false);

            if (MetricRepo.isInit.get()) {
                MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
            }

            return tid;
        } catch (DuplicatedRequestException e) {
            throw e;
        } catch (Exception e) {
            if (MetricRepo.isInit.get()) {
                MetricRepo.COUNTER_TXN_REJECT.increase(1L);
            }
            throw e;
        } finally {
            dbTransactionMgr.writeUnlock();
        }
    }
    
    private void checkRunningTxnExceedLimit(DatabaseTransactionMgr dbTransactionMgr, Long dbId, LoadJobSourceType sourceType) throws BeginTransactionException {
        switch (sourceType) {
            case ROUTINE_LOAD_TASK:
                // no need to check limit for routine load task:
                // 1. the number of running routine load tasks is limited by Config.max_routine_load_task_num_per_be
                // 2. if we add routine load txn to runningTxnNums, runningTxnNums will always be occupied by routine load,
                //    and other txn may not be able to submitted.
                break;
            default:
                if (dbTransactionMgr.getRunningTxnNums() >= Config.max_running_txn_num_per_db) {
                    throw new BeginTransactionException("current running txns on db " + dbId + " is "
                            + dbTransactionMgr.getRunningTxnNums() + ", larger than limit " + Config.max_running_txn_num_per_db);
                }
                break;
        }
    }

    public TransactionStatus getLabelState(long dbId, String label) {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);

        dbTransactionMgr.readLock();
        try {
            Set<Long> existingTxnIds = dbTransactionMgr.getTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return TransactionStatus.UNKNOWN;
            }
            // find the latest txn (which id is largest)
            long maxTxnId = existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).get();
            return dbTransactionMgr.getTransactionState(maxTxnId).getTransactionStatus();
        } finally {
            dbTransactionMgr.readUnlock();
        }
    }

    public void commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransaction(dbId, transactionId, tabletCommitInfos, null);
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
     * @throws UserException
     * @throws TransactionCommitFailedException
     * @note it is necessary to optimize the `lock` mechanism and `lock` scope resulting from wait lock long time
     * @note callers should get db.write lock before call this api
     */
    public void commitTransaction(long dbId, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                  TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }
        
        LOG.debug("try to commit transaction: {}", transactionId);
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = catalog.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        TransactionState transactionState = null;
        dbTransactionMgr.readLock();
        try {
            transactionState = dbTransactionMgr.getTransactionState(transactionId);
        } finally {
            dbTransactionMgr.readUnlock();
        }
        if (transactionState == null
                || transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionCommitFailedException(
                    transactionState == null ? "transaction not found" : transactionState.getReason());
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            LOG.debug("transaction is already visible: {}", transactionId);
            return;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            LOG.debug("transaction is already committed: {}", transactionId);
            return;
        }
        
        if (tabletCommitInfos == null || tabletCommitInfos.isEmpty()) {
            throw new TransactionCommitFailedException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }
        
        TabletInvertedIndex tabletInvertedIndex = catalog.getTabletInvertedIndex();
        Map<Long, Set<Long>> tabletToBackends = new HashMap<>();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();
        // 2. validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if index is dropped, it does not matter.
        // if table or partition is dropped during load, just ignore that tablet,
        // because we should allow dropping rollup or partition during load
        for (TabletCommitInfo tabletCommitInfo : tabletCommitInfos) {
            long tabletId = tabletCommitInfo.getTabletId();
            long tableId = tabletInvertedIndex.getTableId(tabletId);
            OlapTable tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                // this can happen when tableId == -1 (tablet being dropping)
                // or table really not exist.
                continue;
            }

            if (tbl.getState() == OlapTableState.RESTORE) {
                throw new LoadException("Table " + tbl.getName() + " is in restore process. "
                        + "Can not load into it");
            }

            long partitionId = tabletInvertedIndex.getPartitionId(tabletId);
            if (tbl.getPartition(partitionId) == null) {
                // this can happen when partitionId == -1 (tablet being dropping)
                // or partition really not exist.
                continue;
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
        
        if (tableToPartition.isEmpty()) {
            // table or all partitions are being dropped
            throw new TransactionCommitFailedException(TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
        }

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                throw new MetaNotFoundException("Table does not exist: " + tableId);
            }
            for (Partition partition : table.getAllPartitions()) {
                if (!tableToPartition.get(tableId).contains(partition.getId())) {
                    continue;
                }

                List<MaterializedIndex> allIndices;
                if (transactionState.getLoadedTblIndexes().isEmpty()) {
                    allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
                } else {
                    allIndices = Lists.newArrayList();
                    for (long indexId : transactionState.getLoadedTblIndexes().get(tableId)) {
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index != null) {
                            allIndices.add(index);
                        }
                    }
                }

                if (table.getState() == OlapTableState.ROLLUP || table.getState() == OlapTableState.SCHEMA_CHANGE) {
                    /*
                     * This is just a optimization that do our best to not let publish version tasks
                     * timeout if table is under rollup or schema change. Because with a short
                     * timeout, a replica's publish version task is more likely to fail. And if
                     * quorum replicas of a tablet fail to publish, the alter job will fail.
                     * 
                     * If the table is not under rollup or schema change, the failure of a replica's
                     * publish version task has a minor effect because the replica can be repaired
                     * by tablet repair process very soon. But the tablet repair process will not
                     * repair rollup replicas.
                     * 
                     * This a kind of best-effort-optimization, if FE restart after commit and
                     * before publish, this 'prolong' information will be lost.
                     */
                    transactionState.prolongPublishTimeout();
                }

                int quorumReplicaNum = table.getPartitionInfo().getReplicationNum(partition.getId()) / 2 + 1;
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        int successReplicaNum = 0;
                        long tabletId = tablet.getId();
                        Set<Long> tabletBackends = tablet.getBackendIds();
                        totalInvolvedBackends.addAll(tabletBackends);
                        Set<Long> commitBackends = tabletToBackends.get(tabletId);
                        // save the error replica ids for current tablet
                        // this param is used for log
                        Set<Long> errorBackendIdsForTablet = Sets.newHashSet();
                        for (long tabletBackend : tabletBackends) {
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, tabletBackend);
                            if (replica == null) {
                                throw new TransactionCommitFailedException("could not find replica for tablet ["
                                        + tabletId + "], backend [" + tabletBackend + "]");
                            }
                            // if the tablet have no replica's to commit or the tablet is a rolling up tablet, the commit backends maybe null
                            // if the commit backends is null, set all replicas as error replicas
                            if (commitBackends != null && commitBackends.contains(tabletBackend)) {
                                // if the backend load success but the backend has some errors previously, then it is not a normal replica
                                // ignore it but not log it
                                // for example, a replica is in clone state
                                if (replica.getLastFailedVersion() < 0) {
                                    ++successReplicaNum;
                                }
                            } else {
                                errorBackendIdsForTablet.add(tabletBackend);
                                errorReplicaIds.add(replica.getId());
                                // not remove rollup task here, because the commit maybe failed
                                // remove rollup task when commit successfully
                            }
                        }

                        if (successReplicaNum < quorumReplicaNum) {
                            LOG.warn("Failed to commit txn [{}]. "
                                             + "Tablet [{}] success replica num is {} < quorum replica num {} "
                                             + "while error backends {}",
                                     transactionId, tablet.getId(), successReplicaNum, quorumReplicaNum,
                                     Joiner.on(",").join(errorBackendIdsForTablet));
                            throw new TabletQuorumFailedException(transactionId, tablet.getId(),
                                                                  successReplicaNum, quorumReplicaNum,
                                                                  errorBackendIdsForTablet);
                        }
                    }
                }
            }
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        dbTransactionMgr.writeLock();
        try {
            dbTransactionMgr.unprotectedCommitTransaction(transactionState, errorReplicaIds, tableToPartition, totalInvolvedBackends,
                                         db);
            txnOperated = true;
        } finally {
            dbTransactionMgr.writeUnlock();
            // after state transform
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
        }
        
        // 6. update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }
    
    public boolean commitAndPublishTransaction(Database db, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, transactionId, tabletCommitInfos, timeoutMillis, null);
    }
    
    public boolean commitAndPublishTransaction(Database db, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        db.writeLock();
        try {
            commitTransaction(db.getId(), transactionId, tabletCommitInfos, txnCommitAttachment);
        } finally {
            db.writeUnlock();
        }
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(db.getId());
        TransactionState transactionState = null;
        dbTransactionMgr.readLock();
        try {
            transactionState = dbTransactionMgr.getTransactionState(transactionId);
        } finally {
            dbTransactionMgr.readUnlock();
        }

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

    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException {
        abortTransaction(transactionId, reason, null);
    }

    public void abortTransaction(Long dbId, Long txnId, String reason, TxnCommitAttachment txnCommitAttachment) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        dbTransactionMgr.abortTransaction(txnId, reason, txnCommitAttachment);
    }

    // for http cancel stream load api
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        Preconditions.checkNotNull(label);
        Long transactionId = null;
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);

        dbTransactionMgr.readLock();
        try {
            Set<Long> existingTxns = dbTransactionMgr.getTxnIdsByLabel(label);
            if (existingTxns == null || existingTxns.isEmpty()) {
                throw new UserException("transaction not found, label=" + label);
            }
            // find PREPARE txn. For one load label, there should be only one PREPARE txn.
            TransactionState prepareTxn = null;
            for (Long txnId : existingTxns) {
                TransactionState txn = dbTransactionMgr.getTransactionState(txnId);
                if (txn.getTransactionStatus() == TransactionStatus.PREPARE) {
                    prepareTxn = txn;
                    break;
                }
            }

            if (prepareTxn == null) {
                throw new UserException("running transaction not found, label=" + label);
            }

            transactionId = prepareTxn.getTransactionId();
        } finally {
            dbTransactionMgr.readUnlock();
        }
        dbTransactionMgr.abortTransaction(transactionId, reason, null);
    }

    /*
     * get all txns which is ready to publish
     * a ready-to-publish txn's partition's visible version should be ONE less than txn's commit version.
     */
    public List<TransactionState> getReadyToPublishTransactions() throws UserException {
        List<TransactionState> transactionStateList = Lists.newArrayList();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            try {
                dbTransactionMgr.readLock();
                // only send task to committed transaction
                List<TransactionState> committedTxnList = dbTransactionMgr.getIdToRunningTransactionState().values().stream()
                        .filter(transactionState -> (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED))
                        .sorted(Comparator.comparing(TransactionState::getCommitTime))
                        .collect(Collectors.toList());
                transactionStateList.addAll(committedTxnList);
            } finally {
                dbTransactionMgr.readUnlock();
            }
        }
        return transactionStateList;
    }
    
    /**
     * if the table is deleted between commit and publish version, then should ignore the partition
     *
     * @param transactionId
     * @param errorReplicaIds
     * @return
     */
    public void finishTransaction(long dbId, long transactionId, Set<Long> errorReplicaIds) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        dbTransactionMgr.readLock();
        TransactionState transactionState = null;
        try {
            transactionState = dbTransactionMgr.getTransactionState(transactionId);
        } finally {
            dbTransactionMgr.readUnlock();
        }
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
            dbTransactionMgr.writeLock();
            try {
                transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                transactionState.setReason("db is dropped");
                LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                dbTransactionMgr.unprotectUpsertTransactionState(transactionState, false);
                return;
            } finally {
                dbTransactionMgr.writeUnlock();
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
                    if (partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
                        LOG.debug("transactionId {} partition commitInfo version {} is not equal with " +
                                        "partition visible version {} plus one, need wait",
                                transactionId,
                                partitionCommitInfo.getVersion(),
                                partition.getVisibleVersion());
                        return;
                    }
                    int quorumReplicaNum = partitionInfo.getReplicationNum(partitionId) / 2 + 1;

                    List<MaterializedIndex> allIndices;
                    if (transactionState.getLoadedTblIndexes().isEmpty()) {
                        allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
                    } else {
                        allIndices = Lists.newArrayList();
                        for (long indexId : transactionState.getLoadedTblIndexes().get(tableId)) {
                            MaterializedIndex index = partition.getIndex(indexId);
                            if (index != null) {
                                allIndices.add(index);
                            }
                        }
                    }

                    for (MaterializedIndex index : allIndices) {
                        for (Tablet tablet : index.getTablets()) {
                            int healthReplicaNum = 0;
                            for (Replica replica : tablet.getReplicas()) {
                                if (!errorReplicaIds.contains(replica.getId())
                                        && replica.getLastFailedVersion() < 0) {
                                    // this means the replica is a healthy replica,
                                    // it is healthy in the past and does not have error in current load
                                    if (replica.checkVersionCatchUp(partition.getVisibleVersion(),
                                            partition.getVisibleVersionHash(), true)) {
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
                                                         + " if its not a upgrate stage, its a fatal error. ", transactionState, replica);
                                    }
                                } else if (replica.getVersion() == partitionCommitInfo.getVersion()
                                        && replica.getVersionHash() == partitionCommitInfo.getVersionHash()) {
                                    // the replica's version and version hash is equal to current transaction partition's version and version hash
                                    // the replica is normal, then remove it from error replica ids
                                    errorReplicaIds.remove(replica.getId());
                                    ++healthReplicaNum;
                                }
                            }

                            if (healthReplicaNum < quorumReplicaNum) {
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
            boolean txnOperated = false;
            dbTransactionMgr.writeLock();
            try {
                transactionState.setErrorReplicas(errorReplicaIds);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                dbTransactionMgr.unprotectUpsertTransactionState(transactionState, false);
                txnOperated = true;
            } finally {
                dbTransactionMgr.writeUnlock();
                transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
            }
            updateCatalogAfterVisible(transactionState, db);
        } finally {
            db.writeUnlock();
        }
        LOG.info("finish transaction {} successfully", transactionState);
    }

    /**
     * check if there exists a load job before the endTransactionId have all finished
     */
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList) {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        dbTransactionMgr.readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : dbTransactionMgr.getIdToRunningTransactionState().entrySet()) {
                if (entry.getValue().getDbId() != dbId || !isIntersectionNotEmpty(entry.getValue().getTableIdList(),
                        tableIdList) || !entry.getValue().isRunning()) {
                    continue;
                }
                if (entry.getKey() <= endTransactionId) {
                    LOG.debug("find a running txn with txn_id={} on db: {}, less than watermark txn_id {}",
                            entry.getKey(), dbId, endTransactionId);
                    return false;
                }
            }
        } finally {
            dbTransactionMgr.readUnlock();
        }
        return true;
    }

    /**
     * check if there exists a intersection between the source tableId list and target tableId list
     * if one of them is null or empty, that means that we don't know related tables in tableList,
     * we think the two lists may have intersection for right ordered txns
     */
    public boolean isIntersectionNotEmpty(List<Long> sourceTableIdList, List<Long> targetTableIdList) {
        if (CollectionUtils.isEmpty(sourceTableIdList) || CollectionUtils.isEmpty(targetTableIdList)) {
            return true;
        }
        for (Long srcValue : sourceTableIdList) {
            for (Long targetValue : targetTableIdList) {
                if (srcValue.equals(targetValue)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * The txn cleaner will run at a fixed interval and try to delete expired and timeout txns:
     * expired: txn is in VISIBLE or ABORTED, and is expired.
     * timeout: txn is in PREPARE, but timeout
     * Todo(kks): currently remove transaction performance is bad, if we want to support
     *  super-high concurrent transaction, we should improve this method
     */
    public void removeExpiredAndTimeoutTxns() {
        long currentMillis = System.currentTimeMillis();
        List<Long> timeoutTxns = Lists.newArrayList();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.removeExpiredTxns();
            timeoutTxns.clear();

            dbTransactionMgr.readLock();
            try {
                for (TransactionState transactionState : dbTransactionMgr.getIdToRunningTransactionState().values()) {
                    if (transactionState.isTimeout(currentMillis)) {
                        // txn is running but timeout, abort it.
                        timeoutTxns.add(transactionState.getTransactionId());
                    }
                }
            } finally {
                dbTransactionMgr.readUnlock();
            }

            // abort timeout txns
            for (Long txnId : timeoutTxns) {
                try {
                    dbTransactionMgr.abortTransaction(txnId, "timeout by txn manager", null);
                    LOG.info("transaction [" + txnId + "] is timeout, abort it by transaction manager");
                } catch (UserException e) {
                    // abort may be failed. it is acceptable. just print a log
                    LOG.warn("abort timeout txn {} failed. msg: {}", txnId, e.getMessage());
                }
            }

        }
    }

    public TransactionState getTransactionState(long dbId, long transactionId) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        dbTransactionMgr.readLock();
        try {
            return dbTransactionMgr.getTransactionState(transactionId);
        } finally {
            dbTransactionMgr.readUnlock();
        }
    }
    
    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
        this.idGenerator.setEditLog(editLog);
    }

    // for replay idToTransactionState
    // check point also run transaction cleaner, the cleaner maybe concurrently modify id to 
    public void replayUpsertTransactionState(TransactionState transactionState) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(transactionState.getDbId());
        Preconditions.checkNotNull(dbTransactionMgr);
        dbTransactionMgr.writeLock();
        try {
            // set transaction status will call txn state change listener
            transactionState.replaySetTransactionStatus();
            Database db = catalog.getDb(transactionState.getDbId());
            if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                LOG.info("replay a committed transaction {}", transactionState);
                updateCatalogAfterCommitted(transactionState, db);
            } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.info("replay a visible transaction {}", transactionState);
                updateCatalogAfterVisible(transactionState, db);
            }
            dbTransactionMgr.unprotectUpsertTransactionState(transactionState, true);
        } finally {
            dbTransactionMgr.writeUnlock();
        }
    }
    
    public void replayDeleteTransactionState(TransactionState transactionState) {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(transactionState.getDbId());
        dbTransactionMgr.deleteTransactionState(transactionState);
    }
    
    private void updateCatalogAfterCommitted(TransactionState transactionState, Database db) {
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTable(tableId);
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
                for (MaterializedIndex index : allIndices) {
                    List<Tablet> tablets = index.getTablets();
                    for (Tablet tablet : tablets) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (errorReplicaIds.contains(replica.getId())) {
                                // should not use partition.getNextVersion and partition.getNextVersionHash because partition's next version hash is generated locally
                                // should get from transaction state
                                replica.updateLastFailedVersion(partitionCommitInfo.getVersion(),
                                                                partitionCommitInfo.getVersionHash());
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
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
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
                                        partition.getVisibleVersionHash(), true)) {
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
                        }
                    }
                } // end for indices
                long version = partitionCommitInfo.getVersion();
                long versionHash = partitionCommitInfo.getVersionHash();
                partition.updateVisibleVersionAndVersionHash(version, versionHash);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("transaction state {} set partition {}'s version to [{}] and version hash to [{}]",
                              transactionState, partition.getId(), version, versionHash);
                }
            }
        }
        return true;
    }

    public List<List<Comparable>> getDbInfo() {
        List<List<Comparable>> infos = new ArrayList<List<Comparable>>();
        List<Long> dbIds = Lists.newArrayList();
        for (Long dbId : dbIdToDatabaseTransactionMgrs.keySet()) {
            dbIds.add(dbId);
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
        return infos;
    }
    
    public List<List<String>> getDbTransStateInfo(long dbId) {
        List<List<String>> infos = Lists.newArrayList();
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);

        dbTransactionMgr.readLock();
        try {
            infos.add(Lists.newArrayList("running", String.valueOf(
                    dbTransactionMgr.getRunningTxnNums() + dbTransactionMgr.getRunningRoutineLoadTxnNums())));
            long finishedNum = dbTransactionMgr.getFinishedTxnNums();
            infos.add(Lists.newArrayList("finished", String.valueOf(finishedNum)));
        } finally {
            dbTransactionMgr.readUnlock();
        }
        return infos;
    }

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        return dbTransactionMgr.getTxnStateInfoList(running, limit);
    }
    
    // get show info of a specified txnId
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        List<List<String>> infos = new ArrayList<List<String>>();
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        return dbTransactionMgr.getSingleTranInfo(dbId, txnId);
    }

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        return dbTransactionMgr.getTableTransInfo(txnId);
    }
    
    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        return dbTransactionMgr.getPartitionTransInfo(tid, tableId);
    }
    
    public int getTransactionNum() {
        int txnNum = 0;
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnNum = txnNum + dbTransactionMgr.getTransactionNum();
        }
        return txnNum;
    }
    
    public TransactionIdGenerator getTransactionIDGenerator() {
        return this.idGenerator;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int numTransactions = getTransactionNum();
        out.writeInt(numTransactions);
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            Map<Long, TransactionState> idToTransactionState = dbTransactionMgr.getIdToRunningTransactionState();
            for (Map.Entry<Long, TransactionState> entry : idToTransactionState.entrySet()) {
                entry.getValue().write(out);
            }

            ArrayDeque<TransactionState>transactionStateDeque = dbTransactionMgr.getFinalStatusTransactionStateDeque();
            for (TransactionState transactionState : transactionStateDeque) {
                transactionState.write(out);
            }
        }

        idGenerator.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        int numTransactions = in.readInt();
        for (int i = 0; i < numTransactions; ++i) {
            TransactionState transactionState = new TransactionState();
            transactionState.readFields(in);
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(transactionState.getDbId());
            dbTransactionMgr.unprotectUpsertTransactionState(transactionState, true);
        }
        idGenerator.readFields(in);
    }

    public TransactionState getTransactionStateByCallbackIdAndStatus(long dbId, long callbackId, Set<TransactionStatus> status) {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        return dbTransactionMgr.getTransactionStateByCallbackIdAndStatus(callbackId, status);
    }

    public TransactionState getTransactionStateByCallbackId(long dbId, long callbackId) {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(dbId);
        return dbTransactionMgr.getTransactionStateByCallbackId(callbackId);
    }

    public List<Pair<Long, Long>> getTransactionIdByCoordinateBe(String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        for (DatabaseTransactionMgr databaseTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnInfos.addAll(databaseTransactionMgr.getTransactionIdByCoordinateBe(coordinateHost, limit));
            if (txnInfos.size() > limit) {
                break;
            }
        }
        return txnInfos.size() > limit ? new ArrayList<>(txnInfos.subList(0, limit)) : txnInfos;
    }

    /**
     * If a Coordinate BE is down when running txn, the txn will remain in FE until killed by timeout
     * So when FE identify the Coordiante BE is down, FE should cancel it initiative
     */
    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit) {
        List<Pair<Long, Long>> transactionIdByCoordinateBe = getTransactionIdByCoordinateBe(coordinateHost, limit);
        for (Pair<Long, Long> txnInfo : transactionIdByCoordinateBe) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactioMgr(txnInfo.getKey());
                dbTransactionMgr.abortTransaction(txnInfo.getValue(), "coordinate BE is down", null);
            } catch (UserException e) {
                LOG.warn("Abort txn on coordinate BE {} failed, msg={}", coordinateHost, e.getMessage());
            }
        }
    }
}
