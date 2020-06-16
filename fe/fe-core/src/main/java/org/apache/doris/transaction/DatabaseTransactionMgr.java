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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.ClearTransactionTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUniqueId;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Transaction Manager in database level, as a component in GlobalTransactionMgr
 * DatabaseTransactionMgr mainly be responsible for the following content:
 * 1. provide read/write lock in database level
 * 2. provide basic txn infos interface in database level to GlobalTransactionMgr
 * 3. do some transaction management, such as add/update/delete transaction.
 * Attention: all api in DatabaseTransactionMgr should be only invoked by GlobalTransactionMgr
 */

public class DatabaseTransactionMgr {

    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgr.class);

    private long dbId;

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock(true);

    // transactionId -> running TransactionState
    private Map<Long, TransactionState> idToRunningTransactionState = Maps.newHashMap();

    // transactionId -> final status TransactionState
    private Map<Long, TransactionState> idToFinalStatusTransactionState = Maps.newHashMap();


    // to store transactionStates with final status
    private ArrayDeque<TransactionState> finalStatusTransactionStateDeque = new ArrayDeque<>();

    // label -> txn ids
    // this is used for checking if label already used. a label may correspond to multiple txns,
    // and only one is success.
    // this member should be consistent with idToTransactionState,
    // which means if a txn exist in idToRunningTransactionState or idToFinalStatusTransactionState
    // it must exists in dbIdToTxnLabels, and vice versa
    private Map<String, Set<Long>> labelToTxnIds = Maps.newHashMap();


    // count the number of running txns of database, except for the routine load txn
    private int runningTxnNums = 0;

    // count only the number of running routine load txns of database
    private int runningRoutineLoadTxnNums = 0;

    private Catalog catalog;

    private EditLog editLog;

    private TransactionIdGenerator idGenerator;

    private List<ClearTransactionTask> clearTransactionTasks = Lists.newArrayList();

    // not realtime usedQuota value to make a fast check for database data quota
    private volatile long usedQuotaDataBytes = -1;

    protected void readLock() {
        this.transactionLock.readLock().lock();
    }

    protected void readUnlock() {
        this.transactionLock.readLock().unlock();
    }

    protected void writeLock() {
        this.transactionLock.writeLock().lock();
    }

    protected void writeUnlock() {
        this.transactionLock.writeLock().unlock();
    }

    public DatabaseTransactionMgr(long dbId, Catalog catalog, TransactionIdGenerator idGenerator) {
        this.dbId = dbId;
        this.catalog = catalog;
        this.idGenerator = idGenerator;
        this.editLog = catalog.getEditLog();
    }

    public long getDbId() {
        return dbId;
    }

    public TransactionState getTransactionState(Long transactionId) {
        readLock();
        try {
            TransactionState transactionState = idToRunningTransactionState.get(transactionId);
            if (transactionState != null) {
                return transactionState;
            } else {
                return idToFinalStatusTransactionState.get(transactionId);
            }
        } finally {
            readUnlock();
        }
    }

    private TransactionState unprotectedGetTransactionState(Long transactionId) {
        TransactionState transactionState = idToRunningTransactionState.get(transactionId);
        if (transactionState != null) {
            return transactionState;
        } else {
            return idToFinalStatusTransactionState.get(transactionId);
        }
    }

    @VisibleForTesting
    protected Set<Long> unprotectedGetTxnIdsByLabel(String label) {
        return labelToTxnIds.get(label);
    }

    @VisibleForTesting
    protected int getRunningTxnNums() {
        return runningTxnNums;
    }

    @VisibleForTesting
    protected int getRunningRoutineLoadTxnNums() {
        return runningRoutineLoadTxnNums;
    }

    @VisibleForTesting
    protected int getFinishedTxnNums() {
        return finalStatusTransactionStateDeque.size();
    }

    public List<List<String>> getTxnStateInfoList(boolean running, int limit) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<TransactionState> transactionStateCollection = null;
        readLock();
        try {
            if (running) {
                transactionStateCollection = idToRunningTransactionState.values();
            } else {
                transactionStateCollection = finalStatusTransactionStateDeque;
            }
            // get transaction order by txn id desc limit 'limit'
            transactionStateCollection.stream()
                    .sorted(TransactionState.TXN_ID_COMPARATOR)
                    .limit(limit)
                    .forEach(t -> {
                        List<String> info = Lists.newArrayList();
                        getTxnStateInfo(t, info);
                        infos.add(info);
                    });
        } finally {
            readUnlock();
        }
        return infos;
    }

    private void getTxnStateInfo(TransactionState txnState, List<String> info) {
        info.add(String.valueOf(txnState.getTransactionId()));
        info.add(txnState.getLabel());
        info.add(txnState.getCoordinator().toString());
        info.add(txnState.getTransactionStatus().name());
        info.add(txnState.getSourceType().name());
        info.add(TimeUtils.longToTimeString(txnState.getPrepareTime()));
        info.add(TimeUtils.longToTimeString(txnState.getCommitTime()));
        info.add(TimeUtils.longToTimeString(txnState.getPublishVersionTime()));
        info.add(TimeUtils.longToTimeString(txnState.getFinishTime()));
        info.add(txnState.getReason());
        info.add(String.valueOf(txnState.getErrorReplicas().size()));
        info.add(String.valueOf(txnState.getCallbackId()));
        info.add(String.valueOf(txnState.getTimeoutMs()));
        info.add(txnState.getErrMsg());
    }

    public long beginTransaction(List<Long> tableIdList, String label, TUniqueId requestId,
                                 TransactionState.TxnCoordinator coordinator, TransactionState.LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws DuplicatedRequestException, LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        checkDatabaseDataQuota();
        writeLock();
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
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds != null && !existingTxnIds.isEmpty()) {
                List<TransactionState> notAbortedTxns = Lists.newArrayList();
                for (long txnId : existingTxnIds) {
                    TransactionState txn = unprotectedGetTransactionState(txnId);
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
                            && notAbortedTxn.getRequestId() != null && notAbortedTxn.getRequestId().equals(requestId)) {
                        // this may be a retry request for same job, just return existing txn id.
                        throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                                notAbortedTxn.getTransactionId(), "");
                    }
                    throw new LabelAlreadyUsedException(label, notAbortedTxn.getTransactionStatus());
                }
            }

            checkRunningTxnExceedLimit(sourceType);

            long tid = idGenerator.getNextTransactionId();
            LOG.info("begin transaction: txn id {} with label {} from coordinator {}, listner id: {}",
                    tid, label, coordinator, listenerId);
            TransactionState transactionState = new TransactionState(dbId, tableIdList, tid, label, requestId, sourceType,
                    coordinator, listenerId, timeoutSecond * 1000);
            transactionState.setPrepareTime(System.currentTimeMillis());
            unprotectUpsertTransactionState(transactionState, false);

            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
            }

            return tid;
        } catch (DuplicatedRequestException e) {
            throw e;
        } catch (Exception e) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_REJECT.increase(1L);
            }
            throw e;
        } finally {
            writeUnlock();
        }
    }


    private void checkDatabaseDataQuota() throws AnalysisException {
        Database db = catalog.getDb(dbId);
        if (db == null) {
            throw new AnalysisException("Database[" + dbId + "] does not exist");
        }

        if (usedQuotaDataBytes == -1) {
            usedQuotaDataBytes = db.getUsedDataQuotaWithLock();
        }

        long dataQuotaBytes = db.getDataQuota();
        if (usedQuotaDataBytes >= dataQuotaBytes) {
            Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuotaBytes);
            String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " "
                    + quotaUnitPair.second;
            throw new AnalysisException("Database[" + db.getFullName()
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public void updateDatabaseUsedQuotaData(long usedQuotaDataBytes) {
        this.usedQuotaDataBytes = usedQuotaDataBytes;
    }

    /**
     * commit transaction process as follows：
     * 1. validate whether `Load` is cancelled
     * 2. validate whether `Table` is deleted
     * 3. validate replicas consistency
     * 4. update transaction state version
     * 5. persistent transactionState
     * 6. update nextVersion because of the failure of persistent transaction resulting in error version
     */
    public void commitTransaction(List<Table> tableList, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                  TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // 1. check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = catalog.getDb(dbId);
        if (null == db) {
            throw new MetaNotFoundException("could not find db [" + dbId + "]");
        }

        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
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
        Map<Long, Table> idToTable = new HashMap<>();
        for (int i = 0; i < tableList.size(); i++) {
            idToTable.put(tableList.get(i).getId(), tableList.get(i));
        }
        // 2. validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if index is dropped, it does not matter.
        // if table or partition is dropped during load, just ignore that tablet,
        // because we should allow dropping rollup or partition during load
        List<Long> tabletIds = tabletCommitInfos.stream().map(
                tabletCommitInfo -> tabletCommitInfo.getTabletId()).collect(Collectors.toList());
        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetaList.size(); i++) {
            TabletMeta tabletMeta = tabletMetaList.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                continue;
            }
            long tabletId = tabletIds.get(i);
            long tableId = tabletMeta.getTableId();
            OlapTable tbl = (OlapTable) idToTable.get(tableId);
            if (tbl == null) {
                // this can happen when tableId == -1 (tablet being dropping)
                // or table really not exist.
                continue;
            }

            if (tbl.getState() == OlapTable.OlapTableState.RESTORE) {
                throw new LoadException("Table " + tbl.getName() + " is in restore process. "
                        + "Can not load into it");
            }

            long partitionId = tabletMeta.getPartitionId();
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
            tabletToBackends.get(tabletId).add(tabletCommitInfos.get(i).getBackendId());
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
                    allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                } else {
                    allIndices = Lists.newArrayList();
                    for (long indexId : transactionState.getLoadedTblIndexes().get(tableId)) {
                        MaterializedIndex index = partition.getIndex(indexId);
                        if (index != null) {
                            allIndices.add(index);
                        }
                    }
                }

                if (table.getState() == OlapTable.OlapTableState.ROLLUP || table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
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
        writeLock();
        try {
            unprotectedCommitTransaction(transactionState, errorReplicaIds, tableToPartition, totalInvolvedBackends,
                    db);
            txnOperated = true;
        } finally {
            writeUnlock();
            // after state transform
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
        }

        // 6. update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    public boolean publishTransaction(Database db, long transactionId, long timeoutMillis) throws TransactionCommitFailedException {
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
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

    public void deleteTransaction(TransactionState transactionState) {
        writeLock();
        try {
            // here we only delete the oldest element, so if element exist in finalStatusTransactionStateDeque,
            // it must at the front of the finalStatusTransactionStateDeque
            if (!finalStatusTransactionStateDeque.isEmpty() &&
            transactionState.getTransactionId() == finalStatusTransactionStateDeque.getFirst().getTransactionId()) {
                finalStatusTransactionStateDeque.pop();
                clearTransactionState(transactionState);
            }
        } finally {
            writeUnlock();
        }
    }

    public TransactionStatus getLabelState(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return TransactionStatus.UNKNOWN;
            }
            // find the latest txn (which id is largest)
            long maxTxnId = existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).get();
            return unprotectedGetTransactionState(maxTxnId).getTransactionStatus();
        } finally {
            readUnlock();
        }
    }

    public List<TransactionState> getCommittedTxnList() {
        readLock();
        try {
            // only send task to committed transaction
            return idToRunningTransactionState.values().stream()
                    .filter(transactionState -> (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED))
                    .sorted(Comparator.comparing(TransactionState::getCommitTime))
                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    public void finishTransaction(long transactionId, Set<Long> errorReplicaIds) throws UserException {
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
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
            writeLock();
            try {
                transactionState.setTransactionStatus(TransactionStatus.ABORTED);
                transactionState.setReason("db is dropped");
                LOG.warn("db is dropped during transaction, abort transaction {}", transactionState);
                unprotectUpsertTransactionState(transactionState, false);
                return;
            } finally {
                writeUnlock();
            }
        }
        List<Long> tableIdList = transactionState.getTableIdList();
        // to be compatiable with old meta version, table List may be empty
        if (tableIdList.isEmpty()) {
           readLock();
           try {
               for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
                   long tableId = tableCommitInfo.getTableId();
                   if (!tableIdList.contains(tableId)) {
                       tableIdList.add(tableId);
                   }
               }
           } finally {
               readUnlock();
           }
        }

        List<Table> tableList = db.getTablesOnIdOrderOrThrowException(tableIdList);
        for (Table table : tableList) {
            table.writeLock();
        }
        try {
            boolean hasError = false;
            Iterator<TableCommitInfo> tableCommitInfoIterator = transactionState.getIdToTableCommitInfos().values().iterator();
            while (tableCommitInfoIterator.hasNext()) {
                TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
                long tableId = tableCommitInfo.getTableId();
                OlapTable table = (OlapTable) db.getTable(tableId);
                // table maybe dropped between commit and publish, ignore this error
                if (table == null) {
                    tableCommitInfoIterator.remove();
                    LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
                            tableId,
                            transactionState);
                    continue;
                }
                PartitionInfo partitionInfo = table.getPartitionInfo();
                Iterator<PartitionCommitInfo> partitionCommitInfoIterator = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
                while (partitionCommitInfoIterator.hasNext()) {
                    PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    // partition maybe dropped between commit and publish version, ignore this error
                    if (partition == null) {
                        partitionCommitInfoIterator.remove();
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
                        String errMsg = String.format("wait for publishing partition %d version %d. self version: %d. table %d",
                                partitionId, partition.getVisibleVersion() + 1, partitionCommitInfo.getVersion(), tableId);
                        transactionState.setErrorMsg(errMsg);
                        return;
                    }
                    int quorumReplicaNum = partitionInfo.getReplicationNum(partitionId) / 2 + 1;

                    List<MaterializedIndex> allIndices;
                    if (transactionState.getLoadedTblIndexes().isEmpty()) {
                        allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
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
                                                + " if its not a upgrade stage, its a fatal error. ", transactionState, replica);
                                    }
                                } else if (replica.getVersion() >= partitionCommitInfo.getVersion()) {
                                    // the replica's version is larger than or equal to current transaction partition's version
                                    // the replica is normal, then remove it from error replica ids
                                    errorReplicaIds.remove(replica.getId());
                                    ++healthReplicaNum;
                                }
                            }

                            if (healthReplicaNum < quorumReplicaNum) {
                                LOG.info("publish version failed for transaction {} on tablet {}, with only {} replicas less than quorum {}",
                                        transactionState, tablet, healthReplicaNum, quorumReplicaNum);
                                String errMsg = String.format("publish on tablet %d failed. succeed replica num %d less than quorum %d."
                                        + " table: %d, partition: %d, publish version: %d",
                                        tablet.getId(), healthReplicaNum, quorumReplicaNum, tableId, partitionId, partition.getVisibleVersion() + 1);
                                transactionState.setErrorMsg(errMsg);
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
            writeLock();
            try {
                transactionState.setErrorReplicas(errorReplicaIds);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.clearErrorMsg();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                unprotectUpsertTransactionState(transactionState, false);
                txnOperated = true;
                // TODO(cmy): We found a very strange problem. When delete-related transactions are processed here,
                // subsequent `updateCatalogAfterVisible()` is called, but it does not seem to be executed here
                // (because the relevant editlog does not see the log of visible transactions).
                // So I add a log here for observation.
                LOG.debug("after set transaction {} to visible", transactionState);
            } finally {
                writeUnlock();
                transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
            }
            updateCatalogAfterVisible(transactionState, db);
        } finally {
            for (int i = tableList.size() - 1; i >=0; i--) {
                tableList.get(i).writeUnlock();
            }
        }
        LOG.info("finish transaction {} successfully", transactionState);
    }

    protected void unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
                                               Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                               Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        transactionState.setCommitTime(System.currentTimeMillis());
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        transactionState.setErrorReplicas(errorReplicaIds);
        for (long tableId : tableToPartition.keySet()) {
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            for (long partitionId : tableToPartition.get(tableId)) {
                OlapTable table = (OlapTable) db.getTable(tableId);
                Partition partition = table.getPartition(partitionId);
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        partition.getNextVersion(), partition.getNextVersionHash(),
                        System.currentTimeMillis() /* use as partition visible time */);
                tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);

        // add publish version tasks. set task to null as a placeholder.
        // tasks will be created when publishing version.
        for (long backendId : totalInvolvedBackends) {
            transactionState.addPublishVersionTask(backendId, null);
        }
    }

    // for add/update/delete TransactionState
    protected void unprotectUpsertTransactionState(TransactionState transactionState, boolean isReplay) {
        // if this is a replay operation, we should not log it
        if (!isReplay) {
            if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE
                    || transactionState.getSourceType() == TransactionState.LoadJobSourceType.FRONTEND) {
                // if this is a prepare txn, and load source type is not FRONTEND
                // no need to persist it. if prepare txn lost, the following commit will just be failed.
                // user only need to retry this txn.
                // The FRONTEND type txn is committed and running asynchronously, so we have to persist it.
                editLog.logInsertTransactionState(transactionState);
            }
        }
        if (!transactionState.getTransactionStatus().isFinalStatus()) {
            if (idToRunningTransactionState.put(transactionState.getTransactionId(), transactionState) == null) {
                if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK) {
                    runningRoutineLoadTxnNums++;
                } else {
                    runningTxnNums++;
                }
            }
        } else {
            if (idToRunningTransactionState.remove(transactionState.getTransactionId()) != null) {
                if (transactionState.getSourceType() == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK) {
                    runningRoutineLoadTxnNums--;
                } else {
                    runningTxnNums--;
                }
            }
            idToFinalStatusTransactionState.put(transactionState.getTransactionId(), transactionState);
            finalStatusTransactionStateDeque.add(transactionState);
        }
        updateTxnLabels(transactionState);
    }

    private void updateTxnLabels(TransactionState transactionState) {
        Set<Long> txnIds = labelToTxnIds.get(transactionState.getLabel());
        if (txnIds == null) {
            txnIds = Sets.newHashSet();
            labelToTxnIds.put(transactionState.getLabel(), txnIds);
        }
        txnIds.add(transactionState.getTransactionId());
    }

    public void abortTransaction(String label, String reason) throws UserException {
        Preconditions.checkNotNull(label);
        long transactionId = -1;
        readLock();
        try {
            Set<Long> existingTxns = unprotectedGetTxnIdsByLabel(label);
            if (existingTxns == null || existingTxns.isEmpty()) {
                throw new TransactionNotFoundException("transaction not found, label=" + label);
            }
            // find PREPARE txn. For one load label, there should be only one PREPARE txn.
            TransactionState prepareTxn = null;
            for (Long txnId : existingTxns) {
                TransactionState txn = unprotectedGetTransactionState(txnId);
                if (txn.getTransactionStatus() == TransactionStatus.PREPARE) {
                    prepareTxn = txn;
                    break;
                }
            }

            if (prepareTxn == null) {
                throw new TransactionNotFoundException("running transaction not found, label=" + label);
            }

            transactionId = prepareTxn.getTransactionId();
        } finally {
            readUnlock();
        }
        abortTransaction(transactionId, reason, null);
    }

    public void abortTransaction(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment) throws UserException {
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job, ignore abort operation", transactionId);
            return;
        }
        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = idToRunningTransactionState.get(transactionId);
        } finally {
            readUnlock();
        }
        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction not found", transactionId);
        }

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;
        writeLock();
        try {
            txnOperated = unprotectAbortTransaction(transactionId, reason);
        } finally {
            writeUnlock();
            transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, reason);
        }

        // send clear txn task to BE to clear the transactions on BE.
        // This is because parts of a txn may succeed in some BE, and these parts of txn should be cleared
        // explicitly, or it will be remained on BE forever
        // (However the report process will do the diff and send clear txn tasks to BE, but that is our
        // last defense)
        if (txnOperated && transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            clearBackendTransactions(transactionState);
        }

        LOG.info("abort transaction: {} successfully", transactionState);
    }

    private boolean unprotectAbortTransaction(long transactionId, String reason)
            throws UserException {
        TransactionState transactionState = unprotectedGetTransactionState(transactionId);
        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction not found", transactionId);
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            return false;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            throw new UserException("transaction's state is already "
                    + transactionState.getTransactionStatus() + ", could not abort");
        }
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.setReason(reason);
        transactionState.setTransactionStatus(TransactionStatus.ABORTED);
        unprotectUpsertTransactionState(transactionState, false);
        for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) {
            AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());
        }
        return true;
    }

    private void clearBackendTransactions(TransactionState transactionState) {
        Preconditions.checkState(transactionState.getTransactionStatus() == TransactionStatus.ABORTED);
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task
        // to all backends.
        List<Long> allBeIds = Catalog.getCurrentSystemInfo().getBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (clearTransactionTasks) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(beId, transactionState.getTransactionId(), Lists.newArrayList());
                clearTransactionTasks.add(task);
            }

            // try to group send tasks, not sending every time a txn is aborted. to avoid too many task rpc.
            if (clearTransactionTasks.size() > allBeIds.size() * 2) {
                batchTask = new AgentBatchTask();
                for (ClearTransactionTask clearTransactionTask : clearTransactionTasks) {
                    batchTask.addTask(clearTransactionTask);
                }
                clearTransactionTasks.clear();
            }
        }

        if (batchTask != null) {
            AgentTaskExecutor.submit(batchTask);
        }
    }


    protected List<List<Comparable>> getTableTransInfo(long txnId) throws AnalysisException {
        List<List<Comparable>> tableInfos = new ArrayList<>();
        readLock();
        try {
            TransactionState transactionState = unprotectedGetTransactionState(txnId);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + txnId + "] does not exist.");
            }

            for (Map.Entry<Long, TableCommitInfo> entry : transactionState.getIdToTableCommitInfos().entrySet()) {
                List<Comparable> tableInfo = new ArrayList<>();
                tableInfo.add(entry.getKey());
                tableInfo.add(Joiner.on(", ").join(entry.getValue().getIdToPartitionCommitInfo().values().stream().map(
                        PartitionCommitInfo::getPartitionId).collect(Collectors.toList())));
                tableInfos.add(tableInfo);
            }
        } finally {
            readUnlock();
        }
        return tableInfos;
    }

    protected List<List<Comparable>> getPartitionTransInfo(long txnId, long tableId) throws AnalysisException {
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        readLock();
        try {
            TransactionState transactionState = unprotectedGetTransactionState(txnId);
            if (null == transactionState) {
                throw new AnalysisException("Transaction[" + txnId + "] does not exist.");
            }

            TableCommitInfo tableCommitInfo = transactionState.getIdToTableCommitInfos().get(tableId);
            Map<Long, PartitionCommitInfo> idToPartitionCommitInfo = tableCommitInfo.getIdToPartitionCommitInfo();
            for (Map.Entry<Long, PartitionCommitInfo> entry : idToPartitionCommitInfo.entrySet()) {
                List<Comparable> partitionInfo = new ArrayList<Comparable>();
                partitionInfo.add(entry.getKey());
                partitionInfo.add(entry.getValue().getVersion());
                partitionInfo.add(entry.getValue().getVersionHash());
                partitionInfos.add(partitionInfo);
            }
        } finally {
            readUnlock();
        }
        return partitionInfos;
    }

    public void removeExpiredTxns(long currentMillis) {
        writeLock();
        try {
            while (!finalStatusTransactionStateDeque.isEmpty()) {
                TransactionState transactionState = finalStatusTransactionStateDeque.getFirst();
                if (transactionState.isExpired(currentMillis)) {
                    finalStatusTransactionStateDeque.pop();
                    clearTransactionState(transactionState);
                    editLog.logDeleteTransactionState(transactionState);
                    LOG.info("transaction [" + transactionState.getTransactionId() + "] is expired, remove it from transaction manager");
                } else {
                    break;
                }

            }
        } finally {
            writeUnlock();
        }
    }

    private void clearTransactionState(TransactionState transactionState) {
        idToFinalStatusTransactionState.remove(transactionState.getTransactionId());
        Set<Long> txnIds = unprotectedGetTxnIdsByLabel(transactionState.getLabel());
        txnIds.remove(transactionState.getTransactionId());
        if (txnIds.isEmpty()) {
            labelToTxnIds.remove(transactionState.getLabel());
        }
    }

    public int getTransactionNum() {
        return idToRunningTransactionState.size() + finalStatusTransactionStateDeque.size();
    }


    public TransactionState getTransactionStateByCallbackIdAndStatus(long callbackId, Set<TransactionStatus> status) {
        readLock();
        try {
            for (TransactionState txn : idToRunningTransactionState.values()) {
                if (txn.getCallbackId() == callbackId && status.contains(txn.getTransactionStatus())) {
                    return txn;
                }
            }
            for (TransactionState txn : finalStatusTransactionStateDeque) {
                if (txn.getCallbackId() == callbackId && status.contains(txn.getTransactionStatus())) {
                    return txn;
                }
            }
        } finally {
            readUnlock();
        }
        return null;
    }

    public TransactionState getTransactionStateByCallbackId(long callbackId) {
        readLock();
        try {
            for (TransactionState txn : idToRunningTransactionState.values()) {
                if (txn.getCallbackId() == callbackId) {
                    return txn;
                }
            }
            for (TransactionState txn : finalStatusTransactionStateDeque) {
                if (txn.getCallbackId() == callbackId) {
                    return txn;
                }
            }
        } finally {
            readUnlock();
        }
        return null;
    }

    public List<Pair<Long, Long>> getTransactionIdByCoordinateBe(String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        readLock();
        try {
            idToRunningTransactionState.values().stream()
                    .filter(t -> (t.getCoordinator().sourceType == TransactionState.TxnSourceType.BE
                            && t.getCoordinator().ip.equals(coordinateHost)))
                    .limit(limit)
                    .forEach(t -> txnInfos.add(new Pair<>(t.getDbId(), t.getTransactionId())));
        } finally {
            readUnlock();
        }
        return txnInfos;
    }

    // get show info of a specified txnId
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        List<List<String>> infos = new ArrayList<List<String>>();
        readLock();
        try {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                throw new AnalysisException("Database[" + dbId + "] does not exist");
            }

            TransactionState txnState = unprotectedGetTransactionState(txnId);
            if (txnState == null) {
                throw new AnalysisException("transaction with id " + txnId + " does not exist");
            }

            if (ConnectContext.get() != null) {
                // check auth
                Set<Long> tblIds = txnState.getIdToTableCommitInfos().keySet();
                for (Long tblId : tblIds) {
                    Table tbl = db.getTable(tblId);
                    if (tbl != null) {
                        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), db.getFullName(),
                                tbl.getName(), PrivPredicate.SHOW)) {
                            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                    "SHOW TRANSACTION",
                                    ConnectContext.get().getQualifiedUser(),
                                    ConnectContext.get().getRemoteIP(),
                                    tbl.getName());
                        }
                    }
                }
            }

            List<String> info = Lists.newArrayList();
            getTxnStateInfo(txnState, info);
            infos.add(info);
        } finally {
            readUnlock();
        }
        return infos;
    }

    protected void checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType sourceType) throws BeginTransactionException {
        switch (sourceType) {
            case ROUTINE_LOAD_TASK:
                // no need to check limit for routine load task:
                // 1. the number of running routine load tasks is limited by Config.max_routine_load_task_num_per_be
                // 2. if we add routine load txn to runningTxnNums, runningTxnNums will always be occupied by routine load,
                //    and other txn may not be able to submitted.
                break;
            default:
                if (runningTxnNums >= Config.max_running_txn_num_per_db) {
                    throw new BeginTransactionException("current running txns on db " + dbId + " is "
                            + runningTxnNums + ", larger than limit " + Config.max_running_txn_num_per_db);
                }
                break;
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
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
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
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            long lastFailedVersion = replica.getLastFailedVersion();
                            long lastFailedVersionHash = replica.getLastFailedVersionHash();
                            long newVersion = newCommitVersion;
                            long newVersionHash = newCommitVersionHash;
                            long lastSuccessVersion = replica.getLastSuccessVersion();
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
                                lastSuccessVersion = newCommitVersion;
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
                            replica.updateVersionInfo(newVersion, newVersionHash, lastFailedVersion, lastFailedVersionHash, lastSuccessVersion, lastSuccessVersionHash);
                        }
                    }
                } // end for indices
                long version = partitionCommitInfo.getVersion();
                long versionTime = partitionCommitInfo.getVersionTime();
                long versionHash = partitionCommitInfo.getVersionHash();
                partition.updateVisibleVersionAndVersionHash(version, versionTime, versionHash);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("transaction state {} set partition {}'s version to [{}] and version hash to [{}]",
                            transactionState, partition.getId(), version, versionHash);
                }
            }
        }
        return true;
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, List<Long> tableIdList) {
        readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
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
            readUnlock();
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


    public List<Long> getTimeoutTxns(long currentMillis) {
        List<Long> timeoutTxns = Lists.newArrayList();
        readLock();
        try {
            for (TransactionState transactionState : idToRunningTransactionState.values()) {
                if (transactionState.isTimeout(currentMillis)) {
                    // txn is running but timeout, abort it.
                    timeoutTxns.add(transactionState.getTransactionId());
                }
            }
        } finally {
            readUnlock();
        }
        return timeoutTxns;
    }

    public void removeExpiredAndTimeoutTxns(long currentMillis) {
        removeExpiredTxns(currentMillis);
        List<Long> timeoutTxns = getTimeoutTxns(currentMillis);
        // abort timeout txns
        for (Long txnId : timeoutTxns) {
            try {
                abortTransaction(txnId, "timeout by txn manager", null);
                LOG.info("transaction [" + txnId + "] is timeout, abort it by transaction manager");
            } catch (UserException e) {
                // abort may be failed. it is acceptable. just print a log
                LOG.warn("abort timeout txn {} failed. msg: {}", txnId, e.getMessage());
            }
        }
    }

    public void replayUpsertTransactionState(TransactionState transactionState) {
        writeLock();
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
            unprotectUpsertTransactionState(transactionState, true);
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getDbTransStateInfo() {
        List<List<String>> infos = Lists.newArrayList();
        readLock();
        try {
            infos.add(Lists.newArrayList("running", String.valueOf(
                    runningTxnNums + runningRoutineLoadTxnNums)));
            long finishedNum = getFinishedTxnNums();
            infos.add(Lists.newArrayList("finished", String.valueOf(finishedNum)));
        } finally {
            readUnlock();
        }
        return infos;
    }

    public void unprotectWriteAllTransactionStates(DataOutput out) throws IOException {
        for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
            entry.getValue().write(out);
        }

        for (TransactionState transactionState : finalStatusTransactionStateDeque) {
            transactionState.write(out);
        }
    }

}
