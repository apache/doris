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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
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
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.ClearTransactionTask;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    // the max number of txn that can be remove per round.
    // set it to avoid holding lock too long when removing too many txns per round.
    private static final int MAX_REMOVE_TXN_PER_ROUND = 10000;

    private final long dbId;

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private final ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock(true);

    // transactionId -> running TransactionState
    private final Map<Long, TransactionState> idToRunningTransactionState = Maps.newHashMap();

    // transactionId -> final status TransactionState
    private final Map<Long, TransactionState> idToFinalStatusTransactionState = Maps.newHashMap();

    // The following 2 queues are to store transactionStates with final status
    // These queues are mainly used to avoid traversing all txns and speed up the cleaning time
    // when cleaning up expired txs.
    // The "Short" queue is used to store the txns of the expire time
    // controlled by Config.streaming_label_keep_max_second.
    // The "Long" queue is used to store the txns of the expire time controlled by Config.label_keep_max_second.
    private final ArrayDeque<TransactionState> finalStatusTransactionStateDequeShort = new ArrayDeque<>();
    private final ArrayDeque<TransactionState> finalStatusTransactionStateDequeLong = new ArrayDeque<>();

    // label -> txn ids
    // this is used for checking if label already used. a label may correspond to multiple txns,
    // and only one is success.
    // this member should be consistent with idToTransactionState,
    // which means if a txn exist in idToRunningTransactionState or idToFinalStatusTransactionState
    // it must exists in dbIdToTxnLabels, and vice versa
    private final Map<String, Set<Long>> labelToTxnIds = Maps.newHashMap();


    // count the number of running txns of database, except for the routine load txn
    private volatile int runningTxnNums = 0;
    private volatile int runningTxnReplicaNums = 0;

    // count only the number of running routine load txns of database
    private volatile int runningRoutineLoadTxnNums = 0;

    private final Env env;

    private final EditLog editLog;

    private final TransactionIdGenerator idGenerator;

    private final List<ClearTransactionTask> clearTransactionTasks = Lists.newArrayList();

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

    public DatabaseTransactionMgr(long dbId, Env env, TransactionIdGenerator idGenerator) {
        this.dbId = dbId;
        this.env = env;
        this.idGenerator = idGenerator;
        this.editLog = env.getEditLog();
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

    public int getRunningTxnNums() {
        return runningTxnNums;
    }

    protected int getRunningRoutineLoadTxnNums() {
        return runningRoutineLoadTxnNums;
    }

    @VisibleForTesting
    protected int getFinishedTxnNums() {
        return idToFinalStatusTransactionState.size();
    }

    public List<List<String>> getTxnStateInfoList(boolean running, int limit) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<TransactionState> transactionStateCollection = null;
        readLock();
        try {
            if (running) {
                transactionStateCollection = idToRunningTransactionState.values();
            } else {
                transactionStateCollection = idToFinalStatusTransactionState.values();
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

    public List<List<String>> getTxnStateInfoList(TransactionStatus status) {
        List<List<String>> infos = Lists.newArrayList();
        Collection<TransactionState> transactionStateCollection = null;
        readLock();
        try {
            if (status == TransactionStatus.VISIBLE || status == TransactionStatus.ABORTED) {
                transactionStateCollection = idToFinalStatusTransactionState.values();
            } else {
                transactionStateCollection = idToRunningTransactionState.values();
            }
            // get transaction order by txn id desc limit 'limit'
            transactionStateCollection.stream()
                    .filter(transactionState -> (transactionState.getTransactionStatus() == status))
                    .sorted(TransactionState.TXN_ID_COMPARATOR)
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
        info.add(TimeUtils.longToTimeString(txnState.getPreCommitTime()));
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
            TransactionState.TxnCoordinator coordinator, TransactionState.LoadJobSourceType sourceType,
            long listenerId, long timeoutSecond)
            throws DuplicatedRequestException, LabelAlreadyUsedException, BeginTransactionException,
            AnalysisException, QuotaExceedException, MetaNotFoundException {
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
                // there should be at most 1 txn in PREPARE/PRECOMMITTED/COMMITTED/VISIBLE status
                Preconditions.checkState(notAbortedTxns.size() <= 1, notAbortedTxns);
                if (!notAbortedTxns.isEmpty()) {
                    TransactionState notAbortedTxn = notAbortedTxns.get(0);
                    if (requestId != null && (notAbortedTxn.getTransactionStatus() == TransactionStatus.PREPARE
                            || notAbortedTxn.getTransactionStatus() == TransactionStatus.PRECOMMITTED)
                            && notAbortedTxn.getRequestId() != null && notAbortedTxn.getRequestId().equals(requestId)) {
                        // this may be a retry request for same job, just return existing txn id.
                        throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                                notAbortedTxn.getTransactionId(), "");
                    }
                    throw new LabelAlreadyUsedException(notAbortedTxn);
                }
            }

            checkRunningTxnExceedLimit(sourceType);

            long tid = idGenerator.getNextTransactionId();
            LOG.info("begin transaction: txn id {} with label {} from coordinator {}, listener id: {}",
                    tid, label, coordinator, listenerId);
            TransactionState transactionState = new TransactionState(dbId, tableIdList,
                    tid, label, requestId, sourceType, coordinator, listenerId, timeoutSecond * 1000);
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

    private void checkDatabaseDataQuota() throws MetaNotFoundException, QuotaExceedException {
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);

        if (usedQuotaDataBytes == -1) {
            usedQuotaDataBytes = db.getUsedDataQuotaWithLock();
        }

        long dataQuotaBytes = db.getDataQuota();
        if (usedQuotaDataBytes >= dataQuotaBytes) {
            throw new QuotaExceedException(db.getFullName(), dataQuotaBytes);
        }
    }

    public void updateDatabaseUsedQuotaData(long usedQuotaDataBytes) {
        this.usedQuotaDataBytes = usedQuotaDataBytes;
    }

    public void preCommitTransaction2PC(List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        // check status
        // the caller method already own db lock, we do not obtain db lock here
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
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
            throw new TransactionCommitFailedException("transaction is already visible");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            LOG.debug("transaction is already committed: {}", transactionId);
            throw new TransactionCommitFailedException("transaction is already committed");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.PRECOMMITTED) {
            LOG.debug("transaction is already pre-committed: {}", transactionId);
            return;
        }

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();

        checkCommitStatus(tableList, transactionState, tabletCommitInfos, txnCommitAttachment, errorReplicaIds,
                          tableToPartition, totalInvolvedBackends);

        unprotectedPreCommitTransaction2PC(transactionState, errorReplicaIds, tableToPartition,
                totalInvolvedBackends, db);
        LOG.info("transaction:[{}] successfully pre-committed", transactionState);
    }

    private void checkCommitStatus(List<Table> tableList, TransactionState transactionState,
                                   List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment,
                                   Set<Long> errorReplicaIds, Map<Long, Set<Long>> tableToPartition,
                                   Set<Long> totalInvolvedBackends) throws UserException {

        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);

        // update transaction state extra if exists
        if (txnCommitAttachment != null) {
            transactionState.setTxnCommitAttachment(txnCommitAttachment);
        }

        TabletInvertedIndex tabletInvertedIndex = env.getTabletInvertedIndex();
        Map<Long, Set<Long>> tabletToBackends = new HashMap<>();
        Map<Long, Table> idToTable = new HashMap<>();
        for (int i = 0; i < tableList.size(); i++) {
            idToTable.put(tableList.get(i).getId(), tableList.get(i));
        }
        // validate potential exists problem: db->table->partition
        // guarantee exist exception during a transaction
        // if index is dropped, it does not matter.
        // if table or partition is dropped during load, just ignore that tablet,
        // because we should allow dropping rollup or partition during load
        List<Long> tabletIds = tabletCommitInfos.stream()
                .map(TabletCommitInfo::getTabletId).collect(Collectors.toList());
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
        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
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

                if (table.getState() == OlapTable.OlapTableState.ROLLUP
                        || table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
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

                int quorumReplicaNum = table.getPartitionInfo()
                        .getReplicaAllocation(partition.getId()).getTotalReplicaNum() / 2 + 1;
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
                        String errorReplicaInfo = new String();
                        for (long tabletBackend : tabletBackends) {
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, tabletBackend);
                            if (replica == null) {
                                throw new TransactionCommitFailedException("could not find replica for tablet ["
                                        + tabletId + "], backend [" + tabletBackend + "]");
                            }
                            // if the tablet have no replica's to commit or the tablet is a rolling up tablet,
                            // the commit backends maybe null
                            // if the commit backends is null, set all replicas as error replicas
                            if (commitBackends != null && commitBackends.contains(tabletBackend)) {
                                // if the backend load success but the backend has some errors previously,
                                // then it is not a normal replica
                                // ignore it but not log it
                                // for example, a replica is in clone state
                                if (replica.getLastFailedVersion() < 0) {
                                    ++successReplicaNum;
                                } else {
                                    errorReplicaInfo += " replica [" + replica.getId() + "], lastFailedVersion ["
                                                        + replica.getLastFailedVersion() + "]";
                                }
                            } else {
                                errorBackendIdsForTablet.add(tabletBackend);
                                errorReplicaIds.add(replica.getId());
                                // not remove rollup task here, because the commit maybe failed
                                // remove rollup task when commit successfully
                                errorReplicaInfo += " replica [" + replica.getId() + "] commitBackends null or "
                                                    + "tabletBackend [" + tabletBackend + "] does not "
                                                    + "in commitBackends";
                            }
                        }

                        if (successReplicaNum < quorumReplicaNum) {
                            LOG.warn("Failed to commit txn [{}]. "
                                            + "Tablet [{}] success replica num is {} < quorum replica num {} "
                                            + "while error backends {} error replica info {}",
                                    transactionState.getTransactionId(), tablet.getId(), successReplicaNum,
                                    quorumReplicaNum, Joiner.on(",").join(errorBackendIdsForTablet),
                                    errorReplicaInfo);
                            throw new TabletQuorumFailedException(transactionState.getTransactionId(), tablet.getId(),
                                    successReplicaNum, quorumReplicaNum,
                                    errorBackendIdsForTablet);
                        }
                    }
                }
            }
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
     */
    public void commitTransaction(List<Table> tableList, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                  TxnCommitAttachment txnCommitAttachment, Boolean is2PC)
            throws UserException {
        // check status
        // the caller method already own tables' write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        if (transactionState == null) {
            LOG.debug("transaction not found: {}", transactionId);
            throw new TransactionCommitFailedException("transaction [" + transactionId + "] not found.");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            LOG.debug("transaction is already aborted: {}", transactionId);
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] is already aborted. abort reason: " + transactionState.getReason());
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            LOG.debug("transaction is already visible: {}", transactionId);
            if (is2PC) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already visible, not pre-committed.");
            }
            return;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            LOG.debug("transaction is already committed: {}", transactionId);
            if (is2PC) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already committed, not pre-committed.");
            }
            return;
        }

        if (is2PC && transactionState.getTransactionStatus() == TransactionStatus.PREPARE) {
            LOG.debug("transaction is prepare, not pre-committed: {}", transactionId);
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] is prepare, not pre-committed.");
        }

        Set<Long> errorReplicaIds = Sets.newHashSet();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        Map<Long, Set<Long>> tableToPartition = new HashMap<>();
        if (!is2PC) {
            checkCommitStatus(tableList, transactionState, tabletCommitInfos, txnCommitAttachment, errorReplicaIds,
                    tableToPartition, totalInvolvedBackends);
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        writeLock();
        try {
            if (is2PC) {
                unprotectedCommitTransaction2PC(transactionState, db);
            } else {
                unprotectedCommitTransaction(transactionState, errorReplicaIds,
                        tableToPartition, totalInvolvedBackends, db);
            }
            txnOperated = true;
        } finally {
            writeUnlock();
            // after state transform
            transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    public boolean waitForTransactionFinished(Database db, long transactionId, long timeoutMillis)
            throws TransactionCommitFailedException {
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
        while (currentTimeMillis < timeoutTimeMillis
                && transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            try {
                transactionState.waitTransactionVisible(timeoutMillis);
            } catch (InterruptedException e) {
                // CHECKSTYLE IGNORE THIS LINE
            }
            currentTimeMillis = System.currentTimeMillis();
        }
        return transactionState.getTransactionStatus() == TransactionStatus.VISIBLE;
    }

    @Deprecated
    // use replayBatchDeleteTransaction instead
    public void replayDeleteTransaction(TransactionState transactionState) {
        writeLock();
        try {
            // here we only delete the oldest element, so if element exist in finalStatusTransactionStateDeque,
            // it must at the front of the finalStatusTransactionStateDeque.
            // check both "short" and "long" queue.
            if (!finalStatusTransactionStateDequeShort.isEmpty()
                    && transactionState.getTransactionId()
                    == finalStatusTransactionStateDequeShort.getFirst().getTransactionId()) {
                finalStatusTransactionStateDequeShort.pop();
                clearTransactionState(transactionState.getTransactionId());
            } else if (!finalStatusTransactionStateDequeLong.isEmpty()
                    && transactionState.getTransactionId()
                    == finalStatusTransactionStateDequeLong.getFirst().getTransactionId()) {
                finalStatusTransactionStateDequeLong.pop();
                clearTransactionState(transactionState.getTransactionId());
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayBatchRemoveTransaction(List<Long> txnIds) {
        writeLock();
        try {
            for (Long txnId : txnIds) {
                // here we only delete the oldest element, so if element exist in finalStatusTransactionStateDeque,
                // it must at the front of the finalStatusTransactionStateDeque
                // check both "short" and "long" queue.
                if (!finalStatusTransactionStateDequeShort.isEmpty()
                        && txnId == finalStatusTransactionStateDequeShort.getFirst().getTransactionId()) {
                    finalStatusTransactionStateDequeShort.pop();
                    clearTransactionState(txnId);
                } else if (!finalStatusTransactionStateDequeLong.isEmpty()
                        && txnId == finalStatusTransactionStateDequeLong.getFirst().getTransactionId()) {
                    finalStatusTransactionStateDequeLong.pop();
                    clearTransactionState(txnId);
                }
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

    public Long getTransactionId(String label) {
        readLock();
        try {
            Set<Long> existingTxnIds = unprotectedGetTxnIdsByLabel(label);
            if (existingTxnIds == null || existingTxnIds.isEmpty()) {
                return null;
            }
            // find the latest txn (which id is largest)
            return existingTxnIds.stream().max(Comparator.comparingLong(Long::valueOf)).get();
        } finally {
            readUnlock();
        }
    }

    public List<TransactionState> getPreCommittedTxnList() {
        readLock();
        try {
            // only send task to preCommitted transaction
            return idToRunningTransactionState.values().stream()
                    .filter(transactionState
                            -> (transactionState.getTransactionStatus() == TransactionStatus.PRECOMMITTED))
                    .sorted(Comparator.comparing(TransactionState::getPreCommitTime))
                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    public List<TransactionState> getCommittedTxnList() {
        readLock();
        try {
            // only send task to committed transaction
            return idToRunningTransactionState.values().stream()
                    .filter(transactionState ->
                            (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED))
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

        // case 1 If database is dropped, then we just throw MetaNotFoundException, because all related tables are
        // already force dropped, we just ignore the transaction with all tables been force dropped.
        // case 2 If at least one table lock successfully, which means that the transaction should be finished for
        // the existed tables while just ignore tables which have been dropped forcefully.
        // case 3 Database exist and all tables already been dropped, this case is same with case1, just finish
        // the transaction with empty commit info only three cases mentioned above may happen, because user cannot
        // drop table without force while there are committed transactions on table and writeLockTablesIfExist is
        // a blocking function, the returned result would be the existed table list which hold write lock
        Database db = env.getInternalCatalog().getDbOrMetaException(transactionState.getDbId());
        List<Long> tableIdList = transactionState.getTableIdList();
        List<? extends TableIf> tableList = db.getTablesOnIdOrderIfExist(tableIdList);
        tableList = MetaLockUtils.writeLockTablesIfExist(tableList);
        try {
            boolean hasError = false;
            Iterator<TableCommitInfo> tableCommitInfoIterator
                    = transactionState.getIdToTableCommitInfos().values().iterator();
            while (tableCommitInfoIterator.hasNext()) {
                TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
                long tableId = tableCommitInfo.getTableId();
                OlapTable table = (OlapTable) db.getTableNullable(tableId);
                // table maybe dropped between commit and publish, ignore this error
                if (table == null) {
                    tableCommitInfoIterator.remove();
                    LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
                            tableId,
                            transactionState);
                    continue;
                }
                PartitionInfo partitionInfo = table.getPartitionInfo();
                Iterator<PartitionCommitInfo> partitionCommitInfoIterator
                        = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
                while (partitionCommitInfoIterator.hasNext()) {
                    PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                    long partitionId = partitionCommitInfo.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    // partition maybe dropped between commit and publish version, ignore this error
                    if (partition == null) {
                        partitionCommitInfoIterator.remove();
                        LOG.warn("partition {} is dropped, skip version check"
                                        + " and remove it from transaction state {}", partitionId, transactionState);
                        continue;
                    }
                    if (partition.getVisibleVersion() != partitionCommitInfo.getVersion() - 1) {
                        LOG.debug("transactionId {} partition commitInfo version {} is not equal with "
                                        + "partition visible version {} plus one, need wait",
                                transactionId,
                                partitionCommitInfo.getVersion(),
                                partition.getVisibleVersion());
                        String errMsg = String.format("wait for publishing partition %d version %d."
                                        + " self version: %d. table %d", partitionId, partition.getVisibleVersion() + 1,
                                partitionCommitInfo.getVersion(), tableId);
                        transactionState.setErrorMsg(errMsg);
                        return;
                    }
                    int quorumReplicaNum = partitionInfo.getReplicaAllocation(partitionId).getTotalReplicaNum() / 2 + 1;

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

                    // check success replica number for each tablet.
                    // a success replica means:
                    //  1. Not in errorReplicaIds: succeed in both commit and publish phase
                    //  2. last failed version < 0: is a health replica before
                    //  3. version catch up: not with a stale version
                    // Here we only check number, the replica version will be updated in updateCatalogAfterVisible()
                    for (MaterializedIndex index : allIndices) {
                        for (Tablet tablet : index.getTablets()) {
                            int healthReplicaNum = 0;
                            for (Replica replica : tablet.getReplicas()) {
                                if (!errorReplicaIds.contains(replica.getId()) && replica.getLastFailedVersion() < 0) {
                                    if (replica.checkVersionCatchUp(partition.getVisibleVersion(), true)) {
                                        ++healthReplicaNum;
                                    }
                                } else if (replica.getVersion() >= partitionCommitInfo.getVersion()) {
                                    // the replica's version is larger than or equal to current transaction
                                    // partition's version the replica is normal, then remove it from error replica ids
                                    // TODO(cmy): actually I have no idea why we need this check
                                    errorReplicaIds.remove(replica.getId());
                                    ++healthReplicaNum;
                                }
                            }

                            if (healthReplicaNum < quorumReplicaNum) {
                                LOG.info("publish version failed for transaction {} on tablet {},"
                                                + " with only {} replicas less than quorum {}",
                                        transactionState, tablet, healthReplicaNum, quorumReplicaNum);
                                String errMsg = String.format("publish on tablet %d failed."
                                                + " succeed replica num %d less than quorum %d."
                                                + " table: %d, partition: %d, publish version: %d",
                                        tablet.getId(), healthReplicaNum, quorumReplicaNum, tableId,
                                        partitionId, partition.getVisibleVersion() + 1);
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
                try {
                    transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
                } catch (UserException e) {
                    LOG.warn("afterStateTransform txn {} failed. msg: {}", transactionId, e.getMessage());
                }
            }
            updateCatalogAfterVisible(transactionState, db);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        // The visible latch should only be counted down after all things are done
        // (finish transaction, write edit log, etc).
        // Otherwise, there is no way for stream load to query the result right after loading finished,
        // even if we call "sync" before querying.
        transactionState.countdownVisibleLatch();
        LOG.info("finish transaction {} successfully", transactionState);
    }

    protected void unprotectedPreCommitTransaction2PC(TransactionState transactionState, Set<Long> errorReplicaIds,
                                                Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                                Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        transactionState.setPreCommitTime(System.currentTimeMillis());
        transactionState.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        transactionState.setErrorReplicas(errorReplicaIds);
        for (long tableId : tableToPartition.keySet()) {
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            for (long partitionId : tableToPartition.get(tableId)) {
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId, -1, -1);
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

    protected void unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
                                                Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                                Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PREPARE) {
            return;
        }
        // update transaction state version
        long commitTime = System.currentTimeMillis();
        transactionState.setCommitTime(commitTime);
        if (MetricRepo.isInit) {
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(commitTime - transactionState.getPrepareTime());
        }
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        transactionState.setErrorReplicas(errorReplicaIds);
        for (long tableId : tableToPartition.keySet()) {
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            for (long partitionId : tableToPartition.get(tableId)) {
                OlapTable table = (OlapTable) db.getTableNullable(tableId);
                Partition partition = table.getPartition(partitionId);
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partitionId,
                        partition.getNextVersion(),
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

    protected void unprotectedCommitTransaction2PC(TransactionState transactionState, Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PRECOMMITTED) {
            LOG.warn("Unknow exception. state of transaction [{}] changed, failed to commit transaction",
                    transactionState.getTransactionId());
            return;
        }
        // update transaction state version
        transactionState.setCommitTime(System.currentTimeMillis());
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);

        Iterator<TableCommitInfo> tableCommitInfoIterator
                = transactionState.getIdToTableCommitInfos().values().iterator();
        while (tableCommitInfoIterator.hasNext()) {
            TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                tableCommitInfoIterator.remove();
                LOG.warn("table {} is dropped, skip and remove it from transaction state {}",
                        tableId,
                        transactionState);
                continue;
            }
            Iterator<PartitionCommitInfo> partitionCommitInfoIterator
                    = tableCommitInfo.getIdToPartitionCommitInfo().values().iterator();
            while (partitionCommitInfoIterator.hasNext()) {
                PartitionCommitInfo partitionCommitInfo = partitionCommitInfoIterator.next();
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                // partition maybe dropped between commit and publish version, ignore this error
                if (partition == null) {
                    partitionCommitInfoIterator.remove();
                    LOG.warn("partition {} is dropped, skip and remove it from transaction state {}",
                            partitionId,
                            transactionState);
                    continue;
                }
                partitionCommitInfo.setVersion(partition.getNextVersion());
                partitionCommitInfo.setVersionTime(System.currentTimeMillis());
            }
        }
        // persist transactionState
        editLog.logInsertTransactionState(transactionState);
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
            if (transactionState.isShortTxn()) {
                finalStatusTransactionStateDequeShort.add(transactionState);
            } else {
                finalStatusTransactionStateDequeLong.add(transactionState);
            }
        }
        updateTxnLabels(transactionState);
    }

    public void registerTxnReplicas(long txnId, int replicaNum) throws UserException {
        writeLock();
        try {
            TransactionState transactionState = idToRunningTransactionState.get(txnId);
            if (transactionState == null) {
                throw new UserException("running transaction not found, txnId=" + txnId);
            }
            transactionState.setReplicaNum(replicaNum);
            runningTxnReplicaNums += replicaNum;
        } finally {
            writeUnlock();
        }
    }

    public int getRunningTxnNum() {
        readLock();
        try {
            return runningTxnNums;
        } finally {
            readUnlock();
        }
    }

    public int getRunningTxnReplicaNum() {
        readLock();
        try {
            return runningTxnReplicaNums;
        } finally {
            readUnlock();
        }
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

    public void abortTransaction(long transactionId, String reason, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job,"
                    + " ignore abort operation", transactionId);
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

    public void abortTransaction2PC(long transactionId) throws UserException {
        LOG.info("begin to abort txn {}", transactionId);
        if (transactionId < 0) {
            LOG.info("transaction id is {}, less than 0, maybe this is an old type load job,"
                    + " ignore abort operation", transactionId);
            return;
        }
        TransactionState transactionState;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }

        if (transactionState == null) {
            throw new TransactionNotFoundException("transaction [" + transactionId + "] not found");
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.ABORTED);
        boolean txnOperated = false;
        writeLock();
        try {
            txnOperated = unprotectAbortTransaction(transactionId, "User Abort");
        } finally {
            writeUnlock();
            transactionState.afterStateTransform(TransactionStatus.ABORTED, txnOperated, "User Abort");
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
            throw new TransactionNotFoundException("transaction [" + transactionId + "] not found.");
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            throw new TransactionNotFoundException("transaction [" + transactionId + "] is already aborted, "
                    + "abort reason: " + transactionState.getReason());
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            throw new UserException("transaction [" + transactionId + "] is already "
                    + transactionState.getTransactionStatus() + ", could not abort.");
        }
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.setReason(reason);
        transactionState.setTransactionStatus(TransactionStatus.ABORTED);
        unprotectUpsertTransactionState(transactionState, false);
        return true;
    }

    private void clearBackendTransactions(TransactionState transactionState) {
        Preconditions.checkState(transactionState.getTransactionStatus() == TransactionStatus.ABORTED);
        // for aborted transaction, we don't know which backends are involved, so we have to send clear task
        // to all backends.
        List<Long> allBeIds = Env.getCurrentSystemInfo().getBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (clearTransactionTasks) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(
                        beId, transactionState.getTransactionId(), Lists.newArrayList());
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
                partitionInfos.add(partitionInfo);
            }
        } finally {
            readUnlock();
        }
        return partitionInfos;
    }

    public void removeExpiredTxns(long currentMillis) {
        List<Long> expiredTxnIds = Lists.newArrayList();
        // delete expired txns
        int leftNum = MAX_REMOVE_TXN_PER_ROUND;
        writeLock();
        try {
            leftNum = unprotectedRemoveExpiredTxns(currentMillis, expiredTxnIds,
                    finalStatusTransactionStateDequeShort, leftNum);
            leftNum = unprotectedRemoveExpiredTxns(currentMillis, expiredTxnIds,
                    finalStatusTransactionStateDequeLong, leftNum);

            if (!expiredTxnIds.isEmpty()) {
                Map<Long, List<Long>> dbExpiredTxnIds = Maps.newHashMap();
                dbExpiredTxnIds.put(dbId, expiredTxnIds);
                BatchRemoveTransactionsOperation op = new BatchRemoveTransactionsOperation(dbExpiredTxnIds);
                editLog.logBatchRemoveTransactions(op);
                LOG.info("Remove {} expired transactions", MAX_REMOVE_TXN_PER_ROUND - leftNum);
            }
        } finally {
            writeUnlock();
        }
    }

    private int unprotectedRemoveExpiredTxns(long currentMillis, List<Long> expiredTxnIds,
                                             ArrayDeque<TransactionState> finalStatusTransactionStateDequeShort,
                                             int maxNumber) {
        int left = maxNumber;
        while (!finalStatusTransactionStateDequeShort.isEmpty() && left > 0) {
            TransactionState transactionState = finalStatusTransactionStateDequeShort.getFirst();
            if (transactionState.isExpired(currentMillis)) {
                finalStatusTransactionStateDequeShort.pop();
                clearTransactionState(transactionState.getTransactionId());
                expiredTxnIds.add(transactionState.getTransactionId());
                left--;
            } else {
                break;
            }
        }
        return left;
    }

    private void clearTransactionState(long txnId) {
        TransactionState transactionState = idToFinalStatusTransactionState.remove(txnId);
        if (transactionState != null) {
            Set<Long> txnIds = unprotectedGetTxnIdsByLabel(transactionState.getLabel());
            txnIds.remove(transactionState.getTransactionId());
            if (txnIds.isEmpty()) {
                labelToTxnIds.remove(transactionState.getLabel());
            }
            LOG.info("transaction [" + txnId + "] is expired, remove it from transaction manager");
        } else {
            // should not happen, add a warn log to observer
            LOG.warn("transaction state is not found when clear transaction: " + txnId);
        }
    }

    public int getTransactionNum() {
        return idToRunningTransactionState.size() + finalStatusTransactionStateDequeShort.size()
                + finalStatusTransactionStateDequeLong.size();
    }


    public TransactionState getTransactionStateByCallbackIdAndStatus(long callbackId, Set<TransactionStatus> status) {
        readLock();
        try {
            for (TransactionState txn : idToRunningTransactionState.values()) {
                if (txn.getCallbackId() == callbackId && status.contains(txn.getTransactionStatus())) {
                    return txn;
                }
            }
            for (TransactionState txn : idToFinalStatusTransactionState.values()) {
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
            for (TransactionState txn : idToFinalStatusTransactionState.values()) {
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
                    .forEach(t -> txnInfos.add(Pair.of(t.getDbId(), t.getTransactionId())));
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
            Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
            TransactionState txnState = unprotectedGetTransactionState(txnId);
            if (txnState == null) {
                throw new AnalysisException("transaction with id " + txnId + " does not exist");
            }

            if (ConnectContext.get() != null) {
                // check auth
                Set<Long> tblIds = txnState.getIdToTableCommitInfos().keySet();
                for (Long tblId : tblIds) {
                    Table tbl = db.getTableNullable(tblId);
                    if (tbl != null) {
                        if (!Env.getCurrentEnv().getAuth().checkTblPriv(ConnectContext.get(), db.getFullName(),
                                tbl.getName(), PrivPredicate.SHOW)) {
                            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                    "SHOW TRANSACTION",
                                    ConnectContext.get().getQualifiedUser(),
                                    ConnectContext.get().getRemoteIP(),
                                    db.getFullName() + ": " + tbl.getName());
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

    protected void checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType sourceType)
            throws BeginTransactionException {
        switch (sourceType) {
            case ROUTINE_LOAD_TASK:
                // no need to check limit for routine load task:
                // 1. the number of running routine load tasks is limited by Config.max_routine_load_task_num_per_be
                // 2. if we add routine load txn to runningTxnNums, runningTxnNums will always be occupied by routine
                //    load, and other txn may not be able to submitted.
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
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when update catalog after committed. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    LOG.warn("partition {} of table {} does not exist when update catalog after committed."
                                    + " transaction: {}, db: {}",
                            partitionId, tableId, transactionState.getTransactionId(), db.getId());
                    continue;
                }
                List<MaterializedIndex> allIndices = partition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex index : allIndices) {
                    List<Tablet> tablets = index.getTablets();
                    for (Tablet tablet : tablets) {
                        for (Replica replica : tablet.getReplicas()) {
                            if (errorReplicaIds.contains(replica.getId())) {
                                // TODO(cmy): do we need to update last failed version here?
                                // because in updateCatalogAfterVisible, it will be updated again.
                                replica.updateLastFailedVersion(partitionCommitInfo.getVersion());
                            }
                        }
                    }
                }
                partition.setNextVersion(partition.getNextVersion() + 1);
            }
        }
    }

    private boolean updateCatalogAfterVisible(TransactionState transactionState, Database db) {
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when update catalog after visible. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            for (PartitionCommitInfo partitionCommitInfo : tableCommitInfo.getIdToPartitionCommitInfo().values()) {
                long partitionId = partitionCommitInfo.getPartitionId();
                long newCommitVersion = partitionCommitInfo.getVersion();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    LOG.warn("partition {} in table {} does not exist when update catalog after visible."
                                    + " transaction: {}, db: {}",
                            partitionId, tableId, transactionState.getTransactionId(), db.getId());
                    continue;
                }
                List<MaterializedIndex> allIndices = partition
                        .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : tablet.getReplicas()) {
                            long lastFailedVersion = replica.getLastFailedVersion();
                            long newVersion = newCommitVersion;
                            long lastSuccessVersion = replica.getLastSuccessVersion();
                            if (!errorReplicaIds.contains(replica.getId())) {
                                if (replica.getLastFailedVersion() > 0) {
                                    // if the replica is a failed replica, then not changing version
                                    newVersion = replica.getVersion();
                                } else if (!replica.checkVersionCatchUp(partition.getVisibleVersion(), true)) {
                                    // this means the replica has error in the past, but we did not observe it
                                    // during upgrade, one job maybe in quorum finished state, for example,
                                    // A,B,C 3 replica A,B 's version is 10, C's version is 10 but C' 10 is abnormal
                                    // should be rollback then we will detect this and set C's last failed version to
                                    // 10 and last success version to 11 this logic has to be replayed
                                    // in checkpoint thread
                                    lastFailedVersion = partition.getVisibleVersion();
                                    newVersion = replica.getVersion();
                                }

                                // success version always move forward
                                lastSuccessVersion = newCommitVersion;
                            } else {
                                // for example, A,B,C 3 replicas, B,C failed during publish version,
                                // then B C will be set abnormal all loading will failed, B,C will have to recovery
                                // by clone, it is very inefficient and maybe lost data Using this method, B,C will
                                // publish failed, and fe will publish again, not update their last failed version
                                // if B is publish successfully in next turn, then B is normal and C will be set
                                // abnormal so that quorum is maintained and loading will go on.
                                newVersion = replica.getVersion();
                                if (newCommitVersion > lastFailedVersion) {
                                    lastFailedVersion = newCommitVersion;
                                }
                            }
                            replica.updateVersionWithFailedInfo(newVersion, lastFailedVersion, lastSuccessVersion);
                        }
                    }
                } // end for indices
                long version = partitionCommitInfo.getVersion();
                long versionTime = partitionCommitInfo.getVersionTime();
                partition.updateVisibleVersionAndTime(version, versionTime);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("transaction state {} set partition {}'s version to [{}]",
                            transactionState, partition.getId(), version);
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
                        tableIdList) || entry.getValue().getTransactionStatus().isFinalStatus()) {
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

    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        boolean shouldAddTableListLock  = transactionState.getTransactionStatus() == TransactionStatus.COMMITTED
                || transactionState.getTransactionStatus() == TransactionStatus.VISIBLE;
        Database db = null;
        List<? extends TableIf> tableList = null;
        if (shouldAddTableListLock) {
            db = env.getInternalCatalog().getDbOrMetaException(transactionState.getDbId());
            tableList = db.getTablesOnIdOrderIfExist(transactionState.getTableIdList());
            tableList = MetaLockUtils.writeLockTablesIfExist(tableList);
        }
        writeLock();
        try {
            // set transaction status will call txn state change listener
            transactionState.replaySetTransactionStatus();
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
            if (shouldAddTableListLock) {
                MetaLockUtils.writeUnlockTables(tableList);
            }
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

        // Use 2 queues instead of idToFinalStatusTransactionState to keep the order in queues.
        for (TransactionState transactionState : finalStatusTransactionStateDequeShort) {
            transactionState.write(out);
        }

        for (TransactionState transactionState : finalStatusTransactionStateDequeLong) {
            transactionState.write(out);
        }
    }

    public void cleanLabel(String label) {
        Set<Long> removedTxnIds = Sets.newHashSet();
        writeLock();
        try {
            if (Strings.isNullOrEmpty(label)) {
                Iterator<Map.Entry<String, Set<Long>>> iter = labelToTxnIds.entrySet().iterator();
                while (iter.hasNext()) {
                    Set<Long> txnIds = iter.next().getValue();
                    Iterator<Long> innerIter = txnIds.iterator();
                    while (innerIter.hasNext()) {
                        long txnId = innerIter.next();
                        if (idToFinalStatusTransactionState.remove(txnId) != null) {
                            innerIter.remove();
                            removedTxnIds.add(txnId);
                        }
                    }
                    if (txnIds.isEmpty()) {
                        iter.remove();
                    }
                }
            } else {
                Set<Long> txnIds = labelToTxnIds.get(label);
                if (txnIds == null) {
                    return;
                }
                Iterator<Long> iter = txnIds.iterator();
                while (iter.hasNext()) {
                    long txnId = iter.next();
                    if (idToFinalStatusTransactionState.remove(txnId) != null) {
                        iter.remove();
                        removedTxnIds.add(txnId);
                    }
                }
                if (txnIds.isEmpty()) {
                    labelToTxnIds.remove(label);
                }
            }
            // remove from finalStatusTransactionStateDequeShort and finalStatusTransactionStateDequeLong
            // So that we can keep consistency in meta image
            finalStatusTransactionStateDequeShort.removeIf(txn -> removedTxnIds.contains(txn.getTransactionId()));
            finalStatusTransactionStateDequeLong.removeIf(txn -> removedTxnIds.contains(txn.getTransactionId()));
        } finally {
            writeUnlock();
        }
        LOG.info("clean {} labels on db {} with label '{}' in database transaction mgr.", removedTxnIds.size(), dbId,
                label);
    }

    public long getTxnNumByStatus(TransactionStatus status) {
        readLock();
        try {
            if (idToRunningTransactionState.size() > 10000) {
                return idToRunningTransactionState.values().parallelStream()
                        .filter(t -> t.getTransactionStatus() == status).count();
            } else {
                return idToRunningTransactionState.values().stream().filter(t -> t.getTransactionStatus() == status)
                        .count();
            }
        } finally {
            readUnlock();
        }
    }
}
