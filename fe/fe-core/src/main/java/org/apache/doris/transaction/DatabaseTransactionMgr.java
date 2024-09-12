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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
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
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.event.DataChangeEvent;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.CleanLabelOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisManager;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.ClearTransactionTask;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

    private enum PublishResult {
        FAILED,
        TIMEOUT_SUCC,  // each tablet has least one replica succ, and timeout
        QUORUM_SUCC,   // each tablet has least quorum replicas succ
    }

    private static final Logger LOG = LogManager.getLogger(DatabaseTransactionMgr.class);
    // the max number of txn that can be remove per round.
    // set it to avoid holding lock too long when removing too many txns per round.
    private static final int MAX_REMOVE_TXN_PER_ROUND = 10000;

    private final long dbId;

    // the lock is used to control the access to transaction states
    // no other locks should be inside this lock
    private final MonitoredReentrantReadWriteLock transactionLock = new MonitoredReentrantReadWriteLock(true);

    // transactionId -> running TransactionState
    private final Map<Long, TransactionState> idToRunningTransactionState = Maps.newHashMap();

    /**
     * the multi table ids that are in transaction, used to check whether a table is in transaction
     * multi table transaction state
     * txnId -> tableId list
     */
    private final ConcurrentHashMap<Long, List<Long>> multiTableRunningTransactionTableIdMaps =
            new ConcurrentHashMap<>();

    // transactionId -> final status TransactionState
    private final Map<Long, TransactionState> idToFinalStatusTransactionState = Maps.newHashMap();
    private final Map<Long, Long> subTxnIdToTxnId = new ConcurrentHashMap<>();

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

    // count the number of running txns of database
    private volatile int runningTxnNums = 0;

    private final Env env;

    private final EditLog editLog;

    private final TransactionIdGenerator idGenerator;

    private final List<ClearTransactionTask> clearTransactionTasks = Lists.newArrayList();

    // not realtime usedQuota value to make a fast check for database data quota
    private volatile long usedQuotaDataBytes = -1;

    private long lockWriteStart;

    private long lockReportingThresholdMs = Config.lock_reporting_threshold_ms;

    private void readLock() {
        this.transactionLock.readLock().lock();
    }

    private void readUnlock() {
        this.transactionLock.readLock().unlock();
    }

    private void writeLock() {
        this.transactionLock.writeLock().lock();
        lockWriteStart = System.currentTimeMillis();
    }

    private void writeUnlock() {
        checkAndLogWriteLockDuration(lockWriteStart, System.currentTimeMillis());
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

    protected TransactionState getTransactionState(Long transactionId) {
        readLock();
        try {
            return unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
        }
    }

    private TransactionState unprotectedGetTransactionState(Long transactionId) {
        TransactionState transactionState = idToRunningTransactionState.get(transactionId);
        if (transactionState != null) {
            return transactionState;
        }
        transactionState = idToFinalStatusTransactionState.get(transactionId);
        if (transactionState != null) {
            return transactionState;
        }
        if (subTxnIdToTxnId.containsKey(transactionId)) {
            return unprotectedGetTransactionState(subTxnIdToTxnId.get(transactionId));
        }
        return null;
    }

    @VisibleForTesting
    protected Set<Long> unprotectedGetTxnIdsByLabel(String label) {
        return labelToTxnIds.get(label);
    }

    protected int getRunningTxnNums() {
        return runningTxnNums;
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

    public Map<Long, List<Long>> getDbRunningTransInfo() {
        Map<Long, List<Long>> infos = Maps.newHashMap();
        readLock();
        try {
            for (Entry<Long, TransactionState> info : idToRunningTransactionState.entrySet()) {
                infos.put(info.getKey(), info.getValue().getTableIdList());
            }
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

    public List<List<String>> getTxnStateInfoList(String labelRegex) {
        List<List<String>> infos = Lists.newArrayList();
        List<TransactionState> transactionStateCollection = Lists.newArrayList();
        readLock();
        try {
            transactionStateCollection.addAll(idToFinalStatusTransactionState.values());
            transactionStateCollection.addAll(idToRunningTransactionState.values());
            // get transaction order by txn id desc
            transactionStateCollection.stream()
                    .filter(transactionState -> (transactionState.getLabel().matches(labelRegex)))
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
        info.add(TimeUtils.longToTimeString(txnState.getLastPublishVersionTime()));
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
        Database db = env.getInternalCatalog().getDbOrMetaException(dbId);
        if (!coordinator.isFromInternal) {
            InternalDatabaseUtil.checkDatabase(db.getFullName(), ConnectContext.get());
        }
        MTMVUtil.checkModifyMTMVData(db, tableIdList, ConnectContext.get());
        checkDatabaseDataQuota();
        Preconditions.checkNotNull(coordinator);
        Preconditions.checkNotNull(label);
        FeNameFormat.checkLabel(label);

        long tid = 0L;
        writeLock();
        try {
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

            checkRunningTxnExceedLimit();

            tid = idGenerator.getNextTransactionId();
            TransactionState transactionState = new TransactionState(dbId, tableIdList,
                    tid, label, requestId, sourceType, coordinator, listenerId, timeoutSecond * 1000);
            transactionState.setPrepareTime(System.currentTimeMillis());
            unprotectUpsertTransactionState(transactionState, false);

            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("begin transaction: txn id {} with label {} from coordinator {}, listener id: {}",
                    tid, label, coordinator, listenerId);
        return tid;
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already visible: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction is already visible");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already committed: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction is already committed");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.PRECOMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already pre-committed: {}", transactionId);
            }
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
        long transactionId = transactionState.getTransactionId();
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
        HashMap<Long, Boolean> tableIdtoRestoring = new HashMap<>();
        for (int i = 0; i < tabletMetaList.size(); i++) {
            // get partition and table of this tablet
            TabletMeta tabletMeta = tabletMetaList.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                continue;
            }
            long tabletId = tabletIds.get(i);
            long tableId = tabletMeta.getTableId();
            OlapTable tbl = (OlapTable) idToTable.get(tableId);
            if (tbl == null) {
                // this can happen when tableId == -1 (tablet being dropping) or table really not exist.
                continue;
            }

            // check relative partition restore here
            long partitionId = tabletMeta.getPartitionId();
            if (tbl.getPartition(partitionId) == null) {
                // this can happen when partitionId == -1 (tablet being dropping) or partition really not exist.
                continue;
            }
            if (tbl.getPartition(partitionId).getState() == PartitionState.RESTORE) {
                // partition in restore process which can not load data
                throw new LoadException("Table [" + tbl.getName() + "], Partition ["
                        + tbl.getPartition(partitionId).getName() + "] is in restore process. Can not load into it");
            }

            // only do check when here's restore on this table now
            if (tbl.getState() == OlapTableState.RESTORE) {
                boolean hasPartitionRestoring = false;
                if (tableIdtoRestoring.containsKey(tableId)) {
                    hasPartitionRestoring = tableIdtoRestoring.get(tableId);
                } else {
                    for (Partition partition : tbl.getPartitions()) {
                        if (partition.getState() == PartitionState.RESTORE) {
                            hasPartitionRestoring = true;
                            break;
                        }
                    }
                    tableIdtoRestoring.put(tableId, hasPartitionRestoring);
                }
                // tbl RESTORE && all partition NOT RESTORE -> whole table restore
                // tbl RESTORE && some partition RESTORE -> just partitions restore, NOT WHOLE TABLE
                // so check wether the whole table restore here
                if (!hasPartitionRestoring) {
                    throw new LoadException(
                            "Table " + tbl.getName() + " is in restore process. " + "Can not load into it");
                }
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
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();

        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableOrMetaException(tableId);
            for (Partition partition : table.getAllPartitions()) {
                if (!tableToPartition.get(tableId).contains(partition.getId())) {
                    continue;
                }

                List<MaterializedIndex> allIndices;
                if (transactionState.getLoadedTblIndexes().isEmpty()
                        || transactionState.getLoadedTblIndexes().get(tableId) == null) {
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

                // (TODO): ignore the alter index if txn id is less than sc sched watermark
                int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partition.getId());
                for (MaterializedIndex index : allIndices) {
                    for (Tablet tablet : index.getTablets()) {
                        tabletSuccReplicas.clear();
                        tabletWriteFailedReplicas.clear();
                        tabletVersionFailedReplicas.clear();
                        long tabletId = tablet.getId();
                        Set<Long> tabletBackends = tablet.getBackendIds();
                        totalInvolvedBackends.addAll(tabletBackends);
                        Set<Long> commitBackends = tabletToBackends.get(tabletId);
                        // save the error replica ids for current tablet
                        // this param is used for log
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
                                    tabletSuccReplicas.add(replica);
                                } else {
                                    tabletVersionFailedReplicas.add(replica);
                                }
                            } else {
                                tabletWriteFailedReplicas.add(replica);
                                errorReplicaIds.add(replica.getId());
                            }
                        }

                        int successReplicaNum = tabletSuccReplicas.size();
                        if (successReplicaNum < loadRequiredReplicaNum) {
                            long now = System.currentTimeMillis();
                            long lastLoadFailedTime = tablet.getLastLoadFailedTime();
                            tablet.setLastLoadFailedTime(now);
                            if (now - lastLoadFailedTime >= 5000L) {
                                Env.getCurrentEnv().getTabletScheduler().tryAddRepairTablet(
                                        tablet, db.getId(), table, partition, index, 0);
                            }

                            String writeDetail = getTabletWriteDetail(tabletSuccReplicas, tabletWriteFailedReplicas,
                                    tabletVersionFailedReplicas);

                            String errMsg = String.format("Failed to commit txn %s, cause tablet %s succ replica num %s"
                                    + " < load required replica num %s. table %s, partition: [ id=%s, commit version %s"
                                    + ", visible version %s ], this tablet detail: %s",
                                    transactionId, tablet.getId(), successReplicaNum, loadRequiredReplicaNum, tableId,
                                    partition.getId(), partition.getCommittedVersion(), partition.getVisibleVersion(),
                                    writeDetail);
                            LOG.info(errMsg);

                            throw new TabletQuorumFailedException(transactionId, errMsg);
                        }
                    }
                }
            }
        }
    }

    private String getTabletWriteDetail(List<Replica> tabletSuccReplicas, List<Replica> tabletWriteFailedReplicas,
            List<Replica> tabletVersionFailedReplicas) {
        String writeDetail = "";
        if (!tabletSuccReplicas.isEmpty()) {
            writeDetail += String.format("%s replicas final succ: { %s }; ",
                    tabletSuccReplicas.size(), Joiner.on(", ").join(
                            tabletSuccReplicas.stream().map(replica -> replica.toStringSimple(true))
                                    .collect(Collectors.toList())));
        }
        if (!tabletWriteFailedReplicas.isEmpty()) {
            writeDetail += String.format("%s replicas write data failed: { %s }; ",
                    tabletWriteFailedReplicas.size(), Joiner.on(", ").join(
                            tabletWriteFailedReplicas.stream().map(replica -> replica.toStringSimple(true))
                                    .collect(Collectors.toList())));
        }
        if (!tabletVersionFailedReplicas.isEmpty()) {
            writeDetail += String.format("%s replicas write data succ but miss previous "
                            + "version: { %s }.",
                    tabletVersionFailedReplicas.size(), Joiner.on(",").join(
                            tabletVersionFailedReplicas.stream().map(replica -> replica.toStringSimple(true))
                                    .collect(Collectors.toList())));
        }

        return writeDetail;
    }

    /**
     * @return true if the transaction need to commit, otherwise false
     */
    private boolean checkTransactionStateBeforeCommit(Database db, List<Table> tableList, long transactionId,
            boolean is2PC, TransactionState transactionState)
            throws TransactionCommitFailedException {
        if (transactionState == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction not found: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction [" + transactionId + "] not found.");
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already aborted: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] is already aborted. abort reason: " + transactionState.getReason());
        }

        if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already visible: {}", transactionId);
            }
            if (is2PC) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already visible, not pre-committed.");
            }
            return false;
        }
        if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is already committed: {}", transactionId);
            }
            if (is2PC) {
                throw new TransactionCommitFailedException("transaction [" + transactionId
                        + "] is already committed, not pre-committed.");
            }
            return false;
        }

        if (is2PC && transactionState.getTransactionStatus() == TransactionStatus.PREPARE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction is prepare, not pre-committed: {}", transactionId);
            }
            throw new TransactionCommitFailedException("transaction [" + transactionId
                    + "] is prepare, not pre-committed.");
        }

        if (transactionState.isPartialUpdate()) {
            if (is2PC) {
                Iterator<TableCommitInfo> tableCommitInfoIterator
                        = transactionState.getIdToTableCommitInfos().values().iterator();
                while (tableCommitInfoIterator.hasNext()) {
                    TableCommitInfo tableCommitInfo = tableCommitInfoIterator.next();
                    long tableId = tableCommitInfo.getTableId();
                    OlapTable table = (OlapTable) db.getTableNullable(tableId);
                    if (table != null && table instanceof OlapTable) {
                        if (!transactionState.checkSchemaCompatibility((OlapTable) table)) {
                            throw new TransactionCommitFailedException("transaction [" + transactionId
                                    + "] check schema compatibility failed, partial update can't commit with"
                                            + " old schema sucessfully .");
                        }
                    }
                }
            } else {
                for (Table table : tableList) {
                    if (table instanceof OlapTable) {
                        if (!transactionState.checkSchemaCompatibility((OlapTable) table)) {
                            throw new TransactionCommitFailedException("transaction [" + transactionId
                                    + "] check schema compatibility failed, partial update can't commit with"
                                            + " old schema sucessfully .");
                        }
                    }
                }
            }
        }
        return true;
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

        if (!checkTransactionStateBeforeCommit(db, tableList, transactionId, is2PC, transactionState)) {
            return;
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
            try {
                transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
            } catch (Throwable e) {
                LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
            }
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db, false);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    public void commitTransaction(long transactionId, List<Table> tableList,
            List<SubTransactionState> subTransactionStates) throws UserException {
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

        if (DebugPointUtil.isEnable("DatabaseTransactionMgr.commitTransaction.failed")) {
            throw new TabletQuorumFailedException(transactionId,
                    "DebugPoint: DatabaseTransactionMgr.commitTransaction.failed");
        }

        if (!checkTransactionStateBeforeCommit(db, tableList, transactionId, false, transactionState)) {
            return;
        }

        // error replica may be duplicated for different sub transaction, but it's ok
        Set<Long> errorReplicaIds = Sets.newHashSet();
        Map<Long, Set<Long>> subTxnToPartition = new HashMap<>();
        Set<Long> totalInvolvedBackends = Sets.newHashSet();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            Map<Long, Set<Long>> tableToPartition = new HashMap<>();
            Table table = subTransactionState.getTable();
            List<TTabletCommitInfo> tabletCommitInfos = subTransactionState.getTabletCommitInfos();
            checkCommitStatus(Lists.newArrayList(table), transactionState,
                    TabletCommitInfo.fromThrift(tabletCommitInfos), null,
                    errorReplicaIds, tableToPartition, totalInvolvedBackends);
            Preconditions.checkState(tableToPartition.size() <= 1, "tableToPartition=" + tableToPartition);
            if (tableToPartition.size() > 0) {
                subTxnToPartition.put(subTransactionState.getSubTransactionId(), tableToPartition.get(table.getId()));
            }
        }

        // before state transform
        transactionState.beforeStateTransform(TransactionStatus.COMMITTED);
        // transaction state transform
        boolean txnOperated = false;
        writeLock();
        try {
            unprotectedCommitTransaction(transactionState, errorReplicaIds, subTxnToPartition, totalInvolvedBackends,
                    subTransactionStates, db);
            txnOperated = true;
        } finally {
            writeUnlock();
            // after state transform
            try {
                transactionState.afterStateTransform(TransactionStatus.COMMITTED, txnOperated);
            } catch (Throwable e) {
                LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
            }
        }

        // update nextVersion because of the failure of persistent transaction resulting in error version
        updateCatalogAfterCommitted(transactionState, db, false);
        LOG.info("transaction:[{}] successfully committed", transactionState);
    }

    public boolean waitForTransactionFinished(DatabaseIf db, long transactionId, long timeoutMillis)
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

    public void replayBatchRemoveTransaction(BatchRemoveTransactionsOperationV2 operation) {
        writeLock();
        try {
            if (operation.getLatestTxnIdForShort() != -1) {
                while (!finalStatusTransactionStateDequeShort.isEmpty()) {
                    TransactionState transactionState = finalStatusTransactionStateDequeShort.pop();
                    clearTransactionState(transactionState.getTransactionId());
                    if (operation.getLatestTxnIdForShort() == transactionState.getTransactionId()) {
                        break;
                    }
                }
            }

            if (operation.getLatestTxnIdForLong() != -1) {
                while (!finalStatusTransactionStateDequeLong.isEmpty()) {
                    TransactionState transactionState = finalStatusTransactionStateDequeLong.pop();
                    clearTransactionState(transactionState.getTransactionId());
                    if (operation.getLatestTxnIdForLong() == transactionState.getTransactionId()) {
                        break;
                    }
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

    protected Long getTransactionIdByLabel(String label) {
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

    protected Long getTransactionIdByLabel(String label, List<TransactionStatus> statusList) throws UserException {
        readLock();
        try {
            TransactionState findTxn = null;
            for (TransactionStatus status : statusList) {
                Set<Long> existingTxns = unprotectedGetTxnIdsByLabel(label);
                if (existingTxns == null || existingTxns.isEmpty()) {
                    throw new TransactionNotFoundException("transaction not found, label=" + label);
                }
                for (Long txnId : existingTxns) {
                    TransactionState txn = unprotectedGetTransactionState(txnId);
                    if (txn.getTransactionStatus() == status) {
                        findTxn = txn;
                        break;
                    }
                }
            }

            if (findTxn == null) {
                throw new TransactionNotFoundException("running transaction not found, label=" + label);
            }

            return findTxn.getTransactionId();
        } finally {
            readUnlock();
        }
    }

    protected List<TransactionState> getPreCommittedTxnList() {
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

    protected List<TransactionState> getCommittedTxnList() {
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

    public void finishTransaction(long transactionId, Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) throws UserException {
        if (DebugPointUtil.isEnable("DatabaseTransactionMgr.stop_finish_transaction")) {
            return;
        }

        TransactionState transactionState = null;
        readLock();
        try {
            transactionState = unprotectedGetTransactionState(transactionId);
        } finally {
            readUnlock();
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
        List<Long> tableIdList;
        if (transactionState.getSubTxnIds() == null) {
            tableIdList = transactionState.getTableIdList();
        } else {
            tableIdList = transactionState.getSubTxnTableCommitInfos().stream().map(s -> s.getTableId()).distinct()
                    .collect(Collectors.toList());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("finish transaction {} with tables {}", transactionId, tableIdList);
        }
        List<? extends TableIf> tableList = db.getTablesOnIdOrderIfExist(tableIdList);
        if (!MetaLockUtils.tryWriteLockTablesIfExist(tableList, 10, TimeUnit.SECONDS)) {
            LOG.warn("finish transaction {} failed, get lock timeout with tables {}", transactionId, tableIdList);
            return;
        }
        PublishResult publishResult;
        try {
            // add all commit errors and publish errors to a single set
            Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
            if (transactionState.getSubTxnIds() == null) {
                List<Pair<OlapTable, Partition>> relatedTblPartitions = Lists.newArrayList();
                if (!finishCheckPartitionVersion(transactionState, db, relatedTblPartitions)) {
                    return;
                }
                publishResult = finishCheckQuorumReplicas(transactionState, relatedTblPartitions, errorReplicaIds);
                if (publishResult == PublishResult.FAILED) {
                    return;
                }
            } else {
                if (!finishCheckPartitionVersionWithSubTxns(transactionState, db)) {
                    return;
                }
                publishResult = finishCheckQuorumReplicas(transactionState, errorReplicaIds);
                if (publishResult == PublishResult.FAILED) {
                    return;
                }
            }
            boolean txnOperated = false;
            writeLock();
            try {
                transactionState.setErrorReplicas(errorReplicaIds);
                transactionState.setFinishTime(System.currentTimeMillis());
                transactionState.clearErrorMsg();
                transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
                setTableVersion(transactionState, db);
                unprotectUpsertTransactionState(transactionState, false);
                txnOperated = true;
                // TODO(cmy): We found a very strange problem. When delete-related transactions are processed here,
                // subsequent `updateCatalogAfterVisible()` is called, but it does not seem to be executed here
                // (because the relevant editlog does not see the log of visible transactions).
                // So I add a log here for observation.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("after set transaction {} to visible", transactionState);
                }
            } finally {
                writeUnlock();
                try {
                    transactionState.afterStateTransform(TransactionStatus.VISIBLE, txnOperated);
                } catch (Throwable e) {
                    LOG.warn("afterStateTransform txn {} failed. exception: ", transactionState, e);
                }
            }
            updateCatalogAfterVisible(transactionState, db, partitionVisibleVersions, backendPartitions);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        // Here, we only wait for the EventProcessor to finish processing the event,
        // but regardless of the success or failure of the result,
        // it does not affect the logic of transaction
        try {
            produceEvent(transactionState, db);
        } catch (Throwable t) {
            // According to normal logic, no exceptions will be thrown,
            // but in order to avoid bugs affecting the original logic, all exceptions are caught
            LOG.warn("produceEvent failed: ", t);
        }

        // The visible latch should only be counted down after all things are done
        // (finish transaction, write edit log, etc).
        // Otherwise, there is no way for stream load to query the result right after loading finished,
        // even if we call "sync" before querying.
        transactionState.countdownVisibleLatch();
        LOG.info("finish transaction {} successfully, publish times {}, publish result {}",
                transactionState, transactionState.getPublishCount(), publishResult.name());
    }

    private void setTableVersion(TransactionState transactionState, Database db) {
        List<TableCommitInfo> tableCommitInfos;
        if (!transactionState.getSubTxnIdToTableCommitInfo().isEmpty()) {
            tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
        } else {
            tableCommitInfos = Lists.newArrayList(transactionState.getIdToTableCommitInfos().values());
        }
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when setTableVersion. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            tableCommitInfo.setVersion(table.getNextVersion());
            tableCommitInfo.setVersionTime(System.currentTimeMillis());
        }
    }

    private void produceEvent(TransactionState transactionState, Database db) throws AnalysisException {
        Collection<TableCommitInfo> tableCommitInfos;
        if (!transactionState.getSubTxnIdToTableCommitInfo().isEmpty()) {
            tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
        } else {
            tableCommitInfos = transactionState.getIdToTableCommitInfos().values();
        }
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when produceEvent. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            Env.getCurrentEnv().getEventProcessor().processEvent(
                    new DataChangeEvent(db.getCatalog().getId(), db.getId(), tableId));
        }
    }

    private boolean finishCheckPartitionVersion(TransactionState transactionState, Database db,
            List<Pair<OlapTable, Partition>> relatedTblPartitions) {
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
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("for table {} partition {}, transactionId {} partition commitInfo version {} is not"
                                                + " equal with partition visible version {} plus one, need wait",
                                table.getId(), partition.getId(), transactionState.getTransactionId(),
                                partitionCommitInfo.getVersion(), partition.getVisibleVersion());
                    }
                    String errMsg = String.format("wait for publishing partition %d version %d."
                                    + " self version: %d. table %d", partitionId, partition.getVisibleVersion() + 1,
                            partitionCommitInfo.getVersion(), tableId);
                    transactionState.setErrorMsg(errMsg);
                    return false;
                }

                relatedTblPartitions.add(Pair.of(table, partition));
            }
        }

        return true;
    }

    private boolean finishCheckPartitionVersionWithSubTxns(TransactionState transactionState, Database db) {
        List<Pair<OlapTable, Partition>> relatedTblPartitions = new ArrayList<>();
        Map<Long, Long> partitionToVisibleVersion = new HashMap<>();
        Iterator<Long> iterator = transactionState.getSubTxnIds().iterator();
        while (iterator.hasNext()) {
            long subTxnId = iterator.next();
            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(subTxnId);
            if (tableCommitInfo == null) {
                continue;
            }
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            // table maybe dropped between commit and publish, ignore this error
            if (table == null) {
                iterator.remove();
                LOG.warn("table {} is dropped, skip version check and remove it from transaction state {}",
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
                    LOG.warn("partition {} is dropped, skip version check"
                            + " and remove it from transaction state {}", partitionId, transactionState);
                    continue;
                }
                long curPartitionVersion = partitionToVisibleVersion.getOrDefault(partitionId,
                        partition.getVisibleVersion());
                if (curPartitionVersion != partitionCommitInfo.getVersion() - 1) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("for table {} partition {}, transactionId {}, subTransactionId {},"
                                        + " partition commitInfo version {} is not"
                                        + " equal with partition expected version {}, visible version {}, need wait",
                                table.getId(), partition.getId(), transactionState.getTransactionId(), subTxnId,
                                partitionCommitInfo.getVersion(), curPartitionVersion + 1,
                                partition.getVisibleVersion());
                    }
                    String errMsg = String.format("wait for publishing partition %d version %d, visible version %d."
                                    + " self version: %d. table %d, transactionId %d, subTransactionId %d", partitionId,
                            curPartitionVersion + 1, partition.getVisibleVersion(), partitionCommitInfo.getVersion(),
                            tableId, transactionState.getTransactionId(), subTxnId);
                    transactionState.setErrorMsg(errMsg);
                    return false;
                }
                partitionToVisibleVersion.put(partitionId, partitionCommitInfo.getVersion());

                relatedTblPartitions.add(Pair.of(table, partition));
            }
        }

        return true;
    }

    private PublishResult finishCheckQuorumReplicas(TransactionState transactionState,
            List<Pair<OlapTable, Partition>> relatedTblPartitions,
            Set<Long> errorReplicaIds) {
        long now = System.currentTimeMillis();
        long firstPublishVersionTime = transactionState.getFirstPublishVersionTime();
        boolean allowPublishOneSucc = false;
        if (Config.publish_wait_time_second > 0 && firstPublishVersionTime > 0
                && now >= firstPublishVersionTime + Config.publish_wait_time_second * 1000L) {
            allowPublishOneSucc = true;
        }

        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
        List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();
        TabletsPublishResultLogs logs = new TabletsPublishResultLogs();

        Map<Long, List<PublishVersionTask>> publishTasks = transactionState.getPublishVersionTasks();
        PublishResult publishResult = PublishResult.QUORUM_SUCC;
        for (Pair<OlapTable, Partition> pair : relatedTblPartitions) {
            OlapTable table = pair.key();
            Partition partition = pair.value();
            long tableId = table.getId();
            long partitionId = partition.getId();
            long newVersion = partition.getVisibleVersion() + 1;
            int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partitionId);
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

            long maxAlterWaterschedTxnId = getMaxAlterWaterschedTxnId(table);

            // check success replica number for each tablet.
            // a success replica means:
            //  1. Not in errorReplicaIds: succeed in both commit and publish phase
            //  2. last failed version < 0: is a health replica before
            //  3. version catch up: not with a stale version
            // Here we only check number, the replica version will be updated in updateCatalogAfterVisible()
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : index.getTablets()) {
                    tabletSuccReplicas.clear();
                    tabletWriteFailedReplicas.clear();
                    tabletVersionFailedReplicas.clear();
                    for (Replica replica : tablet.getReplicas()) {
                        List<PublishVersionTask> publishVersionTasks = publishTasks.get(replica.getBackendId());
                        Preconditions.checkState(publishVersionTasks == null || publishVersionTasks.size() == 1,
                                "publish tasks: " + publishVersionTasks);
                        PublishVersionTask publishVersionTask = null;
                        if (publishVersionTasks != null) {
                            publishVersionTask = publishVersionTasks.get(0);
                        }
                        checkReplicaContinuousVersionSucc(Lists.newArrayList(transactionState.getTransactionId()),
                                maxAlterWaterschedTxnId, tablet.getId(), replica, newVersion, newVersion,
                                Lists.newArrayList(publishVersionTask), errorReplicaIds, tabletSuccReplicas,
                                tabletWriteFailedReplicas, tabletVersionFailedReplicas);
                    }

                    publishResult = checkQuorumReplicas(transactionState, tableId, partition, tablet,
                            loadRequiredReplicaNum, allowPublishOneSucc, newVersion, tabletSuccReplicas,
                            tabletWriteFailedReplicas, tabletVersionFailedReplicas, publishResult, logs);
                    if (publishResult == PublishResult.QUORUM_SUCC) {
                        tablet.setLastLoadFailedTime(-1L);
                    }
                }
            }
        }

        boolean needLog = publishResult != PublishResult.FAILED
                || now - transactionState.getLastPublishLogTime() > Config.publish_fail_log_interval_second * 1000L;
        if (needLog) {
            transactionState.setLastPublishLogTime(now);
            logs.log();
        }

        return publishResult;
    }

    private long getMaxAlterWaterschedTxnId(OlapTable table) {
        List<AlterJobV2> unfinishedAlterJobs;
        if (table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
            unfinishedAlterJobs = Env.getCurrentEnv().getAlterInstance().getSchemaChangeHandler()
                    .getUnfinishedAlterJobV2ByTableId(table.getId());
        } else if (table.getState() == OlapTable.OlapTableState.ROLLUP) {
            unfinishedAlterJobs = Env.getCurrentEnv().getAlterInstance().getMaterializedViewHandler()
                    .getUnfinishedAlterJobV2ByTableId(table.getId());
        } else {
            unfinishedAlterJobs = Lists.newArrayList();
        }
        return unfinishedAlterJobs.stream().mapToLong(AlterJobV2::getWatershedTxnId).max().orElse(-1);
    }

    private void checkReplicaContinuousVersionSucc(List<Long> subTxnIds, long alterWaterschedTxnId, long tabletId,
            Replica replica, long minReplicaVersion, long maxReplicaVersion,
            List<PublishVersionTask> replicaPublishTasks, Set<Long> errorReplicaIds, List<Replica> tabletSuccReplicas,
            List<Replica> tabletWriteFailedReplicas, List<Replica> tabletVersionFailedReplicas) {
        boolean success = true;
        for (int i = 0; i < subTxnIds.size(); i++) {
            PublishVersionTask task = replicaPublishTasks.get(i);
            success = (task != null && task.isFinished() && task.getSuccTablets().containsKey(tabletId)) || (
                    replica.getState() == Replica.ReplicaState.ALTER && (!Config.publish_version_check_alter_replica
                            || subTxnIds.get(i) < alterWaterschedTxnId || alterWaterschedTxnId == -1));
            if (!success) {
                break;
            }
        }
        if (success) {
            errorReplicaIds.remove(replica.getId());
        } else {
            errorReplicaIds.add(replica.getId());
        }

        if (!errorReplicaIds.contains(replica.getId())) {
            if (replica.checkVersionCatchUp(minReplicaVersion - 1, true)) {
                tabletSuccReplicas.add(replica);
            } else {
                tabletVersionFailedReplicas.add(replica);
            }
        } else if (replica.getVersion() >= maxReplicaVersion) {
            tabletSuccReplicas.add(replica);
            errorReplicaIds.remove(replica.getId());
        } else {
            tabletWriteFailedReplicas.add(replica);
        }
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
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            PartitionInfo tblPartitionInfo = table.getPartitionInfo();
            for (long partitionId : tableToPartition.get(tableId)) {
                String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
                PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(
                        partitionId, partitionRange, -1, -1,
                        table.isTemporaryPartition(partitionId));
                tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    private PartitionCommitInfo generatePartitionCommitInfo(OlapTable table, long partitionId, long partitionVersion) {
        PartitionInfo tblPartitionInfo = table.getPartitionInfo();
        String partitionRange = tblPartitionInfo.getPartitionRangeString(partitionId);
        return new PartitionCommitInfo(partitionId, partitionRange,
                partitionVersion, System.currentTimeMillis() /* use as partition visible time */,
                table.isTemporaryPartition(partitionId));
    }

    protected void unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
                                                Map<Long, Set<Long>> tableToPartition, Set<Long> totalInvolvedBackends,
                                                Database db) {
        checkBeforeUnprotectedCommitTransaction(transactionState, errorReplicaIds);

        for (long tableId : tableToPartition.keySet()) {
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
            for (long partitionId : tableToPartition.get(tableId)) {
                Partition partition = table.getPartition(partitionId);
                tableCommitInfo.addPartitionCommitInfo(
                        generatePartitionCommitInfo(table, partitionId, partition.getNextVersion()));
            }
            transactionState.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    private void checkBeforeUnprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds) {
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

        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);
    }

    protected void unprotectedCommitTransaction(TransactionState transactionState, Set<Long> errorReplicaIds,
            Map<Long, Set<Long>> subTxnToPartition, Set<Long> totalInvolvedBackends,
            List<SubTransactionState> subTransactionStates, Database db) {
        checkBeforeUnprotectedCommitTransaction(transactionState, errorReplicaIds);

        Map<Long, List<SubTransactionState>> tableToSubTransactionState = new HashMap<>();
        for (SubTransactionState subTransactionState : subTransactionStates) {
            long tableId = subTransactionState.getTable().getId();
            tableToSubTransactionState.computeIfAbsent(tableId, k -> new ArrayList<>()).add(subTransactionState);
        }

        for (Entry<Long, List<SubTransactionState>> entry : tableToSubTransactionState.entrySet()) {
            long tableId = entry.getKey();
            List<SubTransactionState> subTransactionStateList = entry.getValue();

            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            long tableNextVersion = table.getNextVersion();
            Map<Long, Long> partitionToVersion = new HashMap<>();

            for (SubTransactionState subTransactionState : subTransactionStateList) {
                Set<Long> partitionIds = subTxnToPartition.get(subTransactionState.getSubTransactionId());
                if (partitionIds == null) {
                    continue;
                }
                TableCommitInfo tableCommitInfo = new TableCommitInfo(tableId);
                tableCommitInfo.setVersion(tableNextVersion);
                tableCommitInfo.setVersionTime(System.currentTimeMillis());

                for (long partitionId : partitionIds) {
                    long partitionNextVersion = table.getPartition(partitionId).getNextVersion();
                    if (partitionToVersion.containsKey(partitionId)) {
                        partitionNextVersion = partitionToVersion.get(partitionId) + 1;
                    }
                    partitionToVersion.put(partitionId, partitionNextVersion);

                    PartitionCommitInfo partitionCommitInfo = generatePartitionCommitInfo(table, partitionId,
                            partitionNextVersion);
                    tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
                    LOG.info("commit txn_id={}, sub_txn_id={}, partition_id={}, version={}",
                            transactionState.getTransactionId(), subTransactionState.getSubTransactionId(),
                            partitionId, partitionNextVersion);
                }
                transactionState.addSubTxnTableCommitInfo(subTransactionState, tableCommitInfo);
            }
        }
        // persist transactionState
        unprotectUpsertTransactionState(transactionState, false);
        transactionState.setInvolvedBackends(totalInvolvedBackends);
    }

    protected void unprotectedCommitTransaction2PC(TransactionState transactionState, Database db) {
        // transaction state is modified during check if the transaction could committed
        if (transactionState.getTransactionStatus() != TransactionStatus.PRECOMMITTED) {
            LOG.warn("Unknown exception. state of transaction [{}] changed, failed to commit transaction",
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
                runningTxnNums++;
            }
        } else {
            if (idToRunningTransactionState.remove(transactionState.getTransactionId()) != null) {
                runningTxnNums--;
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

    public int getRunningTxnNumsWithLock() {
        readLock();
        try {
            return runningTxnNums;
        } finally {
            readUnlock();
        }
    }

    private void updateTxnLabels(TransactionState transactionState) {
        Set<Long> txnIds = labelToTxnIds.computeIfAbsent(transactionState.getLabel(), k -> Sets.newHashSet());
        txnIds.add(transactionState.getTransactionId());
    }

    public void abortTransaction(String label, String reason) throws UserException {
        Preconditions.checkNotNull(label);
        List<TransactionStatus> status = new ArrayList<>();
        status.add(TransactionStatus.PREPARE);
        long transactionId = getTransactionIdByLabel(label, status);
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
        List<Long> allBeIds = Env.getCurrentSystemInfo().getAllBackendIds(false);
        AgentBatchTask batchTask = null;
        synchronized (clearTransactionTasks) {
            for (Long beId : allBeIds) {
                ClearTransactionTask task = new ClearTransactionTask(
                        beId, transactionState.getTransactionId(), Lists.newArrayList());
                clearTransactionTasks.add(task);
            }
            if (transactionState.getSubTxnIds() != null) {
                for (long subTxnId : transactionState.getSubTxnIds()) {
                    for (Long beId : allBeIds) {
                        ClearTransactionTask task = new ClearTransactionTask(beId, subTxnId, Lists.newArrayList());
                        clearTransactionTasks.add(task);
                    }
                }
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

            Iterator<TableCommitInfo> idToTableCommitInfos;
            if (transactionState.getSubTxnIds() != null) {
                idToTableCommitInfos = transactionState.getSubTxnTableCommitInfos().iterator();
            } else {
                idToTableCommitInfos = transactionState.getIdToTableCommitInfos().values().iterator();
            }
            while (idToTableCommitInfos.hasNext()) {
                TableCommitInfo tableCommitInfo = idToTableCommitInfos.next();
                List<Comparable> tableInfo = new ArrayList<>();
                tableInfo.add(tableCommitInfo.getTableId());
                tableInfo.add(Joiner.on(", ").join(tableCommitInfo.getIdToPartitionCommitInfo().values().stream().map(
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

            List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
            if (transactionState.getSubTxnIds() != null) {
                for (TableCommitInfo tableCommitInfo : transactionState.getSubTxnTableCommitInfos()) {
                    if (tableCommitInfo.getTableId() == tableId) {
                        partitionCommitInfos.addAll(tableCommitInfo.getIdToPartitionCommitInfo().values());
                    }
                }
            } else {
                partitionCommitInfos.addAll(
                        transactionState.getIdToTableCommitInfos().get(tableId).getIdToPartitionCommitInfo().values());
            }
            for (PartitionCommitInfo partitionCommitInfo : partitionCommitInfos) {
                List<Comparable> partitionInfo = new ArrayList<>();
                partitionInfo.add(partitionCommitInfo.getPartitionId());
                partitionInfo.add(partitionCommitInfo.getVersion());
                partitionInfos.add(partitionInfo);
            }
        } finally {
            readUnlock();
        }
        return partitionInfos;
    }

    public void removeUselessTxns(long currentMillis) {
        // delete expired txns
        writeLock();
        try {
            Pair<Long, Integer> expiredTxnsInfoForShort = unprotectedRemoveUselessTxns(currentMillis,
                    finalStatusTransactionStateDequeShort, MAX_REMOVE_TXN_PER_ROUND);
            Pair<Long, Integer> expiredTxnsInfoForLong = unprotectedRemoveUselessTxns(currentMillis,
                    finalStatusTransactionStateDequeLong,
                    MAX_REMOVE_TXN_PER_ROUND - expiredTxnsInfoForShort.second);
            int numOfClearedTransaction = expiredTxnsInfoForShort.second + expiredTxnsInfoForLong.second;
            if (numOfClearedTransaction > 0) {
                BatchRemoveTransactionsOperationV2 op = new BatchRemoveTransactionsOperationV2(dbId,
                        expiredTxnsInfoForShort.first, expiredTxnsInfoForLong.first);
                editLog.logBatchRemoveTransactions(op);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Remove {} expired transactions", numOfClearedTransaction);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private Pair<Long, Integer> unprotectedRemoveUselessTxns(long currentMillis,
            ArrayDeque<TransactionState> finalStatusTransactionStateDeque, int left) {
        long latestTxnId = -1;
        int numOfClearedTransaction = 0;
        while (!finalStatusTransactionStateDeque.isEmpty() && numOfClearedTransaction < left) {
            TransactionState transactionState = finalStatusTransactionStateDeque.getFirst();
            if (transactionState.isExpired(currentMillis)) {
                finalStatusTransactionStateDeque.pop();
                clearTransactionState(transactionState.getTransactionId());
                latestTxnId = transactionState.getTransactionId();
                numOfClearedTransaction++;
            } else {
                break;
            }
        }
        while ((Config.label_num_threshold > 0 && finalStatusTransactionStateDeque.size() > Config.label_num_threshold)
                && numOfClearedTransaction < left) {
            TransactionState transactionState = finalStatusTransactionStateDeque.getFirst();
            if (transactionState.getFinishTime() != -1) {
                finalStatusTransactionStateDeque.pop();
                clearTransactionState(transactionState.getTransactionId());
                latestTxnId = transactionState.getTransactionId();
                numOfClearedTransaction++;
            } else {
                break;
            }
        }
        return Pair.of(latestTxnId, numOfClearedTransaction);
    }

    private void clearTransactionState(long txnId) {
        TransactionState transactionState = idToFinalStatusTransactionState.remove(txnId);
        if (transactionState != null) {
            Set<Long> txnIds = unprotectedGetTxnIdsByLabel(transactionState.getLabel());
            txnIds.remove(transactionState.getTransactionId());
            if (txnIds.isEmpty()) {
                labelToTxnIds.remove(transactionState.getLabel());
            }
            cleanSubTransactions(txnId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction [" + txnId + "] is expired, remove it from transaction manager");
            }
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

    public List<Pair<Long, Long>> getPrepareTransactionIdByCoordinateBe(long coordinateBeId,
            String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        readLock();
        try {
            idToRunningTransactionState.values().stream()
                    .filter(t -> (t.getCoordinator().sourceType == TransactionState.TxnSourceType.BE
                            && t.getTransactionStatus() == TransactionStatus.PREPARE
                            && t.getCoordinator().ip.equals(coordinateHost)
                            && (t.getCoordinator().id == 0 || t.getCoordinator().id == coordinateBeId)))
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
                        if (!Env.getCurrentEnv().getAccessManager()
                                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                                        db.getFullName(),
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

    protected void checkRunningTxnExceedLimit()
            throws BeginTransactionException, MetaNotFoundException {
        long txnQuota = env.getInternalCatalog().getDbOrMetaException(dbId).getTransactionQuotaSize();
        if (runningTxnNums >= txnQuota) {
            throw new BeginTransactionException("current running txns on db " + dbId + " is "
                    + runningTxnNums + ", larger than limit " + txnQuota);
        }
    }

    private void updateCatalogAfterCommitted(TransactionState transactionState, Database db, boolean isReplay) {
        if (transactionState.getSubTxnIds() != null) {
            List<TableCommitInfo> tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
            updatePartitionNextVersion(transactionState, db, isReplay, tableCommitInfos);
        } else {
            updatePartitionNextVersion(transactionState, db, isReplay,
                    Lists.newArrayList(transactionState.getIdToTableCommitInfos().values()));
        }
    }

    private void updatePartitionNextVersion(TransactionState transactionState, Database db, boolean isReplay,
            List<TableCommitInfo> tableCommitInfos) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("txn_id={}, table commit info={}", transactionState.getTransactionId(), tableCommitInfos);
        }
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        List<Replica> tabletSuccReplicas = Lists.newArrayList();
        List<Replica> tabletFailedReplicas = Lists.newArrayList();
        // one replica should set last_failed_version once
        Set<Long> failedVersionSetReplicas = new HashSet<>();

        Map<Partition, Long> partitionToVersionMap = Maps.newHashMap();
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
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
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex index : allIndices) {
                    List<Tablet> tablets = index.getTablets();
                    for (Tablet tablet : tablets) {
                        tabletFailedReplicas.clear();
                        tabletSuccReplicas.clear();
                        for (Replica replica : tablet.getReplicas()) {
                            if (errorReplicaIds.contains(replica.getId())) {
                                if (!failedVersionSetReplicas.contains(replica.getId())) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("txn_id={}, set replica={}, last_failed_version={}",
                                                transactionState.getTransactionId(), replica.getId(),
                                                partitionCommitInfo.getVersion());
                                    }
                                    // TODO(cmy): do we need to update last failed version here?
                                    // because in updateCatalogAfterVisible, it will be updated again.
                                    replica.updateLastFailedVersion(partitionCommitInfo.getVersion());
                                    failedVersionSetReplicas.add(replica.getId());
                                }
                                tabletFailedReplicas.add(replica);
                            } else {
                                tabletSuccReplicas.add(replica);
                            }
                        }
                        if (!isReplay && !tabletFailedReplicas.isEmpty()) {
                            LOG.info("some replicas load data failed for committed txn {} on version {}, table {}, "
                                            + "partition {}, tablet {}, {} replicas load data succ: {}, "
                                            + "{} replicas load data fail: {}",
                                    transactionState.getTransactionId(), partitionCommitInfo.getVersion(),
                                    tableId, partitionId, tablet.getId(), tabletSuccReplicas.size(),
                                    Joiner.on(", ").join(tabletSuccReplicas.stream()
                                            .map(replica -> replica.toStringSimple(true))
                                            .collect(Collectors.toList())),
                                    tabletFailedReplicas.size(),
                                    Joiner.on(", ").join(tabletFailedReplicas.stream()
                                            .map(replica -> replica.toStringSimple(true))
                                            .collect(Collectors.toList())));
                        }
                    }
                }
                if (!partitionToVersionMap.containsKey(partition)
                        || partitionToVersionMap.get(partition) < partitionCommitInfo.getVersion()) {
                    partitionToVersionMap.put(partition, partitionCommitInfo.getVersion());
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("txn_id={}, partition to next version={}", transactionState.getTransactionId(),
                    partitionToVersionMap);
        }
        for (Entry<Partition, Long> entry : partitionToVersionMap.entrySet()) {
            Partition partition = entry.getKey();
            long version = entry.getValue();
            partition.setNextVersion(version + 1);
            LOG.debug("set partition={}, next_version={}", partition.getId(), partition.getNextVersion());
        }
    }

    private boolean updateCatalogAfterVisible(TransactionState transactionState, Database db,
            Map<Long, Long> partitionVisibleVersions, Map<Long, Set<Long>> backendPartitions) {
        Set<Long> errorReplicaIds = transactionState.getErrorReplicas();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        List<Long> newPartitionLoadedTableIds = new ArrayList<>();
        // one replica should set last_failed_version once
        Set<Long> failedVersionSetReplicas = new HashSet<>();

        Collection<TableCommitInfo> tableCommitInfos;
        if (!transactionState.getSubTxnIdToTableCommitInfo().isEmpty()) {
            tableCommitInfos = transactionState.getSubTxnTableCommitInfos();
        } else {
            tableCommitInfos = transactionState.getIdToTableCommitInfos().values();
        }
        Map<Long, Triple<Long, Long, Partition>> partitionMap = new HashMap<>();
        Map<Long, Triple<Long, Long, OlapTable>> tableMap = new HashMap<>();
        for (TableCommitInfo tableCommitInfo : tableCommitInfos) {
            long tableId = tableCommitInfo.getTableId();
            OlapTable table = (OlapTable) db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("table {} does not exist when update catalog after visible. transaction: {}, db: {}",
                        tableId, transactionState.getTransactionId(), db.getId());
                continue;
            }
            transactionState.addTableIndexes(table);
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
                            long newVersion = replica.getVersion();
                            long lastSuccessVersion = replica.getLastSuccessVersion();
                            if (!errorReplicaIds.contains(replica.getId())) {
                                newVersion = newCommitVersion;
                                if (!replica.checkVersionCatchUp(partition.getVisibleVersion(), true)) {
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
                                if (!failedVersionSetReplicas.contains(replica.getId())) {
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
                                    failedVersionSetReplicas.add(replica.getId());
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("txn_id={}, set replica={}, last_failed_version={}",
                                                transactionState.getTransactionId(), replica.getId(),
                                                partitionCommitInfo.getVersion());
                                    }
                                }
                            }
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("txn_id={}, set replica={}, version={}, last_failed_version={}, "
                                                + "last_success_version={}", transactionState.getTransactionId(),
                                        replica.getId(), newVersion, lastFailedVersion, lastSuccessVersion);
                            }
                            replica.updateVersionWithFailed(newVersion, lastFailedVersion, lastSuccessVersion);
                            if (newVersion == Partition.PARTITION_INIT_VERSION + 1) {
                                index.setRowCountReported(false);
                            }
                            Set<Long> partitionIds = backendPartitions.get(replica.getBackendId());
                            if (partitionIds == null) {
                                partitionIds = Sets.newHashSet();
                                backendPartitions.put(replica.getBackendId(), partitionIds);
                            }
                            partitionIds.add(partitionId);
                        }
                    }
                } // end for indices
                long version = partitionCommitInfo.getVersion();
                long versionTime = partitionCommitInfo.getVersionTime();
                if (partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION
                        && version > Partition.PARTITION_INIT_VERSION) {
                    newPartitionLoadedTableIds.add(tableId);
                }
                partitionMap.compute(partitionId, (k, v) -> {
                    if (v == null || version > v.getLeft()) {
                        return Triple.of(version, versionTime, partition);
                    }
                    return v;
                });
            }

            tableMap.compute(tableId, (k, v) -> {
                if (v == null || tableCommitInfo.getVersion() > v.getLeft()) {
                    return Triple.of(tableCommitInfo.getVersion(), tableCommitInfo.getVersionTime(), table);
                }
                return v;
            });
        }
        for (Entry<Long, Triple<Long, Long, Partition>> entry : partitionMap.entrySet()) {
            long version = entry.getValue().getLeft();
            long versionTime = entry.getValue().getMiddle();
            Partition partition = entry.getValue().getRight();
            partition.updateVisibleVersionAndTime(version, versionTime);
            partitionVisibleVersions.put(partition.getId(), version);
            if (LOG.isDebugEnabled()) {
                LOG.debug("transaction state {} set partition {}'s visible version to [{}]",
                        transactionState, partition.getId(), version);
            }
        }
        for (Entry<Long, Triple<Long, Long, OlapTable>> entry : tableMap.entrySet()) {
            long version = entry.getValue().getLeft();
            long versionTime = entry.getValue().getMiddle();
            OlapTable table = entry.getValue().getRight();
            table.updateVisibleVersionAndTime(version, versionTime);
        }
        analysisManager.setNewPartitionLoaded(newPartitionLoadedTableIds);
        analysisManager.updateUpdatedRows(transactionState.getTableIdToTabletDeltaRows(),
                db.getId(), transactionState.getTransactionId());
        return true;
    }

    public List<TransactionState> getUnFinishedPreviousLoad(long endTransactionId, List<Long> tableIdList) {
        readLock();
        List<TransactionState> unFishedTxns = new ArrayList<>();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
                if (entry.getValue().getDbId() != dbId || !isIntersectionNotEmpty(entry.getValue().getTableIdList(),
                        tableIdList) || entry.getValue().getTransactionStatus().isFinalStatus()) {
                    continue;
                }
                if (entry.getKey() <= endTransactionId) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("find a running txn with txn_id={} on db: {}, less than watermark txn_id {}",
                                entry.getKey(), dbId, endTransactionId);
                    }
                    unFishedTxns.add(entry.getValue());
                }
            }
        } finally {
            readUnlock();
        }
        return unFishedTxns;
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
                    LOG.info("find a running txn with txn_id={} on db: {}, less than watermark txn_id {}",
                            entry.getKey(), dbId, endTransactionId);
                    return false;
                }
            }
        } finally {
            readUnlock();
        }
        return true;
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, long tableId, long partitionId) {
        readLock();
        try {
            for (Map.Entry<Long, TransactionState> entry : idToRunningTransactionState.entrySet()) {
                TransactionState transactionState = entry.getValue();
                if (entry.getKey() > endTransactionId
                        || transactionState.getTransactionStatus().isFinalStatus()
                        || transactionState.getDbId() != dbId
                        || !transactionState.getTableIdList().contains(tableId)) {
                    continue;
                }

                if (transactionState.getTransactionStatus() == TransactionStatus.COMMITTED) {
                    TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfo(tableId);
                    // txn not contains this partition
                    if (tableCommitInfo != null
                            && tableCommitInfo.getIdToPartitionCommitInfo().get(partitionId) == null) {
                        continue;
                    }
                }

                return false;
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
        removeUselessTxns(currentMillis);
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
                updateCatalogAfterCommitted(transactionState, db, true);
            } else if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) {
                updateCatalogAfterVisible(transactionState, db, Maps.newHashMap(), Maps.newHashMap());
            }
            unprotectUpsertTransactionState(transactionState, true);
        } finally {
            writeUnlock();
            LOG.info("replay a {} transaction {}",
                     transactionState.getTransactionStatus(), transactionState);
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
                    runningTxnNums)));
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

    protected void cleanLabel(String label, boolean isReplay) {
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
                            cleanSubTransactions(txnId);
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
                        cleanSubTransactions(txnId);
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

            if (!isReplay) {
                CleanLabelOperationLog log = new CleanLabelOperationLog(dbId, label);
                Env.getCurrentEnv().getEditLog().logCleanLabel(log);
            }
        } finally {
            writeUnlock();
        }
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

    /**
     * Check write lock holding time, if it exceeds threshold, print this hint log.
     *
     * @param lockStart holing lock start time.
     * @param lockEnd release lock time.
     */
    private void checkAndLogWriteLockDuration(long lockStart, long lockEnd) {
        long duration = lockEnd - lockStart;
        if (duration > lockReportingThresholdMs) {
            StringBuilder msgBuilder = new StringBuilder();
            msgBuilder.append("lock is held at ")
                    .append(lockStart)
                    .append(".And release after ")
                    .append(duration)
                    .append(" ms.")
                    .append("Call stack is :\n")
                    .append(getStackTrace(Thread.currentThread()));
            LOG.info(msgBuilder.toString());
        }
    }

    private static String getStackTrace(Thread t) {
        final StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder msgBuilder = new StringBuilder();
        for (StackTraceElement e : stackTrace) {
            msgBuilder.append(e.toString()).append("\n");
        }
        return msgBuilder.toString();
    }

    /**
     * Update transaction table ids by transaction id.
     * it's used for multi table transaction.
     */
    protected void updateMultiTableRunningTransactionTableIds(long transactionId, List<Long> tableIds) {
        if (CollectionUtils.isEmpty(tableIds)) {
            return;
        }
        //idToRunningTransactionState.get(transactionId).
        if (null == idToRunningTransactionState.get(transactionId)) {
            return;
        }
        idToRunningTransactionState.get(transactionId).setTableIdList(tableIds);
    }

    private PublishResult finishCheckQuorumReplicas(TransactionState transactionState, Set<Long> errorReplicaIds) {
        Database database = Env.getCurrentInternalCatalog().getDbNullable(transactionState.getDbId());
        if (database == null) {
            return PublishResult.QUORUM_SUCC;
        }
        long now = System.currentTimeMillis();
        long firstPublishVersionTime = transactionState.getFirstPublishVersionTime();
        boolean allowPublishOneSucc = false;
        if (Config.publish_wait_time_second > 0 && firstPublishVersionTime > 0
                && now >= firstPublishVersionTime + Config.publish_wait_time_second * 1000L) {
            allowPublishOneSucc = true;
        }
        TabletsPublishResultLogs logs = new TabletsPublishResultLogs();

        Map<Long, List<PublishVersionTask>> publishTasks = transactionState.getPublishVersionTasks();
        PublishResult publishResult = PublishResult.QUORUM_SUCC;
        Map<Long, List<Long>> partitionToSubTxns = new HashMap<>();
        for (Long subTxnId : transactionState.getSubTxnIds()) {
            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(subTxnId);
            if (tableCommitInfo == null) {
                continue;
            }
            Table tableIf = database.getTableNullable(tableCommitInfo.getTableId());
            if (tableIf == null) {
                continue;
            }
            OlapTable table = (OlapTable) tableIf;
            for (Entry<Long, PartitionCommitInfo> entry : tableCommitInfo.getIdToPartitionCommitInfo()
                    .entrySet()) {
                long partitionId = entry.getKey();
                Partition partition = table.getPartition(partitionId);
                if (partition == null) {
                    continue;
                }
                partitionToSubTxns.compute(partitionId, (k, v) -> {
                    if (v == null) {
                        v = Lists.newArrayList();
                    }
                    v.add(subTxnId);
                    return v;
                });
            }
        }
        for (Entry<Long, List<Long>> entry : partitionToSubTxns.entrySet()) {
            long partitionId = entry.getKey();
            List<Long> subTxnIds = entry.getValue();
            if (subTxnIds.isEmpty()) {
                continue;
            }
            TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfoBySubTxnId(subTxnIds.get(0));
            long tableId = tableCommitInfo.getTableId();
            Table tableIf = database.getTableNullable(tableCommitInfo.getTableId());
            if (tableIf == null) {
                continue;
            }
            OlapTable table = (OlapTable) tableIf;
            Partition partition = table.getPartition(partitionId);
            if (partition == null) {
                continue;
            }
            long maxAlterWaterschedTxnId = getMaxAlterWaterschedTxnId(table);
            int loadRequiredReplicaNum = table.getLoadRequiredReplicaNum(partitionId);
            // TODO should use sub transaction load indexes
            List<MaterializedIndex> allIndices;
            if (transactionState.getLoadedTblIndexes().isEmpty()
                    || transactionState.getLoadedTblIndexes().get(tableId) == null) {
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
            long minSubTxnId = subTxnIds.get(0);
            long minVersion = transactionState.getTableCommitInfoBySubTxnId(minSubTxnId).getIdToPartitionCommitInfo()
                    .get(partitionId).getVersion();
            long maxSubTxnId = subTxnIds.get(subTxnIds.size() - 1);
            long maxVersion = transactionState.getTableCommitInfoBySubTxnId(maxSubTxnId).getIdToPartitionCommitInfo()
                    .get(partitionId).getVersion();
            LOG.debug("txn_id={}, partition={}, sub_txn_ids={}, min_version={}, max_version={}",
                    transactionState.getTransactionId(), partitionId, subTxnIds, minVersion, maxVersion);
            // check success replica number for each tablet.
            // a success replica means:
            //  1. Not in errorReplicaIds: succeed in both commit and publish phase
            //  2. last failed version < 0: is a health replica before
            //  3. version catch up: not with a stale version
            // Here we only check number, the replica version will be updated in updateCatalogAfterVisible()
            for (MaterializedIndex index : allIndices) {
                for (Tablet tablet : partition.getIndex(index.getId()).getTablets()) {
                    List<Replica> tabletSuccReplicas = Lists.newArrayList();
                    List<Replica> tabletWriteFailedReplicas = Lists.newArrayList();
                    List<Replica> tabletVersionFailedReplicas = Lists.newArrayList();
                    for (Replica replica : tablet.getReplicas()) {
                        List<PublishVersionTask> publishVersionTasks = publishTasks.get(replica.getBackendId());
                        List<PublishVersionTask> replicaTasks = new ArrayList<>();
                        for (Long subTransactionId : subTxnIds) {
                            PublishVersionTask publishVersionTask = null;
                            if (publishVersionTasks != null) {
                                List<PublishVersionTask> matchedTasks = publishVersionTasks.stream()
                                        .filter(t -> t != null && t.getTransactionId() == subTransactionId
                                                && t.getPartitionVersionInfos().stream()
                                                .anyMatch(s -> s.getPartitionId() == partitionId))
                                        .collect(Collectors.toList());
                                Preconditions.checkState(matchedTasks.size() <= 1,
                                        "matched publish tasks: " + matchedTasks);
                                if (matchedTasks.size() == 1) {
                                    publishVersionTask = matchedTasks.get(0);
                                }
                            }
                            replicaTasks.add(publishVersionTask);
                        }
                        checkReplicaContinuousVersionSucc(subTxnIds, maxAlterWaterschedTxnId, tablet.getId(), replica,
                                minVersion, maxVersion, replicaTasks, errorReplicaIds, tabletSuccReplicas,
                                tabletWriteFailedReplicas, tabletVersionFailedReplicas);
                        LOG.debug("after checkReplicaContinuousVersion for txn_id={}, sub_txn_ids={}, "
                                        + "partition={}, tablet_id={}, replica={}, min_version={}, max_version={}, "
                                        + "success_replicas={}, error_replicas={}, write_failed_replicas={}, "
                                        + "version_failed_replicas={}", transactionState.getTransactionId(), subTxnIds,
                                partition.getId(), tablet.getId(), replica.getId(), minVersion, maxVersion,
                                tabletSuccReplicas, errorReplicaIds, tabletWriteFailedReplicas,
                                tabletVersionFailedReplicas);
                    }

                    publishResult = checkQuorumReplicas(transactionState, tableId, partition, tablet,
                            loadRequiredReplicaNum, allowPublishOneSucc, maxVersion, tabletSuccReplicas,
                            tabletWriteFailedReplicas, tabletVersionFailedReplicas, publishResult, logs);
                    if (publishResult == PublishResult.QUORUM_SUCC) {
                        tablet.setLastLoadFailedTime(-1L);
                    }
                }
            }
        }

        boolean needLog = publishResult != PublishResult.FAILED
                || now - transactionState.getLastPublishLogTime() > Config.publish_fail_log_interval_second * 1000L
                || LOG.isDebugEnabled();
        if (needLog) {
            transactionState.setLastPublishLogTime(now);
            logs.log();
        }

        return publishResult;
    }

    private class TabletsPublishResultLogs {
        public List<String> quorumSuccLogs = Lists.newArrayList();
        public List<String> timeoutSuccLogs = Lists.newArrayList();
        public List<String> failedLogs = Lists.newArrayList();

        public void addQuorumSuccLog(String log) {
            if (quorumSuccLogs.size() < 16) {
                quorumSuccLogs.add(log);
            }
        }

        public void addTimeoutSuccLog(String log) {
            if (timeoutSuccLogs.size() < 16) {
                timeoutSuccLogs.add(log);
            }
        }

        public void addFailedLog(String log) {
            if (failedLogs.size() < 16) {
                failedLogs.add(log);
            }
        }

        public void log() {
            // log failed logs
            for (String log : failedLogs) {
                LOG.info(log);
            }
            // log timeout succ logs
            for (String log : timeoutSuccLogs) {
                LOG.info(log);
            }
            // log quorum succ logs
            for (String log : quorumSuccLogs) {
                LOG.info(log);
            }
        }
    }

    private PublishResult checkQuorumReplicas(TransactionState transactionState, long tableId, Partition partition,
            Tablet tablet, int loadRequiredReplicaNum, boolean allowPublishOneSucc, long newVersion,
            List<Replica> tabletSuccReplicas, List<Replica> tabletWriteFailedReplicas,
            List<Replica> tabletVersionFailedReplicas, PublishResult publishResult, TabletsPublishResultLogs logs) {
        long partitionId = partition.getId();
        int healthReplicaNum = tabletSuccReplicas.size();
        if (healthReplicaNum >= loadRequiredReplicaNum) {
            boolean hasFailedReplica = !tabletWriteFailedReplicas.isEmpty() || !tabletVersionFailedReplicas.isEmpty();
            if (hasFailedReplica) {
                String writeDetail = getTabletWriteDetail(tabletSuccReplicas, tabletWriteFailedReplicas,
                        tabletVersionFailedReplicas);
                logs.addQuorumSuccLog(String.format("publish version quorum succ for transaction %s on tablet %s"
                                + " with version %s, and has failed replicas, load require replica num %s. "
                                + "table %s, partition: [ id=%s, commit version=%s ], tablet detail: %s",
                                transactionState, tablet.getId(), newVersion, loadRequiredReplicaNum, tableId,
                                partitionId, partition.getCommittedVersion(), writeDetail));
            }
            return publishResult;
        }

        String writeDetail = getTabletWriteDetail(tabletSuccReplicas, tabletWriteFailedReplicas,
                tabletVersionFailedReplicas);
        if (allowPublishOneSucc && healthReplicaNum > 0) {
            if (publishResult == PublishResult.QUORUM_SUCC) {
                publishResult = PublishResult.TIMEOUT_SUCC;
            }
            // We can not do any thing except retrying,
            // because publish task is assigned a version,
            // and doris does not permit discontinuous
            // versions.
            //
            // If a timeout happens, it means that the rowset
            // that are being publised exists on a few replicas we should go
            // ahead, otherwise data may be lost and thre
            // publish task hangs forever.
            logs.addTimeoutSuccLog(String.format("publish version timeout succ for transaction %s on tablet %s "
                            + "with version %s, and has failed replicas, load require replica num %s. "
                            + "table %s, partition %s, tablet detail: %s", transactionState, tablet.getId(), newVersion,
                    loadRequiredReplicaNum, tableId, partitionId, writeDetail));
        } else {
            publishResult = PublishResult.FAILED;
            String errMsg = String.format(
                    "publish on tablet %d failed." + " succeed replica num %d < load required replica num %d."
                            + " table: %d, partition: %d, publish version: %d", tablet.getId(), healthReplicaNum,
                    loadRequiredReplicaNum, tableId, partitionId, newVersion);
            transactionState.setErrorMsg(errMsg);
            logs.addFailedLog(String.format("publish version failed for transaction %s on tablet %s with version"
                            + " %s, and has failed replicas, load required replica num %s. table %s, "
                            + "partition %s, tablet detail: %s", transactionState, tablet.getId(), newVersion,
                    loadRequiredReplicaNum, tableId, partitionId, writeDetail));
        }
        return publishResult;
    }

    protected void addSubTransaction(long transactionId, long subTransactionId) {
        subTxnIdToTxnId.put(subTransactionId, transactionId);
    }

    protected void removeSubTransaction(long subTransactionId) {
        subTxnIdToTxnId.remove(subTransactionId);
    }

    private void cleanSubTransactions(long transactionId) {
        Iterator<Entry<Long, Long>> iterator = subTxnIdToTxnId.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Long, Long> entry = iterator.next();
            if (entry.getValue() == transactionId) {
                iterator.remove();
            }
        }
    }
}
