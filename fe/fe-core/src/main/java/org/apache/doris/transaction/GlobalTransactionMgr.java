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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.metric.AutoMappedMetric;
import org.apache.doris.metric.GaugeMetricImpl;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transaction Manager
 * 1. begin
 * 2. commit
 * 3. abort
 * Attention: all api in txn manager should get db lock or load lock first, then get txn manager's lock,
 * or there will be dead lock
 */
public class GlobalTransactionMgr implements GlobalTransactionMgrIface {
    private static final Logger LOG = LogManager.getLogger(GlobalTransactionMgr.class);

    private Map<Long, DatabaseTransactionMgr> dbIdToDatabaseTransactionMgrs;

    private TransactionIdGenerator idGenerator;
    private TxnStateCallbackFactory callbackFactory;

    private Env env;

    public GlobalTransactionMgr(Env env) {
        this.env = env;
        this.dbIdToDatabaseTransactionMgrs = Maps.newConcurrentMap();
        this.idGenerator = new TransactionIdGenerator();
        this.callbackFactory = new TxnStateCallbackFactory();
    }

    @Override
    public void setEditLog(EditLog editLog) {
        this.idGenerator.setEditLog(editLog);
    }

    @Override
    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    protected DatabaseTransactionMgr getDatabaseTransactionMgr(long dbId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTransactionMgr == null) {
            throw new AnalysisException("databaseTransactionMgr[" + dbId + "] does not exist");
        }
        return dbTransactionMgr;
    }

    @Override
    public void addDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.putIfAbsent(dbId,
                new DatabaseTransactionMgr(dbId, env, idGenerator)) == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("add database transaction manager for db {}", dbId);
            }
        }
    }

    @Override
    public void removeDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.remove(dbId) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("remove database transaction manager for db {}", dbId);
            }
        }
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }

    /**
     * the app could specify the transaction id
     * <p>
     * requestId is used to judge that whether the request is a internal retry request
     * if label already exist, and requestId are equal, we return the exist tid, and consider this 'begin'
     * as success.
     * requestId == null is for compatibility
     *
     * @param coordinator
     * @throws BeginTransactionException
     * @throws DuplicatedRequestException
     */
    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        try {
            if (Config.disable_load_job) {
                throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
            }

            switch (sourceType) {
                case BACKEND_STREAMING:
                    checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                            Config.min_load_timeout_second);
                    break;
                default:
                    checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second,
                            Config.min_load_timeout_second);
            }

            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.beginTransaction(tableIdList, label, requestId,
                coordinator, sourceType, listenerId, timeoutSecond);
        } catch (DuplicatedRequestException e) {
            throw e;
        } catch (Exception e) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_REJECT.increase(1L);
            }
            throw e;
        }
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (!MetaLockUtils.tryWriteLockTablesOrMetaException(tableList, timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=("
                    + StringUtils.join(tableList, ",") + ")");
        }
        try {
            preCommitTransaction2PC(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
    }

    public void preCommitTransaction2PC(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("try to pre-commit transaction: {}", transactionId);
        }
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.preCommitTransaction2PC(tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
    }

    public void commitTransaction(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, null);
    }

    @Override
    public void afterCommitTxnResp(CommitTxnResponse commitTxnResponse) {
    }

    /**
     * @param transactionId
     * @param tabletCommitInfos
     * @return
     * @throws UserException
     * @throws TransactionCommitFailedException
     * @note it is necessary to optimize the `lock` mechanism and `lock` scope resulting from wait lock long time
     * @note callers should get all tables' write locks before call this api
     */
    public void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("try to commit transaction: {}", transactionId);
        }
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.commitTransaction(tableList, transactionId, tabletCommitInfos, txnCommitAttachment, false);
    }

    /**
     * @note callers should get all tables' write locks before call this api
     */
    public void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<SubTransactionState> subTransactionStates, long timeout) throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("try to commit transaction: {}", transactionId);
        }
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.commitTransaction(transactionId, tableList, subTransactionStates);
    }

    private void commitTransaction2PC(long dbId, long transactionId)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }

        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.commitTransaction(null, transactionId, null, null, true);
    }

    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis, null);
    }

    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!MetaLockUtils.tryWriteLockTablesOrMetaException(tableList, timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=("
                    + StringUtils.join(tableList, ",") + ")");
        }
        try {
            commitTransaction(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        stopWatch.stop();
        long publishTimeoutMillis = timeoutMillis - stopWatch.getTime();
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(db.getId());
        if (publishTimeoutMillis < 0) {
            // here commit transaction successfully cost too much time
            // to cause that publishTimeoutMillis is less than zero,
            // so we just return false to indicate publish timeout
            return false;
        }
        return dbTransactionMgr.waitForTransactionFinished(db, transactionId, publishTimeoutMillis);
    }

    public boolean commitAndPublishTransaction(DatabaseIf db, long transactionId,
            List<SubTransactionState> subTransactionStates, long timeoutMillis) throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Long> tableIdList = subTransactionStates.stream().map(s -> s.getTable().getId()).distinct()
                .collect(Collectors.toList());
        List<? extends TableIf> tableIfList = db.getTablesOnIdOrderOrThrowException(tableIdList);
        List<Table> tableList = tableIfList.stream().map(t -> (Table) t).collect(Collectors.toList());
        if (!MetaLockUtils.tryWriteLockTablesOrMetaException(tableList, timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=("
                    + StringUtils.join(tableList, ",") + ")");
        }
        try {
            commitTransaction(db.getId(), tableList, transactionId, subTransactionStates, timeoutMillis);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        stopWatch.stop();
        long publishTimeoutMillis = timeoutMillis - stopWatch.getTime();
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(db.getId());
        if (publishTimeoutMillis < 0) {
            // here commit transaction successfully cost too much time
            // to cause that publishTimeoutMillis is less than zero,
            // so we just return false to indicate publish timeout
            return false;
        }
        return dbTransactionMgr.waitForTransactionFinished(db, transactionId, publishTimeoutMillis);
    }

    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (!MetaLockUtils.tryWriteLockTablesOrMetaException(tableList, timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=("
                    + StringUtils.join(tableList, ",") + ")");
        }
        try {
            commitTransaction2PC(db.getId(), transactionId);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
        stopWatch.stop();
        LOG.info("stream load tasks are committed successfully. txns: {}. time cost: {} ms."
                + " data will be visable later.", transactionId, stopWatch.getTime());
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason) throws UserException {
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        TransactionState transactionState = getDatabaseTransactionMgr(dbId).getTransactionState(transactionId);
        if (transactionState == null) {
            LOG.info("try to cancel one txn which has no txn state. txn id: {}.", transactionId);
            return;
        }
        List<Table> tableList = db.getTablesOnIdOrderIfExist(transactionState.getTableIdList());
        abortTransaction(dbId, transactionId, reason, null, tableList);
    }

    @Override
    public void abortTransaction(Long dbId, Long txnId, String reason,
            TxnCommitAttachment txnCommitAttachment, List<Table> tableList) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        if (!MetaLockUtils.tryWriteLockTablesOrMetaException(tableList, 5000, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=("
                    + StringUtils.join(tableList, ",") + ")");
        }
        try {
            dbTransactionMgr.abortTransaction(txnId, reason, txnCommitAttachment);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
    }

    // for http cancel stream load api
    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.abortTransaction(label, reason);
    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId, List<Table> tableList) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        if (!MetaLockUtils.tryWriteLockTablesOrMetaException(tableList, 5000, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=("
                    + StringUtils.join(tableList, ",") + ")");
        }
        try {
            dbTransactionMgr.abortTransaction2PC(transactionId);
        } finally {
            MetaLockUtils.writeUnlockTables(tableList);
        }
    }

    /*
     * get all txns which is ready to publish
     * a ready-to-publish txn's partition's visible version should be ONE less than txn's commit version.
     */
    public List<TransactionState> getReadyToPublishTransactions() {
        List<TransactionState> transactionStateList = Lists.newArrayList();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            transactionStateList.addAll(dbTransactionMgr.getCommittedTxnList());
        }
        return transactionStateList;
    }

    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (tableId == null && partitionId == null) {
            return !dbTransactionMgr.getCommittedTxnList().isEmpty();
        }

        List<TransactionState> committedTxnList = dbTransactionMgr.getCommittedTxnList();
        for (TransactionState transactionState : committedTxnList) {
            if (transactionState.getTableIdList().contains(tableId)) {
                if (partitionId == null) {
                    return true;
                }
                TableCommitInfo tableCommitInfo = transactionState.getTableCommitInfo(tableId);
                if (tableCommitInfo == null) {
                    // FIXME: this is a bug, should not happen
                    // If table id is in transaction state's table list, and it is COMMITTED,
                    // table commit info should not be null.
                    // return true to avoid following process.
                    LOG.warn("unexpected error. tableCommitInfo is null. dbId: {} tableId: {}, partitionId: {},"
                                    + " transactionState: {}",
                            dbId, tableId, partitionId, transactionState);
                    return true;
                }
                if (tableCommitInfo.getPartitionCommitInfo(partitionId) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * if the table is deleted between commit and publish version, then should ignore the partition
     *
     * @param dbId
     * @param transactionId
     * @return
     */
    public void finishTransaction(long dbId, long transactionId, Map<Long, Long> partitionVisibleVersions,
            Map<Long, Set<Long>> backendPartitions) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.finishTransaction(transactionId, partitionVisibleVersions, backendPartitions);
    }

    /**
     * Check whether a load job already exists before
     * checking all `TransactionId` related with this load job have finished.
     * finished
     *
     * @throws AnalysisException is database does not exist anymore
     */
    @Override
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.isPreviousTransactionsFinished(endTransactionId, tableIdList);
        } catch (AnalysisException e) {
            // NOTICE: At present, this situation will only happen when the database no longer exists.
            // In fact, getDatabaseTransactionMgr() should explicitly throw a MetaNotFoundException,
            // but changing the type of exception will cause a large number of code changes,
            // which is not worth the loss.
            // So here just simply think that AnalysisException only means that db does not exist.
            LOG.warn("Check whether all previous transactions in db [" + dbId + "] finished failed", e);
            throw e;
        }
    }

    /**
     * Check whether a load job for a partition already exists before
     * checking all `TransactionId` related with this load job have finished.
     * finished
     *
     * @throws AnalysisException is database does not exist anymore
     */
    @Override
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, long tableId,
            long partitionId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.isPreviousTransactionsFinished(endTransactionId, tableId, partitionId);
    }

    /**
     * The txn cleaner will run at a fixed interval and try to delete expired and timeout txns:
     * expired: txn is in VISIBLE or ABORTED, and is expired.
     * timeout: txn is in PREPARE, but timeout
     */
    @Override
    public void removeExpiredAndTimeoutTxns() {
        long currentMillis = System.currentTimeMillis();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.removeExpiredAndTimeoutTxns(currentMillis);
        }
    }

    @Override
    public void cleanLabel(Long dbId, String label, boolean isReplay) throws Exception {
        getDatabaseTransactionMgr(dbId).cleanLabel(label, isReplay);
    }

    @Override
    public void updateMultiTableRunningTransactionTableIds(Long dbId, Long transactionId, List<Long> tableIds)
            throws UserException {
        getDatabaseTransactionMgr(dbId).updateMultiTableRunningTransactionTableIds(transactionId, tableIds);
    }

    @Override
    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException {
        long dbId = request.getDbId();
        int commitTimeoutSec = Config.commit_timeout_second;
        TransactionStatus txnStatus = null;
        for (int i = 0; i < commitTimeoutSec; ++i) {
            Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
            TWaitingTxnStatusResult statusResult = new TWaitingTxnStatusResult();
            statusResult.status = new TStatus();
            if (request.isSetTxnId()) {
                long txnId = request.getTxnId();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr().getTransactionState(dbId, txnId);
                if (txnState == null) {
                    throw new AnalysisException("txn does not exist: " + txnId);
                }
                txnStatus = txnState.getTransactionStatus();
                if (!txnState.getReason().trim().isEmpty()) {
                    statusResult.status.setErrorMsgsIsSet(true);
                    statusResult.status.addToErrorMsgs(txnState.getReason());
                }
            } else {
                txnStatus = getLabelState(dbId, request.getLabel());
            }
            if (txnStatus == TransactionStatus.UNKNOWN || txnStatus.isFinalStatus()) {
                statusResult.setTxnStatusId(txnStatus.value());
                return statusResult;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                LOG.info("commit sleep exception.", e);
            }
        }
        if (txnStatus == TransactionStatus.COMMITTED) {
            TWaitingTxnStatusResult statusResult = new TWaitingTxnStatusResult();
            statusResult.status = new TStatus();
            statusResult.setTxnStatusId(txnStatus.value());
            return statusResult;
        }
        throw new TimeoutException("Operation is timeout, txn status is " + txnStatus);
    }

    @Override
    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.updateDatabaseUsedQuotaData(usedQuotaDataBytes);
    }

    @Override
    public void abortTxnWhenCoordinateBeRestart(long coordinateBeId, String coordinateHost, long beStartTime) {
        List<Pair<Long, Long>> transactionIdByCoordinateBe
                = getPrepareTransactionIdByCoordinateBe(coordinateBeId, coordinateHost, Integer.MAX_VALUE);
        for (Pair<Long, Long> txnInfo : transactionIdByCoordinateBe) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(txnInfo.first);
                TransactionState transactionState = dbTransactionMgr.getTransactionState(txnInfo.second);
                long coordStartTime = transactionState.getCoordinator().startTime;
                if (coordStartTime > 0 && coordStartTime < beStartTime) {
                    dbTransactionMgr.abortTransaction(txnInfo.second, "coordinate BE restart", null);
                }
            } catch (UserException e) {
                LOG.warn("Abort txn on coordinate BE {} failed, msg={}", coordinateHost, e.getMessage());
            }
        }
    }

    /**
     * If a Coordinate BE is down when running txn, the txn will remain in FE until killed by timeout
     * So when FE identify the Coordinate BE is down, FE should cancel it initiative
     */
    @Override
    public void abortTxnWhenCoordinateBeDown(long coordinateBeId, String coordinateHost, int limit) {
        List<Pair<Long, Long>> transactionIdByCoordinateBe
                = getPrepareTransactionIdByCoordinateBe(coordinateBeId, coordinateHost, limit);
        for (Pair<Long, Long> txnInfo : transactionIdByCoordinateBe) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(txnInfo.first);
                dbTransactionMgr.abortTransaction(txnInfo.second, "coordinate BE is down", null);
            } catch (UserException e) {
                LOG.warn("Abort txn on coordinate BE {} failed, msg={}", coordinateHost, e.getMessage());
            }
        }
    }

    @Override
    public TransactionStatus getLabelState(long dbId, String label) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getLabelState(label);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction status by label " + label + " failed", e);
            return TransactionStatus.UNKNOWN;
        }

    }

    @Override
    public Long getTransactionId(Long dbId, String label) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionIdByLabel(label);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction id by label " + label + " failed", e);
            return null;
        }
    }

    public TransactionState getTransactionState(long dbId, long transactionId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionState(transactionId);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction {} in db {} failed. msg: {}", transactionId, dbId, e.getMessage());
            return null;
        }
    }

    @Override
    public List<TransactionState> getPreCommittedTxnList(Long dbId) throws AnalysisException {
        return getDatabaseTransactionMgr(dbId).getPreCommittedTxnList();
    }

    @Override
    public Long getTransactionIdByLabel(Long dbId, String label, List<TransactionStatus> statusList)
            throws UserException {
        return getDatabaseTransactionMgr(dbId).getTransactionIdByLabel(label, statusList);
    }

    /**
     * It is a non thread safe method, only invoked by checkpoint thread
     * without any lock or image dump thread with db lock
     */
    @Override
    public int getTransactionNum() {
        int txnNum = 0;
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnNum += dbTransactionMgr.getTransactionNum();
        }
        return txnNum;
    }

    @Override
    public List<List<String>> getDbInfo() {
        List<List<String>> infos = new ArrayList<>();
        long totalRunningNum = 0;
        List<Long> dbIds = Lists.newArrayList(dbIdToDatabaseTransactionMgrs.keySet());
        Collections.sort(dbIds);
        for (long dbId : dbIds) {
            List<String> info = new ArrayList<>();
            info.add(String.valueOf(dbId));
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            info.add(db.getFullName());
            long runningNum = 0;
            try {
                DatabaseTransactionMgr dbMgr = getDatabaseTransactionMgr(dbId);
                runningNum = dbMgr.getRunningTxnNums();
                totalRunningNum += runningNum;
            } catch (AnalysisException e) {
                LOG.warn("get database running transaction num failed", e);
            }
            info.add(String.valueOf(runningNum));
            infos.add(info);
        }
        List<String> info = Arrays.asList("0", "Total", String.valueOf(totalRunningNum));
        infos.add(info);
        return infos;
    }

    @Override
    public List<List<String>> getDbTransStateInfo(Long dbId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getDbTransStateInfo();
        } catch (AnalysisException e) {
            LOG.warn("Get db [" + dbId + "] transactions info failed", e);
            return Lists.newArrayList();
        }
    }

    @Override
    public List<List<String>> getDbTransInfo(Long dbId, boolean running, int limit) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTxnStateInfoList(running, limit);
    }

    public Map<Long, List<Long>> getDbRunningTransInfo(long dbId) throws AnalysisException {
        return Optional.ofNullable(dbIdToDatabaseTransactionMgrs.get(dbId))
                .map(DatabaseTransactionMgr::getDbRunningTransInfo).orElse(Maps.newHashMap());
    }

    @Override
    public List<List<String>> getDbTransInfoByStatus(Long dbId, TransactionStatus status) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTxnStateInfoList(status);
    }

    @Override
    public List<List<String>> getDbTransInfoByLabelMatch(long dbId, String label) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTxnStateInfoList(label);
    }

    @Override
    public long getTxnNumByStatus(TransactionStatus status) {
        long counter = 0;
        for (DatabaseTransactionMgr dbMgr : dbIdToDatabaseTransactionMgrs.values()) {
            counter += dbMgr.getTxnNumByStatus(status);
        }
        return counter;
    }

    // get show info of a specified txnId
    @Override
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getSingleTranInfo(dbId, txnId);
    }

    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTableTransInfo(txnId);
    }

    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getPartitionTransInfo(tid, tableId);
    }

    @Override
    public TransactionIdGenerator getTransactionIDGenerator() {
        return this.idGenerator;
    }

    @Deprecated
    private TransactionState getTransactionStateByCallbackIdAndStatus(
            long dbId, long callbackId, Set<TransactionStatus> status) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionStateByCallbackIdAndStatus(callbackId, status);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction by callbackId and status failed", e);
            return null;
        }
    }

    @Deprecated
    private TransactionState getTransactionStateByCallbackId(long dbId, long callbackId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionStateByCallbackId(callbackId);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction by callbackId failed", e);
            return null;
        }
    }

    protected List<Pair<Long, Long>> getPrepareTransactionIdByCoordinateBe(long coordinateBeId,
            String coordinateHost, int limit) {
        ArrayList<Pair<Long, Long>> txnInfos = new ArrayList<>();
        for (DatabaseTransactionMgr databaseTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnInfos.addAll(databaseTransactionMgr.getPrepareTransactionIdByCoordinateBe(
                        coordinateBeId, coordinateHost, limit));
            if (txnInfos.size() > limit) {
                break;
            }
        }
        return txnInfos.size() > limit ? new ArrayList<>(txnInfos.subList(0, limit)) : txnInfos;
    }

    @Override
    public long getAllRunningTxnNum() {
        return updateTxnMetric(databaseTransactionMgr -> (long) databaseTransactionMgr.getRunningTxnNumsWithLock(),
                MetricRepo.DB_GAUGE_TXN_NUM);
    }

    @Override
    public long getAllPublishTxnNum() {
        return updateTxnMetric(
                databaseTransactionMgr -> (long) databaseTransactionMgr.getCommittedTxnList().size(),
                MetricRepo.DB_GAUGE_PUBLISH_TXN_NUM);
    }

    private long updateTxnMetric(Function<DatabaseTransactionMgr, Long> metricSupplier,
            AutoMappedMetric<GaugeMetricImpl<Long>> metric) {
        long total = 0;
        for (DatabaseTransactionMgr mgr : dbIdToDatabaseTransactionMgrs.values()) {
            long num = metricSupplier.apply(mgr).longValue();
            total += num;
            Database db = Env.getCurrentInternalCatalog().getDbNullable(mgr.getDbId());
            if (db != null) {
                metric.getOrAdd(db.getFullName()).setValue(num);
            }
        }
        return total;
    }

    @Override
    public int getRunningTxnNums(Long dbId) throws AnalysisException {
        return Optional.ofNullable(dbIdToDatabaseTransactionMgrs.get(dbId))
                .map(DatabaseTransactionMgr::getRunningTxnNums).orElse(0);
    }

    @Override
    public Long getNextTransactionId() {
        return this.idGenerator.getNextTransactionId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int numTransactions = getTransactionNum();
        out.writeInt(numTransactions);
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.unprotectWriteAllTransactionStates(out);
        }
        idGenerator.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int numTransactions = in.readInt();
        for (int i = 0; i < numTransactions; ++i) {
            TransactionState transactionState = TransactionState.read(in);
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
                dbTransactionMgr.unprotectUpsertTransactionState(transactionState, true);
            } catch (AnalysisException e) {
                LOG.warn("failed to get db transaction manager for txn: {}", transactionState);
                throw new IOException("Read transaction states failed", e);
            }
        }
        idGenerator.readFields(in);
    }

    // for replay idToTransactionState
    // check point also run transaction cleaner, the cleaner maybe concurrently modify id to
    @Override
    public void replayUpsertTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
            dbTransactionMgr.replayUpsertTransactionState(transactionState);
        } catch (AnalysisException e) {
            throw new MetaNotFoundException(e);
        }
    }

    @Override
    @Deprecated
    // Use replayBatchDeleteTransactions instead
    public void replayDeleteTransactionState(TransactionState transactionState) throws MetaNotFoundException {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
            dbTransactionMgr.replayDeleteTransaction(transactionState);
        } catch (AnalysisException e) {
            throw new MetaNotFoundException(e);
        }
    }

    @Override
    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) {
        Map<Long, List<Long>> dbTxnIds = operation.getDbTxnIds();
        for (Long dbId : dbTxnIds.keySet()) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
                dbTransactionMgr.replayBatchRemoveTransaction(dbTxnIds.get(dbId));
            } catch (AnalysisException e) {
                LOG.warn("replay batch remove transactions failed. db " + dbId, e);
            }
        }
    }

    @Override
    public void replayBatchRemoveTransactionV2(BatchRemoveTransactionsOperationV2 operation) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(operation.getDbId());
            dbTransactionMgr.replayBatchRemoveTransaction(operation);
        } catch (AnalysisException e) {
            LOG.warn("replay batch remove transactions failed. db " + operation.getDbId(), e);
        }
    }

    @Override
    public void addSubTransaction(long dbId, long transactionId, long subTransactionId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            dbTransactionMgr.addSubTransaction(transactionId, subTransactionId);
        } catch (AnalysisException e) {
            LOG.warn("add sub transaction failed. db " + dbId, e);
        }
    }

    @Override
    public void removeSubTransaction(long dbId, long subTransactionId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            dbTransactionMgr.removeSubTransaction(subTransactionId);
        } catch (AnalysisException e) {
            LOG.warn("remove sub transaction failed. db " + dbId, e);
        }
    }
}
