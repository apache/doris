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

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

    private TransactionIdGenerator idGenerator = new TransactionIdGenerator();
    private TxnStateCallbackFactory callbackFactory = new TxnStateCallbackFactory();
    
    private Catalog catalog;

    public GlobalTransactionMgr(Catalog catalog) {
        this.catalog = catalog;
    }
    
    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    public DatabaseTransactionMgr getDatabaseTransactionMgr(long dbId) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = dbIdToDatabaseTransactionMgrs.get(dbId);
        if (dbTransactionMgr == null) {
            throw new AnalysisException("databaseTransactionMgr[" + dbId + "] does not exist");
        }
        return dbTransactionMgr;
    }

    public void addDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.putIfAbsent(dbId, new DatabaseTransactionMgr(dbId, catalog, idGenerator)) == null) {
            LOG.debug("add database transaction manager for db {}", dbId);
        }
    }

    public void removeDatabaseTransactionMgr(Long dbId) {
        if (dbIdToDatabaseTransactionMgrs.remove(dbId) != null) {
            LOG.debug("remove database transaction manager for db {}", dbId);
        }
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

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second, Config.min_load_timeout_second);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.beginTransaction(tableIdList, label, requestId, coordinator, sourceType, listenerId, timeoutSecond);
    }

    private void checkValidTimeoutSecond(long timeoutSecond, int maxLoadTimeoutSecond, int minLoadTimeOutSecond) throws AnalysisException {
        if (timeoutSecond > maxLoadTimeoutSecond ||
                timeoutSecond < minLoadTimeOutSecond) {
            throw new AnalysisException("Invalid timeout. Timeout should between "
                    + minLoadTimeOutSecond + " and " + maxLoadTimeoutSecond
                    + " seconds");
        }
    }

    public TransactionStatus getLabelState(long dbId, String label) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getLabelState(label);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction status by label " + label + " failed", e);
            return TransactionStatus.UNKNOWN;
        }

    }

    public void commitTransaction(long dbId, List<Table> tableList, long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, null);
    }
    
    /**
     * @param transactionId
     * @param tabletCommitInfos
     * @return
     * @throws UserException
     * @throws TransactionCommitFailedException
     * @note it is necessary to optimize the `lock` mechanism and `lock` scope resulting from wait lock long time
     * @note callers should get db.write lock before call this api
     */
    public void commitTransaction(long dbId, List<Table> tableList, long transactionId, List<TabletCommitInfo> tabletCommitInfos,
                                  TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException("disable_load_job is set to true, all load jobs are prevented");
        }
        
        LOG.debug("try to commit transaction: {}", transactionId);
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.commitTransaction(tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
    }
    
    public boolean commitAndPublishTransaction(Database db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis, null);
    }
    
    public boolean commitAndPublishTransaction(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
            TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        if (!MetaLockUtils.tryWriteLockTables(tableList, timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new UserException("get tableList write lock timeout, tableList=(" + StringUtils.join(tableList, ",") + ")");
        }
        try {
            commitTransaction(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        } finally {
           MetaLockUtils.writeUnlockTables(tableList);
        }
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(db.getId());
        return dbTransactionMgr.publishTransaction(db, transactionId, timeoutMillis);
   }

    public void abortTransaction(long dbId, long transactionId, String reason) throws UserException {
        abortTransaction(dbId, transactionId, reason, null);
    }

    public void abortTransaction(Long dbId, Long txnId, String reason, TxnCommitAttachment txnCommitAttachment) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.abortTransaction(txnId, reason, txnCommitAttachment);
    }

    // for http cancel stream load api
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.abortTransaction(label, reason);
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

        for (TransactionState transactionState : dbTransactionMgr.getCommittedTxnList()) {
            if (transactionState.getTableIdList().contains(tableId)) {
                if (partitionId == null) {
                    return true;
                } else if (transactionState.getTableCommitInfo(tableId).getPartitionCommitInfo(partitionId) != null){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * if the table is deleted between commit and publish version, then should ignore the partition
     *
     * @param transactionId
     * @param errorReplicaIds
     * @return
     */
    public void finishTransaction(long dbId, long transactionId, Set<Long> errorReplicaIds) throws UserException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.finishTransaction(transactionId, errorReplicaIds);
    }

    /**
     * Check whether a load job already exists before checking all `TransactionId` related with this load job have finished.
     * finished
     * 
     * @throws AnalysisException is database does not exist anymore
     */
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
     * The txn cleaner will run at a fixed interval and try to delete expired and timeout txns:
     * expired: txn is in VISIBLE or ABORTED, and is expired.
     * timeout: txn is in PREPARE, but timeout
     */
    public void removeExpiredAndTimeoutTxns() {
        long currentMillis = System.currentTimeMillis();
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            dbTransactionMgr.removeExpiredAndTimeoutTxns(currentMillis);
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
    
    public void setEditLog(EditLog editLog) {
        this.idGenerator.setEditLog(editLog);
    }

    // for replay idToTransactionState
    // check point also run transaction cleaner, the cleaner maybe concurrently modify id to 
    public void replayUpsertTransactionState(TransactionState transactionState) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
            dbTransactionMgr.replayUpsertTransactionState(transactionState);
        } catch (AnalysisException e) {
            LOG.warn("replay upsert transaction [" + transactionState.getTransactionId() + "] failed", e);
        }

    }
    
    public void replayDeleteTransactionState(TransactionState transactionState) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(transactionState.getDbId());
            dbTransactionMgr.deleteTransaction(transactionState);
        } catch (AnalysisException e) {
            LOG.warn("replay delete transaction [" + transactionState.getTransactionId() + "] failed", e);
        }
    }

    public List<List<Comparable>> getDbInfo() {
        List<List<Comparable>> infos = new ArrayList<List<Comparable>>();
        List<Long> dbIds = Lists.newArrayList(dbIdToDatabaseTransactionMgrs.keySet());
        for (long dbId : dbIds) {
            List<Comparable> info = new ArrayList<Comparable>();
            info.add(dbId);
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                continue;
            }
            info.add(db.getFullName());
            infos.add(info);
        }
        return infos;
    }
    
    public List<List<String>> getDbTransStateInfo(long dbId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getDbTransStateInfo();
        } catch (AnalysisException e) {
            LOG.warn("Get db [" + dbId + "] transactions info failed", e);
            return Lists.newArrayList();
        }
    }

    public List<List<String>> getDbTransInfo(long dbId, boolean running, int limit) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        return dbTransactionMgr.getTxnStateInfoList(running, limit);
    }
    
    // get show info of a specified txnId
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

    /**
     * It is a non thread safe method, only invoked by checkpoint thread without any lock or image dump thread with db lock
     */
    public int getTransactionNum() {
        int txnNum = 0;
        for (DatabaseTransactionMgr dbTransactionMgr : dbIdToDatabaseTransactionMgrs.values()) {
            txnNum += dbTransactionMgr.getTransactionNum();
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
            dbTransactionMgr.unprotectWriteAllTransactionStates(out);
        }
        idGenerator.write(out);
    }
    
    public void readFields(DataInput in) throws IOException {
        int numTransactions = in.readInt();
        for (int i = 0; i < numTransactions; ++i) {
            TransactionState transactionState = new TransactionState();
            transactionState.readFields(in);
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

    public TransactionState getTransactionStateByCallbackIdAndStatus(long dbId, long callbackId, Set<TransactionStatus> status) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionStateByCallbackIdAndStatus(callbackId, status);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction by callbackId and status failed", e);
            return null;
        }
    }

    public TransactionState getTransactionStateByCallbackId(long dbId, long callbackId) {
        try {
            DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
            return dbTransactionMgr.getTransactionStateByCallbackId(callbackId);
        } catch (AnalysisException e) {
            LOG.warn("Get transaction by callbackId failed", e);
            return null;
        }
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
     * So when FE identify the Coordinate BE is down, FE should cancel it initiative
     */
    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit) {
        List<Pair<Long, Long>> transactionIdByCoordinateBe = getTransactionIdByCoordinateBe(coordinateHost, limit);
        for (Pair<Long, Long> txnInfo : transactionIdByCoordinateBe) {
            try {
                DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(txnInfo.first);
                dbTransactionMgr.abortTransaction(txnInfo.second, "coordinate BE is down", null);
            } catch (UserException e) {
                LOG.warn("Abort txn on coordinate BE {} failed, msg={}", coordinateHost, e.getMessage());
            }
        }
    }

    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        DatabaseTransactionMgr dbTransactionMgr = getDatabaseTransactionMgr(dbId);
        dbTransactionMgr.updateDatabaseUsedQuotaData(usedQuotaDataBytes);
    }
}
