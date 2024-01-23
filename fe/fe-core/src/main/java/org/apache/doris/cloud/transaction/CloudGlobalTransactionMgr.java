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

package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.proto.Cloud.AbortTxnRequest;
import org.apache.doris.cloud.proto.Cloud.AbortTxnResponse;
import org.apache.doris.cloud.proto.Cloud.BeginTxnRequest;
import org.apache.doris.cloud.proto.Cloud.BeginTxnResponse;
import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictRequest;
import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictResponse;
import org.apache.doris.cloud.proto.Cloud.CleanTxnLabelRequest;
import org.apache.doris.cloud.proto.Cloud.CleanTxnLabelResponse;
import org.apache.doris.cloud.proto.Cloud.CommitTxnRequest;
import org.apache.doris.cloud.proto.Cloud.CommitTxnResponse;
import org.apache.doris.cloud.proto.Cloud.GetCurrentMaxTxnRequest;
import org.apache.doris.cloud.proto.Cloud.GetCurrentMaxTxnResponse;
import org.apache.doris.cloud.proto.Cloud.GetTxnRequest;
import org.apache.doris.cloud.proto.Cloud.GetTxnResponse;
import org.apache.doris.cloud.proto.Cloud.LoadJobSourceTypePB;
import org.apache.doris.cloud.proto.Cloud.MetaServiceCode;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.proto.Cloud.UniqueIdPB;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.BatchRemoveTransactionsOperation;
import org.apache.doris.persist.BatchRemoveTransactionsOperationV2;
import org.apache.doris.persist.EditLog;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionCommitFailedException;
import org.apache.doris.transaction.TransactionIdGenerator;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;
import org.apache.doris.transaction.TxnStateCallbackFactory;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class CloudGlobalTransactionMgr implements GlobalTransactionMgrIface {
    private static final Logger LOG = LogManager.getLogger(CloudGlobalTransactionMgr.class);
    private static final String NOT_SUPPORTED_MSG = "Not supported in cloud mode";

    private TxnStateCallbackFactory callbackFactory;

    public CloudGlobalTransactionMgr() {
        this.callbackFactory = new TxnStateCallbackFactory();
    }

    public void setEditLog(EditLog editLog) {
        //do nothing
    }

    @Override
    public TxnStateCallbackFactory getCallbackFactory() {
        return callbackFactory;
    }

    @Override
    public void addDatabaseTransactionMgr(Long dbId) {
        // do nothing in cloud mode
    }

    @Override
    public void removeDatabaseTransactionMgr(Long dbId) {
        // do nothing in cloud mode
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TxnCoordinator coordinator,
            LoadJobSourceType sourceType, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {
        return beginTransaction(dbId, tableIdList, label, null, coordinator, sourceType, -1, timeoutSecond);
    }

    @Override
    public long beginTransaction(long dbId, List<Long> tableIdList, String label, TUniqueId requestId,
            TxnCoordinator coordinator, LoadJobSourceType sourceType, long listenerId, long timeoutSecond)
            throws AnalysisException, LabelAlreadyUsedException, BeginTransactionException, DuplicatedRequestException,
            QuotaExceedException, MetaNotFoundException {

        LOG.info("try to begin transaction, dbId: {}, label: {}", dbId, label);
        if (Config.disable_load_job) {
            throw new AnalysisException("disable_load_job is set to true, all load jobs are prevented");
        }

        switch (sourceType) {
            case BACKEND_STREAMING:
                checkValidTimeoutSecond(timeoutSecond, Config.max_stream_load_timeout_second,
                        Config.min_load_timeout_second);
                break;
            default:
                checkValidTimeoutSecond(timeoutSecond, Config.max_load_timeout_second, Config.min_load_timeout_second);
        }

        BeginTxnResponse beginTxnResponse = null;
        int retryTime = 0;

        try {
            Preconditions.checkNotNull(coordinator);
            Preconditions.checkNotNull(label);
            FeNameFormat.checkLabel(label);

            TxnInfoPB.Builder txnInfoBuilder = TxnInfoPB.newBuilder();
            txnInfoBuilder.setDbId(dbId);
            txnInfoBuilder.addAllTableIds(tableIdList);
            txnInfoBuilder.setLabel(label);
            txnInfoBuilder.setListenerId(listenerId);

            if (requestId != null) {
                UniqueIdPB.Builder uniqueIdBuilder = UniqueIdPB.newBuilder();
                uniqueIdBuilder.setHi(requestId.getHi());
                uniqueIdBuilder.setLo(requestId.getLo());
                txnInfoBuilder.setRequestId(uniqueIdBuilder);
            }

            txnInfoBuilder.setCoordinator(TxnUtil.txnCoordinatorToPb(coordinator));
            txnInfoBuilder.setLoadJobSourceType(LoadJobSourceTypePB.forNumber(sourceType.value()));
            txnInfoBuilder.setTimeoutMs(timeoutSecond * 1000);
            txnInfoBuilder.setPrecommitTimeoutMs(Config.stream_load_default_precommit_timeout_second * 1000);

            final BeginTxnRequest beginTxnRequest = BeginTxnRequest.newBuilder()
                    .setTxnInfo(txnInfoBuilder.build())
                    .setCloudUniqueId(Config.cloud_unique_id)
                    .build();

            while (retryTime < Config.cloud_meta_service_rpc_failed_retry_times) {
                LOG.debug("retryTime:{}, beginTxnRequest:{}", retryTime, beginTxnRequest);
                beginTxnResponse = MetaServiceProxy.getInstance().beginTxn(beginTxnRequest);
                LOG.debug("retryTime:{}, beginTxnResponse:{}", retryTime, beginTxnResponse);

                if (beginTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                LOG.info("beginTxn KV_TXN_CONFLICT, retryTime:{}", retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(beginTxnResponse);
            Preconditions.checkNotNull(beginTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("beginTxn failed, exception:", e);
            throw new BeginTransactionException("beginTxn failed, errMsg:" + e.getMessage());
        }

        if (beginTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            switch (beginTxnResponse.getStatus().getCode()) {
                case TXN_DUPLICATED_REQ:
                    throw new DuplicatedRequestException(DebugUtil.printId(requestId),
                            beginTxnResponse.getDupTxnId(), beginTxnResponse.getStatus().getMsg());
                case TXN_LABEL_ALREADY_USED:
                    throw new LabelAlreadyUsedException(label);
                default:
                    if (MetricRepo.isInit) {
                        MetricRepo.COUNTER_TXN_REJECT.increase(1L);
                    }
                    throw new BeginTransactionException(beginTxnResponse.getStatus().getMsg());
            }
        }

        long txnId = beginTxnResponse.getTxnId();
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_BEGIN.increase(1L);
        }
        return txnId;
    }

    @Override
    public void preCommitTransaction2PC(Database db, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        Preconditions.checkState(false, "should not implement this in derived class");
    }

    @Override
    public void commitTransaction(long dbId, List<Table> tableList,
            long transactionId, List<TabletCommitInfo> tabletCommitInfos)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, null);
    }

    @Override
    public void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment)
            throws UserException {
        commitTransaction(dbId, tableList, transactionId, tabletCommitInfos, txnCommitAttachment, false);
    }

    private void commitTransaction(long dbId, List<Table> tableList, long transactionId,
            List<TabletCommitInfo> tabletCommitInfos, TxnCommitAttachment txnCommitAttachment, boolean is2PC)
            throws UserException {

        LOG.info("try to commit transaction, transactionId: {}", transactionId);
        if (Config.disable_load_job) {
            throw new TransactionCommitFailedException(
                    "disable_load_job is set to true, all load jobs are not allowed");
        }


        CommitTxnRequest.Builder builder = CommitTxnRequest.newBuilder();
        builder.setDbId(dbId)
                .setTxnId(transactionId)
                .setIs2Pc(is2PC)
                .setCloudUniqueId(Config.cloud_unique_id);

        if (txnCommitAttachment != null) {
            if (txnCommitAttachment instanceof LoadJobFinalOperation) {
                LoadJobFinalOperation loadJobFinalOperation = (LoadJobFinalOperation) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .loadJobFinalOperationToPb(loadJobFinalOperation));
            } else if (txnCommitAttachment instanceof RLTaskTxnCommitAttachment) {
                RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment = (RLTaskTxnCommitAttachment) txnCommitAttachment;
                builder.setCommitAttachment(TxnUtil
                        .rlTaskTxnCommitAttachmentToPb(rlTaskTxnCommitAttachment));
            } else {
                throw new UserException("invalid txnCommitAttachment");
            }
        }

        final CommitTxnRequest commitTxnRequest = builder.build();
        CommitTxnResponse commitTxnResponse = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.cloud_meta_service_rpc_failed_retry_times) {
                LOG.debug("retryTime:{}, commitTxnRequest:{}", retryTime, commitTxnRequest);
                commitTxnResponse = MetaServiceProxy.getInstance().commitTxn(commitTxnRequest);
                LOG.debug("retryTime:{}, commitTxnResponse:{}", retryTime, commitTxnResponse);
                if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("commitTxn KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", transactionId, retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(commitTxnResponse);
            Preconditions.checkNotNull(commitTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("commitTxn failed, transactionId:{}, exception:", transactionId, e);
            throw new UserException("commitTxn() failed, errMsg:" + e.getMessage());
        }

        if (commitTxnResponse.getStatus().getCode() != MetaServiceCode.OK
                && commitTxnResponse.getStatus().getCode() != MetaServiceCode.TXN_ALREADY_VISIBLE) {
            LOG.warn("commitTxn failed, transactionId:{}, retryTime:{}, commitTxnResponse:{}",
                    transactionId, retryTime, commitTxnResponse);
            StringBuilder internalMsgBuilder =
                    new StringBuilder("commitTxn failed, transactionId:");
            internalMsgBuilder.append(transactionId);
            internalMsgBuilder.append(" code:");
            internalMsgBuilder.append(commitTxnResponse.getStatus().getCode());
            throw new UserException("internal error, " + internalMsgBuilder.toString());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(commitTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb != null) {
            LOG.info("commitTxn, run txn callback, transactionId:{} callbackId:{}, txnState:{}",
                    txnState.getTransactionId(), txnState.getCallbackId(), txnState);
            cb.afterCommitted(txnState, true);
            cb.afterVisible(txnState, true);
        }
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_SUCCESS.increase(1L);
            MetricRepo.HISTO_TXN_EXEC_LATENCY.update(txnState.getCommitTime() - txnState.getPrepareTime());
        }
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis)
            throws UserException {
        return commitAndPublishTransaction(db, tableList, transactionId, tabletCommitInfos, timeoutMillis, null);
    }

    @Override
    public boolean commitAndPublishTransaction(DatabaseIf db, List<Table> tableList, long transactionId,
                                               List<TabletCommitInfo> tabletCommitInfos, long timeoutMillis,
                                               TxnCommitAttachment txnCommitAttachment) throws UserException {
        commitTransaction(db.getId(), tableList, transactionId, tabletCommitInfos, txnCommitAttachment);
        return true;
    }

    @Override
    public void commitTransaction2PC(Database db, List<Table> tableList, long transactionId, long timeoutMillis)
            throws UserException {
        commitTransaction(db.getId(), tableList, transactionId, null, null, true);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason) throws UserException {
        abortTransaction(dbId, transactionId, reason, null, null);
    }

    @Override
    public void abortTransaction(Long dbId, Long transactionId, String reason,
            TxnCommitAttachment txnCommitAttachment, List<Table> tableList) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, transactionId:{}", dbId, transactionId);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        int retryTime = 0;
        try {
            while (retryTime < Config.cloud_meta_service_rpc_failed_retry_times) {
                LOG.debug("retryTime:{}, abortTxnRequest:{}", retryTime, abortTxnRequest);
                abortTxnResponse = MetaServiceProxy
                        .getInstance().abortTxn(abortTxnRequest);
                LOG.debug("retryTime:{}, abortTxnResponse:{}", retryTime, abortTxnResponse);
                if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("abortTxn KV_TXN_CONFLICT, transactionId:{}, retryTime:{}", transactionId, retryTime);
                backoff();
                retryTime++;
                continue;
            }
            Preconditions.checkNotNull(abortTxnResponse);
            Preconditions.checkNotNull(abortTxnResponse.getStatus());
        } catch (RpcException e) {
            LOG.warn("abortTxn failed, transactionId:{}, Exception", transactionId, e);
            throw new UserException("abortTxn failed, errMsg:" + e.getMessage());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb != null) {
            LOG.info("run txn callback, txnId:{} callbackId:{}", txnState.getTransactionId(),
                    txnState.getCallbackId());
            cb.afterAborted(txnState, true, txnState.getReason());
        }
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_FAILED.increase(1L);
        }
    }

    @Override
    public void abortTransaction(Long dbId, String label, String reason) throws UserException {
        LOG.info("try to abort transaction, dbId:{}, label:{}", dbId, label);

        AbortTxnRequest.Builder builder = AbortTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setLabel(label);
        builder.setReason(reason);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final AbortTxnRequest abortTxnRequest = builder.build();
        AbortTxnResponse abortTxnResponse = null;
        int retryTime = 0;

        try {
            while (retryTime < Config.cloud_meta_service_rpc_failed_retry_times) {
                LOG.debug("retyTime:{}, abortTxnRequest:{}", retryTime, abortTxnRequest);
                abortTxnResponse = MetaServiceProxy
                        .getInstance().abortTxn(abortTxnRequest);
                LOG.debug("retryTime:{}, abortTxnResponse:{}", retryTime, abortTxnResponse);
                if (abortTxnResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }

                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("abortTxn KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
                continue;
            }
            Preconditions.checkNotNull(abortTxnResponse);
            Preconditions.checkNotNull(abortTxnResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("abortTxn failed, label:{}, exception:", label, e);
            throw new UserException("abortTxn failed, errMsg:" + e.getMessage());
        }

        TransactionState txnState = TxnUtil.transactionStateFromPb(abortTxnResponse.getTxnInfo());
        TxnStateChangeCallback cb = callbackFactory.getCallback(txnState.getCallbackId());
        if (cb == null) {
            LOG.info("no callback to run for this txn, txnId:{} callbackId:{}", txnState.getTransactionId(),
                        txnState.getCallbackId());
            return;
        }

        LOG.info("run txn callback, txnId:{} callbackId:{}", txnState.getTransactionId(), txnState.getCallbackId());
        cb.afterAborted(txnState, true, txnState.getReason());
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_TXN_FAILED.increase(1L);
        }
    }

    @Override
    public void abortTransaction2PC(Long dbId, long transactionId, List<Table> tableList) throws UserException {
        LOG.info("try to abortTransaction2PC, dbId:{}, transactionId:{}", dbId, transactionId);
        abortTransaction(dbId, transactionId, "User Abort", null, null);
        LOG.info("abortTransaction2PC successfully, dbId:{}, transactionId:{}", dbId, transactionId);
    }

    @Override
    public List<TransactionState> getReadyToPublishTransactions() {
        //do nothing for CloudGlobalTransactionMgr
        return new ArrayList<TransactionState>();
    }

    @Override
    public boolean existCommittedTxns(Long dbId, Long tableId, Long partitionId) {
        //do nothing for CloudGlobalTransactionMgr
        return false;
    }

    @Override
    public void finishTransaction(long dbId, long transactionId) throws UserException {
        throw new UserException("Disallow to call finishTransaction()");
    }

    @Override
    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        LOG.info("isPreviousTransactionsFinished(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new AnalysisException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                    .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new AnalysisException(checkTxnConflictResponse.getStatus().getMsg());
        }
        return checkTxnConflictResponse.getFinished();
    }

    public boolean isPreviousTransactionsFinished(long endTransactionId, long dbId, long tableId,
                                                  long partitionId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    public boolean isPreviousNonTimeoutTxnFinished(long endTransactionId, long dbId, List<Long> tableIdList)
            throws AnalysisException {
        LOG.info("isPreviousNonTimeoutTxnFinished(), endTransactionId:{}, dbId:{}, tableIdList:{}",
                endTransactionId, dbId, tableIdList);

        if (endTransactionId <= 0) {
            throw new AnalysisException("Invaid endTransactionId:" + endTransactionId);
        }
        CheckTxnConflictRequest.Builder builder = CheckTxnConflictRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setEndTxnId(endTransactionId);
        builder.addAllTableIds(tableIdList);
        builder.setCloudUniqueId(Config.cloud_unique_id);
        builder.setIgnoreTimeoutTxn(true);

        final CheckTxnConflictRequest checkTxnConflictRequest = builder.build();
        CheckTxnConflictResponse checkTxnConflictResponse = null;
        try {
            LOG.info("CheckTxnConflictRequest:{}", checkTxnConflictRequest);
            checkTxnConflictResponse = MetaServiceProxy
                    .getInstance().checkTxnConflict(checkTxnConflictRequest);
            LOG.info("CheckTxnConflictResponse: {}", checkTxnConflictResponse);
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        if (checkTxnConflictResponse.getStatus().getCode() != MetaServiceCode.OK) {
            throw new AnalysisException(checkTxnConflictResponse.getStatus().getMsg());
        }
        return checkTxnConflictResponse.getFinished();
    }

    @Override
    public void removeExpiredAndTimeoutTxns() {
        // do nothing in cloud mode
    }

    public void cleanLabel(Long dbId, String label, boolean isReplay) throws Exception {
        LOG.info("try to cleanLabel dbId: {}, label:{}", dbId, label);
        CleanTxnLabelRequest.Builder builder = CleanTxnLabelRequest.newBuilder();
        builder.setDbId(dbId).setCloudUniqueId(Config.cloud_unique_id);

        if (!Strings.isNullOrEmpty(label)) {
            builder.addLabels(label);
        }

        final CleanTxnLabelRequest cleanTxnLabelRequest = builder.build();
        CleanTxnLabelResponse cleanTxnLabelResponse = null;
        int retryTime = 0;

        try {
            // 5 times retry is enough for clean label
            while (retryTime < 5) {
                LOG.debug("retryTime:{}, cleanTxnLabel:{}", retryTime, cleanTxnLabelRequest);
                cleanTxnLabelResponse = MetaServiceProxy.getInstance().cleanTxnLabel(cleanTxnLabelRequest);
                LOG.debug("retryTime:{}, cleanTxnLabel:{}", retryTime, cleanTxnLabelResponse);
                if (cleanTxnLabelResponse.getStatus().getCode() != MetaServiceCode.KV_TXN_CONFLICT) {
                    break;
                }
                // sleep random [20, 200] ms, avoid txn conflict
                LOG.info("cleanTxnLabel KV_TXN_CONFLICT, dbId:{}, label:{}, retryTime:{}", dbId, label, retryTime);
                backoff();
                retryTime++;
                continue;
            }

            Preconditions.checkNotNull(cleanTxnLabelResponse);
            Preconditions.checkNotNull(cleanTxnLabelResponse.getStatus());
        } catch (Exception e) {
            LOG.warn("cleanTxnLabel failed, dbId:{}, exception:", dbId, e);
            throw new UserException("cleanTxnLabel failed, errMsg:" + e.getMessage());
        }

        if (cleanTxnLabelResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.warn("cleanTxnLabel failed, dbId:{} label:{} retryTime:{} cleanTxnLabelResponse:{}",
                    dbId, label, retryTime, cleanTxnLabelResponse);
            throw new UserException("cleanTxnLabel failed, errMsg:" + cleanTxnLabelResponse.getStatus().getMsg());
        }
        return;
    }

    @Override
    public void updateMultiTableRunningTransactionTableIds(Long dbId, Long transactionId, List<Long> tableIds)
            throws UserException {
        throw new UserException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void putTransactionTableNames(Long dbId, Long transactionId, List<Long> tableIds)
            throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request)
            throws AnalysisException, TimeoutException {
        long dbId = request.getDbId();
        int commitTimeoutSec = Config.commit_timeout_second;
        for (int i = 0; i < commitTimeoutSec; ++i) {
            Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
            TWaitingTxnStatusResult statusResult = new TWaitingTxnStatusResult();
            statusResult.status = new TStatus();
            TransactionStatus txnStatus = null;
            if (request.isSetTxnId()) {
                long txnId = request.getTxnId();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
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
        throw new TimeoutException("Operation is timeout");
    }

    @Override
    public void updateDatabaseUsedQuotaData(long dbId, long usedQuotaDataBytes) throws AnalysisException {
        // do nothing in cloud mode
    }

    @Override
    public void abortTxnWhenCoordinateBeDown(String coordinateHost, int limit) {
        // do nothing in cloud mode
    }

    @Override
    public TransactionStatus getLabelState(long dbId, String label) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public Long getTransactionId(Long dbId, String label) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public TransactionState getTransactionState(long dbId, long transactionId) {
        LOG.info("try to get transaction state, dbId:{}, transactionId:{}", dbId, transactionId);
        GetTxnRequest.Builder builder = GetTxnRequest.newBuilder();
        builder.setDbId(dbId);
        builder.setTxnId(transactionId);
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final GetTxnRequest getTxnRequest = builder.build();
        GetTxnResponse getTxnResponse = null;
        try {
            LOG.info("getTxnRequest:{}", getTxnRequest);
            getTxnResponse = MetaServiceProxy
                    .getInstance().getTxn(getTxnRequest);
            LOG.info("getTxnRequest: {}", getTxnResponse);
        } catch (RpcException e) {
            LOG.info("getTransactionState exception: {}", e.getMessage());
            return null;
        }

        if (getTxnResponse.getStatus().getCode() != MetaServiceCode.OK || !getTxnResponse.hasTxnInfo()) {
            LOG.info("getTransactionState exception: {}, {}", getTxnResponse.getStatus().getCode(),
                    getTxnResponse.getStatus().getMsg());
            return null;
        }
        return TxnUtil.transactionStateFromPb(getTxnResponse.getTxnInfo());
    }

    @Override
    public Long getTransactionIdByLabel(Long dbId, String label, List<TransactionStatus> statusList)
            throws UserException {
        throw new UserException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<TransactionState> getPreCommittedTxnList(Long dbId) throws AnalysisException {
        // todo
        return new ArrayList<TransactionState>();
    }

    @Override
    public int getTransactionNum() {
        return 0;
    }

    @Override
    public Long getNextTransactionId() throws UserException {
        GetCurrentMaxTxnRequest.Builder builder = GetCurrentMaxTxnRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id);

        final GetCurrentMaxTxnRequest getCurrentMaxTxnRequest = builder.build();
        GetCurrentMaxTxnResponse getCurrentMaxTxnResponse = null;
        try {
            LOG.info("GetCurrentMaxTxnRequest:{}", getCurrentMaxTxnRequest);
            getCurrentMaxTxnResponse = MetaServiceProxy
                    .getInstance().getCurrentMaxTxnId(getCurrentMaxTxnRequest);
            LOG.info("GetCurrentMaxTxnResponse: {}", getCurrentMaxTxnResponse);
        } catch (RpcException e) {
            LOG.warn("getNextTransactionId() RpcException: {}", e.getMessage());
            throw new UserException("getNextTransactionId() RpcException: " + e.getMessage());
        }

        if (getCurrentMaxTxnResponse.getStatus().getCode() != MetaServiceCode.OK) {
            LOG.info("getNextTransactionId() failed, code: {}, msg: {}",
                    getCurrentMaxTxnResponse.getStatus().getCode(), getCurrentMaxTxnResponse.getStatus().getMsg());
            throw new UserException("getNextTransactionId() failed, msg:"
                    + getCurrentMaxTxnResponse.getStatus().getMsg());
        }
        return getCurrentMaxTxnResponse.getCurrentMaxTxnId();
    }

    @Override
    public int getRunningTxnNums(Long dbId) throws AnalysisException {
        return 0;
    }

    @Override
    public long getTxnNumByStatus(TransactionStatus status) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getAllRunningTxnNum() {
        return 0;
    }

    @Override
    public long getAllPublishTxnNum() {
        return 0;
    }

    /**
     * backoff policy implement by sleep random ms in [20ms, 200ms]
     */
    private void backoff() {
        int randomMillis = 20 + (int) (Math.random() * (200 - 20));
        try {
            Thread.sleep(randomMillis);
        } catch (InterruptedException e) {
            LOG.info("InterruptedException: ", e);
        }
    }

    @Override
    public TransactionIdGenerator getTransactionIDGenerator() throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbInfo() throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbTransStateInfo(Long dbId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbTransInfo(Long dbId, boolean running, int limit) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getDbTransInfoByStatus(Long dbId, TransactionStatus status) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<String>> getSingleTranInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<Comparable>> getTableTransInfo(long dbId, long txnId) throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public List<List<Comparable>> getPartitionTransInfo(long dbId, long tid, long tableId)
            throws AnalysisException {
        throw new AnalysisException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new IOException(NOT_SUPPORTED_MSG);
    }

    @Override
    public void replayUpsertTransactionState(TransactionState transactionState) throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Deprecated
    public void replayDeleteTransactionState(TransactionState transactionState) throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public void replayBatchRemoveTransactions(BatchRemoveTransactionsOperation operation) throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }

    @Override
    public void replayBatchRemoveTransactionV2(BatchRemoveTransactionsOperationV2 operation)
            throws Exception {
        throw new Exception(NOT_SUPPORTED_MSG);
    }
}
