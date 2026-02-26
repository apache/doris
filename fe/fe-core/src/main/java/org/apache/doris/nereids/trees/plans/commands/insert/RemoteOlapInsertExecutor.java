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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.doris.FeServiceClient;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.RemoteOlapTableSink;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TAbortRemoteTxnRequest;
import org.apache.doris.thrift.TAbortRemoteTxnResult;
import org.apache.doris.thrift.TBeginRemoteTxnRequest;
import org.apache.doris.thrift.TBeginRemoteTxnResult;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.thrift.TCommitRemoteTxnResult;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Remote executor for Doris Catalog remote insert.
 * Local insert implementation remains in {@link OlapInsertExecutor}.
 *
 * This executor is responsible for wiring Doris Catalog remote transaction
 * lifecycle (begin / commit / abort) while reusing the generic insert execution pipeline
 * defined in {@link AbstractInsertExecutor}.
 */
public class RemoteOlapInsertExecutor extends OlapInsertExecutor {

    private static final Logger LOG = LogManager.getLogger(RemoteOlapInsertExecutor.class);

    public RemoteOlapInsertExecutor(ConnectContext ctx, RemoteOlapTable table,
            String labelName, org.apache.doris.nereids.NereidsPlanner planner,
            java.util.Optional<InsertCommandContext> insertCtx, boolean emptyInsert,
            long jobId) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert, jobId);
    }

    @Override
    public void beginTransaction() {
        RemoteDorisExternalCatalog remoteCatalog = ((RemoteOlapTable) table).getCatalog();
        FeServiceClient client = remoteCatalog.getFeServiceClient();
        String remoteDbName = database.getFullName();
        String remoteTableName = table.getName();

        TBeginRemoteTxnRequest request = new TBeginRemoteTxnRequest();
        request.setCatalog(database.getCatalog().getName());
        request.setDb(remoteDbName);
        request.setTbl(remoteTableName);
        request.setLabel(labelName);
        long timeoutSeconds = getTimeout();
        if (timeoutSeconds > 0) {
            request.setTimeoutMs(timeoutSeconds * 1000L);
        }

        try {
            TBeginRemoteTxnResult result = client.beginRemoteTxn(request);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                switch (result.getStatus().getStatusCode()) {
                    case NOT_AUTHORIZED:
                        throw new AuthenticationException(result.getStatus().getErrorMsgs().get(0));
                    case LABEL_ALREADY_EXISTS:
                        throw new LabelAlreadyUsedException(result.getStatus().getErrorMsgs().get(0));
                    case TOO_MANY_TASKS:
                        throw new BeginTransactionException(result.getStatus().getErrorMsgs().get(0));
                    case LIMIT_REACH:
                        throw new QuotaExceedException(result.getStatus().getErrorMsgs().get(0));
                    case NOT_FOUND:
                        throw new MetaNotFoundException(result.getStatus().getErrorMsgs().get(0));
                    case ANALYSIS_ERROR:
                    case INTERNAL_ERROR:
                    default:
                        throw new AnalysisException(result.getStatus().getErrorMsgs().get(0));
                }
            }
            this.txnId = result.getTxnId();
            LOG.warn("begin remote txn success, catalog={}, db={}, table={}, label={}, txnId={}",
                    database.getCatalog().getName(), remoteDbName, remoteTableName, labelName, txnId);
        } catch (Exception e) {
            LOG.info("begin remote txn failed, catalog={}, db={}, table={}, label={}, errMsg={}",
                    database.getCatalog().getName(), remoteDbName, remoteTableName, labelName, e.getMessage());
            throw new AnalysisException(Util.getRootCauseMessage(e), e);
        }
    }

    @Override
    public void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        RemoteOlapTableSink remoteOlapTableSink = (RemoteOlapTableSink) sink;
        PhysicalOlapTableSink physicalOlapTableSink = (PhysicalOlapTableSink) physicalSink;
        OlapInsertCommandContext olapInsertCtx = (OlapInsertCommandContext) insertCtx.orElse(
                new OlapInsertCommandContext(true));

        boolean isStrictMode = ctx.getSessionVariable().getEnableInsertStrict()
                && physicalOlapTableSink.isPartialUpdate()
                && physicalOlapTableSink.getDmlCommandType() == DMLCommandType.INSERT;
        try {
            long timeout = getTimeout();
            long dbId = database.getId();
            remoteOlapTableSink.init(ctx.queryId(), txnId, dbId,
                    timeout,
                    ctx.getSessionVariable().getSendBatchParallelism(),
                    false,
                    isStrictMode,
                    timeout, olapInsertCtx);

            if (fragment.getPlanRoot() instanceof ExchangeNode
                    && fragment.getDataPartition().getType() == TPartitionType.OLAP_TABLE_SINK_HASH_PARTITIONED) {
                DataSink childFragmentSink = fragment.getChild(0).getSink();
                DataStreamSink dataStreamSink = null;
                if (childFragmentSink instanceof MultiCastDataSink) {
                    MultiCastDataSink multiCastDataSink = (MultiCastDataSink) childFragmentSink;
                    int outputExchangeId = (fragment.getPlanRoot()).getId().asInt();
                    for (DataStreamSink currentDataStreamSink : multiCastDataSink.getDataStreamSinks()) {
                        int sinkExchangeId = currentDataStreamSink.getExchNodeId().asInt();
                        if (outputExchangeId == sinkExchangeId) {
                            dataStreamSink = currentDataStreamSink;
                            break;
                        }
                    }
                    if (dataStreamSink == null) {
                        throw new IllegalStateException("Can not find DataStreamSink in the MultiCastDataSink");
                    }
                } else if (childFragmentSink instanceof DataStreamSink) {
                    dataStreamSink = (DataStreamSink) childFragmentSink;
                } else {
                    throw new IllegalStateException("Unsupported DataSink: " + childFragmentSink);
                }

                dataStreamSink.setTabletSinkSchemaParam(remoteOlapTableSink.getOlapTableSchemaParam());
                dataStreamSink.setTabletSinkPartitionParam(remoteOlapTableSink.getOlapTablePartitionParam());
                dataStreamSink.setTabletSinkTupleDesc(remoteOlapTableSink.getTupleDescriptor());
                List<TOlapTableLocationParam> locationParams = remoteOlapTableSink.getOlapTableLocationParams();
                dataStreamSink.setTabletSinkLocationParam(locationParams.get(0));
                dataStreamSink.setTabletSinkTxnId(remoteOlapTableSink.getTxnId());
                dataStreamSink.setTabletSinkExprs(fragment.getOutputExprs());
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    protected void onComplete() throws UserException {
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            try {
                String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
                Env.getCurrentGlobalTransactionMgr().abortTransaction(
                        database.getId(), txnId,
                        (errMsg == null ? "unknown reason" : errMsg));
            } catch (Exception abortTxnException) {
                LOG.warn("errors when abort txn. {}", ctx.getQueryIdentifier(), abortTxnException);
            }
        }

        RemoteDorisExternalCatalog remoteCatalog = ((RemoteOlapTable) table).getCatalog();
        FeServiceClient client = remoteCatalog.getFeServiceClient();

        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest();
        request.setTxnId(txnId);
        request.setCatalog(database.getCatalog().getName());
        request.setDb(database.getFullName());
        request.setTbl(table.getName());
        request.setCommitInfos(coordinator.getCommitInfos());
        request.setInsertVisibleTimeoutMs(ctx.getSessionVariable().getInsertVisibleTimeoutMs());
        try {
            TCommitRemoteTxnResult result = client.commitRemoteTxn(request);
            if (result.getStatus().getStatusCode() == TStatusCode.OK) {
                if (result.isTxnStatus()) {
                    txnStatus = TransactionStatus.VISIBLE;
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }
                LOG.info("commit remote txn success, catalog={}, dbId={}, txnId={}, status={}",
                                    remoteCatalog.getName(), database.getId(), txnId, txnStatus);
            } else {
                switch (result.getStatus().getStatusCode()) {
                    case NOT_AUTHORIZED:
                        throw new AuthenticationException(result.getStatus().getErrorMsgs().get(0));
                    default:
                        throw new UserException(result.getStatus().getErrorMsgs().get(0));
                }
            }
        } catch (UserException e) {
            LOG.warn("commit remote txn failed, catalog={}, dbId={}, txnId={}, status={}, err={}",
                                    remoteCatalog.getName(), database.getId(), txnId, txnStatus, e.getMessage());
            throw e;
        } catch (Exception e) {
            throw new UserException(Util.getRootCauseMessage(e), e);
        }
    }

    /**
     * Abort remote transaction when insert into remote Doris table failed.
     * This method is best-effort and will not throw exception to user.
     */
    @Override
    protected void abortTransactionOnFail() throws Exception {
        RemoteDorisExternalCatalog remoteCatalog = ((RemoteOlapTable) table).getCatalog();
        FeServiceClient client = remoteCatalog.getFeServiceClient();
        TAbortRemoteTxnRequest request = new TAbortRemoteTxnRequest();
        request.setTxnId(txnId);
        request.setCatalog(database.getCatalog().getName());
        request.setDb(database.getFullName());
        try {
            TAbortRemoteTxnResult result = client.abortRemoteTxn(request);
            if (result.getStatus().getStatusCode() == TStatusCode.OK) {
                LOG.info("abort remote txn success, catalog={}, txnId={} ",
                        remoteCatalog.getName(), txnId);
            } else {
                LOG.warn("abort remote transaction failed. catalog={}, txnId={}, err={}",
                        remoteCatalog.getName(), txnId, result.getStatus().getErrorMsgs().get(0));
                throw new UserException(result.getStatus().getErrorMsgs().get(0));
            }
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("abort remote transaction failed unexpectedly. catalog={}, txnId={}, err={}",
                    remoteCatalog.getName(), txnId, e.getMessage(), e);
            throw new Exception(Util.getRootCauseMessage(e), e);
        }
    }

    private String buildFinalErrorMessage(Throwable t) {
        String localErrMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String firstErrorMsgPart = "";
        String urlPart = "";
        if (!Strings.isNullOrEmpty(coordinator.getFirstErrorMsg())) {
            firstErrorMsgPart = StringUtils.abbreviate(coordinator.getFirstErrorMsg(),
                    org.apache.doris.common.Config.first_error_msg_max_length);
        }
        if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
            urlPart = coordinator.getTrackingUrl();
        }
        return InsertUtils.getFinalErrorMsg(localErrMsg, firstErrorMsgPart, urlPart);
    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.warn("insert [{}] with query id {} failed", labelName, queryId, t);
        if (txnId != INVALID_TXN_ID) {
            try {
                abortTransactionOnFail();
            } catch (Exception abortTxnException) {
                LOG.warn("insert [{}] with query id {} abort txn {} failed",
                        labelName, queryId, txnId, abortTxnException);
            }
        }
        String finalErrorMsg = buildFinalErrorMessage(t);
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, finalErrorMsg);
    }

    @Override
    protected void afterExec(StmtExecutor executor) {
        // Go here, which means:
        // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
        // 2. transaction failed but Config.using_old_load_usage_pattern is true.
        // we will record the load job info for these 2 cases
        try {
            // Do not register job if job id is -1.
            if (!Config.enable_nereids_load && jobId != -1) {
                ((RemoteOlapTable) table).getCatalog().getFeServiceClient().recordFinishedLoadJob(
                        labelName, txnId, database.getCatalog().getName(), database.getFullName(), table.getName(),
                        createTime, errMsg, coordinator.getTrackingUrl(), coordinator.getFirstErrorMsg(), jobId);
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
            errMsg = "Record info of insert load with error " + e.getMessage();
        }
        setReturnInfo();
    }
}
