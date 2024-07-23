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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSink;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Insert executor for olap table
 */
public class OlapInsertExecutor extends AbstractInsertExecutor {
    protected static final long INVALID_TXN_ID = -1L;
    private static final Logger LOG = LogManager.getLogger(OlapInsertExecutor.class);
    protected long txnId = INVALID_TXN_ID;
    protected TransactionStatus txnStatus = TransactionStatus.ABORTED;

    /**
     * constructor
     */
    public OlapInsertExecutor(ConnectContext ctx, Table table,
            String labelName, NereidsPlanner planner, Optional<InsertCommandContext> insertCtx, boolean emptyInsert) {
        super(ctx, table, labelName, planner, insertCtx, emptyInsert);
    }

    public long getTxnId() {
        return txnId;
    }

    @Override
    public void beginTransaction() {
        if (isGroupCommitHttpStream()) {
            LOG.info("skip begin transaction for group commit http stream");
            return;
        }
        try {
            this.txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    database.getId(), ImmutableList.of(table.getId()), labelName,
                    new TxnCoordinator(TxnSourceType.FE, 0,
                            FrontendOptions.getLocalHostAddress(),
                            ExecuteEnv.getInstance().getStartupTime()),
                    LoadJobSourceType.INSERT_STREAMING, ctx.getExecTimeout());
        } catch (Exception e) {
            throw new AnalysisException("begin transaction failed. " + e.getMessage(), e);
        }
    }

    @Override
    public void finalizeSink(PlanFragment fragment, DataSink sink, PhysicalSink physicalSink) {
        OlapTableSink olapTableSink = (OlapTableSink) sink;
        PhysicalOlapTableSink physicalOlapTableSink = (PhysicalOlapTableSink) physicalSink;
        OlapInsertCommandContext olapInsertCtx = (OlapInsertCommandContext) insertCtx.orElse(
                new OlapInsertCommandContext(true));

        boolean isStrictMode = ctx.getSessionVariable().getEnableInsertStrict()
                && physicalOlapTableSink.isPartialUpdate()
                && physicalOlapTableSink.getDmlCommandType() == DMLCommandType.INSERT;
        try {
            // TODO refactor this to avoid call legacy planner's function
            long timeout = getTimeout();
            olapTableSink.init(ctx.queryId(), txnId, database.getId(),
                    timeout,
                    ctx.getSessionVariable().getSendBatchParallelism(),
                    false,
                    isStrictMode,
                    timeout);
            // complete and set commands both modify thrift struct
            olapTableSink.complete(new Analyzer(Env.getCurrentEnv(), ctx));
            if (!olapInsertCtx.isAllowAutoPartition()) {
                olapTableSink.setAutoPartition(false);
            }
            if (olapInsertCtx.isAutoDetectOverwrite()) {
                olapTableSink.setAutoDetectOverwite(true);
                olapTableSink.setOverwriteGroupId(olapInsertCtx.getOverwriteGroupId());
            }
            // update

            // set schema and partition info for tablet id shuffle exchange
            if (fragment.getPlanRoot() instanceof ExchangeNode
                    && fragment.getDataPartition().getType() == TPartitionType.TABLET_SINK_SHUFFLE_PARTITIONED) {
                DataStreamSink dataStreamSink = (DataStreamSink) (fragment.getChild(0).getSink());
                Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
                dataStreamSink.setTabletSinkSchemaParam(olapTableSink.createSchema(
                        database.getId(), olapTableSink.getDstTable(), analyzer));
                dataStreamSink.setTabletSinkPartitionParam(olapTableSink.createPartition(
                        database.getId(), olapTableSink.getDstTable(), analyzer));
                dataStreamSink.setTabletSinkTupleDesc(olapTableSink.getTupleDescriptor());
                List<TOlapTableLocationParam> locationParams = olapTableSink
                        .createLocation(olapTableSink.getDstTable());
                dataStreamSink.setTabletSinkLocationParam(locationParams.get(0));
                dataStreamSink.setTabletSinkTxnId(olapTableSink.getTxnId());
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (!isGroupCommitHttpStream()) {
            TransactionState state = Env.getCurrentGlobalTransactionMgr().getTransactionState(database.getId(), txnId);
            if (state == null) {
                throw new AnalysisException("txn does not exist: " + txnId);
            }
            addTableIndexes(state);
            if (physicalOlapTableSink.isPartialUpdate()) {
                state.setSchemaForPartialUpdate((OlapTable) table);
            }
        }
    }

    protected void addTableIndexes(TransactionState state) {
        state.addTableIndexes((OlapTable) table);
    }

    @Override
    protected void beforeExec() {
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.info("start insert [{}] with query id {} and txn id {}", labelName, queryId, txnId);
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
        } else if (Env.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                database, Lists.newArrayList((Table) table),
                txnId,
                TabletCommitInfo.fromThrift(coordinator.getCommitInfos()),
                ctx.getSessionVariable().getInsertVisibleTimeoutMs())) {
            txnStatus = TransactionStatus.VISIBLE;
        } else {
            txnStatus = TransactionStatus.COMMITTED;
        }
        if (Config.isCloudMode()) {
            String clusterName = ctx.getCloudCluster();
            if (ctx.getSessionVariable().enableMultiClusterSyncLoad()
                    && clusterName != null && !clusterName.isEmpty()) {
                CloudSystemInfoService infoService = (CloudSystemInfoService) Env.getCurrentSystemInfo();
                List<List<Backend>> backendsList = infoService
                                                        .getCloudClusterNames()
                                                        .stream()
                                                        .filter(name -> !name.equals(clusterName))
                                                        .map(name -> infoService.getBackendsByClusterName(name))
                                                        .collect(Collectors.toList());
                List<Long> allTabletIds = ((OlapTable) table).getAllTabletIds();
                StmtExecutor.syncLoadForTablets(backendsList, allTabletIds);
            }
        }
    }

    @Override
    protected void onFail(Throwable t) {
        errMsg = t.getMessage() == null ? "unknown reason" : t.getMessage();
        String queryId = DebugUtil.printId(ctx.queryId());
        // if any throwable being thrown during insert operation, first we should abort this txn
        LOG.warn("insert [{}] with query id {} failed", labelName, queryId, t);
        if (txnId != INVALID_TXN_ID) {
            try {
                Env.getCurrentGlobalTransactionMgr().abortTransaction(
                        database.getId(), txnId, errMsg);
            } catch (Exception abortTxnException) {
                // just print a log if abort txn failed. This failure do not need to pass to user.
                // user only concern abort how txn failed.
                LOG.warn("insert [{}] with query id {} abort txn {} failed",
                        labelName, queryId, txnId, abortTxnException);
            }
        }
        // retry insert into from select when meet E-230 in cloud
        if (Config.isCloudMode() && t.getMessage().contains(FeConstants.CLOUD_RETRY_E230)) {
            return;
        }
        StringBuilder sb = new StringBuilder(t.getMessage());
        if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
            sb.append(". url: ").append(coordinator.getTrackingUrl());
        }
        ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
    }

    @Override
    protected void afterExec(StmtExecutor executor) {
        // Go here, which means:
        // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
        // 2. transaction failed but Config.using_old_load_usage_pattern is true.
        // we will record the load job info for these 2 cases
        try {
            // the statement parsed by Nereids is saved at executor::parsedStmt.
            StatementBase statement = executor.getParsedStmt();
            UserIdentity userIdentity;
            //if we use job scheduler, parse statement will not set user identity,so we need to get it from context
            if (null == statement) {
                userIdentity = ctx.getCurrentUserIdentity();
            } else {
                userIdentity = statement.getUserInfo();
            }
            EtlJobType etlJobType = EtlJobType.INSERT;
            if (0 != jobId) {
                etlJobType = EtlJobType.INSERT_JOB;
            }
            if (!Config.enable_nereids_load) {
                // just record for loadv2 here
                ctx.getEnv().getLoadManager()
                        .recordFinishedLoadJob(labelName, txnId, database.getFullName(),
                                table.getId(),
                                etlJobType, createTime, errMsg,
                                coordinator.getTrackingUrl(), userIdentity, jobId);
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
            errMsg = "Record info of insert load with error " + e.getMessage();
        }

        setReturnInfo();
    }

    protected void setReturnInfo() {
        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(labelName).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(txnId).append("'");
        if (table.getType() == TableType.MATERIALIZED_VIEW) {
            sb.append("', 'rows':'").append(loadedRows).append("'");
        }
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");

        ctx.getState().setOk(loadedRows, filteredRows, sb.toString());
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(txnId, labelName, database.getFullName(), table.getName(),
                txnStatus, loadedRows, filteredRows);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) loadedRows);
    }

    public long getTimeout() {
        return ctx.getExecTimeout();
    }

    private boolean isGroupCommitHttpStream() {
        return ConnectContext.get() != null && ConnectContext.get().isGroupCommit();
    }
}
