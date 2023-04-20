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

package org.apache.doris.nereids.txn;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.MasterTxnExecutor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * transaction wrapper for Nereids
 */
public class Transaction {
    public static final Logger LOG = LogManager.getLogger(Transaction.class);
    private final ConnectContext ctx;
    private final NereidsPlanner planner;
    private final long createAt;
    private final long txnId;
    private final String labelName;
    private final Database database;
    private final Table table;
    private long loadedRows = 0;
    private int filteredRows = 0;
    private TransactionStatus txnStatus = TransactionStatus.ABORTED;
    private String errMsg = "";
    private final Coordinator coordinator;

    /**
     * constructor
     */
    public Transaction(ConnectContext ctx, Database database, Table table, String labelName, NereidsPlanner planner)
            throws UserException {
        this.ctx = ctx;
        this.labelName = labelName;
        this.database = database;
        this.table = table;
        this.planner = planner;
        this.coordinator = new Coordinator(ctx, null, planner, ctx.getStatsErrorEstimator());
        this.txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                database.getId(), ImmutableList.of(table.getId()), labelName,
                new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                LoadJobSourceType.INSERT_STREAMING, ctx.getExecTimeout());
        this.createAt = System.currentTimeMillis();
    }

    public long getTxnId() {
        return txnId;
    }

    private void beginTxn() throws TException, UserException, ExecutionException, InterruptedException,
            TimeoutException {
        TransactionEntry txnEntry = ctx.getTxnEntry();
        TTxnParams txnConf = txnEntry.getTxnConf();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        long timeoutSecond = ctx.getExecTimeout();

        txnConf.setDbId(database.getId()).setTbl(table.getName()).setDb(database.getFullName());

        txnEntry.setTable(table);
        txnEntry.setDb(database);

        if (Env.getCurrentEnv().isMaster()) {
            txnConf.setTxnId(txnId);
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            txnConf.setToken(token);
        } else {
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(ctx);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();

            request.setDb(txnConf.getDb())
                    .setTbl(txnConf.getTbl())
                    .setToken(token)
                    .setCluster(database.getClusterName())
                    .setLabel(labelName)
                    .setUser("")
                    .setUserIp("")
                    .setPasswd("");

            TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
            txnConf.setTxnId(result.getTxnId());
            txnConf.setToken(token);
        }

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();

        long maxExecMemByte = sessionVariable.getMaxExecMemByte();
        String timeZone = sessionVariable.getTimeZone();
        int sendBatchParallelism = sessionVariable.getSendBatchParallelism();

        request.setTxnId(txnConf.getTxnId()).setDb(txnConf.getDb())
                .setTbl(txnConf.getTbl())
                .setFileType(TFileType.FILE_STREAM)
                .setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND)
                .setThriftRpcTimeoutMs(5000)
                .setLoadId(ctx.queryId())
                .setExecMemLimit(maxExecMemByte)
                .setTimeout((int) timeoutSecond)
                .setTimezone(timeZone)
                .setSendBatchParallelism(sendBatchParallelism);

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        executor.beginTransaction(request);
    }

    /**
     * execute insert txn for insert into select command.
     */
    public void executeInsertIntoSelectCommand(InsertIntoTableCommand command)
            throws Exception {
        if (ctx.isTxnModel()) {
            PhysicalPlan physicalPlan = command.getPhysicalQuery();
            Preconditions.checkArgument(physicalPlan instanceof PhysicalOneRowRelation,
                    "insert into select in txn model is not supported");
            beginTxn();
            loadedRows = executeInsertIntoValuesCommand(null);
        } else {
            LOG.info("Do insert [{}] with query id: {}", labelName, DebugUtil.printId(ctx.queryId()));
            try {
                handleTransactionForNotInTxnModel();
            } catch (Throwable t) {
                errMsg = t.getMessage();
                handleTransactionErrorForNotInTxnModel(t);
            } finally {
                if (ctx.getSessionVariable().enableProfile() && coordinator != null) {
                    coordinator.endProfile(true);
                }
                QeProcessorImpl.INSTANCE.unregisterQuery(ctx.queryId());
            }

            // Go here, which means:
            // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
            // 2. transaction failed but Config.using_old_load_usage_pattern is true.
            // we will record the load job info for these 2 cases
            try {
                ctx.getEnv().getLoadManager().recordFinishedLoadJob(
                        labelName, txnId, database.getFullName(), table.getId(),
                        EtlJobType.INSERT, createAt, errMsg,
                        coordinator.getTrackingUrl(), ctx.getCurrentUserIdentity());
            } catch (MetaNotFoundException e) {
                LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
                errMsg = "Record info of insert load with error " + e.getMessage();
            }
        }

        setResultString();
    }

    /**
     * execute insert txn for insert into select literal list command and insert into values command.
     */
    public long executeInsertIntoValuesCommand(List<List<Expr>> values)
            throws TException, UserException, ExecutionException, InterruptedException, TimeoutException {
        // TODO
        // TransactionStatus status = TransactionStatus.PREPARE;
        if (ctx.isTxnIniting()) {
            beginTxn();
        }
        TransactionEntry entry = ctx.getTxnEntry();
        int schemaSize = entry.getTable().getBaseSchema().size();
        for (List<Expr> valueList : values) {
            if (schemaSize != valueList.size()) {
                throw new TException(String.format("Column count: %d doesn't match value count: %d",
                        schemaSize, valueList.size()));
            }
            InternalService.PDataRow data = StmtExecutor.getRowStringValue(valueList);
            if (data == null) {
                continue;
            }
            List<InternalService.PDataRow> dataToSend = entry.getDataToSend();
            dataToSend.add(data);
            if (dataToSend.size() >= StmtExecutor.MAX_DATA_TO_SEND_FOR_TXN) {
                // send data
                InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(entry);
                executor.sendData();
            }
        }
        entry.setRowsInTransaction(entry.getRowsInTransaction() + values.size());
        return entry.getRowsInTransaction();
    }

    private void handleTransactionForNotInTxnModel() throws Exception {
        coordinator.setLoadZeroTolerance(ctx.getSessionVariable().getEnableInsertStrict());
        coordinator.setQueryType(TQueryType.LOAD);

        QeProcessorImpl.INSTANCE.registerQuery(ctx.queryId(), coordinator);

        coordinator.exec();
        int execTimeout = ctx.getExecTimeout();
        LOG.debug("Insert execution timeout:{}", execTimeout);
        boolean notTimeout = coordinator.join(execTimeout);
        if (!coordinator.isDone()) {
            coordinator.cancel();
            if (notTimeout) {
                errMsg = coordinator.getExecStatus().getErrorMsg();
                ErrorReport.reportDdlException("There exists unhealthy backend. "
                        + errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_EXECUTE_TIMEOUT);
            }
        }

        if (!coordinator.getExecStatus().ok()) {
            errMsg = coordinator.getExecStatus().getErrorMsg();
            LOG.warn("insert failed: {}", errMsg);
            ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
        }

        LOG.debug("delta files is {}", coordinator.getDeltaUrls());

        if (coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
            loadedRows = Long.parseLong(coordinator.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
        }
        if (coordinator.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null) {
            filteredRows = Integer.parseInt(coordinator.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
        }

        // if in strict mode, insert will fail if there are filtered rows
        if (ctx.getSessionVariable().getEnableInsertStrict()) {
            if (filteredRows > 0) {
                ctx.getState().setError(ErrorCode.ERR_FAILED_WHEN_INSERT,
                        "Insert has filtered data in strict mode, tracking_url=" + coordinator.getTrackingUrl());
            }
        }

        if (table.getType() != TableType.OLAP && table.getType() != TableType.MATERIALIZED_VIEW) {
            // no need to add load job.
            // MySQL table is already being inserted.
            ctx.getState().setOk(loadedRows, filteredRows, null);
            return;
        }

        if (Env.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                database, Lists.newArrayList(table), txnId,
                TabletCommitInfo.fromThrift(coordinator.getCommitInfos()),
                ctx.getSessionVariable().getInsertVisibleTimeoutMs())) {
            txnStatus = TransactionStatus.VISIBLE;
        } else {
            txnStatus = TransactionStatus.COMMITTED;
        }
    }

    private void handleTransactionErrorForNotInTxnModel(Throwable t) {
        // if any throwable being thrown during insert operation, first we should abort this txn
        LOG.warn("handle insert stmt fail: {}", labelName, t);
        try {
            Env.getCurrentGlobalTransactionMgr().abortTransaction(database.getId(), txnId,
                    t.getMessage() == null ? "unknown reason" : t.getMessage());
        } catch (Exception abortTxnException) {
            // just print a log if abort txn failed. This failure do not need to pass to user.
            // user only concern abort how txn failed.
            LOG.warn("errors when abort txn", abortTxnException);
        }

        if (!Config.using_old_load_usage_pattern) {
            // if not using old load usage pattern, error will be returned directly to user
            StringBuilder sb = new StringBuilder(t.getMessage());
            if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
                sb.append(". url: ").append(coordinator.getTrackingUrl());
            }
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
        }

        /*
         * If config 'using_old_load_usage_pattern' is true.
         * Doris will return a label to user, and user can use this label to check load job's status,
         * which exactly like the old insert stmt usage pattern.
         */
    }

    private void setResultString() {
        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder()
                .append("{'label':'").append(labelName)
                .append("', 'status':'")
                .append(txnStatus.name())
                .append("', 'txnId':'")
                .append(txnId).append("'");

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
}
