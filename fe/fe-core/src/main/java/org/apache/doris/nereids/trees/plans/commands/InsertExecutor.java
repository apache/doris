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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DefaultValueSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.InsertStreamTxnExecutor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * transaction wrapper for Nereids
 */
public class InsertExecutor {

    private static final Logger LOG = LogManager.getLogger(InsertExecutor.class);
    private static final long INVALID_TXN_ID = -1L;

    private final ConnectContext ctx;
    private final Coordinator coordinator;
    private final String labelName;
    private final Database database;
    private final Table table;
    private final long createAt = System.currentTimeMillis();
    private long loadedRows = 0;
    private int filteredRows = 0;
    private long txnId = INVALID_TXN_ID;
    private TransactionStatus txnStatus = TransactionStatus.ABORTED;
    private String errMsg = "";

    /**
     * constructor
     */
    public InsertExecutor(ConnectContext ctx, Database database, Table table,
            String labelName, NereidsPlanner planner) {
        this.ctx = ctx;
        this.coordinator = new Coordinator(ctx, null, planner, ctx.getStatsErrorEstimator());
        this.labelName = labelName;
        this.database = database;
        this.table = table;
    }

    public long getTxnId() {
        return txnId;
    }

    /**
     * begin transaction if necessary
     */
    public void beginTransaction() {
        if (!(table instanceof OlapTable)) {
            return;
        }
        try {
            this.txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    database.getId(), ImmutableList.of(table.getId()), labelName,
                    new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    LoadJobSourceType.INSERT_STREAMING, ctx.getExecTimeout());
        } catch (Exception e) {
            throw new AnalysisException("begin transaction failed. " + e.getMessage(), e);
        }
    }

    /**
     * finalize sink to complete enough info for sink execution
     */
    public void finalizeSink(DataSink sink, boolean isPartialUpdate, boolean isFromInsert,
            boolean allowAutoPartition) {
        if (!(sink instanceof OlapTableSink)) {
            return;
        }
        Preconditions.checkState(table instanceof OlapTable,
                "sink is OlapTableSink, but table type is " + table.getType());
        OlapTableSink olapTableSink = (OlapTableSink) sink;
        boolean isStrictMode = ctx.getSessionVariable().getEnableInsertStrict()
                && isPartialUpdate
                && isFromInsert;
        try {
            // TODO refactor this to avoid call legacy planner's function
            olapTableSink.init(ctx.queryId(), txnId, database.getId(),
                    ctx.getExecTimeout(),
                    ctx.getSessionVariable().getSendBatchParallelism(),
                    false,
                    isStrictMode);
            olapTableSink.complete(new Analyzer(Env.getCurrentEnv(), ctx));
            if (!allowAutoPartition) {
                olapTableSink.setAutoPartition(false);
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        TransactionState state = Env.getCurrentGlobalTransactionMgr().getTransactionState(database.getId(), txnId);
        if (state == null) {
            throw new AnalysisException("txn does not exist: " + txnId);
        }
        state.addTableIndexes((OlapTable) table);
        if (isPartialUpdate) {
            state.setSchemaForPartialUpdate((OlapTable) table);
        }
    }

    /**
     * execute insert txn for insert into select command.
     */
    public void executeSingleInsertTransaction(StmtExecutor executor, long jobId) {
        String queryId = DebugUtil.printId(ctx.queryId());
        LOG.info("start insert [{}] with query id {} and txn id {}", labelName, queryId, txnId);
        Throwable throwable = null;

        try {
            coordinator.setLoadZeroTolerance(ctx.getSessionVariable().getEnableInsertStrict());
            coordinator.setQueryType(TQueryType.LOAD);
            executor.getProfile().setExecutionProfile(coordinator.getExecutionProfile());
            QeProcessorImpl.INSTANCE.registerQuery(ctx.queryId(), coordinator);
            coordinator.exec();
            int execTimeout = ctx.getExecTimeout();
            LOG.debug("insert [{}] with query id {} execution timeout is {}", labelName, queryId, execTimeout);
            boolean notTimeout = coordinator.join(execTimeout);
            if (!coordinator.isDone()) {
                coordinator.cancel();
                if (notTimeout) {
                    errMsg = coordinator.getExecStatus().getErrorMsg();
                    ErrorReport.reportDdlException("there exists unhealthy backend. "
                            + errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_EXECUTE_TIMEOUT);
                }
            }
            if (!coordinator.getExecStatus().ok()) {
                errMsg = coordinator.getExecStatus().getErrorMsg();
                LOG.warn("insert [{}] with query id {} failed, {}", labelName, queryId, errMsg);
                ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
            }
            LOG.debug("insert [{}] with query id {} delta files is {}",
                    labelName, queryId, coordinator.getDeltaUrls());
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
                    return;
                }
            }

            if (table.getType() != TableType.OLAP && table.getType() != TableType.MATERIALIZED_VIEW) {
                // no need to add load job.
                // MySQL table is already being inserted.
                ctx.getState().setOk(loadedRows, filteredRows, null);
                return;
            }

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
                    database, Lists.newArrayList(table),
                    txnId,
                    TabletCommitInfo.fromThrift(coordinator.getCommitInfos()),
                    ctx.getSessionVariable().getInsertVisibleTimeoutMs())) {
                txnStatus = TransactionStatus.VISIBLE;
            } else {
                txnStatus = TransactionStatus.COMMITTED;
            }

        } catch (Throwable t) {
            // if any throwable being thrown during insert operation, first we should abort this txn
            LOG.warn("insert [{}] with query id {} failed", labelName, queryId, t);
            if (txnId != INVALID_TXN_ID) {
                try {
                    Env.getCurrentGlobalTransactionMgr().abortTransaction(
                            database.getId(), txnId,
                            t.getMessage() == null ? "unknown reason" : t.getMessage());
                } catch (Exception abortTxnException) {
                    // just print a log if abort txn failed. This failure do not need to pass to user.
                    // user only concern abort how txn failed.
                    LOG.warn("insert [{}] with query id {} abort txn {} failed",
                            labelName, queryId, txnId, abortTxnException);
                }
            }

            if (!Config.using_old_load_usage_pattern) {
                // if not using old load usage pattern, error will be returned directly to user
                StringBuilder sb = new StringBuilder(t.getMessage());
                if (!Strings.isNullOrEmpty(coordinator.getTrackingUrl())) {
                    sb.append(". url: ").append(coordinator.getTrackingUrl());
                }
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
                return;
            }

            /*
             * If config 'using_old_load_usage_pattern' is true.
             * Doris will return a label to user, and user can use this label to check load job's status,
             * which exactly like the old insert stmt usage pattern.
             */
            throwable = t;
        } finally {
            executor.updateProfile(true);
            QeProcessorImpl.INSTANCE.unregisterQuery(ctx.queryId());
        }

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
                                etlJobType, createAt, throwable == null ? "" : throwable.getMessage(),
                                coordinator.getTrackingUrl(), userIdentity, jobId);
            }
        } catch (MetaNotFoundException e) {
            LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
            errMsg = "Record info of insert load with error " + e.getMessage();
        }

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

    /**
     * execute insert values in transaction.
     */
    public static void executeBatchInsertTransaction(ConnectContext ctx, String dbName, String tableName,
            List<Column> columns, List<List<NamedExpression>> constantExprsList) {
        if (ctx.isTxnIniting()) { // first time, begin txn
            beginBatchInsertTransaction(ctx, dbName, tableName, columns);
        }
        if (!ctx.getTxnEntry().getTxnConf().getDb().equals(dbName)
                || !ctx.getTxnEntry().getTxnConf().getTbl().equals(tableName)) {
            throw new AnalysisException("Only one table can be inserted in one transaction.");
        }

        TransactionEntry txnEntry = ctx.getTxnEntry();
        int effectRows = 0;
        for (List<NamedExpression> row : constantExprsList) {
            ++effectRows;
            InternalService.PDataRow data = getRowStringValue(row);
            if (data == null) {
                continue;
            }
            List<InternalService.PDataRow> dataToSend = txnEntry.getDataToSend();
            dataToSend.add(data);
            if (dataToSend.size() >= StmtExecutor.MAX_DATA_TO_SEND_FOR_TXN) {
                // send data
                InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
                try {
                    executor.sendData();
                } catch (Exception e) {
                    throw new AnalysisException("send data to be failed, because " + e.getMessage(), e);
                }
            }
        }
        txnEntry.setRowsInTransaction(txnEntry.getRowsInTransaction() + effectRows);

        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        String sb = "{'label':'" + ctx.getTxnEntry().getLabel()
                + "', 'status':'" + TransactionStatus.PREPARE.name()
                + "', 'txnId':'" + ctx.getTxnEntry().getTxnConf().getTxnId() + "'"
                + "}";

        ctx.getState().setOk(effectRows, 0, sb);
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(
                ctx.getTxnEntry().getTxnConf().getTxnId(),
                ctx.getTxnEntry().getLabel(),
                dbName,
                tableName,
                TransactionStatus.PREPARE,
                effectRows,
                0);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows(effectRows);
    }

    private static InternalService.PDataRow getRowStringValue(List<NamedExpression> cols) {
        if (cols.isEmpty()) {
            return null;
        }
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (Expression expr : cols) {
            while (expr instanceof Alias || expr instanceof Cast) {
                expr = expr.child(0);
            }
            if (!(expr instanceof Literal)) {
                throw new AnalysisException(
                        "do not support non-literal expr in transactional insert operation: " + expr.toSql());
            }
            if (expr instanceof NullLiteral) {
                row.addColBuilder().setValue(StmtExecutor.NULL_VALUE_FOR_LOAD);
            } else if (expr instanceof ArrayLiteral) {
                row.addColBuilder().setValue(String.format("\"%s\"",
                        ((ArrayLiteral) expr).toLegacyLiteral().getStringValueForArray()));
            } else {
                row.addColBuilder().setValue(String.format("\"%s\"",
                        ((Literal) expr).toLegacyLiteral().getStringValue()));
            }
        }
        return row.build();
    }

    private static void beginBatchInsertTransaction(ConnectContext ctx,
            String dbName, String tblName, List<Column> columns) {
        TransactionEntry txnEntry = ctx.getTxnEntry();
        TTxnParams txnConf = txnEntry.getTxnConf();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        long timeoutSecond = ctx.getExecTimeout();
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(dbName, s -> new AnalysisException("database is invalid for dbName: " + s));
        Table tblObj = dbObj.getTableOrException(tblName, s -> new AnalysisException("table is invalid: " + s));
        txnConf.setDbId(dbObj.getId()).setTbl(tblName).setDb(dbName);
        txnEntry.setTable(tblObj);
        txnEntry.setDb(dbObj);
        String label = txnEntry.getLabel();
        try {
            long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    txnConf.getDbId(), Lists.newArrayList(tblObj.getId()),
                    label, new TransactionState.TxnCoordinator(
                            TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    sourceType, timeoutSecond);
            txnConf.setTxnId(txnId);
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            txnConf.setToken(token);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        long maxExecMemByte = sessionVariable.getMaxExecMemByte();
        String timeZone = sessionVariable.getTimeZone();
        int sendBatchParallelism = sessionVariable.getSendBatchParallelism();
        request.setTxnId(txnConf.getTxnId())
                .setDb(txnConf.getDb())
                .setTbl(txnConf.getTbl())
                .setColumns(columns.stream()
                        .map(Column::getName)
                        .map(n -> n.replace("`", "``"))
                        .map(n -> "`" + n + "`")
                        .collect(Collectors.joining(",")))
                .setFileType(TFileType.FILE_STREAM)
                .setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND)
                .setThriftRpcTimeoutMs(5000)
                .setLoadId(ctx.queryId())
                .setExecMemLimit(maxExecMemByte)
                .setTimeout((int) timeoutSecond)
                .setTimezone(timeZone)
                .setSendBatchParallelism(sendBatchParallelism)
                .setTrimDoubleQuotes(true)
                .setSequenceCol(columns.stream()
                        .filter(c -> Column.SEQUENCE_COL.equalsIgnoreCase(c.getName()))
                        .map(Column::getName)
                        .findFirst()
                        .orElse(null));

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        try {
            executor.beginTransaction(request);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    /**
     * normalize plan to let it could be process correctly by nereids
     */
    public static Plan normalizePlan(Plan plan, TableIf table) {
        UnboundTableSink<? extends Plan> unboundTableSink = (UnboundTableSink<? extends Plan>) plan;

        if (table instanceof OlapTable && ((OlapTable) table).getKeysType() == KeysType.UNIQUE_KEYS
                && unboundTableSink.isPartialUpdate()) {
            // check the necessary conditions for partial updates
            OlapTable olapTable = (OlapTable) table;
            if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                throw new AnalysisException("Partial update is only allowed on "
                        + "unique table with merge-on-write enabled.");
            }
            if (unboundTableSink.getDMLCommandType() == DMLCommandType.INSERT) {
                if (unboundTableSink.getColNames().isEmpty()) {
                    throw new AnalysisException("You must explicitly specify the columns to be updated when "
                            + "updating partial columns using the INSERT statement.");
                }
                for (Column col : olapTable.getFullSchema()) {
                    Optional<String> insertCol = unboundTableSink.getColNames().stream()
                            .filter(c -> c.equalsIgnoreCase(col.getName())).findFirst();
                    if (col.isKey() && !insertCol.isPresent()) {
                        throw new AnalysisException("Partial update should include all key columns, missing: "
                                + col.getName());
                    }
                }
            }
        }

        Plan query = unboundTableSink.child();
        if (!(query instanceof LogicalInlineTable)) {
            return plan;
        }
        LogicalInlineTable logicalInlineTable = (LogicalInlineTable) query;
        ImmutableList.Builder<LogicalPlan> oneRowRelationBuilder = ImmutableList.builder();
        List<Column> columns = table.getBaseSchema(false);

        for (List<NamedExpression> values : logicalInlineTable.getConstantExprsList()) {
            ImmutableList.Builder<NamedExpression> constantExprs = ImmutableList.builder();
            if (values.isEmpty()) {
                if (CollectionUtils.isNotEmpty(unboundTableSink.getColNames())) {
                    throw new AnalysisException("value list should not be empty if columns are specified");
                }
                for (Column column : columns) {
                    constantExprs.add(generateDefaultExpression(column));
                }
            } else {
                if (CollectionUtils.isNotEmpty(unboundTableSink.getColNames())) {
                    if (values.size() != unboundTableSink.getColNames().size()) {
                        throw new AnalysisException("Column count doesn't match value count");
                    }
                    for (int i = 0; i < values.size(); i++) {
                        if (values.get(i) instanceof DefaultValueSlot) {
                            boolean hasDefaultValue = false;
                            for (Column column : columns) {
                                if (unboundTableSink.getColNames().get(i).equalsIgnoreCase(column.getName())) {
                                    constantExprs.add(generateDefaultExpression(column));
                                    hasDefaultValue = true;
                                }
                            }
                            if (!hasDefaultValue) {
                                throw new AnalysisException("Unknown column '"
                                        + unboundTableSink.getColNames().get(i) + "' in target table.");
                            }
                        } else {
                            constantExprs.add(values.get(i));
                        }
                    }
                } else {
                    if (values.size() != columns.size()) {
                        throw new AnalysisException("Column count doesn't match value count");
                    }
                    for (int i = 0; i < columns.size(); i++) {
                        if (values.get(i) instanceof DefaultValueSlot) {
                            constantExprs.add(generateDefaultExpression(columns.get(i)));
                        } else {
                            constantExprs.add(values.get(i));
                        }
                    }
                }
            }
            oneRowRelationBuilder.add(new UnboundOneRowRelation(
                    StatementScopeIdGenerator.newRelationId(), constantExprs.build()));
        }
        List<LogicalPlan> oneRowRelations = oneRowRelationBuilder.build();
        if (oneRowRelations.size() == 1) {
            return plan.withChildren(oneRowRelations.get(0));
        } else {
            return plan.withChildren(
                    LogicalPlanBuilder.reduceToLogicalPlanTree(0, oneRowRelations.size() - 1,
                            oneRowRelations, Qualifier.ALL));
        }
    }

    /**
     * get target table from names.
     */
    public static TableIf getTargetTable(Plan plan, ConnectContext ctx) {
        if (!(plan instanceof UnboundTableSink)) {
            throw new AnalysisException("the root of plan should be UnboundTableSink"
                    + " but it is " + plan.getType());
        }
        UnboundTableSink<? extends Plan> unboundTableSink = (UnboundTableSink<? extends Plan>) plan;
        List<String> tableQualifier = RelationUtil.getQualifierName(ctx, unboundTableSink.getNameParts());
        return RelationUtil.getDbAndTable(tableQualifier, ctx.getEnv()).second;
    }

    private static NamedExpression generateDefaultExpression(Column column) {
        try {
            if (column.getDefaultValue() == null) {
                throw new AnalysisException("Column has no default value, column=" + column.getName());
            }
            if (column.getDefaultValueExpr() != null) {
                Expression defualtValueExpression = new NereidsParser().parseExpression(
                        column.getDefaultValueExpr().toSqlWithoutTbl());
                if (!(defualtValueExpression instanceof UnboundAlias)) {
                    defualtValueExpression = new UnboundAlias(defualtValueExpression);
                }
                return (NamedExpression) defualtValueExpression;
            } else {
                return new Alias(Literal.of(column.getDefaultValue())
                        .checkedCastTo(DataType.fromCatalogType(column.getType())),
                        column.getName());
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }
}
