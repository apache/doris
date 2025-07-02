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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.nereids.load.NereidsStreamLoadTask;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertRequest;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

// Used to generate a plan fragment for a group commit
// we only support OlapTable now.
public class GroupCommitPlanner {
    private static final Logger LOG = LogManager.getLogger(GroupCommitPlanner.class);
    public static final String SCHEMA_CHANGE = " is blocked on schema change";
    private static final int MAX_RETRY = 3;

    private Database db;
    private OlapTable table;
    public int baseSchemaVersion;
    private int targetColumnSize;
    private TUniqueId loadId;
    private long backendId;
    private ByteString execPlanFragmentParamsBytes;

    public GroupCommitPlanner(Database db, OlapTable table, List<String> targetColumnNames, TUniqueId queryId,
            String groupCommit)
            throws UserException, TException {
        this.db = db;
        this.table = table;
        this.baseSchemaVersion = table.getBaseSchemaVersion();
        if (Env.getCurrentEnv().getGroupCommitManager().isBlock(this.table.getId())) {
            String msg = "insert table " + this.table.getId() + SCHEMA_CHANGE;
            LOG.info(msg);
            throw new DdlException(msg);
        }
        TStreamLoadPutRequest streamLoadPutRequest = new TStreamLoadPutRequest();
        if (targetColumnNames != null) {
            streamLoadPutRequest.setColumns("`" + String.join("`,`", targetColumnNames) + "`");
            if (targetColumnNames.stream().anyMatch(col -> col.equalsIgnoreCase(Column.SEQUENCE_COL))) {
                streamLoadPutRequest.setSequenceCol(Column.SEQUENCE_COL);
            }
        }
        streamLoadPutRequest
                .setDb(db.getFullName())
                .setMaxFilterRatio(ConnectContext.get().getSessionVariable().enableInsertStrict ? 0
                        : ConnectContext.get().getSessionVariable().insertMaxFilterRatio)
                .setTbl(table.getName())
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND).setThriftRpcTimeoutMs(5000).setLoadId(queryId)
                .setTrimDoubleQuotes(true).setGroupCommitMode(groupCommit)
                .setStrictMode(ConnectContext.get().getSessionVariable().enableInsertStrict);
        NereidsStreamLoadTask streamLoadTask = NereidsStreamLoadTask.fromTStreamLoadPutRequest(streamLoadPutRequest);
        NereidsStreamLoadPlanner planner = new NereidsStreamLoadPlanner(db, table, streamLoadTask);
        // Will using load id as query id in fragment
        // TODO support pipeline
        TPipelineFragmentParams tRequest = planner.plan(streamLoadTask.getId());
        for (Map.Entry<Integer, List<TScanRangeParams>> entry : tRequest.local_params.get(0)
                .per_node_scan_ranges.entrySet()) {
            for (TScanRangeParams scanRangeParams : entry.getValue()) {
                scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setFormatType(
                        TFileFormatType.FORMAT_PROTO);
                scanRangeParams.scan_range.ext_scan_range.file_scan_range.params.setCompressType(
                        TFileCompressType.PLAIN);
            }
        }
        List<TScanRangeParams> scanRangeParams = tRequest.local_params.get(0).per_node_scan_ranges.values().stream()
                .flatMap(Collection::stream).collect(Collectors.toList());
        Preconditions.checkState(scanRangeParams.size() == 1);
        loadId = queryId;
        // see BackendServiceProxy#execPlanFragmentsAsync
        TPipelineFragmentParamsList paramsList = new TPipelineFragmentParamsList();
        paramsList.addToParamsList(tRequest);
        execPlanFragmentParamsBytes = ByteString.copyFrom(new TSerializer().serialize(paramsList));
    }

    public PGroupCommitInsertResponse executeGroupCommitInsert(ConnectContext ctx,
            List<InternalService.PDataRow> rows)
            throws DdlException, RpcException, ExecutionException, InterruptedException, LoadException {
        Backend backend = Env.getCurrentEnv().getGroupCommitManager().selectBackendForGroupCommit(table.getId(), ctx);
        backendId = backend.getId();
        PGroupCommitInsertRequest request = PGroupCommitInsertRequest.newBuilder()
                .setExecPlanFragmentRequest(InternalService.PExecPlanFragmentRequest.newBuilder()
                        .setRequest(execPlanFragmentParamsBytes)
                        .setCompact(false).setVersion(InternalService.PFragmentRequestVersion.VERSION_3).build())
                .setLoadId(Types.PUniqueId.newBuilder().setHi(loadId.hi).setLo(loadId.lo)
                .build()).addAllData(rows)
                .build();
        LOG.info("query_id={}, rows={}, reuse group commit query_id={} ", DebugUtil.printId(ctx.queryId()),
                rows.size(), DebugUtil.printId(loadId));
        Future<PGroupCommitInsertResponse> future = BackendServiceProxy.getInstance()
                .groupCommitInsert(new TNetworkAddress(backend.getHost(), backend.getBrpcPort()), request);
        return future.get();
    }

    public long getBackendId() {
        return backendId;
    }

    private static InternalService.PDataRow getOneRow(List<Expr> row) throws UserException {
        InternalService.PDataRow data = StmtExecutor.getRowStringValue(row, FormatOptions.getDefault());
        if (LOG.isDebugEnabled()) {
            LOG.debug("add row: [{}]", data.getColList().stream().map(c -> c.getValue())
                    .collect(Collectors.joining(",")));
        }
        return data;
    }

    private static List<InternalService.PDataRow> getRows(int targetColumnSize, List<Expr> rows) throws UserException {
        List<InternalService.PDataRow> data = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += targetColumnSize) {
            List<Expr> row = rows.subList(i, Math.min(i + targetColumnSize, rows.size()));
            data.add(getOneRow(row));
        }
        return data;
    }

    // prepare command
    public static void executeGroupCommitInsert(ConnectContext ctx, PreparedStatementContext preparedStmtCtx,
            StatementContext statementContext) throws Exception {
        PrepareCommand prepareCommand = preparedStmtCtx.command;
        InsertIntoTableCommand command = (InsertIntoTableCommand) (prepareCommand.getLogicalPlan());
        OlapTable table = (OlapTable) command.getTable(ctx);
        for (int retry = 0; retry < MAX_RETRY; retry++) {
            if (Env.getCurrentEnv().getGroupCommitManager().isBlock(table.getId())) {
                String msg = "insert table " + table.getId() + SCHEMA_CHANGE;
                LOG.info(msg);
                throw new DdlException(msg);
            }
            boolean reuse = false;
            GroupCommitPlanner groupCommitPlanner;
            if (preparedStmtCtx.groupCommitPlanner.isPresent()
                    && table.getId() == preparedStmtCtx.groupCommitPlanner.get().table.getId()
                    && table.getBaseSchemaVersion() == preparedStmtCtx.groupCommitPlanner.get().baseSchemaVersion) {
                groupCommitPlanner = preparedStmtCtx.groupCommitPlanner.get();
                reuse = true;
            } else {
                // call nereids planner to check to sql
                command.initPlan(ctx, new StmtExecutor(new ConnectContext(), ""), false);
                List<String> targetColumnNames = command.getTargetColumns();
                groupCommitPlanner = EnvFactory.getInstance()
                        .createGroupCommitPlanner((Database) table.getDatabase(), table,
                                targetColumnNames, ctx.queryId(),
                                ConnectContext.get().getSessionVariable().getGroupCommit());
                // TODO use planner column size
                groupCommitPlanner.targetColumnSize = targetColumnNames == null ? table.getBaseSchema().size() :
                        targetColumnNames.size();
                preparedStmtCtx.groupCommitPlanner = Optional.of(groupCommitPlanner);
            }
            if (statementContext.getIdToPlaceholderRealExpr().size() % groupCommitPlanner.targetColumnSize != 0) {
                throw new DdlException("Column size: " + statementContext.getIdToPlaceholderRealExpr().size()
                        + " does not match with target column size: " + groupCommitPlanner.targetColumnSize);
            }
            List<Expr> valueExprs = statementContext.getIdToPlaceholderRealExpr().values().stream()
                    .map(v -> ((Literal) v).toLegacyLiteral()).collect(Collectors.toList());
            List<InternalService.PDataRow> rows = getRows(groupCommitPlanner.targetColumnSize, valueExprs);
            PGroupCommitInsertResponse response = groupCommitPlanner.executeGroupCommitInsert(ctx, rows);
            Pair<Boolean, Boolean> needRetryAndReplan = groupCommitPlanner.handleResponse(ctx, retry + 1 < MAX_RETRY,
                    reuse, response);
            if (needRetryAndReplan.first) {
                if (needRetryAndReplan.second) {
                    preparedStmtCtx.groupCommitPlanner = Optional.empty();
                }
            } else {
                break;
            }
        }
    }

    // return <need_retry, need_replan>
    private Pair<Boolean, Boolean> handleResponse(ConnectContext ctx, boolean canRetry, boolean reuse,
            PGroupCommitInsertResponse response) throws DdlException {
        TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
        ProtocolStringList errorMsgsList = response.getStatus().getErrorMsgsList();
        if (canRetry && code != TStatusCode.OK && !errorMsgsList.isEmpty()) {
            if (errorMsgsList.get(0).contains("schema version not match")) {
                LOG.info("group commit insert failed. query: {}, db: {}, table: {}, schema version: {}, "
                                + "backend: {}, status: {}", DebugUtil.printId(ctx.queryId()), db.getId(),
                        table.getId(), baseSchemaVersion, backendId, response.getStatus());
                return Pair.of(true, true);
            } else if (errorMsgsList.get(0).contains("can not get a block queue")) {
                return Pair.of(true, false);
            }
        }
        if (code != TStatusCode.OK) {
            handleInsertFailed(ctx, response);
        } else {
            setReturnInfo(ctx, reuse, response);
        }
        return Pair.of(false, false);
    }

    private void handleInsertFailed(ConnectContext ctx, PGroupCommitInsertResponse response) throws DdlException {
        String errMsg = "group commit insert failed. db: " + db.getId() + ", table: " + table.getId()
                + ", query: " + DebugUtil.printId(ctx.queryId()) + ", backend: " + backendId
                + ", status: " + response.getStatus();
        if (response.hasErrorUrl()) {
            errMsg += ", error url: " + response.getErrorUrl();
        }
        ErrorReport.reportDdlException(errMsg.replaceAll("%", "%%"), ErrorCode.ERR_FAILED_WHEN_INSERT);
    }

    private void setReturnInfo(ConnectContext ctx, boolean reuse, PGroupCommitInsertResponse response) {
        String labelName = response.getLabel();
        TransactionStatus txnStatus = TransactionStatus.PREPARE;
        long txnId = response.getTxnId();
        long loadedRows = response.getLoadedRows();
        long filteredRows = (int) response.getFilteredRows();
        String errorUrl = response.getErrorUrl();
        // the same as {@OlapInsertExecutor#setReturnInfo}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(labelName).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(txnId).append("'");
        if (table.getType() == TableType.MATERIALIZED_VIEW) {
            sb.append("', 'rows':'").append(loadedRows).append("'");
        }
        /*if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }*/
        if (!Strings.isNullOrEmpty(errorUrl)) {
            sb.append(", 'err_url':'").append(errorUrl).append("'");
        }
        sb.append(", 'query_id':'").append(DebugUtil.printId(ctx.queryId())).append("'");
        if (reuse) {
            sb.append(", 'reuse_group_commit_plan':'").append(true).append("'");
        }
        sb.append("}");

        ctx.getState().setOk(loadedRows, (int) filteredRows, sb.toString());
        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        ctx.setOrUpdateInsertResult(txnId, labelName, db.getFullName(), table.getName(),
                txnStatus, loadedRows, (int) filteredRows);
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) loadedRows);
    }
}
