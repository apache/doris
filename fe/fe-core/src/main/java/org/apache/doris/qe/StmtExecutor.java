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

package org.apache.doris.qe;

import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AnalyzeDBStmt;
import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.AnalyzeTblStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.ExecuteStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertOverwriteTableStmt;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.LoadType;
import org.apache.doris.analysis.LockTablesStmt;
import org.apache.doris.analysis.NativeInsertStmt;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.PrepareStmt;
import org.apache.doris.analysis.PrepareStmt.PreparedType;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.ReplaceTableClause;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.SetVar.SetVarType;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtRewriter;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TransactionBeginStmt;
import org.apache.doris.analysis.TransactionCommitStmt;
import org.apache.doris.analysis.TransactionRollbackStmt;
import org.apache.doris.analysis.TransactionStmt;
import org.apache.doris.analysis.UnifiedLoadStmt;
import org.apache.doris.analysis.UnlockTablesStmt;
import org.apache.doris.analysis.UnsetVariableStmt;
import org.apache.doris.analysis.UnsupportedStmt;
import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuditLog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.NereidsSqlCacheManager;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileScanNode;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.datasource.tvf.source.TVFScanNode;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.loadv2.LoadManagerAdapter;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlOkPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.ProxyMysqlChannel;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlanProcess;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.DoNotFallbackException;
import org.apache.doris.nereids.exceptions.MustFallbackException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.Forward;
import org.apache.doris.nereids.trees.plans.commands.NotAllowFallback;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.commands.UnsupportedCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.GroupCommitScanNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.proto.InternalService.POutfileWriteSuccessRequest;
import org.apache.doris.proto.InternalService.POutfileWriteSuccessResult;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.qe.cache.SqlCache;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.InternalQueryBuffer;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TResultFileSink;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

// Do one COM_QUERY process.
// first: Parse receive byte array to statement struct.
// second: Do handle function for statement.
public class StmtExecutor {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    public static final int MAX_DATA_TO_SEND_FOR_TXN = 100;
    public static final String NULL_VALUE_FOR_LOAD = "\\N";
    private ConnectContext context;
    private final StatementContext statementContext;
    private MysqlSerializer serializer;
    private OriginStatement originStmt;
    private StatementBase parsedStmt;
    private Analyzer analyzer;
    private ProfileType profileType = ProfileType.QUERY;

    @Setter
    private volatile Coordinator coord = null;
    private MasterOpExecutor masterOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    private Planner planner;
    private boolean isProxy;
    private ShowResultSet proxyShowResultSet = null;
    private Data.PQueryStatistics.Builder statisticsForAuditLog;
    private boolean isCached;
    private String stmtName;
    private StatementBase prepareStmt = null;
    private String mysqlLoadId;
    // Distinguish from prepare and execute command
    private boolean isExecuteStmt = false;
    // Handle selects that fe can do without be
    private boolean isHandleQueryInFe = false;
    // The profile of this execution
    private final Profile profile;
    private Boolean isForwardedToMaster = null;

    private ExecuteStmt execStmt;
    PrepareStmtContext preparedStmtCtx = null;

    // The result schema if "dry_run_query" is true.
    // Only one column to indicate the real return row numbers.
    private static final CommonResultSetMetaData DRY_RUN_QUERY_METADATA = new CommonResultSetMetaData(
            Lists.newArrayList(new Column("ReturnedRows", PrimitiveType.STRING)));

    // this constructor is mainly for proxy
    public StmtExecutor(ConnectContext context, OriginStatement originStmt, boolean isProxy) {
        Preconditions.checkState(context.getConnectType().equals(ConnectType.MYSQL));
        this.context = context;
        this.context.setExecutor(this);
        this.originStmt = originStmt;
        this.serializer = context.getMysqlChannel().getSerializer();
        this.isProxy = isProxy;
        this.statementContext = new StatementContext(context, originStmt);
        this.context.setStatementContext(statementContext);
        this.profile = new Profile("Query", this.context.getSessionVariable().enableProfile,
                this.context.getSessionVariable().profileLevel,
                this.context.getSessionVariable().getEnablePipelineXEngine());
    }

    // for test
    public StmtExecutor(ConnectContext context, String stmt) {
        this(context, new OriginStatement(stmt, 0), false);
        this.stmtName = stmt;
    }

    // constructor for receiving parsed stmt from connect processor
    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this.context = ctx;
        this.parsedStmt = parsedStmt;
        this.originStmt = parsedStmt.getOrigStmt();
        if (context.getConnectType() == ConnectType.MYSQL) {
            this.serializer = context.getMysqlChannel().getSerializer();
        } else {
            this.serializer = null;
        }
        this.isProxy = false;
        if (parsedStmt instanceof LogicalPlanAdapter) {
            this.statementContext = ((LogicalPlanAdapter) parsedStmt).getStatementContext();
            this.statementContext.setConnectContext(ctx);
            this.statementContext.setOriginStatement(originStmt);
            this.statementContext.setParsedStatement(parsedStmt);
        } else {
            this.statementContext = new StatementContext(ctx, originStmt);
            this.statementContext.setParsedStatement(parsedStmt);
        }
        this.context.setStatementContext(statementContext);
        this.profile = new Profile("Query", context.getSessionVariable().enableProfile(),
                context.getSessionVariable().profileLevel, context.getSessionVariable().getEnablePipelineXEngine());
    }

    public boolean isProxy() {
        return isProxy;
    }

    public static InternalService.PDataRow getRowStringValue(List<Expr> cols,
            FormatOptions options) throws UserException {
        if (cols.isEmpty()) {
            return null;
        }
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (Expr expr : cols) {
            if (expr instanceof PlaceHolderExpr) {
                expr = ((PlaceHolderExpr) expr).getLiteral();
            }
            if (!expr.isLiteralOrCastExpr()) {
                throw new UserException(
                        "do not support non-literal expr in transactional insert operation: " + expr.toSql());
            }
            if (expr instanceof NullLiteral) {
                row.addColBuilder().setValue(NULL_VALUE_FOR_LOAD);
            } else if (expr instanceof ArrayLiteral) {
                row.addColBuilder().setValue("\"" + expr.getStringValueForStreamLoad(options) + "\"");
            } else {
                String stringValue = expr.getStringValueForStreamLoad(options);
                if (stringValue.equals(NULL_VALUE_FOR_LOAD) || stringValue.startsWith("\"") || stringValue.endsWith(
                        "\"")) {
                    row.addColBuilder().setValue("\"" + stringValue + "\"");
                } else {
                    row.addColBuilder().setValue(stringValue);
                }
            }
        }
        return row.build();
    }

    private Map<String, String> getSummaryInfo(boolean isFinished) {
        long currentTimestamp = System.currentTimeMillis();
        SummaryBuilder builder = new SummaryBuilder();
        builder.profileId(DebugUtil.printId(context.queryId()));
        if (Version.DORIS_BUILD_VERSION_MAJOR == 0) {
            builder.dorisVersion(Version.DORIS_BUILD_SHORT_HASH);
        } else {
            builder.dorisVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
        }
        builder.taskType(profileType.name());
        builder.startTime(TimeUtils.longToTimeString(context.getStartTime()));
        if (isFinished) {
            builder.endTime(TimeUtils.longToTimeString(currentTimestamp));
            builder.totalTime(DebugUtil.getPrettyStringMs(currentTimestamp - context.getStartTime()));
        }
        builder.taskState(!isFinished && context.getState().getStateType().equals(MysqlStateType.OK) ? "RUNNING"
                : context.getState().toString());
        builder.user(context.getQualifiedUser());
        builder.defaultCatalog(context.getCurrentCatalog().getName());
        builder.defaultDb(context.getDatabase());
        builder.workloadGroup(context.getWorkloadGroupName());
        builder.sqlStatement(originStmt == null ? "" : originStmt.originStmt);
        builder.isCached(isCached ? "Yes" : "No");

        Map<String, Integer> beToInstancesNum = coord == null ? Maps.newTreeMap() : coord.getBeToInstancesNum();
        builder.totalInstancesNum(String.valueOf(beToInstancesNum.values().stream().reduce(0, Integer::sum)));
        builder.instancesNumPerBe(
                beToInstancesNum.entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue())
                        .collect(Collectors.joining(",")));
        builder.parallelFragmentExecInstance(String.valueOf(context.sessionVariable.getParallelExecInstanceNum()));
        builder.traceId(context.getSessionVariable().getTraceId());
        builder.isNereids(context.getState().isNereids ? "Yes" : "No");
        builder.isPipeline(context.getSessionVariable().getEnablePipelineEngine() ? "Yes" : "No");
        return builder.build();
    }

    public Planner planner() {
        return planner;
    }

    public void setPlanner(Planner planner) {
        this.planner = planner;
    }

    public boolean isForwardToMaster() {
        if (isForwardedToMaster == null) {
            isForwardedToMaster = shouldForwardToMaster();
        }
        return isForwardedToMaster;
    }

    private boolean shouldForwardToMaster() {
        if (Env.getCurrentEnv().isMaster()) {
            return false;
        }

        if (Config.enable_bdbje_debug_mode) {
            return false;
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if (isQuery() && !Env.getCurrentEnv().isMaster()
                && (!Env.getCurrentEnv().canRead() || debugForwardAllQueries() || Config.force_forward_all_queries)) {
            return true;
        }

        if (redirectStatus == null) {
            return false;
        } else {
            return redirectStatus.isForwardToMaster();
        }
    }

    private boolean debugForwardAllQueries() {
        DebugPoint debugPoint = DebugPointUtil.getDebugPoint("StmtExecutor.forward_all_queries");
        return debugPoint != null && debugPoint.param("forwardAllQueries", false);
    }

    public ByteBuffer getOutputPacket() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getOutputPacket();
        }
    }

    public ShowResultSet getProxyShowResultSet() {
        return proxyShowResultSet;
    }

    public ShowResultSet getShowResultSet() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getProxyResultSet();
        }
    }

    public String getProxyStatus() {
        if (masterOpExecutor == null) {
            return MysqlStateType.UNKNOWN.name();
        }
        return masterOpExecutor.getProxyStatus();
    }

    public int getProxyStatusCode() {
        if (masterOpExecutor == null) {
            return MysqlStateType.UNKNOWN.ordinal();
        }
        return masterOpExecutor.getProxyStatusCode();
    }

    public String getProxyErrMsg() {
        if (masterOpExecutor == null) {
            return MysqlStateType.UNKNOWN.name();
        }
        return masterOpExecutor.getProxyErrMsg();
    }

    public boolean isSyncLoadKindStmt() {
        if (parsedStmt == null) {
            return false;
        }
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            return logicalPlan instanceof InsertIntoTableCommand
                    || logicalPlan instanceof InsertOverwriteTableCommand
                    || (logicalPlan instanceof CreateTableCommand
                    && ((CreateTableCommand) logicalPlan).isCtasCommand())
                    || logicalPlan instanceof DeleteFromCommand;
        }
        return parsedStmt instanceof InsertStmt || parsedStmt instanceof InsertOverwriteTableStmt
                || parsedStmt instanceof CreateTableAsSelectStmt || parsedStmt instanceof DeleteStmt;
    }

    public boolean isAnalyzeStmt() {
        if (parsedStmt == null) {
            return false;
        }
        return parsedStmt instanceof AnalyzeStmt;
    }

    /**
     * Used for audit in ConnectProcessor.
     * <p>
     * TODO: There are three interface in StatementBase be called when doing audit:
     *      toDigest needAuditEncryption when parsedStmt is not a query
     *      and isValuesOrConstantSelect when parsedStmt is instance of InsertStmt.
     *      toDigest: is used to compute Statement fingerprint for blocking some queries
     *      needAuditEncryption: when this interface return true,
     *          log statement use toSql function instead of log original string
     *      isValuesOrConstantSelect: when this interface return true, original string is truncated at 1024
     *
     * @return parsed and analyzed statement for Stale planner.
     *         an unresolved LogicalPlan wrapped with a LogicalPlanAdapter for Nereids.
     */
    public StatementBase getParsedStmt() {
        return parsedStmt;
    }

    public boolean isHandleQueryInFe() {
        return isHandleQueryInFe;
    }

    // query with a random sql
    public void execute() throws Exception {
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        execute(queryId);
    }

    public boolean notAllowFallback(NereidsException e) {
        if (e.getException() instanceof DoNotFallbackException) {
            return true;
        }
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            return logicalPlan instanceof NotAllowFallback;
        }
        return false;
    }

    public void execute(TUniqueId queryId) throws Exception {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (context.getConnectType() == ConnectType.ARROW_FLIGHT_SQL) {
            context.setReturnResultFromLocal(true);
        }

        try {
            if (parsedStmt instanceof LogicalPlanAdapter
                    || (parsedStmt == null && sessionVariable.isEnableNereidsPlanner())) {
                try {
                    executeByNereids(queryId);
                } catch (NereidsException | ParseException e) {
                    if (context.getMinidump() != null && context.getMinidump().toString(4) != null) {
                        MinidumpUtils.saveMinidumpString(context.getMinidump(), DebugUtil.printId(context.queryId()));
                    }
                    // try to fall back to legacy planner
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("nereids cannot process statement\n{}\n because of {}",
                                originStmt.originStmt, e.getMessage(), e);
                    }
                    if (e instanceof NereidsException && notAllowFallback((NereidsException) e)) {
                        LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                        throw new AnalysisException(e.getMessage());
                    }
                    // FIXME: Force fallback for:
                    //  1. group commit because nereids does not support it (see the following `isGroupCommit` variable)
                    //  2. insert into command because some nereids cases fail (including case1)
                    //  Skip force fallback for:
                    //  1. Transaction insert because nereids support `insert into select` while legacy does not
                    //  2. Nereids support insert into external table while legacy does not
                    boolean isInsertCommand = parsedStmt != null
                            && parsedStmt instanceof LogicalPlanAdapter
                            && ((LogicalPlanAdapter) parsedStmt).getLogicalPlan() instanceof InsertIntoTableCommand;
                    /*boolean isGroupCommit = (Config.wait_internal_group_commit_finish
                            || context.sessionVariable.isEnableInsertGroupCommit()) && isInsertCommand;*/
                    boolean isExternalTableInsert = false;
                    if (isInsertCommand) {
                        isExternalTableInsert = ((InsertIntoTableCommand) ((LogicalPlanAdapter) parsedStmt)
                                .getLogicalPlan()).isExternalTableSink();
                    }
                    boolean forceFallback = isInsertCommand && !isExternalTableInsert && !context.isTxnModel();
                    if (e instanceof NereidsException
                            && !(((NereidsException) e).getException() instanceof MustFallbackException)
                            && !context.getSessionVariable().enableFallbackToOriginalPlanner
                            && !forceFallback) {
                        LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                        context.getState().setError(e.getMessage());
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("fall back to legacy planner on statement:\n{}", originStmt.originStmt);
                    }
                    parsedStmt = null;
                    planner = null;
                    isForwardedToMaster = null;
                    redirectStatus = null;
                    // Attention: currently exception from nereids does not mean an Exception to user terminal
                    // unless user does not allow fallback to lagency planner. But state of query
                    // has already been set to Error in this case, it will have some side effect on profile result
                    // and audit log. So we need to reset state to OK if query cancel be processd by lagency.
                    context.getState().reset();
                    context.getState().setNereids(false);
                    executeByLegacy(queryId);
                }
            } else {
                executeByLegacy(queryId);
            }
        } finally {
            // revert Session Value
            try {
                VariableMgr.revertSessionValue(sessionVariable);
                // origin value init
                sessionVariable.setIsSingleSetVar(false);
                sessionVariable.clearSessionOriginValue();
            } catch (DdlException e) {
                LOG.warn("failed to revert Session value. {}", context.getQueryIdentifier(), e);
                context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            }
        }
    }

    public void checkBlockRules() throws AnalysisException {
        checkBlockRulesByRegex(originStmt);
        checkBlockRulesByScan(planner);
    }

    public void checkBlockRulesByRegex(OriginStatement originStmt) throws AnalysisException {
        if (originStmt == null) {
            return;
        }
        Env.getCurrentEnv().getSqlBlockRuleMgr().matchSql(
                originStmt.originStmt, context.getSqlHash(), context.getQualifiedUser());
    }

    public void checkBlockRulesByScan(Planner planner) throws AnalysisException {
        if (planner == null) {
            return;
        }
        List<ScanNode> scanNodeList = planner.getScanNodes();
        for (ScanNode scanNode : scanNodeList) {
            if (scanNode instanceof OlapScanNode || scanNode instanceof FileScanNode) {
                Env.getCurrentEnv().getSqlBlockRuleMgr().checkLimitations(
                        scanNode.getSelectedPartitionNum(),
                        scanNode.getSelectedSplitNum(),
                        scanNode.getCardinality(),
                        context.getQualifiedUser());

            }
        }
    }

    private void executeByNereids(TUniqueId queryId) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Nereids start to execute query:\n {}", originStmt.originStmt);
        }
        context.setQueryId(queryId);
        context.setStartTime();
        profile.getSummaryProfile().setQueryBeginTime();
        List<List<String>> changedSessionVar = VariableMgr.dumpChangedVars(context.getSessionVariable());
        profile.setChangedSessionVar(DebugUtil.prettyPrintChangedSessionVar(changedSessionVar));
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        parseByNereids();
        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                "Nereids only process LogicalPlanAdapter, but parsedStmt is " + parsedStmt.getClass().getName());
        context.getState().setNereids(true);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            if (isForwardToMaster()) {
                throw new UserException("Forward master command is not supported for prepare statement");
            }
            logicalPlan = new PrepareCommand(String.valueOf(context.getStmtId()),
                    logicalPlan, statementContext.getPlaceholders(), originStmt);

        }
        // when we in transaction mode, we only support insert into command and transaction command
        if (context.isTxnModel()) {
            if (!(logicalPlan instanceof BatchInsertIntoTableCommand
                    || logicalPlan instanceof InsertIntoTableCommand
                    || logicalPlan instanceof UnsupportedCommand)) {
                String errMsg = "This is in a transaction, only insert, commit, rollback is acceptable.";
                throw new NereidsException(errMsg, new AnalysisException(errMsg));
            }
        }
        if (logicalPlan instanceof Command) {
            if (logicalPlan instanceof Forward) {
                redirectStatus = ((Forward) logicalPlan).toRedirectStatus();
                if (isForwardToMaster()) {
                    // before forward to master, we also need to set profileType in this node
                    if (logicalPlan instanceof InsertIntoTableCommand) {
                        profileType = ProfileType.LOAD;
                    }
                    if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
                        throw new UserException("Forward master command is not supported for prepare statement");
                    }
                    if (isProxy) {
                        // This is already a stmt forwarded from other FE.
                        // If we goes here, means we can't find a valid Master FE(some error happens).
                        // To avoid endless forward, throw exception here.
                        throw new NereidsException(new UserException("The statement has been forwarded to master FE("
                                + Env.getCurrentEnv().getSelfNode().getHost() + ") and failed to execute"
                                + " because Master FE is not ready. You may need to check FE's status"));
                    }
                    forwardToMaster();
                    if (masterOpExecutor != null && masterOpExecutor.getQueryId() != null) {
                        context.setQueryId(masterOpExecutor.getQueryId());
                    }
                    return;
                }
            }

            // Query following createting table would throw table not exist error.
            // For example.
            // t1: client issues create table to master fe
            // t2: client issues query sql to observer fe, the query would fail due to not exist table in plan phase.
            // t3: observer fe receive editlog creating the table from the master fe
            syncJournalIfNeeded();
            try {
                ((Command) logicalPlan).run(context, this);
            } catch (MustFallbackException | DoNotFallbackException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Command({}) process failed.", originStmt.originStmt, e);
                }
                throw new NereidsException("Command(" + originStmt.originStmt + ") process failed.", e);
            } catch (QueryStateException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Command({}) process failed.", originStmt.originStmt, e);
                }
                context.setState(e.getQueryState());
                throw new NereidsException("Command(" + originStmt.originStmt + ") process failed",
                        new AnalysisException(e.getMessage(), e));
            } catch (UserException e) {
                // Return message to info client what happened.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Command({}) process failed.", originStmt.originStmt, e);
                }
                context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                throw new NereidsException("Command (" + originStmt.originStmt + ") process failed",
                        new AnalysisException(e.getMessage(), e));
            } catch (Exception e) {
                // Maybe our bug
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Command({}) process failed.", originStmt.originStmt, e);
                }
                context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
                throw new NereidsException("Command (" + originStmt.originStmt + ") process failed.",
                        new AnalysisException(e.getMessage() == null ? e.toString() : e.getMessage(), e));
            }
        } else {
            context.getState().setIsQuery(true);
            if (isForwardToMaster()) {
                // some times the follower's meta data is out of date.
                // so we need forward the query to master until the meta data is sync with master
                if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
                    throw new UserException("Forward master command is not supported for prepare statement");
                }
                if (isProxy) {
                    // This is already a stmt forwarded from other FE.
                    // If we goes here, means we can't find a valid Master FE(some error happens).
                    // To avoid endless forward, throw exception here.
                    throw new NereidsException(new UserException("The statement has been forwarded to master FE("
                            + Env.getCurrentEnv().getSelfNode().getHost() + ") and failed to execute"
                            + " because Master FE is not ready. You may need to check FE's status"));
                }
                redirectStatus = RedirectStatus.NO_FORWARD;
                forwardToMaster();
                if (masterOpExecutor != null && masterOpExecutor.getQueryId() != null) {
                    context.setQueryId(masterOpExecutor.getQueryId());
                }
                return;
            }
            // create plan
            // Query following createting table would throw table not exist error.
            // For example.
            // t1: client issues create table to master fe
            // t2: client issues query sql to observer fe, the query would fail due to not exist table in
            //     plan phase.
            // t3: observer fe receive editlog creating the table from the master fe
            syncJournalIfNeeded();
            planner = new NereidsPlanner(statementContext);
            if (context.getSessionVariable().isEnableMaterializedViewRewrite()) {
                statementContext.addPlannerHook(InitMaterializationContextHook.INSTANCE);
            }
            try {
                planner.plan(parsedStmt, context.getSessionVariable().toThrift());
                checkBlockRules();
            } catch (MustFallbackException | DoNotFallbackException e) {
                LOG.warn("Nereids plan query failed:\n{}", originStmt.originStmt, e);
                throw new NereidsException("Command(" + originStmt.originStmt + ") process failed.", e);
            } catch (Exception e) {
                LOG.warn("Nereids plan query failed:\n{}", originStmt.originStmt, e);
                throw new NereidsException(new AnalysisException(e.getMessage(), e));
            }
            profile.getSummaryProfile().setQueryPlanFinishTime();
            handleQueryWithRetry(queryId);
        }
    }

    private void parseByNereids() {
        if (parsedStmt != null) {
            return;
        }
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(originStmt.originStmt, context.getSessionVariable());
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        if (statements.size() <= originStmt.idx) {
            throw new ParseException("Nereids parse failed. Parser get " + statements.size() + " statements,"
                    + " but we need at least " + originStmt.idx + " statements.");
        }
        parsedStmt = statements.get(originStmt.idx);
    }

    public void finalizeQuery() {
        // The final profile report occurs after be returns the query data, and the profile cannot be
        // received after unregisterQuery(), causing the instance profile to be lost, so we should wait
        // for the profile before unregisterQuery().
        updateProfile(true);
        QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
    }

    private void handleQueryWithRetry(TUniqueId queryId) throws Exception {
        // queue query here
        int retryTime = Config.max_query_retry_time;
        for (int i = 0; i < retryTime; i++) {
            try {
                // reset query id for each retry
                if (i > 0) {
                    UUID uuid = UUID.randomUUID();
                    TUniqueId newQueryId = new TUniqueId(uuid.getMostSignificantBits(),
                            uuid.getLeastSignificantBits());
                    AuditLog.getQueryAudit().log("Query {} {} times with new query id: {}",
                            DebugUtil.printId(queryId), i, DebugUtil.printId(newQueryId));
                    context.setQueryId(newQueryId);
                }
                if (context.getConnectType() == ConnectType.ARROW_FLIGHT_SQL) {
                    context.setReturnResultFromLocal(false);
                }
                handleQueryStmt();
                break;
            } catch (RpcException e) {
                if (i == retryTime - 1) {
                    throw e;
                }
                if (context.getConnectType().equals(ConnectType.MYSQL) && !context.getMysqlChannel().isSend()) {
                    LOG.warn("retry {} times. stmt: {}", (i + 1), parsedStmt.getOrigStmt().originStmt);
                } else {
                    throw e;
                }
            } finally {
                if (context.isReturnResultFromLocal()) {
                    finalizeQuery();
                }
            }
        }
    }

    // Execute one statement with queryId
    // The queryId will be set in ConnectContext
    // This queryId will also be sent to master FE for exec master only query.
    // query id in ConnectContext will be changed when retry exec a query or master FE return a different one.
    // Exception:
    // IOException: talk with client failed.
    public void executeByLegacy(TUniqueId queryId) throws Exception {
        context.setStartTime();

        profile.getSummaryProfile().setQueryBeginTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        context.setQueryId(queryId);

        // set isQuery first otherwise this state will be lost if some error occurs
        if (parsedStmt instanceof QueryStmt) {
            context.getState().setIsQuery(true);
        }

        try {
            // parsedStmt maybe null here, we parse it. Or the predicate will not work.
            parseByLegacy();
            if (context.isTxnModel() && !(parsedStmt instanceof InsertStmt)
                    && !(parsedStmt instanceof TransactionStmt)) {
                throw new TException("This is in a transaction, only insert, commit, rollback is acceptable.");
            }
            // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
            analyzeVariablesInStmt();

            if (!context.isTxnModel()) {
                // analyze this query
                analyze(context.getSessionVariable().toThrift());

                if (isForwardToMaster()) {
                    // before forward to master, we also need to set profileType in this node
                    if (parsedStmt instanceof InsertStmt) {
                        InsertStmt insertStmt = (InsertStmt) parsedStmt;
                        if (!insertStmt.getQueryStmt().isExplain()) {
                            profileType = ProfileType.LOAD;
                        }
                    }
                    if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
                        throw new UserException("Forward master command is not supported for prepare statement");
                    }
                    if (isProxy) {
                        // This is already a stmt forwarded from other FE.
                        // If goes here, which means we can't find a valid Master FE(some error happens).
                        // To avoid endless forward, throw exception here.
                        throw new UserException("The statement has been forwarded to master FE("
                                + Env.getCurrentEnv().getSelfNode().getHost() + ") and failed to execute"
                                + " because Master FE is not ready. You may need to check FE's status");
                    }
                    forwardToMaster();
                    if (masterOpExecutor != null && masterOpExecutor.getQueryId() != null) {
                        context.setQueryId(masterOpExecutor.getQueryId());
                    }
                    return;
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("no need to transfer to Master. stmt: {}", context.getStmtId());
                    }
                }
            } else {
                // Query following createting table would throw table not exist error.
                // For example.
                // t1: client issues create table to master fe
                // t2: client issues query sql to observer fe, the query would fail due to not exist table
                //     in plan phase.
                // t3: observer fe receive editlog creating the table from the master fe
                syncJournalIfNeeded();
                analyzer = new Analyzer(context.getEnv(), context);
                parsedStmt.analyze(analyzer);
            }
            parsedStmt.checkPriv();
            if (prepareStmt instanceof PrepareStmt && !isExecuteStmt) {
                handlePrepareStmt();
                return;
            }

            // sql/sqlHash block
            checkBlockRules();
            if (parsedStmt instanceof QueryStmt) {
                handleQueryWithRetry(queryId);
            } else if (parsedStmt instanceof SetStmt) {
                handleSetStmt();
            } else if (parsedStmt instanceof UnsetVariableStmt) {
                handleUnsetVariableStmt();
            } else if (parsedStmt instanceof SwitchStmt) {
                handleSwitchStmt();
            } else if (parsedStmt instanceof UseStmt) {
                handleUseStmt();
            } else if (parsedStmt instanceof TransactionStmt) {
                handleTransactionStmt();
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                handleCtasStmt();
            } else if (parsedStmt instanceof InsertOverwriteTableStmt) {
                handleIotStmt();
            } else if (parsedStmt instanceof InsertStmt) { // Must ahead of DdlStmt because InsertStmt is its subclass
                InsertStmt insertStmt = (InsertStmt) parsedStmt;
                if (insertStmt.needLoadManager()) {
                    // TODO(tsy): will eventually try to handle native insert and external insert together
                    // add a branch for external load
                    handleExternalInsertStmt();
                } else {
                    try {
                        if (!insertStmt.getQueryStmt().isExplain()) {
                            profileType = ProfileType.LOAD;
                        }
                        handleInsertStmt();
                    } catch (Throwable t) {
                        LOG.warn("handle insert stmt fail: {}", t.getMessage());
                        // the transaction of this insert may already begin, we will abort it at outer finally block.
                        throw t;
                    }
                }
            } else if (parsedStmt instanceof LoadStmt) {
                handleLoadStmt();
            } else if (parsedStmt instanceof UpdateStmt) {
                handleUpdateStmt();
            } else if (parsedStmt instanceof DdlStmt) {
                if (parsedStmt instanceof DeleteStmt) {
                    if (((DeleteStmt) parsedStmt).getInsertStmt() != null) {
                        handleDeleteStmt();
                    } else {
                        Env.getCurrentEnv()
                                .getDeleteHandler()
                                .process((DeleteStmt) parsedStmt, context.getState());
                    }
                } else {
                    handleDdlStmt();
                }
            } else if (parsedStmt instanceof ShowStmt) {
                handleShow();
            } else if (parsedStmt instanceof KillStmt) {
                handleKill();
            } else if (parsedStmt instanceof ExportStmt) {
                handleExportStmt();
            } else if (parsedStmt instanceof UnlockTablesStmt) {
                handleUnlockTablesStmt();
            } else if (parsedStmt instanceof LockTablesStmt) {
                handleLockTablesStmt();
            } else if (parsedStmt instanceof UnsupportedStmt) {
                handleUnsupportedStmt();
            } else if (parsedStmt instanceof AnalyzeStmt) {
                handleAnalyzeStmt();
            } else {
                context.getState().setError(ErrorCode.ERR_NOT_SUPPORTED_YET, "Do not support this query.");
            }
        } catch (IOException e) {
            LOG.warn("execute IOException. {}", context.getQueryIdentifier(), e);
            // the exception happens when interact with client
            // this exception shows the connection is gone
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
            throw e;
        } catch (UserException e) {
            // analysis exception only print message, not print the stack
            LOG.warn("execute Exception. {}", context.getQueryIdentifier(), e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (JdbcClientException e) {
            LOG.warn("execute Exception. {}", context.getQueryIdentifier(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getMessage());
        } catch (Exception e) {
            LOG.warn("execute Exception. {}", context.getQueryIdentifier(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + Util.getRootCauseMessage(e));
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        } finally {
            if (!context.isTxnModel() && parsedStmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) parsedStmt;
                // The transaction of an insert operation begin at analyze phase.
                // So we should abort the transaction at this finally block if it encounters exception.
                if (!insertStmt.needLoadManager() && insertStmt.isTransactionBegin()
                        && context.getState().getStateType() == MysqlStateType.ERR) {
                    try {
                        String errMsg = Strings.emptyToNull(context.getState().getErrorMessage());
                        Env.getCurrentGlobalTransactionMgr().abortTransaction(
                                insertStmt.getDbObj().getId(), insertStmt.getTransactionId(),
                                (errMsg == null ? "unknown reason" : errMsg));
                    } catch (Exception abortTxnException) {
                        LOG.warn("errors when abort txn. {}", context.getQueryIdentifier(), abortTxnException);
                    }
                }
            }
        }
    }

    private void syncJournalIfNeeded() throws Exception {
        final Env env = context.getEnv();
        if (env.isMaster() || !context.getSessionVariable().enableStrongConsistencyRead) {
            return;
        }
        new MasterOpExecutor(context).syncJournal();
    }

    /**
     * get variables in stmt.
     *
     * @throws DdlException
     */
    private void analyzeVariablesInStmt() throws DdlException {
        analyzeVariablesInStmt(parsedStmt);
    }

    private void analyzeVariablesInStmt(StatementBase statement) throws DdlException {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (statement instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) statement;
            Map<String, String> optHints = selectStmt.getSelectList().getOptHints();
            if (optHints == null) {
                optHints = new HashMap<>();
            }
            if (optHints != null) {
                sessionVariable.setIsSingleSetVar(true);
                if (selectStmt.isFromInsert()) {
                    optHints.put("enable_page_cache", "false");
                }
                for (String key : optHints.keySet()) {
                    VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(optHints.get(key))));
                }
            }
        }
    }

    private boolean isQuery() {
        return parsedStmt instanceof QueryStmt
                || (parsedStmt instanceof LogicalPlanAdapter
                && !(((LogicalPlanAdapter) parsedStmt).getLogicalPlan() instanceof Command));
    }

    private void forwardToMaster() throws Exception {
        masterOpExecutor = new MasterOpExecutor(originStmt, context, redirectStatus, isQuery());
        if (LOG.isDebugEnabled()) {
            LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());
        }
        masterOpExecutor.execute();
        if (parsedStmt instanceof SetStmt) {
            SetStmt setStmt = (SetStmt) parsedStmt;
            setStmt.modifySetVarsForExecute();
            for (SetVar var : setStmt.getSetVars()) {
                VariableMgr.setVarForNonMasterFE(context.getSessionVariable(), var);
            }
        } else if (parsedStmt instanceof UnsetVariableStmt) {
            UnsetVariableStmt unsetStmt = (UnsetVariableStmt) parsedStmt;
            if (unsetStmt.isApplyToAll()) {
                VariableMgr.setAllVarsToDefaultValue(context.getSessionVariable(), SetType.SESSION);
            } else {
                String defaultValue = VariableMgr.getDefaultValue(unsetStmt.getVariable());
                if (defaultValue == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, unsetStmt.getVariable());
                }
                SetVar var = new SetVar(SetType.SESSION, unsetStmt.getVariable(),
                        new StringLiteral(defaultValue), SetVarType.SET_SESSION_VAR);
                VariableMgr.setVar(context.getSessionVariable(), var);
            }
        }
    }

    public void updateProfile(boolean isFinished) {
        if (!context.getSessionVariable().enableProfile()) {
            return;
        }
        // If any error happened in update profile, we should ignore this error
        // and ensure the sql is finished normally. For example, if update profile
        // failed, the insert stmt should be success
        try {
            profile.updateSummary(context.startTime, getSummaryInfo(isFinished), isFinished, this.planner);
        } catch (Throwable t) {
            LOG.warn("failed to update profile, ignore this error", t);
        }
    }

    // Analyze one statement to structure in memory.
    public void analyze(TQueryOptions tQueryOptions) throws UserException, InterruptedException, Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to analyze stmt: {}, forwarded stmt id: {}", context.getStmtId(),
                    context.getForwardedStmtId());
        }

        parseByLegacy();

        boolean preparedStmtReanalyzed = false;
        if (parsedStmt instanceof ExecuteStmt) {
            execStmt = (ExecuteStmt) parsedStmt;
            preparedStmtCtx = context.getPreparedStmt(execStmt.getName());
            if (preparedStmtCtx == null) {
                throw new UserException("Could not execute, since `" + execStmt.getName() + "` not exist");
            }
            // parsedStmt may already by set when constructing this StmtExecutor();
            ((PrepareStmt) preparedStmtCtx.stmt).asignValues(execStmt.getArgs());
            parsedStmt = ((PrepareStmt) preparedStmtCtx.stmt).getInnerStmt();
            planner = preparedStmtCtx.planner;
            analyzer = preparedStmtCtx.analyzer;
            prepareStmt = preparedStmtCtx.stmt;
            if (LOG.isDebugEnabled()) {
                LOG.debug("already prepared stmt: {}", preparedStmtCtx.stmtString);
            }
            isExecuteStmt = true;
            if (!((PrepareStmt) preparedStmtCtx.stmt).needReAnalyze()) {
                // Return directly to bypass analyze and plan
                return;
            }
            // continue analyze
            preparedStmtReanalyzed = true;
            preparedStmtCtx.stmt.reset();
            // preparedStmtCtx.stmt.analyze(analyzer);
        }

        // yiguolei: insert stmt's grammar analysis will write editlog,
        // so that we check if the stmt should be forward to master here
        // if the stmt should be forward to master, then just return here and the master will do analysis again
        if (isForwardToMaster()) {
            return;
        }

        // Query following createting table would throw table not exist error.
        // For example.
        // t1: client issues create table to master fe
        // t2: client issues query sql to observer fe, the query would fail due to not exist table in
        //     plan phase.
        // t3: observer fe receive editlog creating the table from the master fe
        syncJournalIfNeeded();
        analyzer = new Analyzer(context.getEnv(), context);

        if (parsedStmt instanceof PrepareStmt || context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
                prepareStmt = new PrepareStmt(parsedStmt,
                        String.valueOf(String.valueOf(context.getStmtId())));
            } else {
                prepareStmt = (PrepareStmt) parsedStmt;
            }
            ((PrepareStmt) prepareStmt).setContext(context);
            prepareStmt.analyze(analyzer);
            // Need analyze inner statement
            parsedStmt =  ((PrepareStmt) prepareStmt).getInnerStmt();
            if (((PrepareStmt) prepareStmt).getPreparedType() == PrepareStmt.PreparedType.STATEMENT) {
                // Skip analyze, do it lazy
                return;
            }
        }

        // Convert show statement to select statement here
        if (parsedStmt instanceof ShowStmt) {
            SelectStmt selectStmt = ((ShowStmt) parsedStmt).toSelectStmt(analyzer);
            if (selectStmt != null) {
                // Need to set origin stmt for new "parsedStmt"(which is selectStmt here)
                // Otherwise, the log printing may result in NPE
                selectStmt.setOrigStmt(parsedStmt.getOrigStmt());
                setParsedStmt(selectStmt);
            }
        }

        // convert unified load stmt here
        if (parsedStmt instanceof UnifiedLoadStmt) {
            // glue code for unified load
            final UnifiedLoadStmt unifiedLoadStmt = (UnifiedLoadStmt) parsedStmt;
            unifiedLoadStmt.init();
            final StatementBase proxyStmt = unifiedLoadStmt.getProxyStmt();
            parsedStmt = proxyStmt;
            if (!(proxyStmt instanceof LoadStmt) && !(proxyStmt instanceof CreateRoutineLoadStmt)) {
                Preconditions.checkState(
                        parsedStmt instanceof InsertStmt,
                        "enable_unified_load=true, should be insert stmt");
            }
        }
        if (parsedStmt instanceof QueryStmt
                || (parsedStmt instanceof InsertStmt && !((InsertStmt) parsedStmt).needLoadManager())
                || parsedStmt instanceof CreateTableAsSelectStmt
                || parsedStmt instanceof InsertOverwriteTableStmt) {
            Map<Long, TableIf> tableMap = Maps.newTreeMap();
            QueryStmt queryStmt;
            Set<String> parentViewNameSet = Sets.newHashSet();
            if (parsedStmt instanceof QueryStmt) {
                queryStmt = (QueryStmt) parsedStmt;
                queryStmt.getTables(analyzer, false, tableMap, parentViewNameSet);
            } else if (parsedStmt instanceof InsertOverwriteTableStmt) {
                InsertOverwriteTableStmt parsedStmt = (InsertOverwriteTableStmt) this.parsedStmt;
                parsedStmt.analyze(analyzer);
                queryStmt = parsedStmt.getQueryStmt();
                queryStmt.getTables(analyzer, false, tableMap, parentViewNameSet);
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                CreateTableAsSelectStmt parsedStmt = (CreateTableAsSelectStmt) this.parsedStmt;
                queryStmt = parsedStmt.getQueryStmt();
                queryStmt.getTables(analyzer, false, tableMap, parentViewNameSet);
            } else if (parsedStmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) parsedStmt;
                insertStmt.getTables(analyzer, tableMap, parentViewNameSet);
            }
            // table id in tableList is in ascending order because that table map is a sorted map
            List<TableIf> tables = Lists.newArrayList(tableMap.values());
            tables.sort((Comparator.comparing(TableIf::getId)));
            int analyzeTimes = 2;
            for (int i = 1; i <= analyzeTimes; i++) {
                MetaLockUtils.readLockTables(tables);
                try {
                    analyzeAndGenerateQueryPlan(tQueryOptions);
                    break;
                } catch (MVSelectFailedException e) {
                    /*
                     * If there is MVSelectFailedException after the first planner,
                     * there will be error mv rewritten in query.
                     * So, the query should be reanalyzed without mv rewritten and planner again.
                     * Attention: Only error rewritten tuple is forbidden to mv rewrite in the second time.
                     */
                    if (i == analyzeTimes) {
                        throw e;
                    } else {
                        resetAnalyzerAndStmt();
                    }
                } catch (UserException e) {
                    throw e;
                } catch (Exception e) {
                    LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                    throw new AnalysisException("Unexpected exception: " + e.getMessage());
                } finally {
                    MetaLockUtils.readUnlockTables(tables);
                }
            }
        } else {
            try {
                parsedStmt.analyze(analyzer);
            } catch (UserException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                throw new AnalysisException("Unexpected exception: " + e.getMessage());
            }
        }
        if (preparedStmtReanalyzed
                && ((PrepareStmt) preparedStmtCtx.stmt).getPreparedType() == PrepareStmt.PreparedType.FULL_PREPARED) {
            ((PrepareStmt) prepareStmt).asignValues(execStmt.getArgs());
            if (LOG.isDebugEnabled()) {
                LOG.debug("update planner and analyzer after prepared statement reanalyzed");
            }
            preparedStmtCtx.planner = planner;
            preparedStmtCtx.analyzer = analyzer;
            Preconditions.checkNotNull(preparedStmtCtx.stmt);
            preparedStmtCtx.analyzer.setPrepareStmt(((PrepareStmt) preparedStmtCtx.stmt));
        }
    }

    private void parseByLegacy() throws AnalysisException, DdlException {
        // parsedStmt may already by set when constructing this StmtExecutor();
        if (parsedStmt == null) {
            // Parse statement with parser generated by CUP&FLEX
            SqlScanner input = new SqlScanner(new StringReader(originStmt.originStmt),
                    context.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);
            try {
                StatementBase parsedStmt = setParsedStmt(SqlParserUtils.getStmt(parser, originStmt.idx));
                parsedStmt.setOrigStmt(originStmt);
                parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            } catch (Error e) {
                LOG.info("error happened when parsing stmt {}, id: {}", originStmt, context.getStmtId(), e);
                throw new AnalysisException("sql parsing error, please check your sql");
            } catch (AnalysisException e) {
                String syntaxError = parser.getErrorMsg(originStmt.originStmt);
                LOG.info("analysis exception happened when parsing stmt {}, id: {}, error: {}",
                        originStmt, context.getStmtId(), syntaxError, e);
                if (syntaxError == null) {
                    throw e;
                } else {
                    throw new AnalysisException(syntaxError, e);
                }
            } catch (Exception e) {
                // TODO(lingbin): we catch 'Exception' to prevent unexpected error,
                // should be removed this try-catch clause future.
                LOG.info("unexpected exception happened when parsing stmt {}, id: {}, error: {}",
                        originStmt, context.getStmtId(), parser.getErrorMsg(originStmt.originStmt), e);
                throw new AnalysisException("Unexpected exception: " + e.getMessage());
            }

            analyzeVariablesInStmt();
        }
        if (context.getSessionVariable().isEnableInsertGroupCommit() && parsedStmt instanceof NativeInsertStmt) {
            NativeInsertStmt nativeInsertStmt = (NativeInsertStmt) parsedStmt;
            nativeInsertStmt.analyzeGroupCommit(new Analyzer(context.getEnv(), context));
            redirectStatus = parsedStmt.getRedirectStatus();
            isForwardedToMaster = shouldForwardToMaster();
        }
        redirectStatus = parsedStmt.getRedirectStatus();
    }

    private void analyzeAndGenerateQueryPlan(TQueryOptions tQueryOptions) throws UserException {
        if (parsedStmt instanceof QueryStmt || parsedStmt instanceof InsertStmt) {
            QueryStmt queryStmt = null;
            if (parsedStmt instanceof QueryStmt) {
                queryStmt = (QueryStmt) parsedStmt;
            }
            if (parsedStmt instanceof InsertStmt) {
                queryStmt = (QueryStmt) ((InsertStmt) parsedStmt).getQueryStmt();
            }
            if (queryStmt.getOrderByElements() != null && queryStmt.getOrderByElements().isEmpty()) {
                queryStmt.removeOrderByElements();
            }
        }
        if (prepareStmt != null) {
            analyzer.setPrepareStmt(((PrepareStmt) prepareStmt));
            if (execStmt != null &&  ((PrepareStmt) prepareStmt).getPreparedType() != PreparedType.FULL_PREPARED) {
                ((PrepareStmt) prepareStmt).asignValues(execStmt.getArgs());
            }
        }
        parsedStmt.analyze(analyzer);
        if (parsedStmt instanceof QueryStmt || parsedStmt instanceof InsertStmt) {
            if (parsedStmt instanceof NativeInsertStmt && ((NativeInsertStmt) parsedStmt).isGroupCommit()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("skip generate query plan for group commit insert");
                }
                return;
            }
            ExprRewriter rewriter = analyzer.getExprRewriter();
            rewriter.reset();
            if (context.getSessionVariable().isEnableFoldConstantByBe()
                    && !context.getSessionVariable().isDebugSkipFoldConstant()) {
                // fold constant expr
                parsedStmt.foldConstant(rewriter, tQueryOptions);
            }
            if (context.getSessionVariable().isEnableRewriteElementAtToSlot()) {
                parsedStmt.rewriteElementAtToSlot(rewriter, tQueryOptions);
            }
            // Apply expr and subquery rewrites.
            ExplainOptions explainOptions = parsedStmt.getExplainOptions();
            boolean reAnalyze = false;

            parsedStmt.rewriteExprs(rewriter);
            reAnalyze = rewriter.changed();
            if (analyzer.containSubquery()) {
                parsedStmt = setParsedStmt(StmtRewriter.rewrite(analyzer, parsedStmt));
                reAnalyze = true;
            }
            if (parsedStmt instanceof SelectStmt) {
                if (StmtRewriter.rewriteByPolicy(parsedStmt, analyzer)
                        || StmtRewriter.rewriteForRandomDistribution(parsedStmt, analyzer)) {
                    reAnalyze = true;
                }
            }
            if (parsedStmt instanceof SetOperationStmt) {
                List<SetOperationStmt.SetOperand> operands = ((SetOperationStmt) parsedStmt).getOperands();
                for (SetOperationStmt.SetOperand operand : operands) {
                    if (StmtRewriter.rewriteByPolicy(operand.getQueryStmt(), analyzer)
                            || StmtRewriter.rewriteForRandomDistribution(operand.getQueryStmt(), analyzer)) {
                        reAnalyze = true;
                    }
                }
            }
            if (parsedStmt instanceof InsertStmt) {
                QueryStmt queryStmt = ((InsertStmt) parsedStmt).getQueryStmt();
                if (queryStmt != null && (StmtRewriter.rewriteByPolicy(queryStmt, analyzer)
                        || StmtRewriter.rewriteForRandomDistribution(queryStmt, analyzer))) {
                    reAnalyze = true;
                }
            }
            if (reAnalyze) {
                // The rewrites should have no user-visible effect. Remember the original result
                // types and column labels to restore them after the rewritten stmt has been
                // reset() and re-analyzed.
                List<Type> origResultTypes = Lists.newArrayList();
                for (Expr e : parsedStmt.getResultExprs()) {
                    origResultTypes.add(e.getType());
                }
                List<String> origColLabels =
                        Lists.newArrayList(parsedStmt.getColLabels());
                // Re-analyze the stmt with a new analyzer.
                analyzer = new Analyzer(context.getEnv(), context);
                // query re-analyze
                parsedStmt.reset();
                if (prepareStmt != null) {
                    analyzer.setPrepareStmt(((PrepareStmt) prepareStmt));
                    if (execStmt != null
                            && ((PrepareStmt) prepareStmt).getPreparedType() != PreparedType.FULL_PREPARED) {
                        ((PrepareStmt) prepareStmt).asignValues(execStmt.getArgs());
                    }
                }
                analyzer.setReAnalyze(true);
                parsedStmt.analyze(analyzer);

                // Restore the original result types and column labels.
                parsedStmt.castResultExprs(origResultTypes);
                parsedStmt.setColLabels(origColLabels);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("rewrittenStmt: " + parsedStmt.toSql());
                }
                if (explainOptions != null) {
                    parsedStmt.setIsExplain(explainOptions);
                }
            }
        }
        profile.getSummaryProfile().setQueryAnalysisFinishTime();
        planner = new OriginalPlanner(analyzer);
        if (parsedStmt instanceof QueryStmt || parsedStmt instanceof InsertStmt) {
            planner.plan(parsedStmt, tQueryOptions);
        }
        profile.getSummaryProfile().setQueryPlanFinishTime();
    }

    private void resetAnalyzerAndStmt() {
        analyzer = new Analyzer(context.getEnv(), context);

        parsedStmt.reset();

        // DORIS-7361
        // Need to reset selectList before second-round analyze,
        // because exprs in selectList could be rewritten by mvExprRewriter
        // in first-round analyze, which could cause analyze failure.
        if (parsedStmt instanceof QueryStmt) {
            ((QueryStmt) parsedStmt).resetSelectList();
        }

        if (parsedStmt instanceof InsertStmt) {
            ((InsertStmt) parsedStmt).getQueryStmt().resetSelectList();
        }

        if (parsedStmt instanceof CreateTableAsSelectStmt) {
            ((CreateTableAsSelectStmt) parsedStmt).getQueryStmt().resetSelectList();
        }
    }

    // Because this is called by other thread
    public void cancel(String message, boolean needWaitCancelComplete) {
        Optional<InsertOverwriteTableCommand> insertOverwriteTableCommand = getInsertOverwriteTableCommand();
        if (insertOverwriteTableCommand.isPresent()) {
            // If the be scheduling has not been triggered yet, cancel the scheduling first
            insertOverwriteTableCommand.get().cancel();
        }
        Coordinator coordRef = coord;
        if (coordRef != null) {
            coordRef.cancel(message);
        }
        if (mysqlLoadId != null) {
            Env.getCurrentEnv().getLoadManager().getMysqlLoadManager().cancelMySqlLoad(mysqlLoadId);
        }
        if (parsedStmt instanceof AnalyzeTblStmt || parsedStmt instanceof AnalyzeDBStmt) {
            Env.getCurrentEnv().getAnalysisManager().cancelSyncTask(context);
        }
        if (insertOverwriteTableCommand.isPresent() && needWaitCancelComplete) {
            // Wait for the command to run or cancel completion
            insertOverwriteTableCommand.get().waitNotRunning();
        }
    }

    public void cancel(String message) {
        cancel(message, true);
    }

    private Optional<InsertOverwriteTableCommand> getInsertOverwriteTableCommand() {
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) parsedStmt;
            LogicalPlan logicalPlan = logicalPlanAdapter.getLogicalPlan();
            if (logicalPlan instanceof InsertOverwriteTableCommand) {
                InsertOverwriteTableCommand insertOverwriteTableCommand = (InsertOverwriteTableCommand) logicalPlan;
                return Optional.of(insertOverwriteTableCommand);
            }
        }
        return Optional.empty();
    }

    // Because this is called by other thread
    public void cancel(Types.PPlanFragmentCancelReason cancelReason) {
        Coordinator coordRef = coord;
        if (coordRef != null) {
            coordRef.cancel(cancelReason, "");
        }
        if (mysqlLoadId != null) {
            Env.getCurrentEnv().getLoadManager().getMysqlLoadManager().cancelMySqlLoad(mysqlLoadId);
        }
        if (parsedStmt instanceof AnalyzeTblStmt || parsedStmt instanceof AnalyzeDBStmt) {
            Env.getCurrentEnv().getAnalysisManager().cancelSyncTask(context);
        }
    }

    // Handle kill statement.
    private void handleKill() throws UserException {
        KillStmt killStmt = (KillStmt) parsedStmt;
        ConnectContext killCtx = null;
        int id = killStmt.getConnectionId();
        String queryId = killStmt.getQueryId();
        if (id == -1) {
            // when killCtx == null, this means the query not in FE,
            // then we just send kill signal to BE
            killCtx = context.getConnectScheduler().getContextWithQueryId(queryId);
        } else {
            killCtx = context.getConnectScheduler().getContext(id);
            if (killCtx == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, id);
            }
        }

        if (killCtx == null) {
            TUniqueId tQueryId = null;
            try {
                tQueryId = DebugUtil.parseTUniqueIdFromString(queryId);
            } catch (NumberFormatException e) {
                throw new UserException(e.getMessage());
            }
            LOG.info("kill query {}", queryId);
            Collection<Backend> nodesToPublish = Env.getCurrentSystemInfo().getIdToBackend().values();
            for (Backend be : nodesToPublish) {
                if (be.isAlive()) {
                    try {
                        BackendServiceProxy.getInstance()
                                .cancelPipelineXPlanFragmentAsync(be.getBrpcAddress(), tQueryId,
                                        Types.PPlanFragmentCancelReason.USER_CANCEL);
                    } catch (Throwable t) {
                        LOG.info("send kill query {} rpc to be {} failed", queryId, be);
                    }
                }
            }
        } else if (context == killCtx) {
            // Suicide
            context.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ConnectContext.get().getQualifiedUser())
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
                    PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, id);
            }

            killCtx.kill(killStmt.isConnectionKill());
        }
        context.getState().setOk();
    }

    // Process set statement.
    private void handleSetStmt() {
        try {
            SetStmt setStmt = (SetStmt) parsedStmt;
            SetExecutor executor = new SetExecutor(context, setStmt);
            executor.execute();
        } catch (DdlException e) {
            LOG.warn("", e);
            // Return error message to client.
            context.getState().setError(ErrorCode.ERR_LOCAL_VARIABLE, e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    // Process unset variable statement.
    private void handleUnsetVariableStmt() {
        try {
            UnsetVariableStmt unsetStmt = (UnsetVariableStmt) parsedStmt;
            if (unsetStmt.isApplyToAll()) {
                VariableMgr.setAllVarsToDefaultValue(context.getSessionVariable(), unsetStmt.getSetType());
            } else {
                String defaultValue = VariableMgr.getDefaultValue(unsetStmt.getVariable());
                if (defaultValue == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, unsetStmt.getVariable());
                }
                SetVar var = new SetVar(unsetStmt.getSetType(), unsetStmt.getVariable(),
                        new StringLiteral(defaultValue), SetVarType.SET_SESSION_VAR);
                VariableMgr.setVar(context.getSessionVariable(), var);
            }
        } catch (DdlException e) {
            LOG.warn("", e);
            // Return error message to client.
            context.getState().setError(ErrorCode.ERR_LOCAL_VARIABLE, e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    // send values from cache.
    // return true if the meta fields has been sent, otherwise, return false.
    // the meta fields must be sent right before the first batch of data(or eos flag).
    // so if it has data(or eos is true), this method must return true.
    private boolean sendCachedValues(MysqlChannel channel, List<InternalService.PCacheValue> cacheValues,
            Queriable selectStmt, boolean isSendFields, boolean isEos)
            throws Exception {
        RowBatch batch = null;
        boolean isSend = isSendFields;
        for (InternalService.PCacheValue value : cacheValues) {
            TResultBatch resultBatch = new TResultBatch();
            // need to set empty list first, to support empty result set.
            resultBatch.setRows(Lists.newArrayList());
            for (ByteString one : value.getRowsList()) {
                resultBatch.addToRows(ByteBuffer.wrap(one.toByteArray()));
            }
            resultBatch.setPacketSeq(1);
            resultBatch.setIsCompressed(false);
            batch = new RowBatch();
            batch.setBatch(resultBatch);
            batch.setEos(true);
            if (!isSend) {
                // send meta fields before sending first data batch.
                sendFields(selectStmt.getColLabels(), selectStmt.getFieldInfos(),
                        exprToType(selectStmt.getResultExprs()));
                isSend = true;
            }
            for (ByteBuffer row : batch.getBatch().getRows()) {
                channel.sendOnePacket(row);
            }
            context.updateReturnRows(batch.getBatch().getRows().size());
        }

        if (isEos) {
            if (batch != null) {
                statisticsForAuditLog = batch.getQueryStatistics() == null
                        ? null : batch.getQueryStatistics().toBuilder();
            }
            if (!isSend) {
                sendFields(selectStmt.getColLabels(), selectStmt.getFieldInfos(),
                        exprToType(selectStmt.getResultExprs()));
                isSend = true;
            }
            context.getState().setEof();
        }
        return isSend;
    }

    /**
     * Handle the SelectStmt via Cache.
     */
    private void handleCacheStmt(CacheAnalyzer cacheAnalyzer, MysqlChannel channel) throws Exception {
        InternalService.PFetchCacheResult cacheResult = null;
        boolean wantToParseSqlForSqlCache = planner instanceof NereidsPlanner
                && CacheAnalyzer.canUseSqlCache(context.getSessionVariable());
        try {
            cacheResult = cacheAnalyzer.getCacheData();
            if (cacheResult == null) {
                if (ConnectContext.get() != null
                        && !ConnectContext.get().getSessionVariable().testQueryCacheHit.equals("none")) {
                    throw new UserException("The variable test_query_cache_hit is set to "
                            + ConnectContext.get().getSessionVariable().testQueryCacheHit
                            + ", but the query cache is not hit.");
                }
            }
        } finally {
            if (wantToParseSqlForSqlCache) {
                String originStmt = parsedStmt.getOrigStmt().originStmt;
                NereidsSqlCacheManager sqlCacheManager = context.getEnv().getSqlCacheManager();
                if (cacheResult != null) {
                    sqlCacheManager.tryAddBeCache(context, originStmt, cacheAnalyzer);
                }
            }
        }

        CacheMode mode = cacheAnalyzer.getCacheMode();
        Queriable queryStmt = (Queriable) parsedStmt;
        boolean isSendFields = false;
        if (cacheResult != null) {
            isCached = true;
            if (cacheAnalyzer.getHitRange() == Cache.HitRange.Full) {
                sendCachedValues(channel, cacheResult.getValuesList(), queryStmt, isSendFields, true);
                return;
            }
            // rewrite sql
            if (mode == CacheMode.Partition) {
                if (cacheAnalyzer.getHitRange() == Cache.HitRange.Left) {
                    isSendFields = sendCachedValues(channel, cacheResult.getValuesList(),
                            queryStmt, isSendFields, false);
                }
                StatementBase newSelectStmt = cacheAnalyzer.getRewriteStmt();
                newSelectStmt.reset();
                analyzer = new Analyzer(context.getEnv(), context);
                newSelectStmt.analyze(analyzer);
                if (parsedStmt instanceof LogicalPlanAdapter) {
                    planner = new NereidsPlanner(statementContext);
                } else {
                    planner = new OriginalPlanner(analyzer);
                }
                planner.plan(newSelectStmt, context.getSessionVariable().toThrift());
            }
        }
        executeAndSendResult(false, isSendFields, queryStmt, channel, cacheAnalyzer, cacheResult);
    }

    // Process a select statement.
    private void handleQueryStmt() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Handling query {} with query id {}",
                          originStmt.originStmt, DebugUtil.printId(context.queryId));
        }

        if (context.getConnectType() == ConnectType.MYSQL) {
            // Every time set no send flag and clean all data in buffer
            context.getMysqlChannel().reset();
        }

        Queriable queryStmt = (Queriable) parsedStmt;

        if (queryStmt.isExplain()) {
            String explainString = planner.getExplainString(queryStmt.getExplainOptions());
            handleExplainStmt(explainString, false);
            LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
            return;
        }

        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) parsedStmt;
            LogicalPlan logicalPlan = logicalPlanAdapter.getLogicalPlan();
            if (logicalPlan instanceof org.apache.doris.nereids.trees.plans.algebra.SqlCache) {
                isCached = true;
            }
        }

        // handle selects that fe can do without be, so we can make sql tools happy, especially the setup step.
        // TODO FE not support doris field type conversion to arrow field type.
        if (!context.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)
                    && context.getCommand() != MysqlCommand.COM_STMT_EXECUTE) {
            Optional<ResultSet> resultSet = planner.handleQueryInFe(parsedStmt);
            if (resultSet.isPresent()) {
                sendResultSet(resultSet.get(), ((Queriable) parsedStmt).getFieldInfos());
                isHandleQueryInFe = true;
                LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
                if (context.getSessionVariable().enableProfile()) {
                    if (profile != null) {
                        this.profile.getSummaryProfile().setExecutedByFrontend(true);
                    }
                }
                return;
            }
        }

        MysqlChannel channel = null;
        if (context.getConnectType().equals(ConnectType.MYSQL)) {
            channel = context.getMysqlChannel();
        }
        boolean isOutfileQuery = queryStmt.hasOutFileClause();
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlanAdapter logicalPlanAdapter = (LogicalPlanAdapter) parsedStmt;
            LogicalPlan logicalPlan = logicalPlanAdapter.getLogicalPlan();
            if (logicalPlan instanceof org.apache.doris.nereids.trees.plans.algebra.SqlCache) {
                NereidsPlanner nereidsPlanner = (NereidsPlanner) planner;
                PhysicalSqlCache physicalSqlCache = (PhysicalSqlCache) nereidsPlanner.getPhysicalPlan();
                sendCachedValues(channel, physicalSqlCache.getCacheValues(), logicalPlanAdapter, false, true);
                return;
            }
        }

        // Sql and PartitionCache
        CacheAnalyzer cacheAnalyzer = new CacheAnalyzer(context, parsedStmt, planner);
        // TODO support arrow flight sql
        // NOTE: If you want to add another condition about SessionVariable, please consider whether
        // add to CacheAnalyzer.commonCacheCondition
        if (channel != null && !isOutfileQuery && CacheAnalyzer.canUseCache(context.getSessionVariable())
                && parsedStmt.getOrigStmt() != null && parsedStmt.getOrigStmt().originStmt != null) {
            if (queryStmt instanceof QueryStmt || queryStmt instanceof LogicalPlanAdapter) {
                handleCacheStmt(cacheAnalyzer, channel);
                LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
                return;
            }
        }

        // handle select .. from xx  limit 0
        // TODO support arrow flight sql
        if (channel != null && parsedStmt instanceof SelectStmt) {
            SelectStmt parsedSelectStmt = (SelectStmt) parsedStmt;
            if (parsedSelectStmt.getLimit() == 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignore handle limit 0 ,sql:{}", parsedSelectStmt.toSql());
                }

                sendFields(queryStmt.getColLabels(), queryStmt.getFieldInfos(), exprToType(queryStmt.getResultExprs()));
                context.getState().setEof();
                LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
                return;
            }
        }

        executeAndSendResult(isOutfileQuery, false, queryStmt, channel, null, null);
        LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
    }

    public void executeAndSendResult(boolean isOutfileQuery, boolean isSendFields,
            Queriable queryStmt, MysqlChannel channel,
            CacheAnalyzer cacheAnalyzer, InternalService.PFetchCacheResult cacheResult) throws Exception {
        // 1. If this is a query with OUTFILE clause, eg: select * from tbl1 into outfile xxx,
        //    We will not send real query result to client. Instead, we only send OK to client with
        //    number of rows selected. For example:
        //          mysql> select * from tbl1 into outfile xxx;
        //          Query OK, 10 rows affected (0.01 sec)
        //
        // 2. If this is a query, send the result expr fields first, and send result data back to client.
        RowBatch batch;
        CoordInterface coordBase = null;
        if (statementContext.isShortCircuitQuery()) {
            ShortCircuitQueryContext shortCircuitQueryContext =
                        statementContext.getShortCircuitQueryContext() != null
                                ? statementContext.getShortCircuitQueryContext()
                                : new ShortCircuitQueryContext(planner, (Queriable) parsedStmt);
            coordBase = new PointQueryExecutor(shortCircuitQueryContext,
                        context.getSessionVariable().getMaxMsgSizeOfResultReceiver());
            context.getState().setIsQuery(true);
        } else if (queryStmt instanceof SelectStmt && ((SelectStmt) parsedStmt).isPointQueryShortCircuit()) {
            // this branch is for legacy planner, to be removed
            coordBase = new PointQueryExec(planner, analyzer,
                    context.getSessionVariable().getMaxMsgSizeOfResultReceiver());
            context.getState().setIsQuery(true);
        } else {
            coord = new Coordinator(context, analyzer, planner, context.getStatsErrorEstimator());
            profile.addExecutionProfile(coord.getExecutionProfile());
            QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                    new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
            coordBase = coord;
        }

        try {
            coordBase.exec();
            profile.getSummaryProfile().setQueryScheduleFinishTime();
            updateProfile(false);

            if (context.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
                Preconditions.checkState(!context.isReturnResultFromLocal());
                profile.getSummaryProfile().setTempStartTime();
                return;
            }

            if (context.isRunProcedure()) {
                // plsql will get the returned results without sending them to mysql client.
                // see org/apache/doris/plsql/executor/DorisRowResult.java
                return;
            }

            boolean isDryRun = ConnectContext.get() != null && ConnectContext.get().getSessionVariable().dryRunQuery;
            while (true) {
                // register the fetch result time.
                profile.getSummaryProfile().setTempStartTime();
                batch = coordBase.getNext();
                profile.getSummaryProfile().freshFetchResultConsumeTime();

                // for outfile query, there will be only one empty batch send back with eos flag
                // call `copyRowBatch()` first, because batch.getBatch() may be null, if result set is empty
                if (cacheAnalyzer != null && !isOutfileQuery && !isDryRun) {
                    cacheAnalyzer.copyRowBatch(batch);
                }
                if (batch.getBatch() != null) {
                    // register send field result time.
                    profile.getSummaryProfile().setTempStartTime();
                    // For some language driver, getting error packet after fields packet
                    // will be recognized as a success result
                    // so We need to send fields after first batch arrived
                    if (!isSendFields) {
                        if (!isOutfileQuery) {
                            sendFields(queryStmt.getColLabels(), queryStmt.getFieldInfos(),
                                    exprToType(queryStmt.getResultExprs()));
                        } else {
                            if (!Strings.isNullOrEmpty(queryStmt.getOutFileClause().getSuccessFileName())) {
                                outfileWriteSuccess(queryStmt.getOutFileClause());
                            }
                            sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                        }
                        isSendFields = true;
                    }
                    for (ByteBuffer row : batch.getBatch().getRows()) {
                        channel.sendOnePacket(row);
                    }
                    profile.getSummaryProfile().freshWriteResultConsumeTime();
                    context.updateReturnRows(batch.getBatch().getRows().size());
                    context.setResultAttachedInfo(batch.getBatch().getAttachedInfos());
                }
                if (batch.isEos()) {
                    break;
                }
            }
            if (cacheAnalyzer != null && !isDryRun) {
                if (cacheResult != null && cacheAnalyzer.getHitRange() == Cache.HitRange.Right) {
                    isSendFields =
                            sendCachedValues(channel, cacheResult.getValuesList(), queryStmt, isSendFields,
                                    false);
                }

                cacheAnalyzer.updateCache();

                Cache cache = cacheAnalyzer.getCache();
                if (cache instanceof SqlCache && !cache.isDisableCache() && planner instanceof NereidsPlanner) {
                    String originStmt = parsedStmt.getOrigStmt().originStmt;
                    context.getEnv().getSqlCacheManager().tryAddBeCache(context, originStmt, cacheAnalyzer);
                }
            }
            if (!isSendFields) {
                if (!isOutfileQuery) {
                    if (ConnectContext.get() != null && isDryRun) {
                        // Return a one row one column result set, with the real result number
                        List<String> data = Lists.newArrayList(batch.getQueryStatistics() == null ? "0"
                                : batch.getQueryStatistics().getReturnedRows() + "");
                        ResultSet resultSet = new CommonResultSet(DRY_RUN_QUERY_METADATA,
                                Collections.singletonList(data));
                        sendResultSet(resultSet);
                        return;
                    } else {
                        sendFields(queryStmt.getColLabels(), queryStmt.getFieldInfos(),
                                exprToType(queryStmt.getResultExprs()));
                    }
                } else {
                    sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                }
            }

            statisticsForAuditLog = batch.getQueryStatistics() == null ? null : batch.getQueryStatistics().toBuilder();
            context.getState().setEof();
            profile.getSummaryProfile().setQueryFetchResultFinishTime();
        } catch (Exception e) {
            // notify all be cancel running fragment
            // in some case may block all fragment handle threads
            // details see issue https://github.com/apache/doris/issues/16203
            Status internalErrorSt = new Status(TStatusCode.INTERNAL_ERROR,
                    "cancel fragment query_id:{} cause {}",
                    DebugUtil.printId(context.queryId()), e.getMessage());
            LOG.warn(internalErrorSt.getErrorMsg());
            coordBase.cancel(Types.PPlanFragmentCancelReason.INTERNAL_ERROR, internalErrorSt.getErrorMsg());
            throw e;
        } finally {
            coordBase.close();
        }
    }

    private void outfileWriteSuccess(OutFileClause outFileClause) throws Exception {
        // 1. set TResultFileSinkOptions
        TResultFileSinkOptions sinkOptions = outFileClause.toSinkOptions();

        // 2. set brokerNetAddress
        StorageType storageType = outFileClause.getBrokerDesc() == null
                ? StorageBackend.StorageType.LOCAL : outFileClause.getBrokerDesc().getStorageType();
        if (storageType == StorageType.BROKER) {
            // set the broker address for OUTFILE sink
            String brokerName = outFileClause.getBrokerDesc().getName();
            FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getAnyBroker(brokerName);
            sinkOptions.setBrokerAddresses(Lists.newArrayList(new TNetworkAddress(broker.host, broker.port)));
        }

        // 3. set TResultFileSink properties
        TResultFileSink sink = new TResultFileSink();
        sink.setFileOptions(sinkOptions);
        sink.setStorageBackendType(storageType.toThrift());

        // 4. get BE
        TNetworkAddress address = null;
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
                break;
            }
        }
        if (address == null) {
            throw new AnalysisException("No Alive backends");
        }

        // 5. send rpc to BE
        POutfileWriteSuccessRequest request = POutfileWriteSuccessRequest.newBuilder()
                .setResultFileSink(ByteString.copyFrom(new TSerializer().serialize(sink))).build();
        Future<POutfileWriteSuccessResult> future = BackendServiceProxy.getInstance()
                .outfileWriteSuccessAsync(address, request);
        InternalService.POutfileWriteSuccessResult result = future.get();
        TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
        String errMsg;
        if (code != TStatusCode.OK) {
            if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                errMsg = result.getStatus().getErrorMsgsList().get(0);
            } else {
                errMsg = "Outfile write success file failed. backend address: "
                        + NetUtils
                        .getHostPortInAccessibleFormat(address.getHostname(), address.getPort());
            }
            throw new AnalysisException(errMsg);
        }
    }

    private void handleTransactionStmt() throws Exception {
        if (context.getConnectType() == ConnectType.MYSQL) {
            // Every time set no send flag and clean all data in buffer
            context.getMysqlChannel().reset();
        }
        context.getState().setOk(0, 0, "");
        // create plan
        if (context.getTxnEntry() != null && context.getTxnEntry().getRowsInTransaction() == 0
                && !context.getTxnEntry().isTransactionBegan()
                && (parsedStmt instanceof TransactionCommitStmt || parsedStmt instanceof TransactionRollbackStmt)) {
            context.setTxnEntry(null);
        } else if (parsedStmt instanceof TransactionBeginStmt) {
            if (context.isTxnModel()) {
                LOG.info("A transaction has already begin");
                return;
            }
            if (context.getTxnEntry() == null) {
                context.setTxnEntry(new TransactionEntry());
            }
            context.getTxnEntry()
                    .setTxnConf(new TTxnParams().setNeedTxn(true).setEnablePipelineTxnLoad(Config.enable_pipeline_load)
                            .setThriftRpcTimeoutMs(5000).setTxnId(-1).setDb("").setTbl("")
                            .setMaxFilterRatio(context.getSessionVariable().getEnableInsertStrict() ? 0
                                    : context.getSessionVariable().getInsertMaxFilterRatio()));
            context.getTxnEntry().setFirstTxnInsert(true);
            StringBuilder sb = new StringBuilder();
            sb.append("{'label':'").append(context.getTxnEntry().getLabel()).append("', 'status':'")
                    .append(TransactionStatus.PREPARE.name());
            sb.append("', 'txnId':'").append("'").append("}");
            context.getState().setOk(0, 0, sb.toString());
        } else if (parsedStmt instanceof TransactionCommitStmt) {
            if (!context.isTxnModel()) {
                LOG.info("No transaction to commit");
                return;
            }
            try {
                TransactionEntry txnEntry = context.getTxnEntry();
                TransactionStatus txnStatus = txnEntry.commitTransaction();
                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(txnEntry.getLabel()).append("', 'status':'")
                        .append(txnStatus.name()).append("', 'txnId':'")
                        .append(txnEntry.getTransactionId()).append("'").append("}");
                context.getState().setOk(0, 0, sb.toString());
            } catch (Exception e) {
                LOG.warn("Txn commit failed", e);
                throw new AnalysisException(e.getMessage());
            } finally {
                context.setTxnEntry(null);
            }
        } else if (parsedStmt instanceof TransactionRollbackStmt) {
            if (!context.isTxnModel()) {
                LOG.info("No transaction to rollback");
                return;
            }
            try {
                TransactionEntry txnEntry = context.getTxnEntry();
                long txnId = txnEntry.abortTransaction();
                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(txnEntry.getLabel()).append("', 'status':'")
                        .append(TransactionStatus.ABORTED.name()).append("', 'txnId':'")
                        .append(txnId).append("'").append("}");
                context.getState().setOk(0, 0, sb.toString());
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            } finally {
                context.setTxnEntry(null);
            }
        } else {
            throw new TException("parsedStmt type is not TransactionStmt");
        }
    }

    private int executeForTxn(InsertStmt insertStmt)
            throws UserException, TException, InterruptedException, ExecutionException, TimeoutException {
        if (context.isTxnIniting()) { // first time, begin txn
            beginTxn(insertStmt.getDbName(),
                    insertStmt.getTbl());
        }
        if (!context.getTxnEntry().getTxnConf().getDb().equals(insertStmt.getDbName())
                || !context.getTxnEntry().getTxnConf().getTbl().equals(insertStmt.getTbl())) {
            throw new TException("Only one table can be inserted in one transaction.");
        }

        QueryStmt queryStmt = insertStmt.getQueryStmt();
        if (!(queryStmt instanceof SelectStmt)) {
            throw new TException("queryStmt is not SelectStmt, insert command error");
        }
        TransactionEntry txnEntry = context.getTxnEntry();
        SelectStmt selectStmt = (SelectStmt) queryStmt;
        int effectRows = 0;
        if (selectStmt.getValueList() != null) {
            Table tbl = txnEntry.getTable();
            int schemaSize = tbl.getBaseSchema(false).size();
            if (parsedStmt instanceof NativeInsertStmt
                    && ((NativeInsertStmt) parsedStmt).getTargetColumnNames() != null) {
                NativeInsertStmt nativeInsertStmt = (NativeInsertStmt) parsedStmt;
                if (nativeInsertStmt.containTargetColumnName(Column.SEQUENCE_COL)) {
                    schemaSize++;
                }
                if (nativeInsertStmt.containTargetColumnName(Column.DELETE_SIGN)) {
                    schemaSize++;
                }
            }
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                // the value columns are columns which are visible to user, so here we use
                // getBaseSchema(), not getFullSchema()
                if (schemaSize != row.size()) {
                    throw new TException("Column count doesn't match value count");
                }
            }
            FormatOptions options = FormatOptions.getDefault();
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                ++effectRows;
                InternalService.PDataRow data = StmtExecutor.getRowStringValue(row, options);
                if (data == null) {
                    continue;
                }
                List<InternalService.PDataRow> dataToSend = txnEntry.getDataToSend();
                dataToSend.add(data);
                if (dataToSend.size() >= MAX_DATA_TO_SEND_FOR_TXN) {
                    // send data
                    InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
                    executor.sendData();
                }
            }
        }
        txnEntry.setRowsInTransaction(txnEntry.getRowsInTransaction() + effectRows);
        return effectRows;
    }

    private void beginTxn(String dbName, String tblName) throws UserException, TException,
            InterruptedException, ExecutionException, TimeoutException {
        TransactionEntry txnEntry = context.getTxnEntry();
        TTxnParams txnConf = txnEntry.getTxnConf();
        SessionVariable sessionVariable = context.getSessionVariable();
        long timeoutSecond = context.getExecTimeout();

        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        Database dbObj = Env.getCurrentInternalCatalog()
                .getDbOrException(dbName, s -> new TException("database is invalid for dbName: " + s));
        Table tblObj = dbObj.getTableOrException(tblName, s -> new TException("table is invalid: " + s));
        txnConf.setDbId(dbObj.getId()).setTbl(tblName).setDb(dbName);
        txnEntry.setTable(tblObj);
        txnEntry.setDb(dbObj);
        String label = txnEntry.getLabel();
        if (Env.getCurrentEnv().isMaster()) {
            long txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(
                    txnConf.getDbId(), Lists.newArrayList(tblObj.getId()), label,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0,
                            FrontendOptions.getLocalHostAddress(),
                            ExecuteEnv.getInstance().getStartupTime()),
                    sourceType, timeoutSecond);
            txnConf.setTxnId(txnId);
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            txnConf.setToken(token);
        } else {
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
            request.setDb(txnConf.getDb()).setTbl(txnConf.getTbl()).setToken(token)
                    .setLabel(label).setUser("").setUserIp("").setPasswd("");
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
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND).setThriftRpcTimeoutMs(5000).setLoadId(context.queryId())
                .setExecMemLimit(maxExecMemByte).setTimeout((int) timeoutSecond)
                .setTimezone(timeZone).setSendBatchParallelism(sendBatchParallelism).setTrimDoubleQuotes(true);
        if (parsedStmt instanceof NativeInsertStmt && ((NativeInsertStmt) parsedStmt).getTargetColumnNames() != null) {
            NativeInsertStmt nativeInsertStmt = (NativeInsertStmt) parsedStmt;
            if (nativeInsertStmt.containTargetColumnName(Column.SEQUENCE_COL)
                    || nativeInsertStmt.containTargetColumnName(Column.DELETE_SIGN)) {
                if (nativeInsertStmt.containTargetColumnName(Column.SEQUENCE_COL)) {
                    request.setSequenceCol(Column.SEQUENCE_COL);
                }
                request.setColumns("`" + String.join("`,`", nativeInsertStmt.getTargetColumnNames()) + "`");
            }
        }

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        executor.beginTransaction(request);
    }

    // Process an insert statement.
    private void handleInsertStmt() throws Exception {
        if (context.getConnectType() == ConnectType.MYSQL) {
            // Every time set no send flag and clean all data in buffer
            context.getMysqlChannel().reset();
        }
        InsertStmt insertStmt = (InsertStmt) parsedStmt;
        // create plan
        if (insertStmt.getQueryStmt().hasOutFileClause()) {
            throw new DdlException("Not support OUTFILE clause in INSERT statement");
        }

        if (insertStmt.getQueryStmt().isExplain()) {
            ExplainOptions explainOptions = insertStmt.getQueryStmt().getExplainOptions();
            insertStmt.setIsExplain(explainOptions);
            String explainString = planner.getExplainString(explainOptions);
            handleExplainStmt(explainString, false);
            return;
        }

        analyzeVariablesInStmt(insertStmt.getQueryStmt());
        long createTime = System.currentTimeMillis();
        Throwable throwable = null;
        long txnId = -1;
        String label = "";
        long loadedRows = 0;
        int filteredRows = 0;
        TransactionStatus txnStatus = TransactionStatus.ABORTED;
        String errMsg = "";
        TableType tblType = insertStmt.getTargetTable().getType();
        boolean isGroupCommit = false;
        boolean reuseGroupCommitPlan = false;
        if (context.isTxnModel()) {
            if (insertStmt.getQueryStmt() instanceof SelectStmt) {
                if (((SelectStmt) insertStmt.getQueryStmt()).getTableRefs().size() > 0) {
                    throw new TException("Insert into ** select is not supported in a transaction");
                }
            }
            txnStatus = TransactionStatus.PREPARE;
            loadedRows = executeForTxn(insertStmt);
            label = context.getTxnEntry().getLabel();
            txnId = context.getTxnEntry().getTxnConf().getTxnId();
        } else if (insertStmt instanceof NativeInsertStmt && ((NativeInsertStmt) insertStmt).isGroupCommit()) {
            isGroupCommit = true;
            NativeInsertStmt nativeInsertStmt = (NativeInsertStmt) insertStmt;
            long dbId = nativeInsertStmt.getTargetTable().getDatabase().getId();
            long tableId = nativeInsertStmt.getTargetTable().getId();
            int maxRetry = 3;
            for (int i = 0; i < maxRetry; i++) {
                GroupCommitPlanner groupCommitPlanner = nativeInsertStmt.planForGroupCommit(context.queryId);
                reuseGroupCommitPlan = nativeInsertStmt.isReuseGroupCommitPlan();
                List<InternalService.PDataRow> rows = groupCommitPlanner.getRows(nativeInsertStmt);
                PGroupCommitInsertResponse response = groupCommitPlanner.executeGroupCommitInsert(context, rows);
                TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
                ProtocolStringList errorMsgsList = response.getStatus().getErrorMsgsList();
                if (code == TStatusCode.DATA_QUALITY_ERROR && !errorMsgsList.isEmpty() && errorMsgsList.get(0)
                        .contains("schema version not match")) {
                    LOG.info("group commit insert failed. stmt: {}, query_id: {}, db_id: {}, table_id: {}"
                                    + ", schema version: {}, backend_id: {}, status: {}, retry: {}",
                            insertStmt.getOrigStmt().originStmt, DebugUtil.printId(context.queryId()), dbId, tableId,
                            nativeInsertStmt.getBaseSchemaVersion(), groupCommitPlanner.getBackend().getId(),
                            response.getStatus(), i);
                    if (i < maxRetry) {
                        List<TableIf> tables = Lists.newArrayList(insertStmt.getTargetTable());
                        tables.sort((Comparator.comparing(TableIf::getId)));
                        MetaLockUtils.readLockTables(tables);
                        try {
                            insertStmt.reset();
                            analyzer = new Analyzer(context.getEnv(), context);
                            analyzeAndGenerateQueryPlan(context.getSessionVariable().toThrift());
                        } finally {
                            MetaLockUtils.readUnlockTables(tables);
                        }
                        continue;
                    } else {
                        errMsg = "group commit insert failed. db_id: " + dbId + ", table_id: " + tableId
                                + ", query_id: " + DebugUtil.printId(context.queryId()) + ", backend_id: "
                                + groupCommitPlanner.getBackend().getId() + ", status: " + response.getStatus();
                        if (response.hasErrorUrl()) {
                            errMsg += ", error url: " + response.getErrorUrl();
                        }
                    }
                } else if (code != TStatusCode.OK) {
                    errMsg = "group commit insert failed. db_id: " + dbId + ", table_id: " + tableId + ", query_id: "
                            + DebugUtil.printId(context.queryId()) + ", backend_id: " + groupCommitPlanner.getBackend()
                            .getId() + ", status: " + response.getStatus();
                    if (response.hasErrorUrl()) {
                        errMsg += ", error url: " + response.getErrorUrl();
                    }
                    ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
                }
                label = response.getLabel();
                txnStatus = TransactionStatus.PREPARE;
                txnId = response.getTxnId();
                loadedRows = response.getLoadedRows();
                filteredRows = (int) response.getFilteredRows();
                break;
            }
        } else {
            label = insertStmt.getLabel();
            LOG.info("Do insert [{}] with query id: {}", label, DebugUtil.printId(context.queryId()));

            try {
                coord = new Coordinator(context, analyzer, planner, context.getStatsErrorEstimator());
                coord.setLoadZeroTolerance(context.getSessionVariable().getEnableInsertStrict());
                coord.setQueryType(TQueryType.LOAD);
                profile.addExecutionProfile(coord.getExecutionProfile());
                QueryInfo queryInfo = new QueryInfo(ConnectContext.get(), this.getOriginStmtInString(), coord);
                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(), queryInfo);

                Table table = insertStmt.getTargetTable();
                if (table instanceof OlapTable) {
                    boolean isEnableMemtableOnSinkNode =
                            ((OlapTable) table).getTableProperty().getUseSchemaLightChange()
                            ? coord.getQueryOptions().isEnableMemtableOnSinkNode() : false;
                    coord.getQueryOptions().setEnableMemtableOnSinkNode(isEnableMemtableOnSinkNode);
                }
                coord.exec();
                int execTimeout = context.getExecTimeout();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Insert {} execution timeout:{}", DebugUtil.printId(context.queryId()), execTimeout);
                }
                boolean notTimeout = coord.join(execTimeout);
                if (!coord.isDone()) {
                    coord.cancel(Types.PPlanFragmentCancelReason.TIMEOUT, "timeout");
                    if (notTimeout) {
                        errMsg = coord.getExecStatus().getErrorMsg();
                        ErrorReport.reportDdlException("There exists unhealthy backend. "
                                + errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_EXECUTE_TIMEOUT);
                    }
                }

                if (!coord.getExecStatus().ok()) {
                    errMsg = coord.getExecStatus().getErrorMsg();
                    LOG.warn("insert failed: {}", errMsg);
                    ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("delta files is {}", coord.getDeltaUrls());
                }

                if (coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
                    loadedRows = Long.parseLong(coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
                }
                if (coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null) {
                    filteredRows = Integer.parseInt(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
                }

                // if in strict mode, insert will fail if there are filtered rows
                if (context.getSessionVariable().getEnableInsertStrict()) {
                    if (filteredRows > 0) {
                        context.getState().setError(ErrorCode.ERR_FAILED_WHEN_INSERT,
                                "Insert has filtered data in strict mode, tracking_url=" + coord.getTrackingUrl());
                        return;
                    }
                } else {
                    if (filteredRows > context.getSessionVariable().getInsertMaxFilterRatio()
                            * (filteredRows + loadedRows)) {
                        context.getState().setError(ErrorCode.ERR_FAILED_WHEN_INSERT,
                                String.format("Insert has too many filtered data %d/%d insert_max_filter_ratio is %f",
                                        filteredRows, filteredRows + loadedRows,
                                        context.getSessionVariable().getInsertMaxFilterRatio()));
                        return;
                    }
                }

                if (tblType != TableType.OLAP && tblType != TableType.MATERIALIZED_VIEW) {
                    // no need to add load job.
                    // MySQL table is already being inserted.
                    context.getState().setOk(loadedRows, filteredRows, null);
                    return;
                }

                if (Env.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                        insertStmt.getDbObj(), Lists.newArrayList(insertStmt.getTargetTable()),
                        insertStmt.getTransactionId(),
                        TabletCommitInfo.fromThrift(coord.getCommitInfos()),
                        context.getSessionVariable().getInsertVisibleTimeoutMs())) {
                    txnStatus = TransactionStatus.VISIBLE;
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }

            } catch (Throwable t) {
                // if any throwable being thrown during insert operation, first we should abort this txn
                LOG.warn("handle insert stmt fail: {}", label, t);
                try {
                    Env.getCurrentGlobalTransactionMgr().abortTransaction(
                            insertStmt.getDbObj().getId(), insertStmt.getTransactionId(),
                            t.getMessage() == null ? "unknown reason" : t.getMessage());
                } catch (Exception abortTxnException) {
                    // just print a log if abort txn failed. This failure do not need to pass to user.
                    // user only concern abort how txn failed.
                    LOG.warn("errors when abort txn", abortTxnException);
                }

                StringBuilder sb = new StringBuilder(t.getMessage());
                if (!Strings.isNullOrEmpty(coord.getTrackingUrl())) {
                    sb.append(". url: " + coord.getTrackingUrl());
                }
                context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
                return;
            } finally {
                if (coord != null) {
                    coord.close();
                }
                finalizeQuery();
            }

            // Go here, which means:
            // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
            // 2. transaction failed but Config.using_old_load_usage_pattern is true.
            // we will record the load job info for these 2 cases
            txnId = insertStmt.getTransactionId();
            try {
                context.getEnv().getLoadManager()
                        .recordFinishedLoadJob(label, txnId, insertStmt.getDbName(),
                                insertStmt.getTargetTable().getId(),
                                EtlJobType.INSERT, createTime, throwable == null ? "" : throwable.getMessage(),
                                coord.getTrackingUrl(), insertStmt.getUserInfo(), 0L);
            } catch (MetaNotFoundException e) {
                LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
                errMsg = "Record info of insert load with error " + e.getMessage();
            }
        }

        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(label).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(txnId).append("'");
        if (tblType == TableType.MATERIALIZED_VIEW) {
            sb.append("', 'rows':'").append(loadedRows).append("'");
        }
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        if (isGroupCommit) {
            sb.append(", 'query_id':'").append(DebugUtil.printId(context.queryId)).append("'");
            if (reuseGroupCommitPlan) {
                sb.append(", 'reuse_group_commit_plan':'").append(true).append("'");
            }
        }
        sb.append("}");

        context.getState().setOk(loadedRows, filteredRows, sb.toString());

        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        context.setOrUpdateInsertResult(txnId, label, insertStmt.getDbName(), insertStmt.getTbl(),
                txnStatus, loadedRows, filteredRows);
        // update it, so that user can get loaded rows in fe.audit.log
        context.updateReturnRows((int) loadedRows);
    }

    private void handleExternalInsertStmt() {
        // TODO(tsy): load refactor, handle external load here
        try {
            InsertStmt insertStmt = (InsertStmt) parsedStmt;
            LoadType loadType = insertStmt.getLoadType();
            if (loadType == LoadType.UNKNOWN) {
                throw new DdlException("Unknown load job type");
            }
            LoadManagerAdapter loadManagerAdapter = context.getEnv().getLoadManagerAdapter();
            loadManagerAdapter.submitLoadFromInsertStmt(context, insertStmt);
            // when complete
            if (loadManagerAdapter.getMysqlLoadId() != null) {
                this.mysqlLoadId = loadManagerAdapter.getMysqlLoadId();
            }
        } catch (UserException e) {
            // Return message to info client what happened.
            if (LOG.isDebugEnabled()) {
                LOG.debug("DDL statement({}) process failed.", originStmt.originStmt, e);
            }
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
    }

    private void handleUnsupportedStmt() {
        if (context.getConnectType() == ConnectType.MYSQL) {
            context.getMysqlChannel().reset();
        }
        // do nothing
        context.getState().setOk();
    }

    private void handleAnalyzeStmt() throws DdlException, AnalysisException, ExecutionException, InterruptedException {
        context.env.getAnalysisManager().createAnalyze((AnalyzeStmt) parsedStmt, isProxy);
    }

    // Process switch catalog
    private void handleSwitchStmt() throws AnalysisException {
        SwitchStmt switchStmt = (SwitchStmt) parsedStmt;
        try {
            context.getEnv().changeCatalog(context, switchStmt.getCatalogName());
        } catch (DdlException e) {
            LOG.warn("", e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void handlePrepareStmt() throws Exception {
        List<String> labels = ((PrepareStmt) prepareStmt).getColLabelsOfPlaceHolders();
        // register prepareStmt
        if (LOG.isDebugEnabled()) {
            LOG.debug("add prepared statement {}, isBinaryProtocol {}",
                    prepareStmt.toSql(), context.getCommand() == MysqlCommand.COM_STMT_PREPARE);
        }
        context.addPreparedStmt(String.valueOf(context.getStmtId()),
                new PrepareStmtContext(prepareStmt,
                            context, planner, analyzer, String.valueOf(context.getStmtId())));
        if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            sendStmtPrepareOK((int) context.getStmtId(), labels);
        }
    }


    // Process use statement.
    private void handleUseStmt() throws AnalysisException {
        UseStmt useStmt = (UseStmt) parsedStmt;
        try {
            if (useStmt.getCatalogName() != null) {
                context.getEnv().changeCatalog(context, useStmt.getCatalogName());
            }
            context.getEnv().changeDb(context, useStmt.getDatabase());
        } catch (DdlException e) {
            LOG.warn("", e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void sendMetaData(ResultSetMetaData metaData) throws IOException {
        sendMetaData(metaData, null);
    }

    private void sendMetaData(ResultSetMetaData metaData, List<FieldInfo> fieldInfos) throws IOException {
        Preconditions.checkState(context.getConnectType() == ConnectType.MYSQL);
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(metaData.getColumnCount());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < metaData.getColumns().size(); i++) {
            Column col = metaData.getColumn(i);
            serializer.reset();
            if (fieldInfos == null) {
                // TODO(zhaochun): only support varchar type
                serializer.writeField(col.getName(), col.getType());
            } else {
                serializer.writeField(fieldInfos.get(i), col.getType());
            }
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    private List<PrimitiveType> exprToStringType(List<Expr> exprs) {
        return exprs.stream().map(e -> PrimitiveType.STRING).collect(Collectors.toList());
    }

    public void sendStmtPrepareOK(int stmtId, List<String> labels) throws IOException {
        Preconditions.checkState(context.getConnectType() == ConnectType.MYSQL);
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response
        serializer.reset();
        // 0x00 OK
        serializer.writeInt1(0);
        // statement_id
        serializer.writeInt4(stmtId);
        // num_columns
        int numColumns = 0;
        serializer.writeInt2(numColumns);
        // num_params
        int numParams = labels.size();
        serializer.writeInt2(numParams);
        // reserved_1
        serializer.writeInt1(0);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        if (numParams > 0) {
            // send field one by one
            // TODO use real type instead of string, for JDBC client it's ok
            // but for other client, type should be correct
            // List<PrimitiveType> types = exprToStringType(labels);
            List<String> colNames = labels;
            for (int i = 0; i < colNames.size(); ++i) {
                serializer.reset();
                // serializer.writeField(colNames.get(i), Type.fromPrimitiveType(types.get(i)));
                serializer.writeField(colNames.get(i), Type.STRING);
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
            serializer.reset();
            if (!context.getMysqlChannel().clientDeprecatedEOF()) {
                MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
                eofPacket.writeTo(serializer);
            } else {
                MysqlOkPacket okPacket = new MysqlOkPacket(context.getState());
                okPacket.writeTo(serializer);
            }
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        context.getMysqlChannel().flush();
        context.getState().setNoop();
    }

    private void sendFields(List<String> colNames, List<Type> types) throws IOException {
        sendFields(colNames, null, types);
    }

    private void sendFields(List<String> colNames, List<FieldInfo> fieldInfos, List<Type> types) throws IOException {
        Preconditions.checkState(context.getConnectType() == ConnectType.MYSQL);
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("sendFields {}", colNames);
        }
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            if (prepareStmt != null && prepareStmt instanceof  PrepareStmt
                    && context.getCommand() == MysqlCommand.COM_STMT_EXECUTE) {
                // Using PreparedStatment pre serializedField to avoid serialize each time
                // we send a field
                byte[] serializedField = ((PrepareStmt) prepareStmt).getSerializedField(colNames.get(i));
                if (serializedField == null) {
                    if (fieldInfos != null) {
                        serializer.writeField(fieldInfos.get(i), types.get(i));
                    } else {
                        serializer.writeField(colNames.get(i), types.get(i));
                    }
                    serializedField = serializer.toArray();
                    ((PrepareStmt) prepareStmt).setSerializedField(colNames.get(i), serializedField);
                }
                context.getMysqlChannel().sendOnePacket(ByteBuffer.wrap(serializedField));
            } else {
                if (fieldInfos != null) {
                    serializer.writeField(fieldInfos.get(i), types.get(i));
                } else {
                    serializer.writeField(colNames.get(i), types.get(i));
                }
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    public void sendResultSet(ResultSet resultSet) throws IOException {
        sendResultSet(resultSet, null);
    }

    public void sendResultSet(ResultSet resultSet, List<FieldInfo> fieldInfos) throws IOException {
        if (context.getConnectType().equals(ConnectType.MYSQL)) {
            context.updateReturnRows(resultSet.getResultRows().size());
            // Send meta data.
            sendMetaData(resultSet.getMetaData(), fieldInfos);

            // Send result set.
            for (List<String> row : resultSet.getResultRows()) {
                serializer.reset();
                for (String item : row) {
                    if (item == null || item.equals(FeConstants.null_string)) {
                        serializer.writeNull();
                    } else {
                        serializer.writeLenEncodedString(item);
                    }
                }
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }

            context.getState().setEof();
        } else if (context.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
            context.updateReturnRows(resultSet.getResultRows().size());
            context.getFlightSqlChannel()
                    .addResult(DebugUtil.printId(context.queryId()), context.getRunningQuery(), resultSet);
            context.getState().setEof();
        } else {
            LOG.error("sendResultSet error connect type");
        }
    }

    // Process show statement
    private void handleShow() throws IOException, AnalysisException, DdlException {
        ShowExecutor executor = new ShowExecutor(context, (ShowStmt) parsedStmt);
        ShowResultSet resultSet = executor.execute();
        if (resultSet == null) {
            // state changed in execute
            return;
        }
        if (isProxy) {
            proxyShowResultSet = resultSet;
            return;
        }

        sendResultSet(resultSet);
    }

    private void handleUnlockTablesStmt() {
    }

    private void handleLockTablesStmt() {
    }

    public void handleShowConstraintStmt(List<List<String>> result) throws IOException {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Name", ScalarType.createVarchar(20)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("Definition", ScalarType.createVarchar(20)))
                .build();
        ResultSet resultSet = new ShowResultSet(metaData, result);
        sendResultSet(resultSet);
    }

    public void handleShowCreateMTMVStmt(List<List<String>> result) throws IOException {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Materialized View", ScalarType.createVarchar(20)))
                .addColumn(new Column("Create Materialized View", ScalarType.createVarchar(30)))
                .build();
        ResultSet resultSet = new ShowResultSet(metaData, result);
        sendResultSet(resultSet);
    }

    public void handleExplainPlanProcessStmt(List<PlanProcess> result) throws IOException {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Rule", ScalarType.createVarchar(-1)))
                .addColumn(new Column("Before", ScalarType.createVarchar(-1)))
                .addColumn(new Column("After", ScalarType.createVarchar(-1)))
                .build();
        if (context.getConnectType() == ConnectType.MYSQL) {
            sendMetaData(metaData);

            for (PlanProcess row : result) {
                serializer.reset();
                serializer.writeLenEncodedString(row.ruleName);
                serializer.writeLenEncodedString(row.beforeShape);
                serializer.writeLenEncodedString(row.afterShape);
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        }
        context.getState().setEof();
    }

    public void handleExplainStmt(String result, boolean isNereids) throws IOException {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Explain String" + (isNereids ? "(Nereids Planner)" : "(Old Planner)"),
                        ScalarType.createVarchar(20)))
                .build();
        if (context.getConnectType() == ConnectType.MYSQL) {
            sendMetaData(metaData);

            // Send result set.
            for (String item : result.split("\n")) {
                serializer.reset();
                serializer.writeLenEncodedString(item);
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        } else if (context.getConnectType() == ConnectType.ARROW_FLIGHT_SQL) {
            context.getFlightSqlChannel()
                    .addResult(DebugUtil.printId(context.queryId()), context.getRunningQuery(), metaData, result);
            context.setReturnResultFromLocal(true);
        }
        context.getState().setEof();
    }

    private void handleLoadStmt() {
        try {
            LoadStmt loadStmt = (LoadStmt) parsedStmt;
            EtlJobType jobType = loadStmt.getEtlJobType();
            if (jobType == EtlJobType.UNKNOWN) {
                throw new DdlException("Unknown load job type");
            }
            if (jobType == EtlJobType.HADOOP) {
                throw new DdlException("Load job by hadoop cluster is disabled."
                        + " Try using broker load. See 'help broker load;'");
            }
            LoadManager loadManager = context.getEnv().getLoadManager();
            if (jobType == EtlJobType.LOCAL_FILE) {
                if (!context.getCapability().supportClientLocalFile()) {
                    context.getState().setError(ErrorCode.ERR_NOT_ALLOWED_COMMAND, "This client is not support"
                            + " to load client local file.");
                    return;
                }
                String loadId = UUID.randomUUID().toString();
                mysqlLoadId = loadId;
                LoadJobRowResult submitResult = loadManager.getMysqlLoadManager()
                        .executeMySqlLoadJobFromStmt(context, loadStmt, loadId);
                context.getState().setOk(submitResult.getRecords(), submitResult.getWarnings(),
                        submitResult.toString());
            } else {
                loadManager.createLoadJobFromStmt(loadStmt);
                context.getState().setOk();
            }
        } catch (UserException e) {
            // Return message to info client what happened.
            if (LOG.isDebugEnabled()) {
                LOG.debug("DDL statement({}) process failed.", originStmt.originStmt, e);
            }
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
    }

    private void handleUpdateStmt() {
        try {
            UpdateStmt updateStmt = (UpdateStmt) parsedStmt;
            parsedStmt = updateStmt.getInsertStmt();
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("update data error, stmt={}", updateStmt.toSql());
            }
        } catch (Exception e) {
            LOG.warn("update data error, stmt={}", parsedStmt.toSql(), e);
        }
    }

    private void handleDeleteStmt() {
        try {
            DeleteStmt deleteStmt = (DeleteStmt) parsedStmt;
            parsedStmt = deleteStmt.getInsertStmt();
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("delete data error, stmt={}", deleteStmt.toSql());
            }
        } catch (Exception e) {
            LOG.warn("delete data error, stmt={}", parsedStmt.toSql(), e);
        }
    }

    private void handleDdlStmt() {
        try {
            DdlExecutor.execute(context.getEnv(), (DdlStmt) parsedStmt);
            if (!(parsedStmt instanceof AnalyzeStmt)) {
                context.getState().setOk();
            }
        } catch (QueryStateException e) {
            LOG.warn("", e);
            context.setState(e.getQueryState());
        } catch (UserException e) {
            // Return message to info client what happened.
            LOG.warn("DDL statement({}) process failed.", originStmt.originStmt, e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
    }

    private void handleExportStmt() throws Exception {
        ExportStmt exportStmt = (ExportStmt) parsedStmt;
        context.getEnv().getExportMgr().addExportJobAndRegisterTask(exportStmt.getExportJob());
    }

    private void handleCtasStmt() {
        CreateTableAsSelectStmt ctasStmt = (CreateTableAsSelectStmt) this.parsedStmt;
        try {
            // create table
            DdlExecutor.execute(context.getEnv(), ctasStmt);
            context.getState().setOk();
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("CTAS create table error, stmt={}", originStmt.originStmt, e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            return;
        }
        if (ctasStmt.isTableHasExists()) {
            return;
        }
        // after success create table insert data
        try {
            parsedStmt = ctasStmt.getInsertStmt();
            parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("CTAS insert data error, stmt={}", ctasStmt.toSql());
                handleCtasRollback(ctasStmt.getCreateTableStmt().getDbTbl());
            }
        } catch (Exception e) {
            LOG.warn("CTAS insert data error, stmt={}", ctasStmt.toSql(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            handleCtasRollback(ctasStmt.getCreateTableStmt().getDbTbl());
        }
    }

    private void handleCtasRollback(TableName table) {
        if (context.getSessionVariable().isDropTableIfCtasFailed()) {
            // insert error drop table
            DropTableStmt dropTableStmt = new DropTableStmt(true, table, true);
            try {
                DdlExecutor.execute(context.getEnv(), dropTableStmt);
            } catch (Exception ex) {
                LOG.warn("CTAS drop table error, stmt={}", parsedStmt.toSql(), ex);
                context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + ex.getMessage());
            }
        }
    }

    private void handleIotStmt() throws AnalysisException {
        ConnectContext.get().setSkipAuth(true);
        try {
            InsertOverwriteTableStmt iotStmt = (InsertOverwriteTableStmt) this.parsedStmt;
            if (iotStmt.getPartitionNames().size() == 0) {
                // insert overwrite table
                handleOverwriteTable(iotStmt);
            } else if (iotStmt.isAutoDetectPartition()) {
                // insert overwrite table auto detect which partitions need to replace
                handleAutoOverwritePartition(iotStmt);
            } else {
                // insert overwrite table with partition
                handleOverwritePartition(iotStmt);
            }
        } finally {
            ConnectContext.get().setSkipAuth(false);
        }
    }

    private void handleOverwriteTable(InsertOverwriteTableStmt iotStmt) {
        UUID uuid = UUID.randomUUID();
        // to comply with naming rules
        TableName tmpTableName = new TableName(null, iotStmt.getDb(), "tmp_table_" + uuid.toString().replace('-', '_'));
        TableName targetTableName = new TableName(null, iotStmt.getDb(), iotStmt.getTbl());
        try {
            // create a tmp table with uuid
            parsedStmt = new CreateTableLikeStmt(false, tmpTableName, targetTableName, null, false);
            parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            execute();
            // if create tmp table err, return
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                // There is already an error message in the execute() function, so there is no need to set it here
                LOG.warn("IOT create table error, stmt={}", originStmt.originStmt);
                return;
            }
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("IOT create a tmp table error, stmt={}", originStmt.originStmt, e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            return;
        }
        // after success create table insert data
        // when overwrite table, allow auto partition or not is controlled by session variable.
        boolean allowAutoPartition = context.getSessionVariable().isEnableAutoCreateWhenOverwrite();
        try {
            parsedStmt = new NativeInsertStmt(tmpTableName, null, new LabelName(iotStmt.getDb(), iotStmt.getLabel()),
                    iotStmt.getQueryStmt(), iotStmt.getHints(), iotStmt.getCols(), allowAutoPartition);
            parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("IOT insert data error, stmt={}", parsedStmt.toSql());
                handleIotRollback(tmpTableName);
                return;
            }
        } catch (Exception e) {
            LOG.warn("IOT insert data error, stmt={}", parsedStmt.toSql(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            handleIotRollback(tmpTableName);
            return;
        }

        // overwrite old table with tmp table
        try {
            List<AlterClause> ops = new ArrayList<>();
            Map<String, String> properties = new HashMap<>();
            properties.put("swap", "false");
            ops.add(new ReplaceTableClause(tmpTableName.getTbl(), properties));
            parsedStmt = new AlterTableStmt(targetTableName, ops);
            parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("IOT overwrite table error, stmt={}", parsedStmt.toSql());
                handleIotRollback(tmpTableName);
                return;
            }
            context.getState().setOk();
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("IOT overwrite table error, stmt={}", parsedStmt.toSql(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            handleIotRollback(tmpTableName);
        }

    }

    private void handleOverwritePartition(InsertOverwriteTableStmt iotStmt) {
        TableName targetTableName = new TableName(null, iotStmt.getDb(), iotStmt.getTbl());
        List<String> partitionNames = iotStmt.getPartitionNames();
        List<String> tempPartitionName = new ArrayList<>();
        try {
            // create tmp partitions with uuid
            for (String partitionName : partitionNames) {
                UUID uuid = UUID.randomUUID();
                // to comply with naming rules
                String tempPartName = "tmp_partition_" + uuid.toString().replace('-', '_');
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new AddPartitionLikeClause(tempPartName, partitionName, true));
                parsedStmt = new AlterTableStmt(targetTableName, ops);
                parsedStmt.setUserInfo(context.getCurrentUserIdentity());
                execute();
                if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                    LOG.warn("IOT create tmp partitions error, stmt={}", originStmt.originStmt);
                    handleIotPartitionRollback(targetTableName, tempPartitionName);
                    return;
                }
                // only when execution succeeded, put the temp partition name into list
                tempPartitionName.add(tempPartName);
            }
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("IOT create tmp table partitions error, stmt={}", originStmt.originStmt, e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            handleIotPartitionRollback(targetTableName, tempPartitionName);
            return;
        }
        // after success add tmp partitions
        // when overwrite partition, auto creating is always disallowed.
        try {
            parsedStmt = new NativeInsertStmt(targetTableName, new PartitionNames(true, tempPartitionName),
                    new LabelName(iotStmt.getDb(), iotStmt.getLabel()), iotStmt.getQueryStmt(),
                    iotStmt.getHints(), iotStmt.getCols(), false);
            parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("IOT insert data error, stmt={}", parsedStmt.toSql());
                handleIotPartitionRollback(targetTableName, tempPartitionName);
                return;
            }
        } catch (Exception e) {
            LOG.warn("IOT insert data error, stmt={}", parsedStmt.toSql(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            handleIotPartitionRollback(targetTableName, tempPartitionName);
            return;
        }

        // overwrite old partition with tmp partition
        try {
            List<AlterClause> ops = new ArrayList<>();
            Map<String, String> properties = new HashMap<>();
            properties.put("use_temp_partition_name", "false");
            ops.add(new ReplacePartitionClause(new PartitionNames(false, partitionNames),
                    new PartitionNames(true, tempPartitionName), properties));
            parsedStmt = new AlterTableStmt(targetTableName, ops);
            parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            execute();
            if (MysqlStateType.ERR.equals(context.getState().getStateType())) {
                LOG.warn("IOT overwrite table partitions error, stmt={}", parsedStmt.toSql());
                handleIotPartitionRollback(targetTableName, tempPartitionName);
                return;
            }
            context.getState().setOk();
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("IOT overwrite table partitions error, stmt={}", parsedStmt.toSql(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
            handleIotPartitionRollback(targetTableName, tempPartitionName);
        }
    }

    private void handleAutoOverwritePartition(InsertOverwriteTableStmt iotStmt) throws AnalysisException {
        throw new AnalysisException(
                "insert overwrite auto detect is not support in legacy planner. use nereids instead");
    }

    private void handleIotRollback(TableName table) {
        // insert error drop the tmp table
        DropTableStmt dropTableStmt = new DropTableStmt(true, table, true);
        try {
            Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), context);
            dropTableStmt.analyze(tempAnalyzer);
            DdlExecutor.execute(context.getEnv(), dropTableStmt);
        } catch (Exception ex) {
            LOG.warn("IOT drop table error, stmt={}", parsedStmt.toSql(), ex);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + ex.getMessage());
        }
    }

    private void handleIotPartitionRollback(TableName targetTableName, List<String> tempPartitionNames) {
        // insert error drop the tmp partitions
        try {
            for (String partitionName : tempPartitionNames) {
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new DropPartitionClause(true, partitionName, true, true));
                AlterTableStmt dropTablePartitionStmt = new AlterTableStmt(targetTableName, ops);
                Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), context);
                dropTablePartitionStmt.analyze(tempAnalyzer);
                DdlExecutor.execute(context.getEnv(), dropTablePartitionStmt);
            }
        } catch (Exception ex) {
            LOG.warn("IOT drop partitions error, stmt={}", parsedStmt.toSql(), ex);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + ex.getMessage());
        }
    }

    public Data.PQueryStatistics getQueryStatisticsForAuditLog() {
        if (statisticsForAuditLog == null) {
            statisticsForAuditLog = Data.PQueryStatistics.newBuilder();
        }
        if (!statisticsForAuditLog.hasScanBytes()) {
            statisticsForAuditLog.setScanBytes(0L);
        }
        if (!statisticsForAuditLog.hasScanRows()) {
            statisticsForAuditLog.setScanRows(0L);
        }
        if (!statisticsForAuditLog.hasReturnedRows()) {
            statisticsForAuditLog.setReturnedRows(0L);
        }
        if (!statisticsForAuditLog.hasCpuMs()) {
            statisticsForAuditLog.setCpuMs(0L);
        }
        return statisticsForAuditLog.build();
    }

    private List<Type> exprToType(List<Expr> exprs) {
        return exprs.stream().map(e -> e.getType()).collect(Collectors.toList());
    }

    public StatementBase setParsedStmt(StatementBase parsedStmt) {
        this.parsedStmt = parsedStmt;
        this.statementContext.setParsedStatement(parsedStmt);
        return parsedStmt;
    }

    public List<ResultRow> executeInternalQuery() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("INTERNAL QUERY: " + originStmt.toString());
        }
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        context.setQueryId(queryId);
        try {
            List<ResultRow> resultRows = new ArrayList<>();
            try {
                if (ConnectContext.get() != null
                        && ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
                    try {
                        parseByNereids();
                        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                                "Nereids only process LogicalPlanAdapter,"
                                        + " but parsedStmt is " + parsedStmt.getClass().getName());
                        context.getState().setNereids(true);
                        context.getState().setIsQuery(true);
                        planner = new NereidsPlanner(statementContext);
                        planner.plan(parsedStmt, context.getSessionVariable().toThrift());
                    } catch (Exception e) {
                        LOG.warn("Fall back to legacy planner, because: {}", e.getMessage(), e);
                        parsedStmt = null;
                        planner = null;
                        context.getState().setNereids(false);
                        analyzer = new Analyzer(context.getEnv(), context);
                        analyze(context.getSessionVariable().toThrift());
                    }
                } else {
                    analyzer = new Analyzer(context.getEnv(), context);
                    analyze(context.getSessionVariable().toThrift());
                }
            } catch (Exception e) {
                LOG.warn("Failed to run internal SQL: {}", originStmt, e);
                throw new RuntimeException("Failed to execute internal SQL. " + Util.getRootCauseMessage(e), e);
            }
            RowBatch batch;
            coord = new Coordinator(context, analyzer, planner, context.getStatsErrorEstimator());
            profile.addExecutionProfile(coord.getExecutionProfile());
            try {
                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                        new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
            } catch (UserException e) {
                throw new RuntimeException("Failed to execute internal SQL. " + Util.getRootCauseMessage(e), e);
            }

            try {
                coord.exec();
            } catch (Exception e) {
                throw new InternalQueryExecutionException(e.getMessage() + Util.getRootCauseMessage(e), e);
            }

            try {
                while (true) {
                    batch = coord.getNext();
                    if (batch == null || batch.isEos()) {
                        LOG.info("Result rows for query {} is {}", DebugUtil.printId(queryId), resultRows.size());
                        return resultRows;
                    } else {
                        if (batch.getBatch().getRows() != null) {
                            context.updateReturnRows(batch.getBatch().getRows().size());
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Batch size for query {} is {}",
                                        DebugUtil.printId(queryId), batch.getBatch().rows.size());
                            }
                        }
                        resultRows.addAll(convertResultBatchToResultRows(batch.getBatch()));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Result size for query {} is currently {}",
                                    DebugUtil.printId(queryId), resultRows.size());
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch internal SQL result. " + Util.getRootCauseMessage(e), e);
            }
        } finally {
            if (coord != null) {
                coord.close();
            }
            AuditLogHelper.logAuditLog(context, originStmt.originStmt, parsedStmt, getQueryStatisticsForAuditLog(),
                    true);
            if (Config.enable_collect_internal_query_profile) {
                updateProfile(true);
            }
            QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
        }
    }

    private List<ResultRow> convertResultBatchToResultRows(TResultBatch batch) {
        List<String> columns = parsedStmt.getColLabels();
        List<ResultRow> resultRows = new ArrayList<>();
        List<ByteBuffer> rows = batch.getRows();
        for (ByteBuffer buffer : rows) {
            List<String> values = Lists.newArrayList();
            InternalQueryBuffer queryBuffer = new InternalQueryBuffer(buffer.slice());

            for (int i = 0; i < columns.size(); i++) {
                String value = queryBuffer.readStringWithLength();
                values.add(value);
            }
            ResultRow resultRow = new ResultRow(values);
            resultRows.add(resultRow);
        }
        return resultRows;
    }

    public Coordinator getCoord() {
        return coord;
    }

    public List<String> getColumns() {
        return parsedStmt.getColLabels();
    }

    public List<Type> getReturnTypes() {
        return exprToType(parsedStmt.getResultExprs());
    }

    private HttpStreamParams generateHttpStreamNereidsPlan(TUniqueId queryId) {
        LOG.info("TUniqueId: {} generate stream load plan", queryId);
        context.setQueryId(queryId);
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        parseByNereids();
        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                "Nereids only process LogicalPlanAdapter, but parsedStmt is " + parsedStmt.getClass().getName());
        context.getState().setNereids(true);
        InsertIntoTableCommand insert = (InsertIntoTableCommand) ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        HttpStreamParams httpStreamParams = new HttpStreamParams();

        try {
            if (!StringUtils.isEmpty(context.getSessionVariable().groupCommit)) {
                if (!Config.wait_internal_group_commit_finish && insert.getLabelName().isPresent()) {
                    throw new AnalysisException("label and group_commit can't be set at the same time");
                }
                context.setGroupCommitStreamLoadSql(true);
            }
            OlapInsertExecutor insertExecutor = (OlapInsertExecutor) insert.initPlan(context, this);
            httpStreamParams.setTxnId(insertExecutor.getTxnId());
            httpStreamParams.setDb(insertExecutor.getDatabase());
            httpStreamParams.setTable(insertExecutor.getTable());
            httpStreamParams.setLabel(insertExecutor.getLabelName());

            PlanNode planRoot = planner.getFragments().get(0).getPlanRoot();
            boolean isValidPlan = !planner.getScanNodes().isEmpty();
            for (ScanNode scanNode : planner.getScanNodes()) {
                if (!(scanNode instanceof TVFScanNode || planRoot instanceof GroupCommitScanNode)) {
                    isValidPlan = false;
                    break;
                }
            }
            if (!isValidPlan) {
                throw new AnalysisException("plan is invalid: " + planRoot.getExplainString());
            }
        } catch (QueryStateException e) {
            LOG.debug("Command(" + originStmt.originStmt + ") process failed.", e);
            context.setState(e.getQueryState());
            throw new NereidsException("Command(" + originStmt.originStmt + ") process failed",
                    new AnalysisException(e.getMessage(), e));
        } catch (UserException e) {
            // Return message to info client what happened.
            LOG.debug("Command(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            throw new NereidsException("Command (" + originStmt.originStmt + ") process failed",
                    new AnalysisException(e.getMessage(), e));
        } catch (Exception e) {
            // Maybe our bug
            LOG.debug("Command (" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
            throw new NereidsException("Command (" + originStmt.originStmt + ") process failed.",
                    new AnalysisException(e.getMessage(), e));
        }
        return httpStreamParams;
    }

    private HttpStreamParams generateHttpStreamLegacyPlan(TUniqueId queryId) throws Exception {
        // Due to executing Nereids, it needs to be reset
        planner = null;
        context.getState().setNereids(false);
        context.setTxnEntry(null);
        context.setQueryId(queryId);
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());
        SqlScanner input = new SqlScanner(new StringReader(originStmt.originStmt),
                context.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        parsedStmt = SqlParserUtils.getFirstStmt(parser);
        if (!StringUtils.isEmpty(context.getSessionVariable().groupCommit)) {
            if (!Config.wait_internal_group_commit_finish && ((NativeInsertStmt) parsedStmt).getLabel() != null) {
                throw new AnalysisException("label and group_commit can't be set at the same time");
            }
            ((NativeInsertStmt) parsedStmt).isGroupCommitStreamLoadSql = true;
        }
        NativeInsertStmt insertStmt = (NativeInsertStmt) parsedStmt;
        analyze(context.getSessionVariable().toThrift());
        HttpStreamParams httpStreamParams = new HttpStreamParams();
        httpStreamParams.setTxnId(insertStmt.getTransactionId());
        httpStreamParams.setDb(insertStmt.getDbObj());
        httpStreamParams.setTable(insertStmt.getTargetTable());
        httpStreamParams.setLabel(insertStmt.getLabel());
        return httpStreamParams;
    }

    public HttpStreamParams generateHttpStreamPlan(TUniqueId queryId) throws Exception {
        SessionVariable sessionVariable = context.getSessionVariable();
        HttpStreamParams httpStreamParams = null;
        try {
            if (sessionVariable.isEnableNereidsPlanner()) {
                try {
                    // disable shuffle for http stream (only 1 sink)
                    sessionVariable.disableStrictConsistencyDmlOnce();
                    httpStreamParams = generateHttpStreamNereidsPlan(queryId);
                } catch (NereidsException | ParseException e) {
                    if (context.getMinidump() != null && context.getMinidump().toString(4) != null) {
                        MinidumpUtils.saveMinidumpString(context.getMinidump(), DebugUtil.printId(context.queryId()));
                    }
                    // try to fall back to legacy planner
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("nereids cannot process statement\n{}\n because of {}",
                                originStmt.originStmt, e.getMessage(), e);
                    }
                    if (e instanceof NereidsException && notAllowFallback((NereidsException) e)) {
                        LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                        throw ((NereidsException) e).getException();
                    }
                    if (e instanceof NereidsException
                            && !context.getSessionVariable().enableFallbackToOriginalPlanner) {
                        LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                        throw ((NereidsException) e).getException();
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("fall back to legacy planner on statement:\n{}", originStmt.originStmt);
                    }
                    // Attention: currently exception from nereids does not mean an Exception to user terminal
                    // unless user does not allow fallback to lagency planner. But state of query
                    // has already been set to Error in this case, it will have some side effect on profile result
                    // and audit log. So we need to reset state to OK if query cancel be processd by lagency.
                    context.getState().reset();
                    context.getState().setNereids(false);
                    httpStreamParams = generateHttpStreamLegacyPlan(queryId);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                httpStreamParams = generateHttpStreamLegacyPlan(queryId);
            }
        } finally {
            // revert Session Value
            try {
                VariableMgr.revertSessionValue(sessionVariable);
                // origin value init
                sessionVariable.setIsSingleSetVar(false);
                sessionVariable.clearSessionOriginValue();
            } catch (DdlException e) {
                LOG.warn("failed to revert Session value. {}", context.getQueryIdentifier(), e);
                context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            }
        }
        return httpStreamParams;
    }

    public SummaryProfile getSummaryProfile() {
        return profile.getSummaryProfile();
    }

    public Profile getProfile() {
        return profile;
    }

    public void setProfileType(ProfileType profileType) {
        this.profileType = profileType;
    }


    public void setProxyShowResultSet(ShowResultSet proxyShowResultSet) {
        this.proxyShowResultSet = proxyShowResultSet;
    }

    public ConnectContext getContext() {
        return context;
    }

    public OriginStatement getOriginStmt() {
        return originStmt;
    }

    public String getOriginStmtInString() {
        if (originStmt != null && originStmt.originStmt != null) {
            return originStmt.originStmt;
        }
        return "";
    }

    public List<ByteBuffer> getProxyQueryResultBufList() {
        return ((ProxyMysqlChannel) context.getMysqlChannel()).getProxyResultBufferList();
    }

    public void sendProxyQueryResult() throws IOException {
        if (masterOpExecutor == null) {
            return;
        }
        List<ByteBuffer> queryResultBufList = masterOpExecutor.getQueryResultBufList();
        for (ByteBuffer byteBuffer : queryResultBufList) {
            context.getMysqlChannel().sendOnePacket(byteBuffer);
        }
    }
}
