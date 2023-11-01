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
import org.apache.doris.analysis.PrepareStmt;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.ReplaceTableClause;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtRewriter;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TransactionBeginStmt;
import org.apache.doris.analysis.TransactionCommitStmt;
import org.apache.doris.analysis.TransactionRollbackStmt;
import org.apache.doris.analysis.TransactionStmt;
import org.apache.doris.analysis.UnifiedLoadStmt;
import org.apache.doris.analysis.UnlockTablesStmt;
import org.apache.doris.analysis.UnsupportedStmt;
import org.apache.doris.analysis.UpdateStmt;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
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
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.LoadJobRowResult;
import org.apache.doris.load.loadv2.LoadManager;
import org.apache.doris.load.loadv2.LoadManagerAdapter;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.Forward;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertRequest;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueOfferToken;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.service.arrowflight.FlightStatementExecutor;
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
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
    private final Object writeProfileLock = new Object();
    private ConnectContext context;
    private final StatementContext statementContext;
    private MysqlSerializer serializer;
    private OriginStatement originStmt;
    private StatementBase parsedStmt;
    private Analyzer analyzer;
    private ProfileType profileType = ProfileType.QUERY;
    private volatile Coordinator coord = null;
    private MasterOpExecutor masterOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    private Planner planner;
    private boolean isProxy;
    private ShowResultSet proxyResultSet = null;
    private Data.PQueryStatistics.Builder statisticsForAuditLog;
    private boolean isCached;
    private String stmtName;
    private PrepareStmt prepareStmt = null;
    private String mysqlLoadId;
    // Distinguish from prepare and execute command
    private boolean isExecuteStmt = false;
    // The profile of this execution
    private final Profile profile;

    // The result schema if "dry_run_query" is true.
    // Only one column to indicate the real return row numbers.
    private static final CommonResultSetMetaData DRY_RUN_QUERY_METADATA = new CommonResultSetMetaData(
            Lists.newArrayList(new Column("ReturnedRows", PrimitiveType.STRING)));

    // this constructor is mainly for proxy
    public StmtExecutor(ConnectContext context, OriginStatement originStmt, boolean isProxy) {
        this.context = context;
        this.originStmt = originStmt;
        this.serializer = context.getMysqlChannel().getSerializer();
        this.isProxy = isProxy;
        this.statementContext = new StatementContext(context, originStmt);
        this.context.setStatementContext(statementContext);
        this.profile = new Profile("Query", this.context.getSessionVariable().enableProfile);
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
        this.serializer = context.getMysqlChannel().getSerializer();
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
        this.profile = new Profile("Query", context.getSessionVariable().enableProfile());
    }

    public static InternalService.PDataRow getRowStringValue(List<Expr> cols) throws UserException {
        if (cols.isEmpty()) {
            return null;
        }
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (Expr expr : cols) {
            if (!expr.isLiteralOrCastExpr()) {
                throw new UserException(
                        "do not support non-literal expr in transactional insert operation: " + expr.toSql());
            }
            if (expr instanceof NullLiteral) {
                row.addColBuilder().setValue(NULL_VALUE_FOR_LOAD);
            } else if (expr instanceof ArrayLiteral) {
                row.addColBuilder().setValue(expr.getStringValueForArray());
            } else {
                row.addColBuilder().setValue(expr.getStringValue());
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
        builder.defaultDb(context.getDatabase());
        builder.workloadGroup(context.getWorkloadGroupName());
        builder.sqlStatement(originStmt.originStmt);
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

    public void addProfileToSpan() {
        Span span = Span.fromContext(Context.current());
        if (!span.isRecording()) {
            return;
        }
        for (Map.Entry<String, String> entry : getSummaryInfo(true).entrySet()) {
            span.setAttribute(entry.getKey(), entry.getValue());
        }
    }

    public Planner planner() {
        return planner;
    }

    public void setPlanner(Planner planner) {
        this.planner = planner;
    }

    public boolean isForwardToMaster() {
        if (Env.getCurrentEnv().isMaster()) {
            return false;
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if (isQuery() && !Env.getCurrentEnv().isMaster()
                && !Env.getCurrentEnv().canRead()) {
            return true;
        }

        if (redirectStatus == null) {
            return false;
        } else {
            return redirectStatus.isForwardToMaster();
        }
    }

    public ByteBuffer getOutputPacket() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getOutputPacket();
        }
    }

    public ShowResultSet getProxyResultSet() {
        return proxyResultSet;
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

    public boolean isSyncLoadKindStmt() {
        if (parsedStmt == null) {
            return false;
        }
        if (parsedStmt instanceof LogicalPlanAdapter) {
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            return logicalPlan instanceof InsertIntoTableCommand
                    || (logicalPlan instanceof CreateTableCommand
                    && ((CreateTableCommand) logicalPlan).isCtasCommand());
        }
        return parsedStmt instanceof InsertStmt || parsedStmt instanceof CreateTableAsSelectStmt;
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
     * an unresolved LogicalPlan wrapped with a LogicalPlanAdapter for Nereids.
     */
    public StatementBase getParsedStmt() {
        return parsedStmt;
    }

    // query with a random sql
    public void execute() throws Exception {
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        execute(queryId);
    }

    public void execute(TUniqueId queryId) throws Exception {
        SessionVariable sessionVariable = context.getSessionVariable();
        Span executeSpan = context.getTracer().spanBuilder("execute").setParent(Context.current()).startSpan();
        try (Scope scope = executeSpan.makeCurrent()) {
            if (parsedStmt instanceof LogicalPlanAdapter
                    || (parsedStmt == null && sessionVariable.isEnableNereidsPlanner())) {
                try {
                    executeByNereids(queryId);
                } catch (NereidsException | ParseException e) {
                    if (context.getMinidump() != null) {
                        MinidumpUtils.saveMinidumpString(context.getMinidump(), DebugUtil.printId(context.queryId()));
                    }
                    // try to fall back to legacy planner
                    LOG.debug("nereids cannot process statement\n" + originStmt.originStmt
                            + "\n because of " + e.getMessage(), e);
                    if (e instanceof NereidsException
                            && !context.getSessionVariable().enableFallbackToOriginalPlanner) {
                        LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                        throw ((NereidsException) e).getException();
                    }
                    LOG.debug("fall back to legacy planner on statement:\n{}", originStmt.originStmt);
                    parsedStmt = null;
                    planner = null;
                    context.getState().setNereids(false);
                    executeByLegacy(queryId);
                }
            } else {
                executeByLegacy(queryId);
            }
        } finally {
            executeSpan.end();
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
            if (scanNode instanceof OlapScanNode) {
                OlapScanNode olapScanNode = (OlapScanNode) scanNode;
                Env.getCurrentEnv().getSqlBlockRuleMgr().checkLimitations(
                        olapScanNode.getSelectedPartitionNum().longValue(),
                        olapScanNode.getSelectedTabletsNum(),
                        olapScanNode.getCardinality(),
                        context.getQualifiedUser());
            }
        }
    }

    private void executeByNereids(TUniqueId queryId) throws Exception {
        LOG.debug("Nereids start to execute query:\n {}", originStmt.originStmt);
        context.setQueryId(queryId);
        context.setStartTime();
        profile.getSummaryProfile().setQueryBeginTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        parseByNereids();
        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                "Nereids only process LogicalPlanAdapter, but parsedStmt is " + parsedStmt.getClass().getName());
        context.getState().setNereids(true);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        if (logicalPlan instanceof Command) {
            if (logicalPlan instanceof Forward) {
                redirectStatus = ((Forward) logicalPlan).toRedirectStatus();
                if (isForwardToMaster()) {
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
            try {
                ((Command) logicalPlan).run(context, this);
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
        } else {
            context.getState().setIsQuery(true);
            if (context.getSessionVariable().enableProfile) {
                ConnectContext.get().setStatsErrorEstimator(new StatsErrorEstimator());
            }
            // create plan
            planner = new NereidsPlanner(statementContext);
            try {
                planner.plan(parsedStmt, context.getSessionVariable().toThrift());
                checkBlockRules();
            } catch (Exception e) {
                LOG.debug("Nereids plan query failed:\n{}", originStmt.originStmt);
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
            statements = new NereidsParser().parseSQL(originStmt.originStmt);
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        if (statements.size() <= originStmt.idx) {
            throw new ParseException("Nereids parse failed. Parser get " + statements.size() + " statements,"
                            + " but we need at least " + originStmt.idx + " statements.");
        }
        parsedStmt = statements.get(originStmt.idx);
    }

    private void handleQueryWithRetry(TUniqueId queryId) throws Exception {
        // queue query here
        syncJournalIfNeeded();
        QueueOfferToken offerRet = null;
        QueryQueue queryQueue = null;
        if (!parsedStmt.isExplain() && Config.enable_workload_group && Config.enable_query_queue
                && context.getSessionVariable().getEnablePipelineEngine()) {
            queryQueue = context.getEnv().getWorkloadGroupMgr().getWorkloadGroupQueryQueue(context);
            try {
                offerRet = queryQueue.offer();
            } catch (InterruptedException e) {
                // this Exception means try lock/await failed, so no need to handle offer result
                LOG.error("error happens when offer queue, query id=" + DebugUtil.printId(queryId) + " ", e);
                throw new RuntimeException("interrupted Exception happens when queue query");
            }
            if (offerRet != null && !offerRet.isOfferSuccess()) {
                String retMsg = "queue failed, reason=" + offerRet.getOfferResultDetail();
                LOG.error("query (id=" + DebugUtil.printId(queryId) + ") " + retMsg);
                throw new UserException(retMsg);
            }
        }

        int retryTime = Config.max_query_retry_time;
        try {
            for (int i = 0; i < retryTime; i++) {
                try {
                    //reset query id for each retry
                    if (i > 0) {
                        UUID uuid = UUID.randomUUID();
                        TUniqueId newQueryId = new TUniqueId(uuid.getMostSignificantBits(),
                                uuid.getLeastSignificantBits());
                        AuditLog.getQueryAudit().log("Query {} {} times with new query id: {}",
                                DebugUtil.printId(queryId), i, DebugUtil.printId(newQueryId));
                        context.setQueryId(newQueryId);
                    }
                    handleQueryStmt();
                    break;
                } catch (RpcException e) {
                    if (i == retryTime - 1) {
                        throw e;
                    }
                    if (!context.getMysqlChannel().isSend()) {
                        LOG.warn("retry {} times. stmt: {}", (i + 1), parsedStmt.getOrigStmt().originStmt);
                    } else {
                        throw e;
                    }
                } finally {
                    // The final profile report occurs after be returns the query data, and the profile cannot be
                    // received after unregisterQuery(), causing the instance profile to be lost, so we should wait
                    // for the profile before unregisterQuery().
                    updateProfile(true);
                    QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
                }
            }
        } finally {
            if (offerRet != null && offerRet.isOfferSuccess()) {
                queryQueue.poll();
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
                Span queryAnalysisSpan =
                        context.getTracer().spanBuilder("query analysis").setParent(Context.current()).startSpan();
                try (Scope ignored = queryAnalysisSpan.makeCurrent()) {
                    // analyze this query
                    analyze(context.getSessionVariable().toThrift());
                } catch (Exception e) {
                    queryAnalysisSpan.recordException(e);
                    throw e;
                } finally {
                    queryAnalysisSpan.end();
                }
                if (isForwardToMaster()) {
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
                    LOG.debug("no need to transfer to Master. stmt: {}", context.getStmtId());
                }
            } else {
                analyzer = new Analyzer(context.getEnv(), context);
                parsedStmt.analyze(analyzer);
            }

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
            if (optHints != null) {
                sessionVariable.setIsSingleSetVar(true);
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
        LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());
        masterOpExecutor.execute();
        if (parsedStmt instanceof SetStmt) {
            SetStmt setStmt = (SetStmt) parsedStmt;
            setStmt.modifySetVarsForExecute();
            for (SetVar var : setStmt.getSetVars()) {
                VariableMgr.setVarForNonMasterFE(context.getSessionVariable(), var);
            }
        }
    }

    public void updateProfile(boolean isFinished) {
        if (!context.getSessionVariable().enableProfile()) {
            return;
        }

        profile.update(context.startTime, getSummaryInfo(isFinished), isFinished,
                context.getSessionVariable().profileLevel, this.planner,
                context.getSessionVariable().getEnablePipelineXEngine());
    }

    // Analyze one statement to structure in memory.
    public void analyze(TQueryOptions tQueryOptions) throws UserException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to analyze stmt: {}, forwarded stmt id: {}", context.getStmtId(),
                    context.getForwardedStmtId());
        }

        parseByLegacy();

        boolean preparedStmtReanalyzed = false;
        PrepareStmtContext preparedStmtCtx = null;
        if (parsedStmt instanceof ExecuteStmt) {
            ExecuteStmt execStmt = (ExecuteStmt) parsedStmt;
            preparedStmtCtx = context.getPreparedStmt(execStmt.getName());
            if (preparedStmtCtx == null) {
                throw new UserException("Could not execute, since `" + execStmt.getName() + "` not exist");
            }
            // parsedStmt may already by set when constructing this StmtExecutor();
            preparedStmtCtx.stmt.asignValues(execStmt.getArgs());
            parsedStmt = preparedStmtCtx.stmt.getInnerStmt();
            planner = preparedStmtCtx.planner;
            analyzer = preparedStmtCtx.analyzer;
            prepareStmt = preparedStmtCtx.stmt;
            LOG.debug("already prepared stmt: {}", preparedStmtCtx.stmtString);
            isExecuteStmt = true;
            if (!preparedStmtCtx.stmt.needReAnalyze()) {
                // Return directly to bypass analyze and plan
                return;
            }
            // continue analyze
            preparedStmtReanalyzed = true;
            preparedStmtCtx.stmt.analyze(analyzer);
        }

        // yiguolei: insert stmt's grammar analysis will write editlog,
        // so that we check if the stmt should be forward to master here
        // if the stmt should be forward to master, then just return here and the master will do analysis again
        if (isForwardToMaster()) {
            return;
        }

        analyzer = new Analyzer(context.getEnv(), context);

        if (parsedStmt instanceof PrepareStmt || context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
                prepareStmt = new PrepareStmt(parsedStmt,
                        String.valueOf(context.getEnv().getNextStmtId()), true /*binary protocol*/);
            } else {
                prepareStmt = (PrepareStmt) parsedStmt;
            }
            prepareStmt.setContext(context);
            prepareStmt.analyze(analyzer);
            // Need analyze inner statement
            parsedStmt = prepareStmt.getInnerStmt();
            if (prepareStmt.getPreparedType() == PrepareStmt.PreparedType.STATEMENT) {
                // Skip analyze, do it lazy
                return;
            }
        }

        // Convert show statement to select statement here
        if (parsedStmt instanceof ShowStmt) {
            SelectStmt selectStmt = ((ShowStmt) parsedStmt).toSelectStmt(analyzer);
            if (selectStmt != null) {
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
        if (preparedStmtReanalyzed) {
            LOG.debug("update planner and analyzer after prepared statement reanalyzed");
            preparedStmtCtx.planner = planner;
            preparedStmtCtx.analyzer = analyzer;
            Preconditions.checkNotNull(preparedStmtCtx.stmt);
            preparedStmtCtx.analyzer.setPrepareStmt(preparedStmtCtx.stmt);
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
        parsedStmt.analyze(analyzer);
        if (parsedStmt instanceof QueryStmt || parsedStmt instanceof InsertStmt) {
            if (parsedStmt instanceof NativeInsertStmt && ((NativeInsertStmt) parsedStmt).isGroupCommit()) {
                LOG.debug("skip generate query plan for group commit insert");
                return;
            }
            ExprRewriter rewriter = analyzer.getExprRewriter();
            rewriter.reset();
            if (context.getSessionVariable().isEnableFoldConstantByBe()) {
                // fold constant expr
                parsedStmt.foldConstant(rewriter, tQueryOptions);

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
                if (StmtRewriter.rewriteByPolicy(parsedStmt, analyzer)) {
                    reAnalyze = true;
                }
            }
            if (parsedStmt instanceof SetOperationStmt) {
                List<SetOperationStmt.SetOperand> operands = ((SetOperationStmt) parsedStmt).getOperands();
                for (SetOperationStmt.SetOperand operand : operands) {
                    if (StmtRewriter.rewriteByPolicy(operand.getQueryStmt(), analyzer)) {
                        reAnalyze = true;
                    }
                }
            }
            if (parsedStmt instanceof InsertStmt) {
                QueryStmt queryStmt = ((InsertStmt) parsedStmt).getQueryStmt();
                if (queryStmt != null && StmtRewriter.rewriteByPolicy(queryStmt, analyzer)) {
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

                if (prepareStmt != null) {
                    // Re-analyze prepareStmt with a new analyzer
                    prepareStmt.reset();
                    prepareStmt.analyze(analyzer);
                }
                // query re-analyze
                parsedStmt.reset();
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
    public void cancel() {
        Coordinator coordRef = coord;
        if (coordRef != null) {
            coordRef.cancel();
        }
        if (mysqlLoadId != null) {
            Env.getCurrentEnv().getLoadManager().getMysqlLoadManager().cancelMySqlLoad(mysqlLoadId);
        }
        if (parsedStmt instanceof AnalyzeTblStmt || parsedStmt instanceof AnalyzeDBStmt) {
            Env.getCurrentEnv().getAnalysisManager().cancelSyncTask(context);
        }
    }

    // Handle kill statement.
    private void handleKill() throws DdlException {
        KillStmt killStmt = (KillStmt) parsedStmt;
        int id = killStmt.getConnectionId();
        ConnectContext killCtx = context.getConnectScheduler().getContext(id);
        if (killCtx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, id);
        }
        if (context == killCtx) {
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
                sendFields(selectStmt.getColLabels(), exprToType(selectStmt.getResultExprs()));
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
                sendFields(selectStmt.getColLabels(), exprToType(selectStmt.getResultExprs()));
                isSend = true;
            }
            context.getState().setEof();
        }
        return isSend;
    }

    /**
     * Handle the SelectStmt via Cache.
     */
    private void handleCacheStmt(CacheAnalyzer cacheAnalyzer, MysqlChannel channel)
            throws Exception {
        InternalService.PFetchCacheResult cacheResult = cacheAnalyzer.getCacheData();
        if (cacheResult == null) {
            if (ConnectContext.get() != null
                    && !ConnectContext.get().getSessionVariable().testQueryCacheHit.equals("none")) {
                throw new UserException("The variable test_query_cache_hit is set to "
                        + ConnectContext.get().getSessionVariable().testQueryCacheHit
                        + ", but the query cache is not hit.");
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
        sendResult(false, isSendFields, queryStmt, channel, cacheAnalyzer, cacheResult);
    }

    // Process a select statement.
    private void handleQueryStmt() throws Exception {
        LOG.info("Handling query {} with query id {}",
                          originStmt.originStmt, DebugUtil.printId(context.queryId));
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        Queriable queryStmt = (Queriable) parsedStmt;

        QueryDetail queryDetail = new QueryDetail(context.getStartTime(),
                DebugUtil.printId(context.queryId()),
                context.getStartTime(), -1, -1,
                QueryDetail.QueryMemState.RUNNING,
                context.getDatabase(),
                originStmt.originStmt);
        context.setQueryDetail(queryDetail);
        QueryDetailQueue.addOrUpdateQueryDetail(queryDetail);

        if (queryStmt.isExplain()) {
            String explainString = planner.getExplainString(queryStmt.getExplainOptions());
            handleExplainStmt(explainString, false);
            LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
            return;
        }

        // handle selects that fe can do without be, so we can make sql tools happy, especially the setup step.
        Optional<ResultSet> resultSet = planner.handleQueryInFe(parsedStmt);
        if (resultSet.isPresent()) {
            sendResultSet(resultSet.get());
            LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
            return;
        }

        MysqlChannel channel = context.getMysqlChannel();
        boolean isOutfileQuery = queryStmt.hasOutFileClause();

        // Sql and PartitionCache
        CacheAnalyzer cacheAnalyzer = new CacheAnalyzer(context, parsedStmt, planner);
        if (cacheAnalyzer.enableCache() && !isOutfileQuery
                && context.getSessionVariable().getSqlSelectLimit() < 0
                && context.getSessionVariable().getDefaultOrderByLimit() < 0) {
            if (queryStmt instanceof QueryStmt || queryStmt instanceof LogicalPlanAdapter) {
                handleCacheStmt(cacheAnalyzer, channel);
                LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
                return;
            }
        }

        // handle select .. from xx  limit 0
        if (parsedStmt instanceof SelectStmt) {
            SelectStmt parsedSelectStmt = (SelectStmt) parsedStmt;
            if (parsedSelectStmt.getLimit() == 0) {
                LOG.info("ignore handle limit 0 ,sql:{}", parsedSelectStmt.toSql());
                sendFields(queryStmt.getColLabels(), exprToType(queryStmt.getResultExprs()));
                context.getState().setEof();
                LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
                return;
            }
        }

        sendResult(isOutfileQuery, false, queryStmt, channel, null, null);
        LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
    }

    private void sendResult(boolean isOutfileQuery, boolean isSendFields, Queriable queryStmt, MysqlChannel channel,
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
        if (queryStmt instanceof SelectStmt && ((SelectStmt) parsedStmt).isPointQueryShortCircuit()) {
            coordBase = new PointQueryExec(planner, analyzer);
        } else {
            coord = new Coordinator(context, analyzer, planner, context.getStatsErrorEstimator());
            if (Config.enable_workload_group && context.sessionVariable.getEnablePipelineEngine()) {
                coord.setTWorkloadGroups(context.getEnv().getWorkloadGroupMgr().getWorkloadGroup(context));
            } else {
                context.setWorkloadGroupName("");
            }
            QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                    new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
            profile.addExecutionProfile(coord.getExecutionProfile());
            coordBase = coord;
        }
        Span queryScheduleSpan =
                context.getTracer().spanBuilder("query schedule").setParent(Context.current()).startSpan();
        try (Scope scope = queryScheduleSpan.makeCurrent()) {
            coordBase.exec();
        } catch (Exception e) {
            queryScheduleSpan.recordException(e);
            throw e;
        } finally {
            queryScheduleSpan.end();
        }
        profile.getSummaryProfile().setQueryScheduleFinishTime();
        updateProfile(false);
        if (coordBase.getInstanceTotalNum() > 1 && LOG.isDebugEnabled()) {
            try {
                LOG.debug("Start to execute fragment. user: {}, db: {}, sql: {}, fragment instance num: {}",
                        context.getQualifiedUser(), context.getDatabase(),
                        parsedStmt.getOrigStmt().originStmt.replace("\n", " "),
                        coordBase.getInstanceTotalNum());
            } catch (Exception e) {
                LOG.warn("Fail to print fragment concurrency for Query.", e);
            }
        }

        Span fetchResultSpan = context.getTracer().spanBuilder("fetch result").setParent(Context.current()).startSpan();
        try (Scope scope = fetchResultSpan.makeCurrent()) {
            while (true) {
                // register the fetch result time.
                profile.getSummaryProfile().setTempStartTime();
                batch = coordBase.getNext();
                profile.getSummaryProfile().freshFetchResultConsumeTime();

                // for outfile query, there will be only one empty batch send back with eos flag
                // call `copyRowBatch()` first, because batch.getBatch() may be null, if result set is empty
                if (cacheAnalyzer != null && !isOutfileQuery) {
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
                            sendFields(queryStmt.getColLabels(), exprToType(queryStmt.getResultExprs()));
                        } else {
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
            if (cacheAnalyzer != null) {
                if (cacheResult != null && cacheAnalyzer.getHitRange() == Cache.HitRange.Right) {
                    isSendFields =
                            sendCachedValues(channel, cacheResult.getValuesList(), (Queriable) queryStmt, isSendFields,
                                    false);
                }

                cacheAnalyzer.updateCache();
            }
            if (!isSendFields) {
                if (!isOutfileQuery) {
                    if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().dryRunQuery) {
                        // Return a one row one column result set, with the real result number
                        List<String> data = Lists.newArrayList(batch.getQueryStatistics() == null ? "0"
                                : batch.getQueryStatistics().getReturnedRows() + "");
                        ResultSet resultSet = new CommonResultSet(DRY_RUN_QUERY_METADATA,
                                Collections.singletonList(data));
                        sendResultSet(resultSet);
                        return;
                    } else {
                        sendFields(queryStmt.getColLabels(), exprToType(queryStmt.getResultExprs()));
                    }
                } else {
                    sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                }
            }

            statisticsForAuditLog = batch.getQueryStatistics() == null ? null : batch.getQueryStatistics().toBuilder();
            context.getState().setEof();
            profile.getSummaryProfile().setQueryFetchResultFinishTime();
        } catch (Exception e) {
            // notify all be cancel runing fragment
            // in some case may block all fragment handle threads
            // details see issue https://github.com/apache/doris/issues/16203
            LOG.warn("cancel fragment query_id:{} cause {}", DebugUtil.printId(context.queryId()), e.getMessage());
            coordBase.cancel(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
            fetchResultSpan.recordException(e);
            throw e;
        } finally {
            fetchResultSpan.end();
            if (coordBase.getInstanceTotalNum() > 1 && LOG.isDebugEnabled()) {
                try {
                    LOG.debug("Finish to execute fragment. user: {}, db: {}, sql: {}, fragment instance num: {}",
                            context.getQualifiedUser(), context.getDatabase(),
                            parsedStmt.getOrigStmt().originStmt.replace("\n", " "),
                            coordBase.getInstanceTotalNum());
                } catch (Exception e) {
                    LOG.warn("Fail to print fragment concurrency for Query.", e);
                }
            }
        }
    }

    private TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request) throws Exception {
        TWaitingTxnStatusResult statusResult = null;
        if (Env.getCurrentEnv().isMaster()) {
            statusResult = Env.getCurrentGlobalTransactionMgr()
                    .getWaitingTxnStatus(request);
        } else {
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            statusResult = masterTxnExecutor.getWaitingTxnStatus(request);
        }
        return statusResult;
    }

    private void handleTransactionStmt() throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        context.getState().setOk(0, 0, "");
        // create plan
        if (context.getTxnEntry() != null && context.getTxnEntry().getRowsInTransaction() == 0
                && (parsedStmt instanceof TransactionCommitStmt || parsedStmt instanceof TransactionRollbackStmt)) {
            context.setTxnEntry(null);
        } else if (parsedStmt instanceof TransactionBeginStmt) {
            if (context.isTxnModel()) {
                LOG.info("A transaction has already begin");
                return;
            }
            TTxnParams txnParams = new TTxnParams();
            txnParams.setNeedTxn(true).setEnablePipelineTxnLoad(Config.enable_pipeline_load)
                    .setThriftRpcTimeoutMs(5000).setTxnId(-1).setDb("").setTbl("");
            if (context.getSessionVariable().getEnableInsertStrict()) {
                txnParams.setMaxFilterRatio(0);
            } else {
                txnParams.setMaxFilterRatio(1.0);
            }
            if (context.getTxnEntry() == null) {
                context.setTxnEntry(new TransactionEntry());
            }
            context.getTxnEntry().setTxnConf(txnParams);
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

            TTxnParams txnConf = context.getTxnEntry().getTxnConf();
            try {
                InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(context.getTxnEntry());
                if (context.getTxnEntry().getDataToSend().size() > 0) {
                    // send rest data
                    executor.sendData();
                }
                // commit txn
                executor.commitTransaction();

                // wait txn visible
                TWaitingTxnStatusRequest request = new TWaitingTxnStatusRequest();
                request.setDbId(txnConf.getDbId()).setTxnId(txnConf.getTxnId());
                request.setLabelIsSet(false);
                request.setTxnIdIsSet(true);

                TWaitingTxnStatusResult statusResult = getWaitingTxnStatus(request);
                TransactionStatus txnStatus = TransactionStatus.valueOf(statusResult.getTxnStatusId());
                if (txnStatus == TransactionStatus.COMMITTED) {
                    throw new AnalysisException("transaction commit successfully, BUT data will be visible later.");
                } else if (txnStatus != TransactionStatus.VISIBLE) {
                    String errMsg = "commit failed, rollback.";
                    if (statusResult.getStatus().isSetErrorMsgs()
                            && statusResult.getStatus().getErrorMsgs().size() > 0) {
                        errMsg = String.join(". ", statusResult.getStatus().getErrorMsgs());
                    }
                    throw new AnalysisException(errMsg);
                }
                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(context.getTxnEntry().getLabel()).append("', 'status':'")
                        .append(txnStatus.name()).append("', 'txnId':'")
                        .append(context.getTxnEntry().getTxnConf().getTxnId()).append("'").append("}");
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
                // abort txn
                InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(context.getTxnEntry());
                executor.abortTransaction();

                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(context.getTxnEntry().getLabel()).append("', 'status':'")
                        .append(TransactionStatus.ABORTED.name()).append("', 'txnId':'")
                        .append(context.getTxnEntry().getTxnConf().getTxnId()).append("'").append("}");
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
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                // the value columns are columns which are visible to user, so here we use
                // getBaseSchema(), not getFullSchema()
                if (schemaSize != row.size()) {
                    throw new TException("Column count doesn't match value count");
                }
            }
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                ++effectRows;
                InternalService.PDataRow data = StmtExecutor.getRowStringValue(row);
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
                    txnConf.getDbId(), Lists.newArrayList(tblObj.getId()),
                    label, new TransactionState.TxnCoordinator(
                            TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    sourceType, timeoutSecond);
            txnConf.setTxnId(txnId);
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            txnConf.setToken(token);
        } else {
            String token = Env.getCurrentEnv().getLoadManager().getTokenManager().acquireToken();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
            request.setDb(txnConf.getDb()).setTbl(txnConf.getTbl()).setToken(token)
                    .setCluster(dbObj.getClusterName()).setLabel(label).setUser("").setUserIp("").setPasswd("");
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
                .setTimezone(timeZone).setSendBatchParallelism(sendBatchParallelism);

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        executor.beginTransaction(request);
    }

    // Process an insert statement.
    private void handleInsertStmt() throws Exception {
        // Every time set no send flag and clean all data in buffer
        if (context.getMysqlChannel() != null) {
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
            NativeInsertStmt nativeInsertStmt = (NativeInsertStmt) insertStmt;
            Backend backend = context.getInsertGroupCommit(insertStmt.getTargetTable().getId());
            if (backend == null || !backend.isAlive()) {
                List<Long> allBackendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
                if (allBackendIds.isEmpty()) {
                    throw new DdlException("No alive backend");
                }
                Collections.shuffle(allBackendIds);
                backend = Env.getCurrentSystemInfo().getBackend(allBackendIds.get(0));
                context.setInsertGroupCommit(insertStmt.getTargetTable().getId(), backend);
            }
            int maxRetry = 3;
            for (int i = 0; i < maxRetry; i++) {
                nativeInsertStmt.planForGroupCommit(context.queryId);
                // handle rows
                List<InternalService.PDataRow> rows = new ArrayList<>();
                SelectStmt selectStmt = (SelectStmt) insertStmt.getQueryStmt();
                if (selectStmt.getValueList() != null) {
                    for (List<Expr> row : selectStmt.getValueList().getRows()) {
                        InternalService.PDataRow data = getRowStringValue(row);
                        rows.add(data);
                    }
                } else {
                    List<Expr> exprList = new ArrayList<>();
                    for (Expr resultExpr : selectStmt.getResultExprs()) {
                        if (resultExpr instanceof SlotRef) {
                            exprList.add(((SlotRef) resultExpr).getDesc().getSourceExprs().get(0));
                        } else {
                            exprList.add(resultExpr);
                        }
                    }
                    InternalService.PDataRow data = getRowStringValue(exprList);
                    rows.add(data);
                }
                TUniqueId loadId = nativeInsertStmt.getLoadId();
                PGroupCommitInsertRequest request = PGroupCommitInsertRequest.newBuilder()
                        .setDbId(insertStmt.getTargetTable().getDatabase().getId())
                        .setTableId(insertStmt.getTargetTable().getId())
                        .setDescTbl(nativeInsertStmt.getTableBytes())
                        .setBaseSchemaVersion(nativeInsertStmt.getBaseSchemaVersion())
                        .setPlanNode(nativeInsertStmt.getPlanBytes())
                        .setScanRangeParams(nativeInsertStmt.getRangeBytes())
                        .setLoadId(Types.PUniqueId.newBuilder().setHi(loadId.hi).setLo(loadId.lo)
                                .build()).addAllData(rows)
                        .build();
                Future<PGroupCommitInsertResponse> future = BackendServiceProxy.getInstance()
                        .groupCommitInsert(new TNetworkAddress(backend.getHost(), backend.getBrpcPort()), request);
                PGroupCommitInsertResponse response = future.get();
                TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
                if (code == TStatusCode.DATA_QUALITY_ERROR) {
                    LOG.info("group commit insert failed. stmt: {}, backend id: {}, status: {}, "
                                    + "schema version: {}, retry: {}", insertStmt.getOrigStmt().originStmt,
                            backend.getId(), response.getStatus(), nativeInsertStmt.getBaseSchemaVersion(), i);
                    if (i < maxRetry) {
                        List<TableIf> tables = Lists.newArrayList(insertStmt.getTargetTable());
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
                        errMsg = "group commit insert failed. backend id: " + backend.getId() + ", status: "
                                + response.getStatus();
                    }
                } else if (code != TStatusCode.OK) {
                    errMsg = "group commit insert failed. backend id: " + backend.getId() + ", status: "
                            + response.getStatus();
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

                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(), coord);

                coord.exec();
                int execTimeout = context.getExecTimeout();
                LOG.debug("Insert {} execution timeout:{}", DebugUtil.printId(context.queryId()), execTimeout);
                boolean notTimeout = coord.join(execTimeout);
                if (!coord.isDone()) {
                    coord.cancel(Types.PPlanFragmentCancelReason.TIMEOUT);
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

                LOG.debug("delta files is {}", coord.getDeltaUrls());

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

                if (!Config.using_old_load_usage_pattern) {
                    // if not using old load usage pattern, error will be returned directly to user
                    StringBuilder sb = new StringBuilder(t.getMessage());
                    if (!Strings.isNullOrEmpty(coord.getTrackingUrl())) {
                        sb.append(". url: " + coord.getTrackingUrl());
                    }
                    context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, sb.toString());
                    return;
                }

                /*
                 * If config 'using_old_load_usage_pattern' is true.
                 * Doris will return a label to user, and user can use this label to check load job's status,
                 * which exactly like the old insert stmt usage pattern.
                 */
                throwable = t;
            } finally {
                updateProfile(true);
                QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
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
                                coord.getTrackingUrl(), insertStmt.getUserInfo());
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
            LOG.debug("DDL statement({}) process failed.", originStmt.originStmt, e);
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
    }

    private void handleUnsupportedStmt() {
        context.getMysqlChannel().reset();
        // do nothing
        context.getState().setOk();
    }

    private void handleAnalyzeStmt() throws DdlException, AnalysisException {
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
        // register prepareStmt
        LOG.debug("add prepared statement {}, isBinaryProtocol {}",
                        prepareStmt.getName(), prepareStmt.isBinaryProtocol());
        context.addPreparedStmt(prepareStmt.getName(),
                new PrepareStmtContext(prepareStmt,
                            context, planner, analyzer, prepareStmt.getName()));
        if (prepareStmt.isBinaryProtocol()) {
            sendStmtPrepareOK();
        }
    }


    // Process use statement.
    private void handleUseStmt() throws AnalysisException {
        UseStmt useStmt = (UseStmt) parsedStmt;
        try {
            if (Strings.isNullOrEmpty(useStmt.getClusterName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER);
            }
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
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(metaData.getColumnCount());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (Column col : metaData.getColumns()) {
            serializer.reset();
            // TODO(zhaochun): only support varchar type
            serializer.writeField(col.getName(), col.getType());
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

    private void sendStmtPrepareOK() throws IOException {
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response
        serializer.reset();
        // 0x00 OK
        serializer.writeInt1(0);
        // statement_id
        serializer.writeInt4(Integer.valueOf(prepareStmt.getName()));
        // num_columns
        int numColumns = 0;
        serializer.writeInt2(numColumns);
        // num_params
        int numParams = prepareStmt.getColLabelsOfPlaceHolders().size();
        serializer.writeInt2(numParams);
        // reserved_1
        serializer.writeInt1(0);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        if (numParams > 0) {
            // send field one by one
            // TODO use real type instead of string, for JDBC client it's ok
            // but for other client, type should be correct
            List<PrimitiveType> types = exprToStringType(prepareStmt.getPlaceHolderExprList());
            List<String> colNames = prepareStmt.getColLabelsOfPlaceHolders();
            LOG.debug("sendFields {}, {}", colNames, types);
            for (int i = 0; i < colNames.size(); ++i) {
                serializer.reset();
                serializer.writeField(colNames.get(i), Type.fromPrimitiveType(types.get(i)));
                context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
            }
        }
        // send EOF if nessessary
        if (!context.getMysqlChannel().clientDeprecatedEOF()) {
            context.getState().setEof();
        } else {
            context.getState().setOk();
        }
    }

    private void sendFields(List<String> colNames, List<Type> types) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        LOG.debug("sendFields {}", colNames);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            if (prepareStmt != null && isExecuteStmt) {
                // Using PreparedStatment pre serializedField to avoid serialize each time
                // we send a field
                byte[] serializedField = prepareStmt.getSerializedField(colNames.get(i));
                if (serializedField == null) {
                    serializer.writeField(colNames.get(i), types.get(i));
                    serializedField = serializer.toArray();
                    prepareStmt.setSerializedField(colNames.get(i), serializedField);
                }
                context.getMysqlChannel().sendOnePacket(ByteBuffer.wrap(serializedField));
            } else {
                serializer.writeField(colNames.get(i), types.get(i));
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
        context.updateReturnRows(resultSet.getResultRows().size());
        // Send meta data.
        sendMetaData(resultSet.getMetaData());

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
            proxyResultSet = resultSet;
            return;
        }

        sendResultSet(resultSet);
    }

    private void handleUnlockTablesStmt() {
    }

    private void handleLockTablesStmt() {
    }

    public void handleExplainStmt(String result, boolean isNereids) throws IOException {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Explain String" + (isNereids ? "(Nereids Planner)" : "(Old Planner)"),
                        ScalarType.createVarchar(20)))
                .build();
        sendMetaData(metaData);

        // Send result set.
        for (String item : result.split("\n")) {
            serializer.reset();
            serializer.writeLenEncodedString(item);
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
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
            LOG.debug("DDL statement({}) process failed.", originStmt.originStmt, e);
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

    private void handleIotStmt() {
        InsertOverwriteTableStmt iotStmt = (InsertOverwriteTableStmt) this.parsedStmt;
        if (iotStmt.getPartitionNames().size() == 0) {
            // insert overwrite table
            handleOverwriteTable(iotStmt);
        } else {
            // insert overwrite table with partition
            handleOverwritePartition(iotStmt);
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
        try {
            parsedStmt = new NativeInsertStmt(tmpTableName, null, new LabelName(iotStmt.getDb(), iotStmt.getLabel()),
                    iotStmt.getQueryStmt(), iotStmt.getHints(), iotStmt.getCols());
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
        try {
            parsedStmt = new NativeInsertStmt(targetTableName, new PartitionNames(true, tempPartitionName),
                    new LabelName(iotStmt.getDb(), iotStmt.getLabel()), iotStmt.getQueryStmt(),
                    iotStmt.getHints(), iotStmt.getCols());
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
        LOG.debug("INTERNAL QUERY: " + originStmt.toString());
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
                        LOG.warn("Arrow Flight SQL fall back to legacy planner, because: {}",
                                e.getMessage(), e);
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

            Span queryScheduleSpan = context.getTracer()
                    .spanBuilder("internal SQL schedule").setParent(Context.current()).startSpan();
            try (Scope scope = queryScheduleSpan.makeCurrent()) {
                coord.exec();
            } catch (Exception e) {
                queryScheduleSpan.recordException(e);
                throw new InternalQueryExecutionException(e.getMessage() + Util.getRootCauseMessage(e), e);
            } finally {
                queryScheduleSpan.end();
            }
            Span fetchResultSpan = context.getTracer().spanBuilder("fetch internal SQL result")
                    .setParent(Context.current()).startSpan();
            try (Scope scope = fetchResultSpan.makeCurrent()) {
                while (true) {
                    batch = coord.getNext();
                    if (batch == null || batch.isEos()) {
                        return resultRows;
                    } else {
                        resultRows.addAll(convertResultBatchToResultRows(batch.getBatch()));
                    }
                }
            } catch (Exception e) {
                fetchResultSpan.recordException(e);
                throw new RuntimeException("Failed to fetch internal SQL result. " + Util.getRootCauseMessage(e), e);
            } finally {
                fetchResultSpan.end();
            }
        } finally {
            AuditLogHelper.logAuditLog(context, originStmt.toString(), parsedStmt, getQueryStatisticsForAuditLog(),
                    true);
            QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
        }
    }

    public void executeArrowFlightQuery(FlightStatementExecutor flightStatementExecutor) {
        LOG.debug("ARROW FLIGHT QUERY: " + originStmt.toString());
        try {
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
                        LOG.warn("fall back to legacy planner, because: {}", e.getMessage(), e);
                        parsedStmt = null;
                        context.getState().setNereids(false);
                        analyzer = new Analyzer(context.getEnv(), context);
                        analyze(context.getSessionVariable().toThrift());
                    }
                } else {
                    analyzer = new Analyzer(context.getEnv(), context);
                    analyze(context.getSessionVariable().toThrift());
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to execute Arrow Flight SQL. " + Util.getRootCauseMessage(e), e);
            }
            coord = new Coordinator(context, analyzer, planner, context.getStatsErrorEstimator());
            profile.addExecutionProfile(coord.getExecutionProfile());
            try {
                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                        new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
            } catch (UserException e) {
                throw new RuntimeException("Failed to execute Arrow Flight SQL. " + Util.getRootCauseMessage(e), e);
            }

            Span queryScheduleSpan = context.getTracer()
                    .spanBuilder("Arrow Flight SQL schedule").setParent(Context.current()).startSpan();
            try (Scope scope = queryScheduleSpan.makeCurrent()) {
                coord.exec();
            } catch (Exception e) {
                queryScheduleSpan.recordException(e);
                LOG.warn("Failed to coord exec Arrow Flight SQL, because: {}", e.getMessage(), e);
                throw new RuntimeException(e.getMessage() + Util.getRootCauseMessage(e), e);
            } finally {
                queryScheduleSpan.end();
            }
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId()); // TODO for query profile
        }
        flightStatementExecutor.setFinstId(coord.getFinstId());
        flightStatementExecutor.setResultFlightServerAddr(coord.getResultFlightServerAddr());
        flightStatementExecutor.setResultInternalServiceAddr(coord.getResultInternalServiceAddr());
        flightStatementExecutor.setResultOutputExprs(coord.getResultOutputExprs());
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

    public SummaryProfile getSummaryProfile() {
        return profile.getSummaryProfile();
    }

    public Profile getProfile() {
        return profile;
    }

    public void setProfileType(ProfileType profileType) {
        this.profileType = profileType;
    }


    public void setProxyResultSet(ShowResultSet proxyResultSet) {
        this.proxyResultSet = proxyResultSet;
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
}


