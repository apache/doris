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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.EnterStmt;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.LockTablesStmt;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SelectListItem;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetOperationStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtRewriter;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.SwitchStmt;
import org.apache.doris.analysis.TransactionBeginStmt;
import org.apache.doris.analysis.TransactionCommitStmt;
import org.apache.doris.analysis.TransactionRollbackStmt;
import org.apache.doris.analysis.TransactionStmt;
import org.apache.doris.analysis.UnlockTablesStmt;
import org.apache.doris.analysis.UnsupportedStmt;
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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LiteralUtils;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.ProfileWriter;
import org.apache.doris.common.util.QueryPlannerProfile;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OriginalPlanner;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TResultBatch;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

// Do one COM_QUERY process.
// first: Parse receive byte array to statement struct.
// second: Do handle function for statement.
public class StmtExecutor implements ProfileWriter {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    private static final int MAX_DATA_TO_SEND_FOR_TXN = 100;
    private static final String NULL_VALUE_FOR_LOAD = "\\N";
    private final Object writeProfileLock = new Object();
    private ConnectContext context;
    private StatementContext statementContext;
    private MysqlSerializer serializer;
    private OriginStatement originStmt;
    private StatementBase parsedStmt;
    private Analyzer analyzer;
    private RuntimeProfile profile;
    private RuntimeProfile summaryProfile;
    private RuntimeProfile plannerRuntimeProfile;
    private volatile boolean isFinishedProfile = false;
    private String queryType = "Query";
    private volatile Coordinator coord = null;
    private MasterOpExecutor masterOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    private Planner planner;
    private boolean isProxy;
    private ShowResultSet proxyResultSet = null;
    private Data.PQueryStatistics.Builder statisticsForAuditLog;
    private boolean isCached;
    private QueryPlannerProfile plannerProfile = new QueryPlannerProfile();

    // this constructor is mainly for proxy
    public StmtExecutor(ConnectContext context, OriginStatement originStmt, boolean isProxy) {
        this.context = context;
        this.originStmt = originStmt;
        this.serializer = context.getSerializer();
        this.isProxy = isProxy;
        this.statementContext = new StatementContext(context, originStmt);
    }

    // this constructor is only for test now.
    public StmtExecutor(ConnectContext context, String stmt) {
        this(context, new OriginStatement(stmt, 0), false);
    }

    // constructor for receiving parsed stmt from connect processor
    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this.context = ctx;
        this.parsedStmt = parsedStmt;
        this.originStmt = parsedStmt.getOrigStmt();
        this.serializer = context.getSerializer();
        this.isProxy = false;
        this.statementContext = new StatementContext(ctx, originStmt);
        this.statementContext.setParsedStatement(parsedStmt);
    }

    public static InternalService.PDataRow getRowStringValue(List<Expr> cols) {
        if (cols.size() == 0) {
            return null;
        }
        InternalService.PDataRow.Builder row = InternalService.PDataRow.newBuilder();
        for (Expr expr : cols) {
            if (expr instanceof NullLiteral) {
                row.addColBuilder().setValue(NULL_VALUE_FOR_LOAD);
            } else {
                row.addColBuilder().setValue(expr.getStringValue());
            }
        }
        return row.build();
    }

    public void setCoord(Coordinator coord) {
        this.coord = coord;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    // At the end of query execution, we begin to add up profile
    private void initProfile(QueryPlannerProfile plannerProfile, boolean waiteBeReport) {
        RuntimeProfile queryProfile;
        // when a query hits the sql cache, `coord` is null.
        if (coord == null) {
            queryProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(context.queryId()));
        } else {
            queryProfile = coord.getQueryProfile();
        }
        if (profile == null) {
            profile = new RuntimeProfile("Query");
            summaryProfile = new RuntimeProfile("Summary");
            profile.addChild(summaryProfile);
            summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(context.getStartTime()));
            updateSummaryProfile(waiteBeReport);
            for (Map.Entry<String, String> entry : getSummaryInfo().entrySet()) {
                summaryProfile.addInfoString(entry.getKey(), entry.getValue());
            }
            summaryProfile.addInfoString(ProfileManager.TRACE_ID, context.getSessionVariable().getTraceId());
            plannerRuntimeProfile = new RuntimeProfile("Execution Summary");
            summaryProfile.addChild(plannerRuntimeProfile);
            profile.addChild(queryProfile);
        } else {
            updateSummaryProfile(waiteBeReport);
        }
        plannerProfile.initRuntimeProfile(plannerRuntimeProfile);

        queryProfile.getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(plannerProfile.getQueryBeginTime()));
        endProfile(waiteBeReport);
    }

    private void endProfile(boolean waitProfileDone) {
        if (context != null && context.getSessionVariable().enableProfile() && coord != null) {
            coord.endProfile(waitProfileDone);
        }
    }

    private void updateSummaryProfile(boolean waiteBeReport) {
        Preconditions.checkNotNull(summaryProfile);
        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - context.getStartTime();
        summaryProfile.addInfoString(ProfileManager.END_TIME,
                waiteBeReport ? TimeUtils.longToTimeString(currentTimestamp) : "N/A");
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE,
                !waiteBeReport && context.getState().getStateType().equals(MysqlStateType.OK) ? "RUNNING" :
                        context.getState().toString());
    }

    private Map<String, String> getSummaryInfo() {
        Map<String, String> infos = Maps.newLinkedHashMap();
        infos.put(ProfileManager.QUERY_ID, DebugUtil.printId(context.queryId()));
        infos.put(ProfileManager.QUERY_TYPE, queryType);
        infos.put(ProfileManager.DORIS_VERSION, Version.DORIS_BUILD_VERSION);
        infos.put(ProfileManager.USER, context.getQualifiedUser());
        infos.put(ProfileManager.DEFAULT_DB, context.getDatabase());
        infos.put(ProfileManager.SQL_STATEMENT, originStmt.originStmt);
        infos.put(ProfileManager.IS_CACHED, isCached ? "Yes" : "No");
        return infos;
    }

    public void addProfileToSpan() {
        Span span = Span.fromContext(Context.current());
        if (!span.isRecording()) {
            return;
        }
        for (Map.Entry<String, String> entry : getSummaryInfo().entrySet()) {
            span.setAttribute(entry.getKey(), entry.getValue());
        }
    }

    public Planner planner() {
        return planner;
    }

    public boolean isForwardToMaster() {
        if (Env.getCurrentEnv().isMaster()) {
            return false;
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if ((parsedStmt instanceof QueryStmt) && !Env.getCurrentEnv().isMaster()
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

    public boolean isQueryStmt() {
        return parsedStmt != null && parsedStmt instanceof QueryStmt;
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
        Span executeSpan = context.getTracer().spanBuilder("execute").setParent(Context.current()).startSpan();
        try (Scope scope = executeSpan.makeCurrent()) {
            execute(queryId);
        } finally {
            executeSpan.end();
        }
    }

    // Execute one statement with queryId
    // The queryId will be set in ConnectContext
    // This queryId will also be sent to master FE for exec master only query.
    // query id in ConnectContext will be changed when retry exec a query or master FE return a different one.
    // Exception:
    // IOException: talk with client failed.
    public void execute(TUniqueId queryId) throws Exception {
        context.setStartTime();

        plannerProfile.setQueryBeginTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        context.setQueryId(queryId);

        try {
            if (context.isTxnModel() && !(parsedStmt instanceof InsertStmt)
                    && !(parsedStmt instanceof TransactionStmt)) {
                throw new TException("This is in a transaction, only insert, commit, rollback is acceptable.");
            }
            // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
            analyzeVariablesInStmt();

            if (!context.isTxnModel()) {
                Span queryAnalysisSpan =
                        context.getTracer().spanBuilder("query analysis").setParent(Context.current()).startSpan();
                try (Scope scope = queryAnalysisSpan.makeCurrent()) {
                    // analyze this query
                    try {
                        analyze(context.getSessionVariable().toThrift());
                    } catch (NereidsException e) {
                        if (!context.getSessionVariable().enableFallbackToOriginalPlanner) {
                            LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                            throw e.getException();
                        }
                        // fall back to legacy planner
                        LOG.info("fall back to legacy planner, because: {}", e.getMessage());
                        parsedStmt = null;
                        analyze(context.getSessionVariable().toThrift());
                    }
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
                                + Env.getCurrentEnv().getSelfNode().first + ") and failed to execute"
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

            if (parsedStmt instanceof QueryStmt || parsedStmt instanceof LogicalPlanAdapter) {
                context.getState().setIsQuery(true);
                if (!parsedStmt.isExplain()) {
                    // sql/sqlHash block
                    try {
                        Env.getCurrentEnv().getSqlBlockRuleMgr().matchSql(
                                originStmt.originStmt, context.getSqlHash(), context.getQualifiedUser());
                    } catch (AnalysisException e) {
                        LOG.warn(e.getMessage());
                        context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                        return;
                    }
                    // limitations: partition_num, tablet_num, cardinality
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

                int retryTime = Config.max_query_retry_time;
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
                        endProfile(true);
                        QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
                    }
                }
            } else if (parsedStmt instanceof SetStmt) {
                handleSetStmt();
            } else if (parsedStmt instanceof EnterStmt) {
                handleEnterStmt();
            } else if (parsedStmt instanceof SwitchStmt) {
                handleSwitchStmt();
            } else if (parsedStmt instanceof UseStmt) {
                handleUseStmt();
            } else if (parsedStmt instanceof TransactionStmt) {
                handleTransactionStmt();
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                handleCtasStmt();
            } else if (parsedStmt instanceof InsertStmt) { // Must ahead of DdlStmt because InserStmt is its subclass
                try {
                    handleInsertStmt();
                    if (!((InsertStmt) parsedStmt).getQueryStmt().isExplain()) {
                        queryType = "Insert";
                    }
                } catch (Throwable t) {
                    LOG.warn("handle insert stmt fail", t);
                    // the transaction of this insert may already begun, we will abort it at outer finally block.
                    throw t;
                }
            } else if (parsedStmt instanceof DdlStmt) {
                handleDdlStmt();
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
            LOG.warn("execute Exception. {}, {}", context.getQueryIdentifier(), e.getMessage());
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Exception e) {
            LOG.warn("execute Exception. {}", context.getQueryIdentifier(), e);
            context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        } finally {
            // revert Session Value
            try {
                SessionVariable sessionVariable = context.getSessionVariable();
                VariableMgr.revertSessionValue(sessionVariable);
                // origin value init
                sessionVariable.setIsSingleSetVar(false);
                sessionVariable.clearSessionOriginValue();
            } catch (DdlException e) {
                LOG.warn("failed to revert Session value. {}", context.getQueryIdentifier(), e);
                context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            }
            if (!context.isTxnModel() && parsedStmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) parsedStmt;
                // The transaction of an insert operation begin at analyze phase.
                // So we should abort the transaction at this finally block if it encounters exception.
                if (insertStmt.isTransactionBegin() && context.getState().getStateType() == MysqlStateType.ERR) {
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

    /**
     * get variables in stmt.
     *
     * @throws DdlException
     */
    private void analyzeVariablesInStmt() throws DdlException {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (parsedStmt != null && parsedStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) parsedStmt;
            Map<String, String> optHints = selectStmt.getSelectList().getOptHints();
            if (optHints != null) {
                sessionVariable.setIsSingleSetVar(true);
                for (String key : optHints.keySet()) {
                    VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(optHints.get(key))));
                }
            }
        }
    }

    private void forwardToMaster() throws Exception {
        boolean isQuery = parsedStmt instanceof QueryStmt;
        masterOpExecutor = new MasterOpExecutor(originStmt, context, redirectStatus, isQuery);
        LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());
        masterOpExecutor.execute();
    }

    @Override
    public void writeProfile(boolean isLastWriteProfile) {
        if (!context.getSessionVariable().enableProfile()) {
            return;
        }
        synchronized (writeProfileLock) {
            if (isFinishedProfile) {
                return;
            }
            initProfile(plannerProfile, isLastWriteProfile);
            profile.computeTimeInChildProfile();
            ProfileManager.getInstance().pushProfile(profile);
            isFinishedProfile = isLastWriteProfile;
        }
    }

    // Analyze one statement to structure in memory.
    public void analyze(TQueryOptions tQueryOptions) throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to analyze stmt: {}, forwarded stmt id: {}",
                    context.getStmtId(), context.getForwardedStmtId());
        }

        parse();

        // yiguolei: insert stmt's grammar analysis will write editlog,
        // so that we check if the stmt should be forward to master here
        // if the stmt should be forward to master, then just return here and the master will do analysis again
        if (isForwardToMaster()) {
            return;
        }

        analyzer = new Analyzer(context.getEnv(), context);
        // Convert show statement to select statement here
        if (parsedStmt instanceof ShowStmt) {
            SelectStmt selectStmt = ((ShowStmt) parsedStmt).toSelectStmt(analyzer);
            if (selectStmt != null) {
                setParsedStmt(selectStmt);
            }
        }

        if (parsedStmt instanceof QueryStmt
                || parsedStmt instanceof InsertStmt
                || parsedStmt instanceof CreateTableAsSelectStmt
                || parsedStmt instanceof LogicalPlanAdapter) {
            Map<Long, TableIf> tableMap = Maps.newTreeMap();
            QueryStmt queryStmt;
            Set<String> parentViewNameSet = Sets.newHashSet();
            if (parsedStmt instanceof QueryStmt) {
                queryStmt = (QueryStmt) parsedStmt;
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
                    if (parsedStmt instanceof LogicalPlanAdapter) {
                        throw new NereidsException(new AnalysisException("Unexpected exception: " + e.getMessage()));
                    }
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
    }

    private void parse() throws AnalysisException, DdlException {
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
            ExprRewriter rewriter = analyzer.getExprRewriter();
            rewriter.reset();
            if (context.getSessionVariable().isEnableFoldConstantByBe()) {
                // fold constant expr
                parsedStmt.foldConstant(rewriter);

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

                // query re-analyze
                parsedStmt.reset();
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
        plannerProfile.setQueryAnalysisFinishTime();

        if (parsedStmt instanceof LogicalPlanAdapter) {
            // create plan
            planner = new NereidsPlanner(statementContext);
        } else {
            planner = new OriginalPlanner(analyzer);
        }
        if (parsedStmt instanceof QueryStmt
                || parsedStmt instanceof InsertStmt
                || parsedStmt instanceof LogicalPlanAdapter) {
            planner.plan(parsedStmt, tQueryOptions);
        }
        // TODO(zc):
        // Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

        plannerProfile.setQueryPlanFinishTime();
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
    }

    // Because this is called by other thread
    public void cancel() {
        Coordinator coordRef = coord;
        if (coordRef != null) {
            coordRef.cancel();
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
                    && !Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(),
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
            SelectStmt selectStmt, boolean isSendFields, boolean isEos)
            throws Exception {
        RowBatch batch = null;
        boolean isSend = isSendFields;
        for (InternalService.PCacheValue value : cacheValues) {
            TResultBatch resultBatch = new TResultBatch();
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
    private void handleCacheStmt(CacheAnalyzer cacheAnalyzer, MysqlChannel channel, SelectStmt selectStmt)
            throws Exception {
        InternalService.PFetchCacheResult cacheResult = cacheAnalyzer.getCacheData();
        CacheMode mode = cacheAnalyzer.getCacheMode();
        SelectStmt newSelectStmt = selectStmt;
        boolean isSendFields = false;
        if (cacheResult != null) {
            isCached = true;
            if (cacheAnalyzer.getHitRange() == Cache.HitRange.Full) {
                sendCachedValues(channel, cacheResult.getValuesList(), newSelectStmt, isSendFields, true);
                return;
            }
            // rewrite sql
            if (mode == CacheMode.Partition) {
                if (cacheAnalyzer.getHitRange() == Cache.HitRange.Left) {
                    isSendFields = sendCachedValues(channel, cacheResult.getValuesList(),
                            newSelectStmt, isSendFields, false);
                }
                newSelectStmt = cacheAnalyzer.getRewriteStmt();
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
        sendResult(false, isSendFields, newSelectStmt, channel, cacheAnalyzer, cacheResult);
    }

    private boolean handleSelectRequestInFe(SelectStmt parsedSelectStmt) throws IOException {
        List<SelectListItem> selectItemList = parsedSelectStmt.getSelectList().getItems();
        List<Column> columns = new ArrayList<>(selectItemList.size());
        ResultSetMetaData metadata = new CommonResultSet.CommonResultSetMetaData(columns);

        List<String> columnLabels = parsedSelectStmt.getColLabels();
        List<String> data = new ArrayList<>();
        for (int i = 0; i < selectItemList.size(); i++) {
            SelectListItem item = selectItemList.get(i);
            Expr expr = item.getExpr();
            String columnName = columnLabels.get(i);
            if (expr instanceof LiteralExpr) {
                columns.add(new Column(columnName, expr.getType()));
                if (expr instanceof NullLiteral) {
                    data.add(null);
                } else if (expr instanceof FloatLiteral) {
                    data.add(LiteralUtils.getStringValue((FloatLiteral) expr));
                } else if (expr instanceof DecimalLiteral) {
                    data.add(((DecimalLiteral) expr).getValue().toPlainString());
                } else if (expr instanceof ArrayLiteral) {
                    data.add(LiteralUtils.getStringValue((ArrayLiteral) expr));
                } else {
                    data.add(expr.getStringValue());
                }
            } else {
                return false;
            }
        }
        ResultSet resultSet = new CommonResultSet(metadata, Collections.singletonList(data));
        sendResultSet(resultSet);
        return true;
    }

    // Process a select statement.
    private void handleQueryStmt() throws Exception {
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
            handleExplainStmt(explainString);
            return;
        }

        // handle selects that fe can do without be, so we can make sql tools happy, especially the setup step.
        if (parsedStmt instanceof SelectStmt && ((SelectStmt) parsedStmt).getTableRefs().isEmpty()) {
            SelectStmt parsedSelectStmt = (SelectStmt) parsedStmt;
            if (handleSelectRequestInFe(parsedSelectStmt)) {
                return;
            }
        }

        MysqlChannel channel = context.getMysqlChannel();
        boolean isOutfileQuery = queryStmt.hasOutFileClause();

        // Sql and PartitionCache
        CacheAnalyzer cacheAnalyzer = new CacheAnalyzer(context, parsedStmt, planner);
        if (cacheAnalyzer.enableCache() && !isOutfileQuery && queryStmt instanceof SelectStmt) {
            handleCacheStmt(cacheAnalyzer, channel, (SelectStmt) queryStmt);
            return;
        }
        sendResult(isOutfileQuery, false, queryStmt, channel, null, null);
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
        coord = new Coordinator(context, analyzer, planner);
        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
        coord.setProfileWriter(this);
        Span queryScheduleSpan =
                context.getTracer().spanBuilder("query schedule").setParent(Context.current()).startSpan();
        try (Scope scope = queryScheduleSpan.makeCurrent()) {
            coord.exec();
        } catch (Exception e) {
            queryScheduleSpan.recordException(e);
            throw e;
        } finally {
            queryScheduleSpan.end();
        }
        plannerProfile.setQueryScheduleFinishTime();
        writeProfile(false);
        Span fetchResultSpan = context.getTracer().spanBuilder("fetch result").setParent(Context.current()).startSpan();
        try (Scope scope = fetchResultSpan.makeCurrent()) {
            while (true) {
                batch = coord.getNext();
                // for outfile query, there will be only one empty batch send back with eos flag
                if (batch.getBatch() != null) {
                    if (cacheAnalyzer != null) {
                        cacheAnalyzer.copyRowBatch(batch);
                    }
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
                    context.updateReturnRows(batch.getBatch().getRows().size());
                }
                if (batch.isEos()) {
                    break;
                }
            }
            if (cacheAnalyzer != null) {
                if (cacheResult != null && cacheAnalyzer.getHitRange() == Cache.HitRange.Right) {
                    isSendFields =
                            sendCachedValues(channel, cacheResult.getValuesList(), (SelectStmt) queryStmt, isSendFields,
                                    false);
                }

                cacheAnalyzer.updateCache();
            }
            if (!isSendFields) {
                if (!isOutfileQuery) {
                    sendFields(queryStmt.getColLabels(), exprToType(queryStmt.getResultExprs()));
                } else {
                    sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                }
            }

            statisticsForAuditLog = batch.getQueryStatistics() == null ? null : batch.getQueryStatistics().toBuilder();
            context.getState().setEof();
            plannerProfile.setQueryFetchResultFinishTime();
        } catch (Exception e) {
            fetchResultSpan.recordException(e);
            throw e;
        } finally {
            fetchResultSpan.end();
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
            txnParams.setNeedTxn(true).setThriftRpcTimeoutMs(5000).setTxnId(-1).setDb("").setTbl("");
            if (context.getSessionVariable().getEnableInsertStrict()) {
                txnParams.setMaxFilterRatio(0);
            } else {
                txnParams.setMaxFilterRatio(1.0);
            }
            if (context.getTxnEntry() == null) {
                context.setTxnEntry(new TransactionEntry());
            }
            TransactionEntry txnEntry = context.getTxnEntry();
            txnEntry.setTxnConf(txnParams);
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

    public int executeForTxn(InsertStmt insertStmt)
            throws UserException, TException, InterruptedException, ExecutionException, TimeoutException {
        if (context.isTxnIniting()) { // first time, begin txn
            beginTxn(insertStmt.getDb(), insertStmt.getTbl());
        }
        if (!context.getTxnEntry().getTxnConf().getDb().equals(insertStmt.getDb())
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
                InternalService.PDataRow data = getRowStringValue(row);
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
        long timeoutSecond = ConnectContext.get().getSessionVariable().getQueryTimeoutS();
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
            String authCodeUuid = Env.getCurrentGlobalTransactionMgr().getTransactionState(
                    txnConf.getDbId(), txnConf.getTxnId()).getAuthCode();
            txnConf.setAuthCodeUuid(authCodeUuid);
        } else {
            String authCodeUuid = UUID.randomUUID().toString();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
            request.setDb(txnConf.getDb()).setTbl(txnConf.getTbl()).setAuthCodeUuid(authCodeUuid)
                    .setCluster(dbObj.getClusterName()).setLabel(label).setUser("").setUserIp("").setPasswd("");
            TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
            txnConf.setTxnId(result.getTxnId());
            txnConf.setAuthCodeUuid(authCodeUuid);
        }

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(txnConf.getTxnId()).setDb(txnConf.getDb())
                .setTbl(txnConf.getTbl())
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND).setThriftRpcTimeoutMs(5000).setLoadId(context.queryId());

        // execute begin txn
        InsertStreamTxnExecutor executor = new InsertStreamTxnExecutor(txnEntry);
        executor.beginTransaction(request);
    }

    // Process a select statement.
    private void handleInsertStmt() throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        // create plan
        InsertStmt insertStmt = (InsertStmt) parsedStmt;
        if (insertStmt.getQueryStmt().hasOutFileClause()) {
            throw new DdlException("Not support OUTFILE clause in INSERT statement");
        }

        if (insertStmt.getQueryStmt().isExplain()) {
            ExplainOptions explainOptions = insertStmt.getQueryStmt().getExplainOptions();
            insertStmt.setIsExplain(explainOptions);
            String explainString = planner.getExplainString(explainOptions);
            handleExplainStmt(explainString);
            return;
        }

        long createTime = System.currentTimeMillis();
        Throwable throwable = null;
        long txnId = -1;
        String label = "";
        long loadedRows = 0;
        int filteredRows = 0;
        TransactionStatus txnStatus = TransactionStatus.ABORTED;
        String errMsg = "";
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
        } else {
            label = insertStmt.getLabel();
            LOG.info("Do insert [{}] with query id: {}", label, DebugUtil.printId(context.queryId()));

            try {
                coord = new Coordinator(context, analyzer, planner);
                coord.setLoadZeroTolerance(context.getSessionVariable().getEnableInsertStrict());
                coord.setQueryType(TQueryType.LOAD);

                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(), coord);

                coord.exec();

                boolean notTimeout = coord.join(context.getSessionVariable().getQueryTimeoutS());
                if (!coord.isDone()) {
                    coord.cancel();
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

                if (insertStmt.getTargetTable().getType() != TableType.OLAP) {
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
                endProfile(true);
                QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
            }

            // Go here, which means:
            // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
            // 2. transaction failed but Config.using_old_load_usage_pattern is true.
            // we will record the load job info for these 2 cases
            txnId = insertStmt.getTransactionId();
            try {
                context.getEnv().getLoadManager()
                        .recordFinishedLoadJob(label, txnId, insertStmt.getDb(), insertStmt.getTargetTable().getId(),
                                EtlJobType.INSERT, createTime, throwable == null ? "" : throwable.getMessage(),
                                coord.getTrackingUrl());
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
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");

        context.getState().setOk(loadedRows, filteredRows, sb.toString());

        // set insert result in connection context,
        // so that user can use `show insert result` to get info of the last insert operation.
        context.setOrUpdateInsertResult(txnId, label, insertStmt.getDb(), insertStmt.getTbl(),
                txnStatus, loadedRows, filteredRows);
        // update it, so that user can get loaded rows in fe.audit.log
        context.updateReturnRows((int) loadedRows);
    }

    private void handleUnsupportedStmt() {
        context.getMysqlChannel().reset();
        // do nothing
        context.getState().setOk();
    }

    // Process switch catalog
    private void handleSwitchStmt() throws AnalysisException {
        SwitchStmt switchStmt = (SwitchStmt) parsedStmt;
        try {
            context.getEnv().changeCatalog(context, switchStmt.getCatalogName());
        } catch (DdlException e) {
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }
        context.getState().setOk();
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
            serializer.writeField(col.getName(), col.getType().getPrimitiveType());
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    private void sendFields(List<String> colNames, List<PrimitiveType> types) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            serializer.writeField(colNames.get(i), types.get(i));
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
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

    private void handleExplainStmt(String result) throws IOException {
        ShowResultSetMetaData metaData =
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("Explain String", ScalarType.createVarchar(20)))
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

    private void handleDdlStmt() {
        try {
            DdlExecutor.execute(context.getEnv(), (DdlStmt) parsedStmt);
            context.getState().setOk();
        } catch (QueryStateException e) {
            context.setState(e.getQueryState());
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

    // Process enter cluster
    private void handleEnterStmt() {
        final EnterStmt enterStmt = (EnterStmt) parsedStmt;
        try {
            context.getEnv().changeCluster(context, enterStmt.getClusterName());
            context.setDatabase("");
        } catch (DdlException e) {
            context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void handleExportStmt() throws Exception {
        ExportStmt exportStmt = (ExportStmt) parsedStmt;
        context.getEnv().getExportMgr().addExportJob(exportStmt);
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
        }
        // after success create table insert data
        if (MysqlStateType.OK.equals(context.getState().getStateType())) {
            try {
                parsedStmt = ctasStmt.getInsertStmt();
                execute();
            } catch (Exception e) {
                LOG.warn("CTAS insert data error, stmt={}", ctasStmt.toSql(), e);
                // insert error drop table
                DropTableStmt dropTableStmt = new DropTableStmt(true, ctasStmt.getCreateTableStmt().getDbTbl(), true);
                try {
                    DdlExecutor.execute(context.getEnv(), dropTableStmt);
                } catch (Exception ex) {
                    LOG.warn("CTAS drop table error, stmt={}", parsedStmt.toSql(), ex);
                    context.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                            "Unexpected exception: " + ex.getMessage());
                }
            }
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
        if (statisticsForAuditLog.hasReturnedRows()) {
            statisticsForAuditLog.setReturnedRows(0L);
        }
        if (!statisticsForAuditLog.hasCpuMs()) {
            statisticsForAuditLog.setCpuMs(0L);
        }
        return statisticsForAuditLog.build();
    }

    private List<PrimitiveType> exprToType(List<Expr> exprs) {
        return exprs.stream().map(e -> e.getType().getPrimitiveType()).collect(Collectors.toList());
    }

    private StatementBase setParsedStmt(StatementBase parsedStmt) {
        this.parsedStmt = parsedStmt;
        this.statementContext.setParsedStatement(parsedStmt);
        return parsedStmt;
    }
}
