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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.SetVar.SetVarType;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.UnsetVariableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud.ClusterStatus;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuditLog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.cache.NereidsSqlCacheManager;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.ProfileManager.ProfileType;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileScanNode;
import org.apache.doris.datasource.tvf.source.TVFScanNode;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlOkPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.ProxyMysqlChannel;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlanProcess;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundBaseExternalTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromCommand;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.EmptyCommand;
import org.apache.doris.nereids.trees.plans.commands.Forward;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.commands.Redirect;
import org.apache.doris.nereids.trees.plans.commands.TransactionCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.BatchInsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapGroupCommitInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.planner.GroupCommitScanNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.Data;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.POutfileWriteSuccessRequest;
import org.apache.doris.proto.InternalService.POutfileWriteSuccessResult;
import org.apache.doris.qe.CommonResultSet.CommonResultSetMetaData;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QeProcessorImpl.QueryInfo;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.SqlCache;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.ResultRow;
import org.apache.doris.statistics.util.InternalQueryBuffer;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService.Client;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TResultFileSink;
import org.apache.doris.thrift.TResultFileSinkOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TSyncLoadForTabletsRequest;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import lombok.Setter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// Do one COM_QUERY process.
// first: Parse receive byte array to statement struct.
// second: Do handle function for statement.
public class StmtExecutor {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    public static final int MAX_DATA_TO_SEND_FOR_TXN = 100;
    private static Set<String> blockSqlAstNames = Sets.newHashSet();

    private Pattern beIpPattern = Pattern.compile("\\[(\\d+):");
    private ConnectContext context;
    private final StatementContext statementContext;
    private MysqlSerializer serializer;
    private OriginStatement originStmt;
    private StatementBase parsedStmt;
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
    private String prepareStmtName; // for prox
    private String mysqlLoadId;
    // Handle selects that fe can do without be
    private boolean isHandleQueryInFe = false;
    // The profile of this execution
    private final Profile profile;
    private Boolean isForwardedToMaster = null;
    // Flag for execute prepare statement, need to use binary protocol resultset
    private boolean isComStmtExecute = false;
    // Handler to process and send blackhole query results
    private BlackholeResultHandler blackholeResultHandler = null;

    // The result schema if "dry_run_query" is true.
    // Only one column to indicate the real return row numbers.
    private static final CommonResultSetMetaData DRY_RUN_QUERY_METADATA = new CommonResultSetMetaData(
            Lists.newArrayList(new Column("ReturnedRows", PrimitiveType.STRING)));

    // this constructor is mainly for proxy
    public StmtExecutor(ConnectContext context, OriginStatement originStmt, boolean isProxy) {
        Preconditions.checkState(context.getConnectType().equals(ConnectType.MYSQL));
        this.context = context;
        if (context != null) {
            context.setExecutor(this);
        }
        this.originStmt = originStmt;
        this.serializer = context.getMysqlChannel().getSerializer();
        this.isProxy = isProxy;
        this.statementContext = new StatementContext(context, originStmt);
        this.context.setStatementContext(statementContext);
        this.profile = new Profile(
                this.context.getSessionVariable().enableProfile(),
                this.context.getSessionVariable().getProfileLevel(),
                this.context.getSessionVariable().getAutoProfileThresholdMs());
    }

    // for test
    public StmtExecutor(ConnectContext context, String stmt) {
        this(context, new OriginStatement(stmt, 0), false);
        this.stmtName = stmt;
    }

    // constructor for receiving parsed stmt from connect processor
    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this(ctx, parsedStmt, false);
    }

    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt, boolean isComStmtExecute) {
        this.context = ctx;
        this.parsedStmt = parsedStmt;
        this.originStmt = parsedStmt.getOrigStmt();
        this.isComStmtExecute = isComStmtExecute;
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
        this.profile = new Profile(
                            context.getSessionVariable().enableProfile(),
                            context.getSessionVariable().getProfileLevel(),
                            context.getSessionVariable().getAutoProfileThresholdMs());
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
            row.addColBuilder().setValue(expr.getStringValueForStreamLoad(options));
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
        // TODO: Never use custom data format when deliverying information between two systems.
        // UI can not order profile by TOTAL_TIME since its not a sortable string (2h1m3s > 2h1s?)
        // to get decoded info, UI need to decode it first, it means others need to
        // reference the implementation of DebugUtil.getPrettyStringMs to figure out the format
        if (isFinished) {
            builder.endTime(TimeUtils.longToTimeString(currentTimestamp));
            builder.totalTime(DebugUtil.getPrettyStringMs(currentTimestamp - context.getStartTime()));
        }
        String taskState = "RUNNING";
        if (isFinished) {
            if (coord != null) {
                taskState = coord.getExecStatus().getErrorCode().name();
            } else {
                taskState = context.getState().toString();
            }
        }
        builder.taskState(taskState);
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
        builder.isNereids(context.getState().isNereids() ? "Yes" : "No");
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
        return masterOpExecutor.getStatusCode();
    }

    public String getProxyErrMsg() {
        if (masterOpExecutor == null) {
            return MysqlStateType.UNKNOWN.name();
        }
        return masterOpExecutor.getErrMsg();
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
        return false;
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

    public boolean isCached() {
        return isCached;
    }

    // query with a random sql
    public void execute() throws Exception {
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        if (Config.enable_print_request_before_execution) {
            LOG.info("begin to execute query {} {}",
                    DebugUtil.printId(queryId), originStmt == null ? "null" : originStmt.originStmt);
        }
        queryRetry(queryId);
    }

    public void queryRetry(TUniqueId queryId) throws Exception {
        TUniqueId firstQueryId = queryId;
        UUID uuid;
        int retryTime = Config.max_query_retry_time;
        retryTime = retryTime <= 0 ? 1 : retryTime + 1;
        // If the query is an `outfile` statement,
        // we execute it only once to avoid exporting redundant data.
        if (parsedStmt instanceof Queriable) {
            retryTime = ((Queriable) parsedStmt).hasOutFileClause() ? 1 : retryTime;
        }
        for (int i = 1; i <= retryTime; i++) {
            try {
                execute(queryId);
                return;
            } catch (UserException e) {
                if (!e.getMessage().contains(FeConstants.CLOUD_RETRY_E230) || i == retryTime) {
                    throw e;
                }
                if (this.coord != null && this.coord.isQueryCancelled()) {
                    throw e;
                }
                TUniqueId lastQueryId = queryId;
                uuid = UUID.randomUUID();
                queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                int randomMillis = 10 + (int) (Math.random() * 10);
                if (i > retryTime / 2) {
                    randomMillis = 20 + (int) (Math.random() * 10);
                }
                if (DebugPointUtil.isEnable("StmtExecutor.retry.longtime")) {
                    randomMillis = 1000;
                }
                LOG.warn("receive E-230 tried={} first queryId={} last queryId={} new queryId={} sleep={}ms",
                        i, DebugUtil.printId(firstQueryId), DebugUtil.printId(lastQueryId),
                        DebugUtil.printId(queryId), randomMillis);
                Thread.sleep(randomMillis);
                context.getState().reset();
            } catch (Exception e) {
                throw e;
            }
        }
    }

    public void execute(TUniqueId queryId) throws Exception {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (context.getConnectType() == ConnectType.ARROW_FLIGHT_SQL) {
            context.setReturnResultFromLocal(true);
        }

        try {
            try {
                executeByNereids(queryId);
            } catch (Exception e) {
                if (context.getMinidump() != null && context.getMinidump().toString(4) != null) {
                    MinidumpUtils.saveMinidumpString(context.getMinidump(), DebugUtil.printId(context.queryId()));
                }
                LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                context.getState().setError(e.getMessage());
                return;
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

        profile.getSummaryProfile().setQueryBeginTime(TimeUtils.getStartTimeMs());
        if (context.getSessionVariable().enableProfile) {
            List<List<String>> changedSessionVar = VariableMgr.dumpChangedVars(context.getSessionVariable());
            profile.setChangedSessionVar(DebugUtil.prettyPrintChangedSessionVar(changedSessionVar));
        }
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        parseByNereids();
        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                "Nereids only process LogicalPlanAdapter, but parsedStmt is " + parsedStmt.getClass().getName());
        context.getState().setNereids(true);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
        checkSqlBlocked(logicalPlan.getClass());
        if (context.getCommand() == MysqlCommand.COM_STMT_PREPARE) {
            if (isForwardToMaster()) {
                throw new UserException("Forward master command is not supported for prepare statement");
            }
            long stmtId = Config.prepared_stmt_start_id > 0
                    ? Config.prepared_stmt_start_id : context.getPreparedStmtId();
            this.prepareStmtName = String.valueOf(stmtId);
            // When proxy executing, this.statementContext is created in constructor.
            // But context.statementContext is created in LogicalPlanBuilder.
            List<Placeholder> placeholders = context == null
                    ? statementContext.getPlaceholders() : context.getStatementContext().getPlaceholders();
            logicalPlan = new PrepareCommand(prepareStmtName, logicalPlan, placeholders, originStmt);
        }
        // when we in transaction mode, we only support insert into command and transaction command
        if (context.isTxnModel()) {
            if (!(logicalPlan instanceof BatchInsertIntoTableCommand || logicalPlan instanceof InsertIntoTableCommand
                    || logicalPlan instanceof UpdateCommand || logicalPlan instanceof DeleteFromUsingCommand
                    || logicalPlan instanceof DeleteFromCommand || logicalPlan instanceof TransactionCommand)) {
                String errMsg = "This is in a transaction, only insert, update, delete, "
                        + "commit, rollback is acceptable.";
                throw new NereidsException(errMsg, new AnalysisException(errMsg));
            }
        }
        if (logicalPlan instanceof Command) {
            if (logicalPlan instanceof Redirect) {
                OlapGroupCommitInsertExecutor.analyzeGroupCommit(context, logicalPlan);
                redirectStatus = ((Redirect) logicalPlan).toRedirectStatus();
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
                ((Command) logicalPlan).verifyCommandSupported(context);
                ((Command) logicalPlan).run(context, this);
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
                if (Config.isCloudMode() && e.getDetailMessage().contains(FeConstants.CLOUD_RETRY_E230)) {
                    throw e;
                }
                context.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                throw new NereidsException("Command (" + originStmt.originStmt + ") process failed",
                        new AnalysisException(e.getMessage(), e));
            } catch (Exception | Error e) {
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
            try {
                planner.plan(parsedStmt, context.getSessionVariable().toThrift());
                checkBlockRules();
            } catch (Exception e) {
                LOG.warn("Nereids plan query failed:\n{}", originStmt.originStmt, e);
                throw new NereidsException(new AnalysisException(e.getMessage(), e));
            }
            profile.getSummaryProfile().setQueryPlanFinishTime();
            handleQueryWithRetry(queryId);
        }
    }

    public static void initBlockSqlAstNames() {
        blockSqlAstNames.clear();
        blockSqlAstNames = Pattern.compile(",")
                .splitAsStream(Config.block_sql_ast_names)
                .map(String::trim)
                .collect(Collectors.toSet());
        if (blockSqlAstNames.isEmpty() && !Config.block_sql_ast_names.isEmpty()) {
            blockSqlAstNames.add(Config.block_sql_ast_names);
        }
    }

    public void checkSqlBlocked(Class<?> clazz) throws UserException {
        if (blockSqlAstNames.contains(clazz.getSimpleName())) {
            throw new UserException("SQL is blocked with AST name: " + clazz.getSimpleName());
        }
    }

    private void parseByNereids() {
        if (parsedStmt != null) {
            return;
        }
        List<StatementBase> statements;
        try {
            getProfile().getSummaryProfile().setParseSqlStartTime(System.currentTimeMillis());
            statements = new NereidsParser().parseSQL(originStmt.originStmt, context.getSessionVariable());
            getProfile().getSummaryProfile().setParseSqlFinishTime(System.currentTimeMillis());
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        if (statements.isEmpty()) {
            // for test only
            parsedStmt = new LogicalPlanAdapter(new EmptyCommand(), new StatementContext());
        } else {
            if (statements.size() <= originStmt.idx) {
                throw new ParseException("Nereids parse failed. Parser get " + statements.size() + " statements,"
                        + " but we need at least " + originStmt.idx + " statements.");
            }
            parsedStmt = statements.get(originStmt.idx);
        }
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
        retryTime = retryTime <= 0 ? 1 : retryTime + 1;
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
                    context.setNeedRegenerateInstanceId(newQueryId);
                    if (Config.isCloudMode()) {
                        // sleep random millis [1000, 1500] ms
                        // in the begining of retryTime/2
                        int randomMillis = 1000 + (int) (Math.random() * (1000 - 500));
                        LOG.debug("stmt executor retry times {}, wait randomMillis:{}, stmt:{}",
                                i, randomMillis, originStmt.originStmt);
                        try {
                            if (i > retryTime / 2) {
                                // sleep random millis [2000, 2500] ms
                                // in the ending of retryTime/2
                                randomMillis = 2000 + (int) (Math.random() * (1000 - 500));
                            }
                            Thread.sleep(randomMillis);
                        } catch (InterruptedException e) {
                            LOG.info("stmt executor sleep wait InterruptedException: ", e);
                        }
                    }
                }
                if (context.getConnectType() == ConnectType.ARROW_FLIGHT_SQL) {
                    context.setReturnResultFromLocal(false);
                }
                handleQueryStmt();
                LOG.info("Query {} finished", DebugUtil.printId(context.queryId));
                break;
            } catch (RpcException | UserException e) {
                if (Config.isCloudMode() && e.getMessage().contains(FeConstants.CLOUD_RETRY_E230)) {
                    throw e;
                }
                // If the previous try is timeout or cancelled, then do not need try again.
                if (this.coord != null && (this.coord.isQueryCancelled() || this.coord.isTimeout())) {
                    throw e;
                }
                LOG.warn("due to exception {} retry {} rpc {} user {}",
                        e.getMessage(), i, e instanceof RpcException, e instanceof UserException);

                boolean isNeedRetry = false;
                if (Config.isCloudMode()) {
                    // cloud mode retry
                    isNeedRetry = false;
                    // errCode = 2, detailMessage = No backend available as scan node,
                    // please check the status of your backends. [10003: not alive]
                    List<String> bes = Env.getCurrentSystemInfo().getAllBackendIds().stream()
                                .map(id -> Long.toString(id)).collect(Collectors.toList());
                    String msg = e.getMessage();
                    if (e instanceof UserException
                            && msg.contains(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG)) {
                        Matcher matcher = beIpPattern.matcher(msg);
                        // here retry planner not be recreated, so
                        // in cloud mode drop node, be id invalid, so need not retry
                        // such as be ids [11000, 11001] -> after drop node 11001
                        // don't need to retry 11001's request
                        if (matcher.find()) {
                            String notAliveBe = matcher.group(1);
                            isNeedRetry = bes.contains(notAliveBe);
                            if (isNeedRetry) {
                                Backend abnormalBe = Env.getCurrentSystemInfo().getBackend(Long.parseLong(notAliveBe));
                                String deadCloudClusterStatus = abnormalBe.getCloudClusterStatus();
                                String deadCloudClusterClusterName = abnormalBe.getCloudClusterName();
                                LOG.info("need retry cluster {} status {}", deadCloudClusterClusterName,
                                        deadCloudClusterStatus);
                                if (Strings.isNullOrEmpty(deadCloudClusterStatus)
                                        || ClusterStatus.valueOf(deadCloudClusterStatus) != ClusterStatus.NORMAL) {
                                    ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                                            .waitForAutoStart(deadCloudClusterClusterName);
                                }
                            }
                        }
                    }
                } else {
                    isNeedRetry = e instanceof RpcException;
                }
                if (i != retryTime - 1 && isNeedRetry
                        && context.getConnectType().equals(ConnectType.MYSQL) && !context.getMysqlChannel().isSend()) {
                    LOG.warn("retry {} times. stmt: {}", (i + 1), parsedStmt.getOrigStmt().originStmt);
                } else {
                    throw e;
                }
            } finally {
                if (context.isReturnResultFromLocal()) {
                    finalizeQuery();
                }
                LOG.debug("Finalize query {}", DebugUtil.printId(context.queryId()));
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
     */
    private void analyzeVariablesInStmt() throws DdlException {
    }

    private boolean isQuery() {
        return parsedStmt instanceof LogicalPlanAdapter
                && !(((LogicalPlanAdapter) parsedStmt).getLogicalPlan() instanceof Command);
    }

    public boolean isProfileSafeStmt() {
        // fe/fe-core/src/main/java/org/apache/doris/nereids/NereidsPlanner.java:131
        // Only generate profile for NereidsPlanner.
        if (!(parsedStmt instanceof LogicalPlanAdapter)) {
            return false;
        }

        LogicalPlan plan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();

        if (plan instanceof InsertIntoTableCommand) {
            LogicalPlan logicalPlan = ((InsertIntoTableCommand) plan).getLogicalQuery();
            // Do not generate profile for insert into t values xxx.
            // t could be an olap-table or an external-table.
            if ((logicalPlan instanceof UnboundTableSink) || (logicalPlan instanceof UnboundBaseExternalTableSink)) {
                if (logicalPlan.children() == null || logicalPlan.children().isEmpty()) {
                    return false;
                }

                for (Plan child : logicalPlan.children()) {
                    // InlineTable means insert into t VALUES xxx.
                    if (child instanceof InlineTable) {
                        return false;
                    }
                }
            }
            return true;
        }

        // Generate profile for:
        // 1. CreateTableCommand(mainly for create as select).
        // 2. LoadCommand.
        // 3. InsertOverwriteTableCommand.
        if ((plan instanceof Command) && !(plan instanceof LoadCommand)
                && !(plan instanceof CreateTableCommand) && !(plan instanceof InsertOverwriteTableCommand)) {
            // Commands like SHOW QUERY PROFILE will not have profile.
            return false;
        } else {
            // 4. For all the other statements.
            return true;
        }
    }

    private void forwardToMaster() throws Exception {
        masterOpExecutor = new MasterOpExecutor(originStmt, context, redirectStatus, isQuery());
        if (LOG.isDebugEnabled()) {
            LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());
        }
        masterOpExecutor.execute();
        if (parsedStmt instanceof LogicalPlanAdapter) {
            // for nereids command
            if (((LogicalPlanAdapter) parsedStmt).getLogicalPlan() instanceof Forward) {
                Forward forward = (Forward) ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
                forward.afterForwardToMaster(context);
            }
        } else if (parsedStmt instanceof SetStmt) {
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
        if (!context.getSessionVariable().enableProfile() || !isProfileSafeStmt()) {
            return;
        }
        // If any error happened in update profile, we should ignore this error
        // and ensure the sql is finished normally. For example, if update profile
        // failed, the insert stmt should be success
        try {
            profile.updateSummary(getSummaryInfo(isFinished), isFinished, this.planner);
            if (planner instanceof NereidsPlanner) {
                NereidsPlanner nereidsPlanner = ((NereidsPlanner) planner);
                profile.setPhysicalPlan(nereidsPlanner.getPhysicalPlan());
            }
        } catch (Throwable t) {
            LOG.warn("failed to update profile, ignore this error", t);
        }
    }

    // Because this is called by other thread
    public void cancel(Status cancelReason, boolean needWaitCancelComplete) {
        if (masterOpExecutor != null) {
            try {
                masterOpExecutor.cancel();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }
        Optional<InsertOverwriteTableCommand> insertOverwriteTableCommand = getInsertOverwriteTableCommand();
        if (insertOverwriteTableCommand.isPresent()) {
            // If the be scheduling has not been triggered yet, cancel the scheduling first
            insertOverwriteTableCommand.get().cancel();
        }
        Coordinator coordRef = coord;
        if (coordRef != null) {
            coordRef.cancel(cancelReason);
        }
        if (mysqlLoadId != null) {
            Env.getCurrentEnv().getLoadManager().getMysqlLoadManager().cancelMySqlLoad(mysqlLoadId);
        }
        if (insertOverwriteTableCommand.isPresent() && needWaitCancelComplete) {
            // Wait for the command to run or cancel completion
            insertOverwriteTableCommand.get().waitNotRunning();
        }
    }

    public void cancel(Status cancelReason) {
        cancel(cancelReason, true);
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

        Queriable queryStmt = (Queriable) parsedStmt;
        boolean isSendFields = false;
        if (cacheResult != null) {
            isCached = true;
            if (cacheAnalyzer.getHitRange() == Cache.HitRange.Full) {
                sendCachedValues(channel, cacheResult.getValuesList(), queryStmt, isSendFields, true);
                return;
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
        if (context.supportHandleByFe()) {
            Optional<ResultSet> resultSet = planner.handleQueryInFe(parsedStmt);
            if (resultSet.isPresent()) {
                sendResultSet(resultSet.get(), ((Queriable) parsedStmt).getFieldInfos());
                isHandleQueryInFe = true;
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
        boolean isBlackHoleQuery = queryStmt.hasBlackHoleClause();
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
        if (channel != null && !isOutfileQuery && !isBlackHoleQuery
                && CacheAnalyzer.canUseCache(context.getSessionVariable())
                && parsedStmt.getOrigStmt() != null && parsedStmt.getOrigStmt().originStmt != null) {
            if (queryStmt instanceof LogicalPlanAdapter) {
                handleCacheStmt(cacheAnalyzer, channel);
                return;
            }
        }

        executeAndSendResult(isOutfileQuery, false, queryStmt, channel, null, null);
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
        boolean isBlackHoleClause = queryStmt.hasBlackHoleClause();
        if (isBlackHoleClause && blackholeResultHandler == null) {
            blackholeResultHandler = new BlackholeResultHandler();
        }
        if (statementContext.isShortCircuitQuery()) {
            ShortCircuitQueryContext shortCircuitQueryContext =
                        statementContext.getShortCircuitQueryContext() != null
                                ? statementContext.getShortCircuitQueryContext()
                                : new ShortCircuitQueryContext(planner, (Queriable) parsedStmt);
            coordBase = new PointQueryExecutor(shortCircuitQueryContext,
                        context.getSessionVariable().getMaxMsgSizeOfResultReceiver());
            context.getState().setIsQuery(true);
        } else if (planner instanceof NereidsPlanner && ((NereidsPlanner) planner).getDistributedPlans() != null) {
            coord = new NereidsCoordinator(context,
                    (NereidsPlanner) planner, context.getStatsErrorEstimator());
            profile.addExecutionProfile(coord.getExecutionProfile());
            QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                    new QueryInfo(context, originStmt.originStmt, coord));
            coordBase = coord;
        } else {
            coord = EnvFactory.getInstance().createCoordinator(
                    context, planner, context.getStatsErrorEstimator());
            profile.addExecutionProfile(coord.getExecutionProfile());
            QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                    new QueryInfo(context, originStmt.originStmt, coord));
            coordBase = coord;
        }

        coordBase.setIsProfileSafeStmt(this.isProfileSafeStmt());

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
                if (cacheAnalyzer != null && !isOutfileQuery && !isBlackHoleClause && !isDryRun) {
                    cacheAnalyzer.copyRowBatch(batch);
                }
                if (batch.getBatch() != null) {
                    // register send field result time.
                    profile.getSummaryProfile().setTempStartTime();
                    // For some language driver, getting error packet after fields packet
                    // will be recognized as a success result
                    // so We need to send fields after first batch arrived
                    if (!isSendFields) {
                        if (isOutfileQuery) {
                            if (!Strings.isNullOrEmpty(queryStmt.getOutFileClause().getSuccessFileName())) {
                                outfileWriteSuccess(queryStmt.getOutFileClause());
                            }
                            sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                        } else if (isBlackHoleClause) {
                            // do nothing
                        } else {
                            sendFields(queryStmt.getColLabels(), queryStmt.getFieldInfos(),
                                    getReturnTypes(queryStmt));
                        }
                        isSendFields = true;
                    }
                    if (isBlackHoleClause) {
                        // For blackhole queries, aggregate data by BE nodes
                        Map<String, String> attachedInfos = batch.getBatch().getAttachedInfos();
                        if (attachedInfos != null && !attachedInfos.isEmpty()) {
                            blackholeResultHandler.processBlackholeData(attachedInfos);
                        }
                        context.updateReturnRows(0);
                    } else {
                        // For non-blackhole queries, send data as before
                        for (ByteBuffer row : batch.getBatch().getRows()) {
                            channel.sendOnePacket(row);
                        }
                        context.updateReturnRows(batch.getBatch().getRows().size());
                    }
                    profile.getSummaryProfile().freshWriteResultConsumeTime();
                    context.addResultAttachedInfo(batch.getBatch().getAttachedInfos());
                }
                if (batch.isEos()) {
                    // For blackhole queries, send aggregated data when query is complete
                    if (isBlackHoleClause && blackholeResultHandler.hasData()) {
                        // Send aggregated results to client
                        blackholeResultHandler.sendAggregatedBlackholeResults(this);
                    }
                    break;
                }
            }
            if (cacheAnalyzer != null && !isDryRun && !isBlackHoleClause) {
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
                if (isOutfileQuery) {
                    sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                } else if (isBlackHoleClause) {
                    // do nothing
                } else {
                    if (ConnectContext.get() != null && isDryRun) {
                        // Return a one row one column result set, with the real result number
                        long rows = 0;
                        if (coordBase instanceof Coordinator) {
                            rows = ((Coordinator) coordBase).getNumReceivedRows();
                        } else if (batch.getQueryStatistics() != null) {
                            rows = batch.getQueryStatistics().getReturnedRows();
                        }
                        List<String> data = Lists.newArrayList(String.valueOf(rows));
                        ResultSet resultSet = new CommonResultSet(DRY_RUN_QUERY_METADATA,
                                Collections.singletonList(data));
                        sendResultSet(resultSet);
                        return;
                    } else {
                        sendFields(queryStmt.getColLabels(), queryStmt.getFieldInfos(),
                                getReturnTypes(queryStmt));
                    }
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
            coordBase.cancel(internalErrorSt);
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
        for (Backend be : Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values()) {
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
        POutfileWriteSuccessResult result = future.get();
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

    public static void syncLoadForTablets(List<List<Backend>> backendsList, List<Long> allTabletIds) {
        backendsList.forEach(backends -> backends.forEach(backend -> {
            if (backend.isAlive()) {
                List<Long> tabletIdList = new ArrayList<Long>();
                Set<Long> beTabletIds = ((CloudEnv) Env.getCurrentEnv())
                                           .getCloudTabletRebalancer()
                                           .getSnapshotTabletsInPrimaryByBeId(backend.getId());
                allTabletIds.forEach(tabletId -> {
                    if (beTabletIds.contains(tabletId)) {
                        tabletIdList.add(tabletId);
                    }
                });
                boolean ok = false;
                TNetworkAddress address = null;
                Client client = null;
                try {
                    address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                    client = ClientPool.backendPool.borrowObject(address);
                    client.syncLoadForTablets(new TSyncLoadForTabletsRequest(allTabletIds));
                    ok = true;
                } catch (Exception e) {
                    LOG.warn(e.getMessage());
                } finally {
                    if (!ok) {
                        ClientPool.backendPool.invalidateObject(address, client);
                    } else {
                        ClientPool.backendPool.returnObject(address, client);
                    }
                }
            }
        }));
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

    public void sendStmtPrepareOK(int stmtId, List<String> labels, List<Slot> output) throws IOException {
        Preconditions.checkState(context.getConnectType() == ConnectType.MYSQL);
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response
        serializer.reset();
        // 0x00 OK
        serializer.writeInt1(0);
        // statement_id
        serializer.writeInt4(stmtId);
        // num_columns
        int numColumns = output == null ? 0 : output.size();
        serializer.writeInt2(numColumns);
        // num_params
        int numParams = labels.size();
        serializer.writeInt2(numParams);
        // reserved_1
        serializer.writeInt1(0);
        if (numParams > 0 || numColumns > 0) {
            // warning_count
            serializer.writeInt2(0);
            // metadata_follows
            serializer.writeInt1(1);
        }
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
        if (numColumns > 0) {
            for (Slot slot : output) {
                serializer.reset();
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalTable().isPresent()) {
                    SlotReference slotReference = (SlotReference) slot;
                    TableIf table = slotReference.getOriginalTable().get();
                    Column column = slotReference.getOriginalColumn().get();
                    DatabaseIf database = table.getDatabase();
                    String dbName = database == null ? "" : database.getFullName();
                    serializer.writeField(dbName, table.getName(), column, false);
                } else {
                    serializer.writeField(slot.getName(), slot.getDataType().toCatalogDataType());
                }
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
        StatementContext statementContext = context.getStatementContext();
        boolean isShortCircuited = statementContext.isShortCircuitQuery()
                        && statementContext.getShortCircuitQueryContext() != null;
        ShortCircuitQueryContext ctx = statementContext.getShortCircuitQueryContext();
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            if (context.getCommand() == MysqlCommand.COM_STMT_EXECUTE && isShortCircuited) {
                // Using PreparedStatment pre serializedField to avoid serialize each time
                // we send a field
                byte[] serializedField = ctx.getSerializedField(i);
                if (serializedField == null) {
                    if (fieldInfos != null) {
                        serializer.writeField(fieldInfos.get(i), types.get(i));
                    } else {
                        serializer.writeField(colNames.get(i), types.get(i));
                    }
                    serializedField = serializer.toArray();
                    ctx.addSerializedField(i, serializedField);
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
            if (isComStmtExecute) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Use binary protocol to set result.");
                }
                sendBinaryResultRow(resultSet);
            } else {
                sendTextResultRow(resultSet);
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

    protected void sendTextResultRow(ResultSet resultSet) throws IOException {
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
    }

    protected void sendBinaryResultRow(ResultSet resultSet) throws IOException {
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value
        ResultSetMetaData metaData = resultSet.getMetaData();
        int nullBitmapLength = (metaData.getColumnCount() + 7 + 2) / 8;
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            // Reserved one byte.
            serializer.writeByte((byte) 0x00);
            byte[] nullBitmap = new byte[nullBitmapLength];
            // Generate null bitmap
            for (int i = 0; i < row.size(); i++) {
                String item = row.get(i);
                if (item == null || item.equals(FeConstants.null_string)) {
                    // The first 2 bits are reserved.
                    int byteIndex = (i + 2) / 8;  // Index of the byte in the bitmap array
                    int bitInByte = (i + 2) % 8;  // Position within the target byte (0-7)
                    nullBitmap[byteIndex] |= (1 << bitInByte);
                }
            }
            // Null bitmap
            serializer.writeBytes(nullBitmap);
            // Non-null columns
            for (int i = 0; i < row.size(); i++) {
                String item = row.get(i);
                if (item != null && !item.equals(FeConstants.null_string)) {
                    Column col = metaData.getColumn(i);
                    switch (col.getType().getPrimitiveType()) {
                        case INT:
                            serializer.writeInt4(Integer.parseInt(item));
                            break;
                        case BIGINT:
                            serializer.writeInt8(Long.parseLong(item));
                            break;
                        case DATETIME:
                        case DATETIMEV2:
                            DateTimeV2Literal datetime = new DateTimeV2Literal(item);
                            long microSecond = datetime.getMicroSecond();
                            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
                            int length = microSecond == 0 ? 7 : 11;
                            serializer.writeInt1(length);
                            serializer.writeInt2((int) (datetime.getYear()));
                            serializer.writeInt1((int) datetime.getMonth());
                            serializer.writeInt1((int) datetime.getDay());
                            serializer.writeInt1((int) datetime.getHour());
                            serializer.writeInt1((int) datetime.getMinute());
                            serializer.writeInt1((int) datetime.getSecond());
                            if (microSecond > 0) {
                                serializer.writeInt4((int) microSecond);
                            }
                            break;
                        default:
                            serializer.writeLenEncodedString(item);
                    }
                }
            }
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
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

    public void handleReplayStmt(String result) throws IOException {
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Plan Replayer dump url",
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

    private boolean isShortCircuitedWithCtx() {
        return statementContext.isShortCircuitQuery()
                        && statementContext.getShortCircuitQueryContext() != null;
    }

    private List<Type> exprToType(List<Expr> exprs) {
        return exprs.stream().map(e -> e.getType()).collect(Collectors.toList());
    }

    public StatementBase setParsedStmt(StatementBase parsedStmt) {
        this.parsedStmt = parsedStmt;
        this.statementContext.setParsedStatement(parsedStmt);
        return parsedStmt;
    }

    public List<Slot> planPrepareStatementSlots() throws Exception {
        parseByNereids();
        Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                "Nereids only process LogicalPlanAdapter,"
                        + " but parsedStmt is " + parsedStmt.getClass().getName());
        NereidsPlanner nereidsPlanner = new NereidsPlanner(statementContext);
        nereidsPlanner.plan(parsedStmt, context.getSessionVariable().toThrift());
        return nereidsPlanner.getPhysicalPlan().getOutput();
    }

    public List<ResultRow> executeInternalQuery() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("INTERNAL QUERY: {}", originStmt.toString());
        }
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        context.setQueryId(queryId);
        if (originStmt.originStmt != null) {
            context.setSqlHash(DigestUtils.md5Hex(originStmt.originStmt));
        }
        try {
            List<ResultRow> resultRows = new ArrayList<>();
            try {
                parseByNereids();
                Preconditions.checkState(parsedStmt instanceof LogicalPlanAdapter,
                        "Nereids only process LogicalPlanAdapter,"
                                + " but parsedStmt is " + parsedStmt.getClass().getName());
                context.getState().setNereids(true);
                context.getState().setIsQuery(true);
                context.getState().setInternal(true);
                planner = new NereidsPlanner(statementContext);
                planner.plan(parsedStmt, context.getSessionVariable().toThrift());
            } catch (Exception e) {
                LOG.warn("Failed to run internal SQL: {}", originStmt, e);
                throw new RuntimeException("Failed to execute internal SQL. " + Util.getRootCauseMessage(e), e);
            }
            RowBatch batch;
            if (Config.enable_collect_internal_query_profile) {
                context.getSessionVariable().enableProfile = true;
            }
            coord = EnvFactory.getInstance().createCoordinator(context,
                    planner, context.getStatsErrorEstimator());
            profile.addExecutionProfile(coord.getExecutionProfile());
            try {
                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                        new QueryInfo(context, originStmt.originStmt, coord));
            } catch (UserException e) {
                throw new RuntimeException("Failed to execute internal SQL. " + Util.getRootCauseMessage(e), e);
            }
            updateProfile(false);
            try {
                coord.exec();
            } catch (Exception e) {
                throw new InternalQueryExecutionException(e.getMessage() + Util.getRootCauseMessage(e), e);
            }

            try {
                while (true) {
                    batch = coord.getNext();
                    Preconditions.checkNotNull(batch, "Batch is Null.");
                    if (batch.isEos()) {
                        LOG.info("Result rows for query {} is {}", DebugUtil.printId(queryId), resultRows.size());
                        return resultRows;
                    } else {
                        // For null and not EOS batch, continue to get the next batch.
                        if (batch.getBatch() == null) {
                            continue;
                        }
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
            QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
            updateProfile(true);
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

    public List<Type> getReturnTypes(Queriable stmt) {
        if (isShortCircuitedWithCtx()) {
            return statementContext.getShortCircuitQueryContext().getReturnTypes();
        }
        return exprToType(stmt.getResultExprs());
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
                context.setGroupCommit(true);
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

    public HttpStreamParams generateHttpStreamPlan(TUniqueId queryId) throws Exception {
        SessionVariable sessionVariable = context.getSessionVariable();
        HttpStreamParams httpStreamParams = null;
        try {
            try {
                // disable shuffle for http stream (only 1 sink)
                sessionVariable.setVarOnce(SessionVariable.ENABLE_STRICT_CONSISTENCY_DML, "false");
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
                if (e instanceof NereidsException) {
                    LOG.warn("Analyze failed. {}", context.getQueryIdentifier(), e);
                    throw ((NereidsException) e).getException();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
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

    public String getPrepareStmtName() {
        return this.prepareStmtName;
    }
}
