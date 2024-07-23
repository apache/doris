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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NotFallbackInParser;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlServerStatusFlag;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.CacheKeyType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.plugin.DialectConverterPlugin;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionEntry;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Process one connection, the life cycle is the same as connection
 */
public abstract class ConnectProcessor {
    public enum ConnectType {
        MYSQL,
        ARROW_FLIGHT_SQL
    }

    private static final Logger LOG = LogManager.getLogger(ConnectProcessor.class);
    protected final ConnectContext ctx;
    protected StmtExecutor executor = null;
    protected ConnectType connectType;
    protected ArrayList<StmtExecutor> returnResultFromRemoteExecutor = new ArrayList<>();

    public ConnectProcessor(ConnectContext context) {
        this.ctx = context;
    }

    public ConnectContext getConnectContext() {
        return ctx;
    }

    public boolean isHandleQueryInFe() {
        return executor.isHandleQueryInFe();
    }

    // change current database of this session.
    protected void handleInitDb(String fullDbName) {
        String catalogName = null;
        String dbName = null;
        String[] dbNames = fullDbName.split("\\.");
        if (dbNames.length == 1) {
            dbName = fullDbName;
        } else if (dbNames.length == 2) {
            catalogName = dbNames[0];
            dbName = dbNames[1];
        } else if (dbNames.length > 2) {
            ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "Only one dot can be in the name: " + fullDbName);
            return;
        }

        //  mysql client
        if (Config.isCloudMode()) {
            try {
                dbName = ((CloudEnv) ctx.getEnv()).analyzeCloudCluster(dbName, ctx);
            } catch (DdlException e) {
                ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                return;
            }
            if (dbName == null || dbName.isEmpty()) {
                return;
            }
        }

        // check catalog and db exists
        if (catalogName != null) {
            CatalogIf catalogIf = ctx.getEnv().getCatalogMgr().getCatalog(catalogName);
            if (catalogIf == null) {
                ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "No match catalog in doris: " + fullDbName);
                return;
            }
            if (catalogIf.getDbNullable(dbName) == null) {
                ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "No match database in doris: " + fullDbName);
                return;
            }
        }
        try {
            if (catalogName != null) {
                ctx.getEnv().changeCatalog(ctx, catalogName);
            }
            ctx.getEnv().changeDb(ctx, dbName);
        } catch (DdlException e) {
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            return;
        } catch (Throwable t) {
            ctx.getState().setError(ErrorCode.ERR_INTERNAL_ERROR, Util.getRootCauseMessage(t));
            return;
        }

        ctx.getState().setOk();
    }

    // set killed flag
    protected void handleQuit() {
        ctx.setKilled();
        ctx.getState().setOk();
    }

    // do nothing
    protected void handlePing() {
        ctx.getState().setOk();
    }

    protected void handleStmtReset() {
        ctx.getState().setOk();
    }

    protected void handleStmtClose(int stmtId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("close stmt id: {}", stmtId);
        }
        ConnectContext.get().removePrepareStmt(String.valueOf(stmtId));
        // No response packet is sent back to the client, see
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
        ctx.getState().setNoop();
    }

    protected static boolean isNull(byte[] bitmap, int position) {
        return (bitmap[position / 8] & (1 << (position & 7))) != 0;
    }

    protected void auditAfterExec(String origStmt, StatementBase parsedStmt,
            Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        if (Config.enable_bdbje_debug_mode) {
            return;
        }
        AuditLogHelper.logAuditLog(ctx, origStmt, parsedStmt, statistics, printFuzzyVariables);
    }

    // only throw an exception when there is a problem interacting with the requesting client
    protected void handleQuery(MysqlCommand mysqlCommand, String originStmt) throws ConnectionException {
        if (Config.isCloudMode()) {
            if (!ctx.getCurrentUserIdentity().isRootUser()
                    && ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getInstanceStatus()
                        == Cloud.InstanceInfoPB.Status.OVERDUE) {
                Exception exception = new Exception("warehouse is overdue!");
                try {
                    handleQueryException(exception, originStmt, null, null);
                } catch (ConnectionException ignore) {
                    // ignore, should not happen
                }
                return;
            }
        }
        try {
            executeQuery(mysqlCommand, originStmt);
        } catch (ConnectionException exception) {
            throw exception;
        } catch (Exception ignored) {
            // saved use handleQueryException
        }
    }

    public void executeQuery(MysqlCommand mysqlCommand, String originStmt) throws Exception {
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
            MetricRepo.increaseClusterRequestAll(ctx.getCloudCluster(false));
        }

        String convertedStmt = convertOriginStmt(originStmt);
        String sqlHash = DigestUtils.md5Hex(convertedStmt);
        ctx.setSqlHash(sqlHash);

        SessionVariable sessionVariable = ctx.getSessionVariable();
        boolean wantToParseSqlFromSqlCache = sessionVariable.isEnableNereidsPlanner()
                && CacheAnalyzer.canUseSqlCache(sessionVariable);
        List<StatementBase> stmts = null;
        Exception nereidsParseException = null;
        Exception nereidsSyntaxException = null;
        long parseSqlStartTime = System.currentTimeMillis();
        List<StatementBase> cachedStmts = null;
        // Currently we add a config to decide whether using PREPARED/EXECUTE command for nereids
        // TODO: after implemented full prepared, we could remove this flag
        boolean nereidsUseServerPrep = sessionVariable.enableServeSidePreparedStatement
                        || mysqlCommand == MysqlCommand.COM_QUERY;
        CacheKeyType cacheKeyType = null;
        if (nereidsUseServerPrep && sessionVariable.isEnableNereidsPlanner()) {
            if (wantToParseSqlFromSqlCache) {
                cachedStmts = parseFromSqlCache(originStmt);
                Optional<SqlCacheContext> sqlCacheContext = ConnectContext.get()
                        .getStatementContext().getSqlCacheContext();
                if (sqlCacheContext.isPresent()) {
                    cacheKeyType = sqlCacheContext.get().getCacheKeyType();
                }
                if (cachedStmts != null) {
                    stmts = cachedStmts;
                }
            }

            if (cachedStmts == null) {
                try {
                    stmts = new NereidsParser().parseSQL(convertedStmt, sessionVariable);
                } catch (NotSupportedException e) {
                    // Parse sql failed, audit it and return
                    handleQueryException(e, convertedStmt, null, null);
                    return;
                } catch (ParseException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Nereids parse sql failed. Reason: {}. Statement: \"{}\".",
                                e.getMessage(), convertedStmt);
                    }
                    // ATTN: Do not set nereidsParseException in this case.
                    // Because ParseException means the sql is not supported by Nereids.
                    // It should be parsed by old parser, so not setting nereidsParseException to avoid
                    // suppressing the exception thrown by old parser.
                    nereidsParseException = e;
                } catch (Exception e) {
                    // TODO: We should catch all exception here until we support all query syntax.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Nereids parse sql failed with other exception. Reason: {}. Statement: \"{}\".",
                                e.getMessage(), convertedStmt);
                    }
                    nereidsSyntaxException = e;
                }
            }
        }

        // stmts == null when Nereids cannot planner this query or Nereids is disabled.
        if (stmts == null) {
            try {
                stmts = parse(convertedStmt);
            } catch (Throwable throwable) {
                // if NereidsParser and oldParser both failed,
                // prove is a new feature implemented only on the nereids,
                // so an error message for the new nereids is thrown
                if (nereidsSyntaxException != null) {
                    throwable = nereidsSyntaxException;
                }
                // Parse sql failed, audit it and return
                handleQueryException(throwable, convertedStmt, null, null);
                return;
            }
        }

        if (mysqlCommand == MysqlCommand.COM_QUERY
                && ctx.getSessionVariable().isEnableNereidsPlanner()
                && !ctx.getSessionVariable().enableFallbackToOriginalPlanner
                && stmts.stream().allMatch(s -> s instanceof NotFallbackInParser)) {
            String errMsg;
            Throwable exception = null;
            if (nereidsParseException != null) {
                errMsg = nereidsParseException.getMessage();
                exception = nereidsParseException;
            } else if (nereidsSyntaxException != null) {
                errMsg = nereidsSyntaxException.getMessage();
                exception = nereidsSyntaxException;
            } else {
                errMsg = "Nereids parse DQL failed. " + originStmt;
            }
            if (exception == null) {
                exception = new AnalysisException(errMsg);
            } else {
                exception = new AnalysisException(errMsg, exception);
            }
            handleQueryException(exception, originStmt, null, null);
            return;
        }

        List<String> origSingleStmtList = null;
        // if stmts.size() > 1, split originStmt to multi singleStmts
        if (stmts.size() > 1) {
            try {
                origSingleStmtList = SqlUtils.splitMultiStmts(convertedStmt);
            } catch (Exception ignore) {
                LOG.warn("Try to parse multi origSingleStmt failed, originStmt: \"{}\"", convertedStmt);
            }
        }
        long parseSqlFinishTime = System.currentTimeMillis();

        boolean usingOrigSingleStmt = origSingleStmtList != null && origSingleStmtList.size() == stmts.size();
        for (int i = 0; i < stmts.size(); ++i) {
            String auditStmt = usingOrigSingleStmt ? origSingleStmtList.get(i) : convertedStmt;
            try {
                ctx.getState().reset();
                if (i > 0) {
                    ctx.resetReturnRows();
                }

                StatementBase parsedStmt = stmts.get(i);
                parsedStmt.setOrigStmt(new OriginStatement(auditStmt, usingOrigSingleStmt ? 0 : i));
                parsedStmt.setUserInfo(ctx.getCurrentUserIdentity());
                executor = new StmtExecutor(ctx, parsedStmt);
                executor.getProfile().getSummaryProfile().setParseSqlStartTime(parseSqlStartTime);
                executor.getProfile().getSummaryProfile().setParseSqlFinishTime(parseSqlFinishTime);
                ctx.setExecutor(executor);

                if (cacheKeyType != null) {
                    SqlCacheContext sqlCacheContext =
                            executor.getContext().getStatementContext().getSqlCacheContext().get();
                    sqlCacheContext.setCacheKeyType(cacheKeyType);
                }

                try {
                    executor.execute();
                    if (connectType.equals(ConnectType.MYSQL)) {
                        if (i != stmts.size() - 1) {
                            ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                            if (ctx.getState().getStateType() != MysqlStateType.ERR) {
                                // here, doris do different with mysql.
                                // when client not request CLIENT_MULTI_STATEMENTS, mysql treat all query as
                                // single statement. Doris treat it with multi statement, but only return
                                // the last statement result.
                                if (getConnectContext().getCapability().isClientMultiStatements()) {
                                    finalizeCommand();
                                }
                            }
                        }
                    } else if (connectType.equals(ConnectType.ARROW_FLIGHT_SQL)) {
                        if (!ctx.isReturnResultFromLocal()) {
                            returnResultFromRemoteExecutor.add(executor);
                        }
                        Preconditions.checkState(ctx.getFlightSqlChannel().resultNum() <= 1);
                        if (ctx.getFlightSqlChannel().resultNum() == 1 && i != stmts.size() - 1) {
                            String errMsg = "Only be one stmt that returns the result and it is at the end. "
                                    + "stmts.size(): " + stmts.size();
                            LOG.warn(errMsg);
                            ctx.getState().setError(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT, errMsg);
                            ctx.getState().setErrType(QueryState.ErrType.OTHER_ERR);
                            break;
                        }
                    }
                    auditAfterExec(auditStmt, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(),
                            true);
                    LOG.debug("Write audit logs for query {}", DebugUtil.printId(ctx.queryId));
                    // execute failed, skip remaining stmts
                    if (ctx.getState().getStateType() == MysqlStateType.ERR) {
                        break;
                    }
                } catch (Throwable throwable) {
                    handleQueryException(throwable, auditStmt, executor.getParsedStmt(),
                            executor.getQueryStatisticsForAuditLog());
                    // execute failed, skip remaining stmts
                    throw throwable;
                }
            } finally {
                StatementContext statementContext = ctx.getStatementContext();
                if (statementContext != null) {
                    statementContext.close();
                }
            }
        }
    }

    private List<StatementBase> parseFromSqlCache(String originStmt) {
        StatementContext statementContext = new StatementContext(ctx, new OriginStatement(originStmt, 0));
        ctx.setStatementContext(statementContext);
        try {
            Optional<Pair<ExplainOptions, String>> explainPlan = NereidsParser.tryParseExplainPlan(originStmt);
            String cacheSqlKey = originStmt;
            if (explainPlan.isPresent()) {
                cacheSqlKey = explainPlan.get().second;
            }
            Env env = ctx.getEnv();
            Optional<LogicalSqlCache> sqlCachePlanOpt = env.getSqlCacheManager().tryParseSql(ctx, cacheSqlKey);
            if (sqlCachePlanOpt.isPresent()) {
                LogicalSqlCache logicalSqlCache = sqlCachePlanOpt.get();
                LogicalPlan parsedPlan = logicalSqlCache;
                if (explainPlan.isPresent()) {
                    ExplainOptions explainOptions = explainPlan.get().first;
                    parsedPlan = new ExplainCommand(
                            explainOptions.getExplainLevel(),
                            parsedPlan,
                            explainOptions.showPlanProcess()
                    );
                }

                LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(parsedPlan, statementContext);
                logicalPlanAdapter.setColLabels(
                        Lists.newArrayList(logicalSqlCache.getColumnLabels())
                );
                logicalPlanAdapter.setFieldInfos(Lists.newArrayList(logicalSqlCache.getFieldInfos()));
                logicalPlanAdapter.setResultExprs(logicalSqlCache.getResultExprs());
                logicalPlanAdapter.setOrigStmt(statementContext.getOriginStatement());
                logicalPlanAdapter.setUserInfo(ctx.getCurrentUserIdentity());
                return ImmutableList.of(logicalPlanAdapter);
            }
        } catch (Throwable t) {
            LOG.warn("Parse from sql cache failed: " + t.getMessage(), t);
        } finally {
            statementContext.releasePlannerResources();
        }
        return null;
    }

    private String convertOriginStmt(String originStmt) {
        String convertedStmt = originStmt;
        @Nullable Dialect sqlDialect = Dialect.getByName(ctx.getSessionVariable().getSqlDialect());
        if (sqlDialect != null && sqlDialect != Dialect.DORIS) {
            PluginMgr pluginMgr = Env.getCurrentEnv().getPluginMgr();
            List<DialectConverterPlugin> plugins = pluginMgr.getActiveDialectPluginList(sqlDialect);
            for (DialectConverterPlugin plugin : plugins) {
                try {
                    String convertedSql = plugin.convertSql(originStmt, ctx.getSessionVariable());
                    if (StringUtils.isNotEmpty(convertedSql)) {
                        convertedStmt = convertedSql;
                        break;
                    }
                } catch (Throwable throwable) {
                    LOG.warn("Convert sql with dialect {} failed, plugin: {}, sql: {}, use origin sql.",
                                sqlDialect, plugin.getClass().getSimpleName(), originStmt, throwable);
                }
            }
        }
        return convertedStmt;
    }

    // Use a handler for exception to avoid big try catch block which is a little hard to understand
    protected void handleQueryException(Throwable throwable, String origStmt,
            StatementBase parsedStmt, Data.PQueryStatistics statistics) throws ConnectionException {
        if (ctx.getMinidump() != null) {
            MinidumpUtils.saveMinidumpString(ctx.getMinidump(), DebugUtil.printId(ctx.queryId()));
        }
        if (throwable instanceof ConnectionException) {
            // Throw this exception to close the connection outside.
            LOG.warn("Process one query failed because ConnectionException: ", throwable);
            throw (ConnectionException) throwable;
        } else if (throwable instanceof IOException) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", throwable);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Doris process failed: " + throwable.getMessage());
        } else if (throwable instanceof UserException) {
            LOG.warn("Process one query failed because.", throwable);
            ctx.getState().setError(((UserException) throwable).getMysqlErrorCode(), throwable.getMessage());
            // set it as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } else if (throwable instanceof NotSupportedException) {
            LOG.warn("Process one query failed because.", throwable);
            ctx.getState().setError(ErrorCode.ERR_NOT_SUPPORTED_YET, throwable.getMessage());
            // set it as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } else {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", throwable);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    throwable.getClass().getSimpleName() + ", msg: " + throwable.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        }
        auditAfterExec(origStmt, parsedStmt, statistics, true);
    }

    // analyze the origin stmt and return multi-statements
    protected List<StatementBase> parse(String originStmt) throws AnalysisException, DdlException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("the originStmts are: {}", originStmt);
        }
        // Parse statement with parser generated by CUP&FLEX
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        try {
            return SqlParserUtils.getMultiStmts(parser);
        } catch (Error e) {
            throw new AnalysisException("Please check your sql, we meet an error when parsing.", e);
        } catch (AnalysisException | DdlException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            if (LOG.isDebugEnabled()) {
                LOG.debug("origin stmt: {}; Analyze error message: {}", originStmt, parser.getErrorMsg(originStmt), e);
            }
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (ArrayStoreException e) {
            throw new AnalysisException("Sql parser can't convert the result to array, please check your sql.", e);
        } catch (Exception e) {
            // TODO(lingbin): we catch 'Exception' to prevent unexpected error,
            // should be removed this try-catch clause future.
            throw new AnalysisException("Internal Error, maybe syntax error or this is a bug: " + e.getMessage(), e);
        }
    }

    // Get the column definitions of a table
    @SuppressWarnings("rawtypes")
    protected void handleFieldList(String tableName) throws ConnectionException {
        // Already get command code.
        if (Strings.isNullOrEmpty(tableName)) {
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_TABLE, "Empty tableName");
            return;
        }
        DatabaseIf db = ctx.getCurrentCatalog().getDbNullable(ctx.getDatabase());
        if (db == null) {
            ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "Unknown database(" + ctx.getDatabase() + ")");
            return;
        }
        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_TABLE, "Unknown table(" + tableName + ")");
            return;
        }

        table.readLock();
        try {
            if (connectType.equals(ConnectType.MYSQL)) {
                MysqlChannel channel = ctx.getMysqlChannel();
                MysqlSerializer serializer = channel.getSerializer();

                // Send fields
                // NOTE: Field list doesn't send number of fields
                List<Column> baseSchema = table.getBaseSchema();
                for (Column column : baseSchema) {
                    serializer.reset();
                    serializer.writeField(db.getFullName(), table.getName(), column, true);
                    channel.sendOnePacket(serializer.toByteBuffer());
                }
            } else if (connectType.equals(ConnectType.ARROW_FLIGHT_SQL)) {
                // TODO
            }
        } catch (Throwable throwable) {
            handleQueryException(throwable, "", null, null);
        } finally {
            table.readUnlock();
        }
        ctx.getState().setEof();
    }

    // only Mysql protocol
    protected ByteBuffer getResultPacket() {
        Preconditions.checkState(connectType.equals(ConnectType.MYSQL));
        MysqlPacket packet = ctx.getState().toResponsePacket();
        if (packet == null) {
            // possible two cases:
            // 1. handler has send request
            // 2. this command need not to send response
            return null;
        }

        MysqlSerializer serializer = ctx.getMysqlChannel().getSerializer();
        serializer.reset();
        packet.writeTo(serializer);
        return serializer.toByteBuffer();
    }

    // When any request is completed, it will generally need to send a response packet to the client
    // This method is used to send a response packet to the client
    // only Mysql protocol
    public void finalizeCommand() throws IOException {
        LOG.debug("Finalize command for query {}", DebugUtil.printId(ctx.queryId));
        Preconditions.checkState(connectType.equals(ConnectType.MYSQL));
        ByteBuffer packet;
        if (executor != null && executor.isForwardToMaster()
                && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            ShowResultSet resultSet = executor.getShowResultSet();
            if (resultSet == null) {
                if (executor.sendProxyQueryResult()) {
                    packet = getResultPacket();
                } else {
                    packet = executor.getOutputPacket();
                }
            } else {
                executor.sendResultSet(resultSet);
                packet = getResultPacket();
            }
        } else {
            packet = getResultPacket();
        }

        if (packet == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("packet == null");
            }
            return;
        }

        LOG.debug("Send to mysql channel for query {}", DebugUtil.printId(ctx.queryId));
        MysqlChannel channel = ctx.getMysqlChannel();
        channel.sendAndFlush(packet);
        // note(wb) we should write profile after return result to mysql client
        // because write profile maybe take too much time
        // explain query stmt do not have profile
        if (executor != null && executor.getParsedStmt() != null && !executor.getParsedStmt().isExplain()
                && (executor.getParsedStmt() instanceof QueryStmt // currently only QueryStmt and insert need profile
                || executor.getParsedStmt() instanceof LogicalPlanAdapter
                || executor.getParsedStmt() instanceof InsertStmt)) {
            executor.updateProfile(true);
            StatsErrorEstimator statsErrorEstimator = ConnectContext.get().getStatsErrorEstimator();
            if (statsErrorEstimator != null) {
                statsErrorEstimator.updateProfile(ConnectContext.get().queryId());
            }
        }
        LOG.debug("End finalizing command for query {}", DebugUtil.printId(ctx.queryId));
    }

    public TMasterOpResult proxyExecute(TMasterOpRequest request) throws TException {
        ctx.setDatabase(request.db);
        ctx.setQualifiedUser(request.user);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.getState().reset();
        if (request.isSetUserIp()) {
            ctx.setRemoteIP(request.getUserIp());
        }
        if (request.isSetStmtId()) {
            ctx.setForwardedStmtId(request.getStmtId());
        }
        if (request.isSetCurrentUserIdent()) {
            UserIdentity currentUserIdentity = UserIdentity.fromThrift(request.getCurrentUserIdent());
            ctx.setCurrentUserIdentity(currentUserIdentity);
        }
        if (request.isFoldConstantByBe()) {
            ctx.getSessionVariable().setEnableFoldConstantByBe(request.foldConstantByBe);
        }

        if (request.isSetSessionVariables()) {
            ctx.getSessionVariable().setForwardedSessionVariables(request.getSessionVariables());
        } else {
            // For compatibility, all following variables are moved to SessionVariables.
            // Should move in future.
            if (request.isSetTimeZone()) {
                ctx.getSessionVariable().setTimeZone(request.getTimeZone());
            }
            if (request.isSetSqlMode()) {
                ctx.getSessionVariable().setSqlMode(request.sqlMode);
            }
            if (request.isSetEnableStrictMode()) {
                ctx.getSessionVariable().setEnableInsertStrict(request.enableStrictMode);
            }
            if (request.isSetCurrentUserIdent()) {
                UserIdentity currentUserIdentity = UserIdentity.fromThrift(request.getCurrentUserIdent());
                ctx.setCurrentUserIdentity(currentUserIdentity);
            }
            if (request.isSetInsertVisibleTimeoutMs()) {
                ctx.getSessionVariable().setInsertVisibleTimeoutMs(request.getInsertVisibleTimeoutMs());
            }
        }

        if (request.isSetQueryOptions()) {
            ctx.getSessionVariable().setForwardedSessionVariables(request.getQueryOptions());
        } else {
            // For compatibility, all following variables are moved to TQueryOptions.
            // Should move in future.
            if (request.isSetExecMemLimit()) {
                ctx.getSessionVariable().setMaxExecMemByte(request.getExecMemLimit());
            }
            if (request.isSetQueryTimeout()) {
                ctx.getSessionVariable().setQueryTimeoutS(request.getQueryTimeout());
            }
        }

        if (request.isSetUserVariables()) {
            ctx.setUserVars(userVariableFromThrift(request.getUserVariables()));
        }

        // set resource tag
        ctx.setResourceTags(Env.getCurrentEnv().getAuth().getResourceTags(ctx.qualifiedUser));

        ctx.setThreadLocalInfo();
        StmtExecutor executor = null;
        try {
            // 0 for compatibility.
            int idx = request.isSetStmtIdx() ? request.getStmtIdx() : 0;
            executor = new StmtExecutor(ctx, new OriginStatement(request.getSql(), idx), true);
            ctx.setExecutor(executor);
            // Set default catalog only if the catalog exists.
            if (request.isSetDefaultCatalog()) {
                CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalog(request.getDefaultCatalog());
                if (catalog != null) {
                    ctx.getEnv().changeCatalog(ctx, request.getDefaultCatalog());
                    // Set default db only when the default catalog is set and the dbname exists in default catalog.
                    if (request.isSetDefaultDatabase()) {
                        DatabaseIf db = ctx.getCurrentCatalog().getDbNullable(request.getDefaultDatabase());
                        if (db != null) {
                            ctx.getEnv().changeDb(ctx, request.getDefaultDatabase());
                        }
                    }
                }
            }

            // set transaction entry for transaction load
            if (request.isSetTxnLoadInfo()) {
                TransactionEntry transactionEntry = new TransactionEntry();
                transactionEntry.setTxnInfoInMaster(request.getTxnLoadInfo());
                ctx.setTxnEntry(transactionEntry);
            }

            TUniqueId queryId; // This query id will be set in ctx
            if (request.isSetQueryId()) {
                queryId = request.getQueryId();
            } else {
                UUID uuid = UUID.randomUUID();
                queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            }
            executor.queryRetry(queryId);
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Doris process failed: " + e.getMessage());
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe Doris bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + e.getMessage());
        }
        // no matter the master execute success or fail, the master must transfer the result to follower
        // and tell the follower the current journalID.
        TMasterOpResult result = new TMasterOpResult();
        if (ctx.queryId() != null
                // If none master FE not set query id or query id was reset in StmtExecutor
                // when a query exec more than once, return it to none master FE.
                && (!request.isSetQueryId() || !request.getQueryId().equals(ctx.queryId()))
        ) {
            result.setQueryId(ctx.queryId());
        }
        result.setMaxJournalId(Env.getCurrentEnv().getMaxJournalId());
        result.setPacket(getResultPacket());
        result.setStatus(ctx.getState().toString());
        if (ctx.getState().getStateType() == MysqlStateType.OK) {
            result.setStatusCode(0);
            if (request.isSetTxnLoadInfo()) {
                TransactionEntry transactionEntry = ConnectContext.get().getTxnEntry();
                // null if this is a commit or rollback command
                if (transactionEntry != null) {
                    result.setTxnLoadInfo(transactionEntry.getTxnInfoInMaster());
                }
            }
        } else {
            result.setStatusCode(ctx.getState().getErrorCode().getCode());
            result.setErrMessage(ctx.getState().getErrorMessage());
        }
        if (executor != null) {
            if (executor.getProxyShowResultSet() != null) {
                result.setResultSet(executor.getProxyShowResultSet().tothrift());
            } else if (!executor.getProxyQueryResultBufList().isEmpty()) {
                result.setQueryResultBufList(executor.getProxyQueryResultBufList());
            }
        }
        return result;
    }

    // only Mysql protocol
    public void processOnce() throws IOException, NotImplementedException {
        throw new NotImplementedException("Not Impl processOnce");
    }

    private Map<String, LiteralExpr> userVariableFromThrift(Map<String, TExprNode> thriftMap) throws TException {
        try {
            Map<String, LiteralExpr> userVariables = Maps.newHashMap();
            for (Map.Entry<String, TExprNode> entry : thriftMap.entrySet()) {
                TExprNode tExprNode = entry.getValue();
                LiteralExpr literalExpr = LiteralExpr.getLiteralExprFromThrift(tExprNode);
                userVariables.put(entry.getKey(), literalExpr);
            }
            return userVariables;
        } catch (AnalysisException e) {
            throw new TException(e.getMessage());
        }
    }
}
