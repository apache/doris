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

import org.apache.doris.analysis.ExecuteStmt;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.telemetry.Telemetry;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlPacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlServerStatusFlag;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Process one mysql connection, receive one packet, process, send one packet.
 */
public class ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ConnectProcessor.class);
    private static final TextMapGetter<Map<String, String>> getter =
            new TextMapGetter<Map<String, String>>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    if (carrier.containsKey(key)) {
                        return carrier.get(key);
                    }
                    return "";
                }
            };
    private final ConnectContext ctx;
    private ByteBuffer packetBuf;
    private StmtExecutor executor = null;

    public ConnectProcessor(ConnectContext context) {
        this.ctx = context;
    }

    // COM_INIT_DB: change current database of this session.
    private void handleInitDb() {
        String fullDbName = new String(packetBuf.array(), 1, packetBuf.limit() - 1);
        if (Strings.isNullOrEmpty(ctx.getClusterName())) {
            ctx.getState().setError(ErrorCode.ERR_CLUSTER_NAME_NULL, "Please enter cluster");
            return;
        }
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
        dbName = ClusterNamespace.getFullName(ctx.getClusterName(), dbName);

        // check catalog and db exists
        if (catalogName != null) {
            CatalogIf catalogIf = ctx.getEnv().getCatalogMgr().getCatalogNullable(catalogName);
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

    // COM_QUIT: set killed flag and then return OK packet.
    private void handleQuit() {
        ctx.setKilled();
        ctx.getState().setOk();
    }

    // process COM_PING statement, do nothing, just return one OK packet.
    private void handlePing() {
        ctx.getState().setOk();
    }

    private void handleStmtReset() {
        ctx.getState().setOk();
    }

    private void handleStmtClose() {
        packetBuf = packetBuf.order(ByteOrder.LITTLE_ENDIAN);
        int stmtId = packetBuf.getInt();
        LOG.debug("close stmt id: {}", stmtId);
        ConnectContext.get().removePrepareStmt(String.valueOf(stmtId));
        // No response packet is sent back to the client, see
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
        ctx.getState().setNoop();
    }

    private void debugPacket() {
        byte[] bytes = packetBuf.array();
        StringBuilder printB = new StringBuilder();
        for (byte b : bytes) {
            if (Character.isLetterOrDigit((char) b & 0xFF)) {
                char x = (char) b;
                printB.append(x);
            } else {
                printB.append("0x" + Integer.toHexString(b & 0xFF));
            }
            printB.append(" ");
        }
        LOG.debug("debug packet {}", printB.toString().substring(0, 200));
    }

    private static boolean isNull(byte[] bitmap, int position) {
        return (bitmap[position / 8] & (1 << (position & 7))) != 0;
    }

    // process COM_EXECUTE, parse binary row data
    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
    private void handleExecute() {
        // debugPacket();
        packetBuf = packetBuf.order(ByteOrder.LITTLE_ENDIAN);
        // parse stmt_id, flags, params
        int stmtId = packetBuf.getInt();
        // flag
        packetBuf.get();
        // iteration_count always 1,
        packetBuf.getInt();
        LOG.debug("execute prepared statement {}", stmtId);
        PrepareStmtContext prepareCtx = ctx.getPreparedStmt(String.valueOf(stmtId));
        if (prepareCtx == null) {
            LOG.debug("No such statement in context, stmtId:{}", stmtId);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR,
                    "msg: Not supported such prepared statement");
            return;
        }
        ctx.setStartTime();
        if (prepareCtx.stmt.getInnerStmt() instanceof QueryStmt) {
            ctx.getState().setIsQuery(true);
        }
        prepareCtx.stmt.setIsPrepared();
        int paramCount = prepareCtx.stmt.getParmCount();
        // null bitmap
        byte[] nullbitmapData = new byte[(paramCount + 7) / 8];
        packetBuf.get(nullbitmapData);
        String stmtStr = "";
        try {
            // new_params_bind_flag
            if ((int) packetBuf.get() != 0) {
                // parse params's types
                for (int i = 0; i < paramCount; ++i) {
                    int typeCode = packetBuf.getChar();
                    LOG.debug("code {}", typeCode);
                    prepareCtx.stmt.placeholders().get(i).setTypeCode(typeCode);
                }
            }
            List<LiteralExpr> realValueExprs = new ArrayList<>();
            // parse param data
            for (int i = 0; i < paramCount; ++i) {
                if (isNull(nullbitmapData, i)) {
                    realValueExprs.add(new NullLiteral());
                    continue;
                }
                LiteralExpr l = prepareCtx.stmt.placeholders().get(i).createLiteralFromType();
                l.setupParamFromBinary(packetBuf);
                realValueExprs.add(l);
            }
            ExecuteStmt executeStmt = new ExecuteStmt(String.valueOf(stmtId), realValueExprs);
            // TODO set real origin statement
            executeStmt.setOrigStmt(new OriginStatement("null", 0));
            executeStmt.setUserInfo(ctx.getCurrentUserIdentity());
            LOG.debug("executeStmt {}", executeStmt);
            executor = new StmtExecutor(ctx, executeStmt);
            ctx.setExecutor(executor);
            executor.execute();
            stmtStr = executeStmt.toSql();
        } catch (Throwable e)  {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
        }
        auditAfterExec(stmtStr, prepareCtx.stmt.getInnerStmt(), null, false);
    }

    private void auditAfterExec(String origStmt, StatementBase parsedStmt,
                    Data.PQueryStatistics statistics, boolean printFuzzyVariables) {
        AuditLogHelper.logAuditLog(ctx, origStmt, parsedStmt, statistics, printFuzzyVariables);
    }

    // Process COM_QUERY statement,
    // only throw an exception when there is a problem interacting with the requesting client
    private void handleQuery(MysqlCommand mysqlCommand) {
        if (MetricRepo.isInit) {
            MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        }
        // convert statement to Java string
        byte[] bytes = packetBuf.array();
        int ending = packetBuf.limit() - 1;
        while (ending >= 1 && bytes[ending] == '\0') {
            ending--;
        }
        String originStmt = new String(bytes, 1, ending, StandardCharsets.UTF_8);

        String sqlHash = DigestUtils.md5Hex(originStmt);
        ctx.setSqlHash(sqlHash);
        ctx.getAuditEventBuilder().reset();
        ctx.getAuditEventBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setClientIp(ctx.getMysqlChannel().getRemoteHostPortString())
                .setUser(ClusterNamespace.getNameFromFullName(ctx.getQualifiedUser()))
                .setSqlHash(ctx.getSqlHash());

        List<StatementBase> stmts = null;

        // Nereids do not support prepare and execute now, so forbid prepare command, only process query command
        if (mysqlCommand == MysqlCommand.COM_QUERY && ctx.getSessionVariable().isEnableNereidsPlanner()) {
            try {
                stmts = new NereidsParser().parseSQL(originStmt);
            } catch (NotSupportedException e) {
                // Parse sql failed, audit it and return
                handleQueryException(e, originStmt, null, null);
                return;
            } catch (Exception e) {
                // TODO: We should catch all exception here until we support all query syntax.
                LOG.debug("Nereids parse sql failed. Reason: {}. Statement: \"{}\".",
                        e.getMessage(), originStmt);
            }
        }

        // stmts == null when Nereids cannot planner this query or Nereids is disabled.
        if (stmts == null) {
            try {
                stmts = parse(originStmt);
            } catch (Throwable throwable) {
                // Parse sql failed, audit it and return
                handleQueryException(throwable, originStmt, null, null);
                return;
            }
        }

        List<String> origSingleStmtList = null;
        // if stmts.size() > 1, split originStmt to multi singleStmts
        if (stmts.size() > 1) {
            try {
                origSingleStmtList = SqlUtils.splitMultiStmts(originStmt);
            } catch (Exception ignore) {
                LOG.warn("Try to parse multi origSingleStmt failed, originStmt: \"{}\"", originStmt);
            }
        }

        boolean usingOrigSingleStmt = origSingleStmtList != null && origSingleStmtList.size() == stmts.size();
        for (int i = 0; i < stmts.size(); ++i) {
            String auditStmt = usingOrigSingleStmt ? origSingleStmtList.get(i) : originStmt;

            ctx.getState().reset();
            if (i > 0) {
                ctx.resetReturnRows();
            }

            StatementBase parsedStmt = stmts.get(i);
            parsedStmt.setOrigStmt(new OriginStatement(originStmt, i));
            parsedStmt.setUserInfo(ctx.getCurrentUserIdentity());
            executor = new StmtExecutor(ctx, parsedStmt);
            ctx.setExecutor(executor);

            try {
                executor.execute();
                if (i != stmts.size() - 1) {
                    ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                    if (ctx.getState().getStateType() != MysqlStateType.ERR) {
                        finalizeCommand();
                    }
                }
                auditAfterExec(auditStmt, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(), true);
                // execute failed, skip remaining stmts
                if (ctx.getState().getStateType() == MysqlStateType.ERR) {
                    break;
                }
            } catch (Throwable throwable) {
                handleQueryException(throwable, auditStmt, executor.getParsedStmt(),
                        executor.getQueryStatisticsForAuditLog());
                // execute failed, skip remaining stmts
                break;
            } finally {
                executor.addProfileToSpan();
            }

        }

    }

    // Use a handler for exception to avoid big try catch block which is a little hard to understand
    private void handleQueryException(Throwable throwable, String origStmt,
                                      StatementBase parsedStmt, Data.PQueryStatistics statistics) {
        if (ctx.getMinidump() != null) {
            MinidumpUtils.saveMinidumpString(ctx.getMinidump(), DebugUtil.printId(ctx.queryId()));
        }
        if (throwable instanceof IOException) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", throwable);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Doris process failed");
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
    private List<StatementBase> parse(String originStmt) throws AnalysisException, DdlException {
        LOG.debug("the originStmts are: {}", originStmt);
        // Parse statement with parser generated by CUP&FLEX
        SqlScanner input = new SqlScanner(new StringReader(originStmt), ctx.getSessionVariable().getSqlMode());
        SqlParser parser = new SqlParser(input);
        try {
            return SqlParserUtils.getMultiStmts(parser);
        } catch (Error e) {
            throw new AnalysisException("Please check your sql, we meet an error when parsing.", e);
        } catch (AnalysisException | DdlException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            LOG.debug("origin stmt: {}; Analyze error message: {}", originStmt, parser.getErrorMsg(originStmt), e);
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
    private void handleFieldList() throws IOException {
        // Already get command code.
        String tableName = new String(MysqlProto.readNulTerminateString(packetBuf), StandardCharsets.UTF_8);
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

        } catch (Throwable throwable) {
            handleQueryException(throwable, "", null, null);
        } finally {
            table.readUnlock();
        }
        ctx.getState().setEof();
    }

    private void dispatch() throws IOException {
        int code = packetBuf.get();
        MysqlCommand command = MysqlCommand.fromCode(code);
        if (command == null) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_COM_ERROR);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR, "Unknown command(" + code + ")");
            LOG.warn("Unknown command(" + code + ")");
            return;
        }
        LOG.debug("handle command {}", command);
        ctx.setCommand(command);
        ctx.setStartTime();

        switch (command) {
            case COM_INIT_DB:
                handleInitDb();
                break;
            case COM_QUIT:
                handleQuit();
                break;
            case COM_QUERY:
            case COM_STMT_PREPARE:
                ctx.initTracer("trace");
                Span rootSpan = ctx.getTracer().spanBuilder("handleQuery").setNoParent().startSpan();
                try (Scope scope = rootSpan.makeCurrent()) {
                    handleQuery(command);
                } catch (Exception e) {
                    rootSpan.recordException(e);
                    throw e;
                } finally {
                    rootSpan.end();
                }
                break;
            case COM_STMT_EXECUTE:
                handleExecute();
                break;
            case COM_FIELD_LIST:
                handleFieldList();
                break;
            case COM_PING:
                handlePing();
                break;
            case COM_STMT_RESET:
                handleStmtReset();
                break;
            case COM_STMT_CLOSE:
                handleStmtClose();
                break;
            default:
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR, "Unsupported command(" + command + ")");
                LOG.warn("Unsupported command(" + command + ")");
                break;
        }
    }

    private ByteBuffer getResultPacket() {
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
    private void finalizeCommand() throws IOException {
        ByteBuffer packet;
        if (executor != null && executor.isForwardToMaster()
                && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            ShowResultSet resultSet = executor.getShowResultSet();
            if (resultSet == null) {
                packet = executor.getOutputPacket();
            } else {
                executor.sendResultSet(resultSet);
                packet = getResultPacket();
                if (packet == null) {
                    LOG.debug("packet == null");
                    return;
                }
            }
        } else {
            packet = getResultPacket();
            if (packet == null) {
                LOG.debug("packet == null");
                return;
            }
        }

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
    }

    public TMasterOpResult proxyExecute(TMasterOpRequest request) {
        ctx.setDatabase(request.db);
        ctx.setQualifiedUser(request.user);
        ctx.setEnv(Env.getCurrentEnv());
        ctx.getState().reset();
        if (request.isSetCluster()) {
            ctx.setCluster(request.cluster);
        }
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

        Map<String, String> traceCarrier = new HashMap<>();
        if (request.isSetTraceCarrier()) {
            traceCarrier = request.getTraceCarrier();
        }
        Context extractedContext = Telemetry.getOpenTelemetry().getPropagators().getTextMapPropagator()
                .extract(Context.current(), traceCarrier, getter);
        // What we want is for the Traceid to remain unchanged during propagation.
        // ctx.initTracer() will be called only if the Context is valid,
        // so that the Traceid generated by SDKTracer is the same as the follower. Otherwise,
        // if the Context is invalid and ctx.initTracer() is called,
        // SDKTracer will generate a different Traceid.
        if (Span.fromContext(extractedContext).getSpanContext().isValid()) {
            ctx.initTracer("master trace");
        }

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
            TUniqueId queryId; // This query id will be set in ctx
            if (request.isSetQueryId()) {
                queryId = request.getQueryId();
            } else {
                UUID uuid = UUID.randomUUID();
                queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            }
            Span masterQuerySpan =
                    ctx.getTracer().spanBuilder("master execute").setParent(extractedContext)
                            .setSpanKind(SpanKind.SERVER).startSpan();
            try (Scope scope = masterQuerySpan.makeCurrent()) {
                executor.execute(queryId);
            } catch (Exception e) {
                masterQuerySpan.recordException(e);
                throw e;
            } finally {
                masterQuerySpan.end();
            }
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
        if (executor != null && executor.getProxyResultSet() != null) {
            result.setResultSet(executor.getProxyResultSet().tothrift());
        }
        return result;
    }

    // Process a MySQL request
    public void processOnce() throws IOException {
        // set status of query to OK.
        ctx.getState().reset();
        executor = null;

        // reset sequence id of MySQL protocol
        final MysqlChannel channel = ctx.getMysqlChannel();
        channel.setSequenceId(0);
        // read packet from channel
        try {
            packetBuf = channel.fetchOnePacket();
            if (packetBuf == null) {
                LOG.warn("Null packet received from network. remote: {}", channel.getRemoteHostPortString());
                throw new IOException("Error happened when receiving packet.");
            }
        } catch (AsynchronousCloseException e) {
            // when this happened, timeout checker close this channel
            // killed flag in ctx has been already set, just return
            return;
        }

        // dispatch
        dispatch();
        // finalize
        finalizeCommand();

        ctx.setCommand(MysqlCommand.COM_SLEEP);
    }

    public void loop() {
        while (!ctx.isKilled()) {
            try {
                processOnce();
            } catch (Exception e) {
                // TODO(zhaochun): something wrong
                LOG.warn("Exception happened in one session(" + ctx + ").", e);
                ctx.setKilled();
                break;
            }
        }
    }
}


