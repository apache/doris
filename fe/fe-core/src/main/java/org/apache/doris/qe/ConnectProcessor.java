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

import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.telemetry.Telemetry;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlPacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlServerStatusFlag;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.proto.Data;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
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
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.StandardCharsets;
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

    private void auditAfterExec(String origStmt, StatementBase parsedStmt, Data.PQueryStatistics statistics) {
        // slow query
        long endTime = System.currentTimeMillis();
        long elapseMs = endTime - ctx.getStartTime();
        SpanContext spanContext = Span.fromContext(Context.current()).getSpanContext();

        ctx.getAuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setState(ctx.getState().toString()).setQueryTime(elapseMs)
                .setScanBytes(statistics == null ? 0 : statistics.getScanBytes())
                .setScanRows(statistics == null ? 0 : statistics.getScanRows())
                .setCpuTimeMs(statistics == null ? 0 : statistics.getCpuMs())
                .setPeakMemoryBytes(statistics == null ? 0 : statistics.getMaxPeakMemoryBytes())
                .setReturnRows(ctx.getReturnRows())
                .setStmtId(ctx.getStmtId())
                .setQueryId(ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()))
                .setTraceId(spanContext.isValid() ? spanContext.getTraceId() : "");

        if (ctx.getState().isQuery()) {
            MetricRepo.COUNTER_QUERY_ALL.increase(1L);
            if (ctx.getState().getStateType() == MysqlStateType.ERR
                    && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.increase(1L);
            } else if (ctx.getState().getStateType() == MysqlStateType.OK) {
                // ok query
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
                if (elapseMs > Config.qe_slow_log_ms) {
                    String sqlDigest = DigestUtils.md5Hex(((Queriable) parsedStmt).toDigest());
                    ctx.getAuditEventBuilder().setSqlDigest(sqlDigest);
                }
            }
            ctx.getAuditEventBuilder().setIsQuery(true);
            ctx.getQueryDetail().setEventTime(endTime);
            ctx.getQueryDetail().setEndTime(endTime);
            ctx.getQueryDetail().setLatency(elapseMs);
            ctx.getQueryDetail().setState(QueryDetail.QueryMemState.FINISHED);
            QueryDetailQueue.addOrUpdateQueryDetail(ctx.getQueryDetail());
        } else {
            ctx.getAuditEventBuilder().setIsQuery(false);
        }

        ctx.getAuditEventBuilder().setFeIp(FrontendOptions.getLocalHostAddress());

        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            ctx.getAuditEventBuilder().setStmt(parsedStmt.toSql());
        } else {
            if (parsedStmt instanceof InsertStmt && ((InsertStmt) parsedStmt).isValuesOrConstantSelect()) {
                // INSERT INTO VALUES may be very long, so we only log at most 1K bytes.
                int length = Math.min(1024, origStmt.length());
                ctx.getAuditEventBuilder().setStmt(origStmt.substring(0, length));
            } else {
                ctx.getAuditEventBuilder().setStmt(origStmt);
            }
        }
        if (!Env.getCurrentEnv().isMaster()) {
            if (ctx.executor.isForwardToMaster()) {
                ctx.getAuditEventBuilder().setState(ctx.executor.getProxyStatus());
            }
        }
        Env.getCurrentAuditEventProcessor().handleAuditEvent(ctx.getAuditEventBuilder().build());
    }

    // Process COM_QUERY statement,
    // only throw an exception when there is a problem interacting with the requesting client
    private void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
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
                .setDb(ctx.getDatabase())
                .setSqlHash(ctx.getSqlHash());

        // execute this query.
        StatementBase parsedStmt = null;
        List<Pair<StatementBase, Data.PQueryStatistics>> auditInfoList = Lists.newArrayList();
        boolean alreadyAddedToAuditInfoList = false;
        try {
            List<StatementBase> stmts = null;
            Exception nereidsParseException = null;
            // ctx could be null in some unit tests
            if (ctx != null && ctx.getSessionVariable().isEnableNereidsPlanner()) {
                NereidsParser nereidsParser = new NereidsParser();
                try {
                    stmts = nereidsParser.parseSQL(originStmt);
                } catch (Exception e) {
                    nereidsParseException = e;
                    // TODO: We should catch all exception here until we support all query syntax.
                    LOG.info(" Fallback to stale planner."
                            + " Nereids cannot process this statement: \"{}\".", originStmt.toString());
                }
            }
            // stmts == null when Nereids cannot planner this query or Nereids is disabled.
            if (stmts == null) {
                stmts = parse(originStmt);
            }
            for (int i = 0; i < stmts.size(); ++i) {
                alreadyAddedToAuditInfoList = false;
                ctx.getState().reset();
                if (i > 0) {
                    ctx.resetReturnRows();
                }
                parsedStmt = stmts.get(i);
                if (parsedStmt instanceof SelectStmt) {
                    if (!ctx.getSessionVariable().enableFallbackToOriginalPlanner) {
                        throw new Exception(String.format("SQL: {}", parsedStmt.toSql()), nereidsParseException);
                    }
                }
                parsedStmt.setOrigStmt(new OriginStatement(originStmt, i));
                parsedStmt.setUserInfo(ctx.getCurrentUserIdentity());
                executor = new StmtExecutor(ctx, parsedStmt);
                ctx.setExecutor(executor);
                executor.execute();

                if (i != stmts.size() - 1) {
                    ctx.getState().serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                    finalizeCommand();
                }
                auditInfoList.add(Pair.of(executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog()));
                alreadyAddedToAuditInfoList = true;
            }
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Doris process failed");
        } catch (UserException e) {
            LOG.warn("Process one query failed because.", e);
            ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
            // set is as ANALYSIS_ERR so that it won't be treated as a query failure.
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        }

        // that means execute some statement failed
        if (!alreadyAddedToAuditInfoList && executor != null) {
            auditInfoList.add(Pair.of(executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog()));
        }

        // audit after exec, analysis query would not be recorded
        if (!auditInfoList.isEmpty()) {
            for (Pair<StatementBase, Data.PQueryStatistics> audit : auditInfoList) {
                auditAfterExec(originStmt.replace("\n", " "), audit.first, audit.second);
            }
        } else if (QueryState.ErrType.ANALYSIS_ERR != ctx.getState().getErrType()) {
            // auditInfoList can be empty if we encounter error.
            auditAfterExec(originStmt.replace("\n", " "), null, null);
        }
        if (executor != null) {
            executor.addProfileToSpan();
        }
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
            throw new AnalysisException("Internal Error, maybe syntax error or this is a bug");
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
            MysqlSerializer serializer = ctx.getSerializer();
            MysqlChannel channel = ctx.getMysqlChannel();

            // Send fields
            // NOTE: Field list doesn't send number of fields
            List<Column> baseSchema = table.getBaseSchema();
            for (Column column : baseSchema) {
                serializer.reset();
                serializer.writeField(db.getFullName(), table.getName(), column, true);
                channel.sendOnePacket(serializer.toByteBuffer());
            }

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
                ctx.initTracer("trace");
                Span rootSpan = ctx.getTracer().spanBuilder("handleQuery").startSpan();
                try (Scope scope = rootSpan.makeCurrent()) {
                    handleQuery();
                } catch (Exception e) {
                    rootSpan.recordException(e);
                    throw e;
                } finally {
                    rootSpan.end();
                }
                break;
            case COM_FIELD_LIST:
                handleFieldList();
                break;
            case COM_PING:
                handlePing();
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

        MysqlSerializer serializer = ctx.getSerializer();
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
        if (executor != null && !executor.getParsedStmt().isExplain()
                && (executor.getParsedStmt() instanceof QueryStmt // currently only QueryStmt and insert need profile
                || executor.getParsedStmt() instanceof LogicalPlanAdapter
                || executor.getParsedStmt() instanceof InsertStmt)) {
            executor.writeProfile(true);
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
        if (request.isSetResourceInfo()) {
            ctx.getSessionVariable().setResourceGroup(request.getResourceInfo().getGroup());
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
