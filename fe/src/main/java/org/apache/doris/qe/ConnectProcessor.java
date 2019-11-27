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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuditLog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlPacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.proto.PQueryStatistics;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.util.List;

/**
 * Process one mysql connection, receive one pakcet, process, send one packet.
 */
public class ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(ConnectProcessor.class);

    private final ConnectContext ctx;
    private ByteBuffer packetBuf;

    private StmtExecutor executor = null;

    public ConnectProcessor(ConnectContext context) {
        this.ctx = context;
    }

    // COM_INIT_DB: change current database of this session.
    private void handleInitDb() {
        String dbName = new String(packetBuf.array(), 1, packetBuf.limit() - 1);
        if (Strings.isNullOrEmpty(ctx.getClusterName())) {
            ctx.getState().setError("Please enter cluster");
            return;
        }
        dbName = ClusterNamespace.getFullName(ctx.getClusterName(), dbName);
        try {
            ctx.getCatalog().changeDb(ctx, dbName);
        } catch (DdlException e) {
            ctx.getState().setError(e.getMessage());
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

    private void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics) {
        // slow query
        long elapseMs = System.currentTimeMillis() - ctx.getStartTime();
        // query state log
        ctx.getAuditBuilder().put("State", ctx.getState());
        ctx.getAuditBuilder().put("Time", elapseMs);
        Preconditions.checkNotNull(statistics); 
        ctx.getAuditBuilder().put("ScanBytes", statistics.scan_bytes);
        ctx.getAuditBuilder().put("ScanRows", statistics.scan_rows);
        ctx.getAuditBuilder().put("ReturnRows", ctx.getReturnRows());
        ctx.getAuditBuilder().put("StmtId", ctx.getStmtId());
        ctx.getAuditBuilder().put("QueryId", ctx.queryId() == null ? "NaN" : DebugUtil.printId(ctx.queryId()));

        if (ctx.getState().isQuery()) {
            MetricRepo.COUNTER_QUERY_ALL.increase(1L);
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR
                    && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.increase(1L);
            } else {
                // ok query
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
            }
            ctx.getAuditBuilder().put("IsQuery", 1);
        } else {
            ctx.getAuditBuilder().put("IsQuery", 0);
        }
        // We put origin query stmt at the end of audit log, for parsing the log more convenient.
        if (!ctx.getState().isQuery() && (parsedStmt != null && parsedStmt.needAuditEncryption())) {
            ctx.getAuditBuilder().put("Stmt", parsedStmt.toSql());
        } else {
            ctx.getAuditBuilder().put("Stmt", origStmt);
        }

        AuditLog.getQueryAudit().log(ctx.getAuditBuilder().toString());

        // slow query
        if (elapseMs > Config.qe_slow_log_ms) {
            AuditLog.getSlowAudit().log(ctx.getAuditBuilder().toString());
        }
    }

    // process COM_QUERY statement,
    // 只有在与请求客户端交互出现问题时候才抛出异常
    private void handleQuery() {
        MetricRepo.COUNTER_REQUEST_ALL.increase(1L);
        // convert statement to Java string
        String stmt = null;
        try {
            byte[] bytes = packetBuf.array();
            int ending = packetBuf.limit() - 1;
            while (ending >= 1 && bytes[ending] == '\0') {
                ending--;
            }
            stmt = new String(bytes, 1, ending, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // impossible
            LOG.error("UTF8 is not supported in this environment.");
            ctx.getState().setError("Unsupported character set(UTF-8)");
            return;
        }
        ctx.getAuditBuilder().reset();
        ctx.getAuditBuilder().put("Client", ctx.getMysqlChannel().getRemoteHostPortString());
        ctx.getAuditBuilder().put("User", ctx.getQualifiedUser());
        ctx.getAuditBuilder().put("Db", ctx.getDatabase());

        // execute this query.
        try {
            executor = new StmtExecutor(ctx, stmt);
            ctx.setExecutor(executor);
            executor.execute();
            // set if this is a QueryStmt
            ctx.getState().setQuery(executor.isQueryStmt());
        } catch (DdlException e) {
            LOG.warn("Process one query failed because DdlException.", e);
            ctx.getState().setError(e.getMessage());
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("Palo process failed");
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
        }

        // audit after exec
        // replace '\n' to '\\n' to make string in one line
        auditAfterExec(stmt.replace("\n", " \\n"), executor.getParsedStmt(), 
                executor.getQueryStatisticsForAuditLog());
    }

    // Get the column definitions of a table
    private void handleFieldList() throws IOException {
        // Already get command code.
        String tableName = null;
        String pattern = null;
        try {
            tableName = new String(MysqlProto.readNulTerminateString(packetBuf), "UTF-8");
            pattern = new String(MysqlProto.readEofString(packetBuf), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Impossible!!!
            LOG.error("Unknown UTF-8 character set.");
            return;
        }
        if (Strings.isNullOrEmpty(tableName)) {
            ctx.getState().setError("Empty tableName");
            return;
        }
        Database db = ctx.getCatalog().getDb(ctx.getDatabase());
        if (db == null) {
            ctx.getState().setError("Unknown database(" + ctx.getDatabase() + ")");
            return;
        }
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ctx.getState().setError("Unknown table(" + tableName + ")");
                return;
            }

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
            db.readUnlock();
        }
        ctx.getState().setEof();
    }

    private void dispatch() throws IOException {
        int code = packetBuf.get();
        MysqlCommand command = MysqlCommand.fromCode(code);
        if (command == null) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_COM_ERROR);
            ctx.getState().setError("Unknown command(" + command + ")");
            LOG.warn("Unknown command(" + command + ")");
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
                handleQuery();
                ctx.setStartTime();
                break;
            case COM_FIELD_LIST:
                handleFieldList();
                break;
            case COM_PING:
                handlePing();
                break;
            default:
                ctx.getState().setError("Unsupported command(" + command + ")");
                LOG.warn("Unsupported command(" + command + ")");
                break;
        }
    }

    private ByteBuffer getResultPacket() {
        MysqlPacket packet = ctx.getState().toResponsePacket();
        if (packet == null) {
            // 当出现此种情况可能有两种可能
            // 1. 处理函数已经发送请求
            // 2. 这个协议不需要发送任何响应包
            return null;
        }

        MysqlSerializer serializer = ctx.getSerializer();
        serializer.reset();
        packet.writeTo(serializer);
        return serializer.toByteBuffer();
    }

    // 当任何一个请求完成后，一般都会需要发送一个响应包给客户端
    // 这个函数用于发送响应包给客户端
    private void finalizeCommand() throws IOException {
        ByteBuffer packet = null;
        if (executor != null && executor.isForwardToMaster()
                && ctx.getState().getStateType() != QueryState.MysqlStateType.ERR) {
            ShowResultSet resultSet = executor.getShowResultSet();
            if (resultSet == null) {
                packet = executor.getOutputPacket();
            } else {
                executor.sendShowResult(resultSet);
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
    }

    public TMasterOpResult proxyExecute(TMasterOpRequest request) {
        ctx.setDatabase(request.db);
        ctx.setQualifiedUser(request.user);
        ctx.setCatalog(Catalog.getInstance());
        ctx.getState().reset();
        if (request.isSetCluster()) {
            ctx.setCluster(request.cluster);
        }
        if (request.isSetResourceInfo()) {
            ctx.getSessionVariable().setResourceGroup(request.getResourceInfo().getGroup());
        }
        if (request.isSetExecMemLimit()) {
            ctx.getSessionVariable().setMaxExecMemByte(request.getExecMemLimit());
        }
        if (request.isSetQueryTimeout()) {
            ctx.getSessionVariable().setQueryTimeoutS(request.getQueryTimeout());
        }
        if (request.isSetUser_ip()) {
            ctx.setRemoteIP(request.getUser_ip());
        }
        if (request.isSetTime_zone()) {
            ctx.getSessionVariable().setTimeZone(request.getTime_zone());                       
        }
        if (request.isSetStmt_id()) {
            ctx.setForwardedStmtId(request.getStmt_id());
        }
        if (request.isSetSqlMode()) {
            ctx.getSessionVariable().setSqlMode(request.sqlMode);
        }
        if (request.isSetLoadMemLimit()) {
            ctx.getSessionVariable().setLoadMemLimit(request.loadMemLimit);
        }
        if (request.isSetEnableStrictMode()) {
            ctx.getSessionVariable().setEnableInsertStrict(request.enableStrictMode);
        }

        ctx.setThreadLocalInfo();

        StmtExecutor executor = null;
        try {
            executor = new StmtExecutor(ctx, request.getSql(), true);
            executor.execute();
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("Palo process failed");
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError("Unexpected exception: " + e.getMessage());
        }
        // no matter the master execute success or fail, the master must transfer the result to follower
        // and tell the follwer the current jounalID.
        TMasterOpResult result = new TMasterOpResult();
        result.setMaxJournalId(Catalog.getInstance().getMaxJournalId().longValue());
        result.setPacket(getResultPacket());
        if (executor != null && executor.getProxyResultSet() != null) {
            result.setResultSet(executor.getProxyResultSet().tothrift());
        }
        return result;
    }

    // 处理一个MySQL请求，接收，处理，返回
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
                LOG.warn("Exception happened in one seesion(" + ctx + ").", e);
                ctx.setKilled();
                break;
            }
        }
    }
}
