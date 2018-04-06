// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.qe;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AuditLog;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.metric.MetricRepo;
import com.baidu.palo.mysql.MysqlChannel;
import com.baidu.palo.mysql.MysqlCommand;
import com.baidu.palo.mysql.MysqlPacket;
import com.baidu.palo.mysql.MysqlProto;
import com.baidu.palo.mysql.MysqlSerializer;
import com.baidu.palo.thrift.TMasterOpRequest;
import com.baidu.palo.thrift.TMasterOpResult;

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

    private void auditAfterExec() {
        MetricRepo.COUNTER_REQUEST_ALL.inc();

        // slow query
        long elapseMs = System.currentTimeMillis() - ctx.getStartTime();
        // query state log
        ctx.getAuditBuilder().put("state", ctx.getState());
        ctx.getAuditBuilder().put("time", elapseMs);
        ctx.getAuditBuilder().put("returnRows", ctx.getReturnRows());
        String auditString = ctx.getAuditBuilder().toString();

        if (auditString.toLowerCase().contains("select") && auditString.toLowerCase().contains("from")) {
            MetricRepo.COUNTER_QUERY_ALL.inc();
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR
                    && ctx.getState().getErrType() != QueryState.ErrType.ANALYSIS_ERR) {
                // err query
                MetricRepo.COUNTER_QUERY_ERR.inc();
                ctx.getAuditBuilder().put("monitor", "yes");
            } else {
                // ok query
                MetricRepo.METER_QUERY.mark();
                MetricRepo.HISTO_QUERY_LATENCY.update(elapseMs);
            }
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
        MetricRepo.METER_REQUEST.mark();
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
        // replace '\n' to '\\\n' to make string in one line
        ctx.getAuditBuilder().put("client", ctx.getMysqlChannel().getRemoteHostString());
        ctx.getAuditBuilder().put("user", ctx.getUser());
        ctx.getAuditBuilder().put("db", ctx.getDatabase());
        ctx.getAuditBuilder().put("query", stmt.replace("\n", "\\n"));

        // execute this query.
        try {
            executor = new StmtExecutor(ctx, stmt);
            executor.execute();
            // needForward = executor.isForwardtoMaster();
            // outputPacket = executor.getOutputPacket();
        } catch (DdlException e) {
            LOG.warn("Process one query failed because DdlException: ", e);
            ctx.getState().setError(e.getMessage());
        } catch (IOException e) {
            // Client failed.
            LOG.warn("Process one query failed because IOException: ", e);
            ctx.getState().setError("Palo process failed");
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError("Maybe palo bug");
        }

        // audit after exec
        auditAfterExec();
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
        ctx.setUser(request.user);
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
            ctx.getState().setError("Maybe palo bug");
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
                LOG.warn("Null packet received from network. remote: {}", channel.getRemoteHostString());
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
