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
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlProto;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Process one mysql connection, receive one packet, process, send one packet.
 */
public class MysqlConnectProcessor extends ConnectProcessor {
    private static final Logger LOG = LogManager.getLogger(MysqlConnectProcessor.class);

    private ByteBuffer packetBuf;

    public MysqlConnectProcessor(ConnectContext context) {
        super(context);
        connectType = ConnectType.MYSQL;
    }

    // COM_INIT_DB: change current database of this session.
    private void handleInitDb() {
        String fullDbName = new String(packetBuf.array(), 1, packetBuf.limit() - 1);
        handleInitDb(fullDbName);
    }

    private void handleStmtClose() {
        packetBuf = packetBuf.order(ByteOrder.LITTLE_ENDIAN);
        int stmtId = packetBuf.getInt();
        handleStmtClose(stmtId);
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
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe palo bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
        }
        auditAfterExec(stmtStr, prepareCtx.stmt.getInnerStmt(), null, false);
    }

    // Process COM_QUERY statement,
    private void handleQuery(MysqlCommand mysqlCommand) {
        // convert statement to Java string
        byte[] bytes = packetBuf.array();
        int ending = packetBuf.limit() - 1;
        while (ending >= 1 && bytes[ending] == '\0') {
            ending--;
        }
        String originStmt = new String(bytes, 1, ending, StandardCharsets.UTF_8);

        handleQuery(mysqlCommand, originStmt);
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
                // COM_QUIT: set killed flag and then return OK packet.
                handleQuit();
                break;
            case COM_QUERY:
            case COM_STMT_PREPARE:
                // Process COM_QUERY statement,
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
                // process COM_PING statement, do nothing, just return one OK packet.
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

    private void handleFieldList() {
        String tableName = new String(MysqlProto.readNulTerminateString(packetBuf), StandardCharsets.UTF_8);
        handleFieldList(tableName);
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


