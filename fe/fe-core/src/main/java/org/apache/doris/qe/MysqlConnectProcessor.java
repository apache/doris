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
import org.apache.doris.analysis.PrepareStmt;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.ExecuteCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("debug packet {}", printB.toString().substring(0, 200));
        }
    }

    private void handleExecute(PrepareStmt prepareStmt, long stmtId) {
        if (prepareStmt.getInnerStmt() instanceof QueryStmt) {
            ctx.getState().setIsQuery(true);
        }
        prepareStmt.setIsPrepared();
        int paramCount = prepareStmt.getParmCount();
        LOG.debug("execute prepared statement {}, paramCount {}", stmtId, paramCount);
        // null bitmap
        String stmtStr = "";
        try {
            List<LiteralExpr> realValueExprs = new ArrayList<>();
            if (paramCount > 0) {
                byte[] nullbitmapData = new byte[(paramCount + 7) / 8];
                packetBuf.get(nullbitmapData);
                // new_params_bind_flag
                if ((int) packetBuf.get() != 0) {
                    // parse params's types
                    for (int i = 0; i < paramCount; ++i) {
                        int typeCode = packetBuf.getChar();
                        LOG.debug("code {}", typeCode);
                        prepareStmt.placeholders().get(i).setTypeCode(typeCode);
                    }
                }
                // parse param data
                for (int i = 0; i < paramCount; ++i) {
                    if (isNull(nullbitmapData, i)) {
                        realValueExprs.add(new NullLiteral());
                        continue;
                    }
                    LiteralExpr l = prepareStmt.placeholders().get(i).createLiteralFromType();
                    boolean isUnsigned = prepareStmt.placeholders().get(i).isUnsigned();
                    l.setupParamFromBinary(packetBuf, isUnsigned);
                    realValueExprs.add(l);
                }
            }
            ExecuteStmt executeStmt = new ExecuteStmt(String.valueOf(stmtId), realValueExprs);
            // TODO set real origin statement
            executeStmt.setOrigStmt(new OriginStatement("null", 0));
            executeStmt.setUserInfo(ctx.getCurrentUserIdentity());
            if (LOG.isDebugEnabled()) {
                LOG.debug("executeStmt {}", executeStmt);
            }
            executor = new StmtExecutor(ctx, executeStmt);
            ctx.setExecutor(executor);
            executor.execute();
            PrepareStmtContext preparedStmtContext = ConnectContext.get().getPreparedStmt(String.valueOf(stmtId));
            if (preparedStmtContext != null) {
                stmtStr = executeStmt.toSql();
            }
        } catch (Throwable e)  {
            // Catch all throwable.
            // If reach here, maybe doris bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
        }
        auditAfterExec(stmtStr, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(), true);
    }

    private void handleExecute(PrepareCommand prepareCommand, long stmtId, PreparedStatementContext prepCtx) {
        int paramCount = prepareCommand.placeholderCount();
        LOG.debug("execute prepared statement {}, paramCount {}", stmtId, paramCount);
        // null bitmap
        String stmtStr = "";
        try {
            StatementContext statementContext = prepCtx.statementContext;
            if (paramCount > 0) {
                byte[] nullbitmapData = new byte[(paramCount + 7) / 8];
                packetBuf.get(nullbitmapData);
                // new_params_bind_flag
                if ((int) packetBuf.get() != 0) {
                    List<Placeholder> typedPlaceholders = new ArrayList<>();
                    // parse params's types
                    for (int i = 0; i < paramCount; ++i) {
                        int typeCode = packetBuf.getChar();
                        LOG.debug("code {}", typeCode);
                        // assign type to placeholders
                        typedPlaceholders.add(
                                prepareCommand.getPlaceholders().get(i).withNewMysqlColType(typeCode));
                    }
                    // rewrite with new prepared statment with type info in placeholders
                    prepCtx.command = prepareCommand.withPlaceholders(typedPlaceholders);
                    prepareCommand = (PrepareCommand) prepCtx.command;
                }
                // parse param data
                for (int i = 0; i < paramCount; ++i) {
                    PlaceholderId exprId = prepareCommand.getPlaceholders().get(i).getPlaceholderId();
                    if (isNull(nullbitmapData, i)) {
                        statementContext.getIdToPlaceholderRealExpr().put(exprId,
                                    new org.apache.doris.nereids.trees.expressions.literal.NullLiteral());
                        continue;
                    }
                    MysqlColType type = prepareCommand.getPlaceholders().get(i).getMysqlColType();
                    boolean isUnsigned = prepareCommand.getPlaceholders().get(i).isUnsigned();
                    Literal l = Literal.getLiteralByMysqlType(type, isUnsigned, packetBuf);
                    statementContext.getIdToPlaceholderRealExpr().put(exprId, l);
                }
            }
            ExecuteCommand executeStmt = new ExecuteCommand(String.valueOf(stmtId), prepareCommand, statementContext);
            // TODO set real origin statement
            if (LOG.isDebugEnabled()) {
                LOG.debug("executeStmt {}", executeStmt);
            }
            StatementBase stmt = new LogicalPlanAdapter(executeStmt, statementContext);
            stmt.setOrigStmt(prepareCommand.getOriginalStmt());
            executor = new StmtExecutor(ctx, stmt);
            ctx.setExecutor(executor);
            executor.execute();
            stmtStr = executeStmt.toSql();
        } catch (Throwable e)  {
            // Catch all throwable.
            // If reach here, maybe doris bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
        }
        auditAfterExec(stmtStr, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(), true);
    }

    // process COM_EXECUTE, parse binary row data
    // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
    private void handleExecute() {
        if (LOG.isDebugEnabled()) {
            debugPacket();
        }
        packetBuf = packetBuf.order(ByteOrder.LITTLE_ENDIAN);
        // parse stmt_id, flags, params
        int stmtId = packetBuf.getInt();
        // flag
        packetBuf.get();
        // iteration_count always 1,
        packetBuf.getInt();
        if (LOG.isDebugEnabled()) {
            LOG.debug("execute prepared statement {}", stmtId);
        }

        PrepareStmtContext prepareCtx = ctx.getPreparedStmt(String.valueOf(stmtId));
        ctx.setStartTime();
        if (prepareCtx != null) {
            // get from lagacy planner context, to be removed
            handleExecute((PrepareStmt) prepareCtx.stmt, stmtId);
        } else {
            // nererids
            PreparedStatementContext preparedStatementContext = ctx.getPreparedStementContext(String.valueOf(stmtId));
            if (preparedStatementContext == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No such statement in context, stmtId:{}", stmtId);
                }
                ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR,
                        "msg: Not supported such prepared statement");
                return;
            }
            handleExecute(preparedStatementContext.command, stmtId, preparedStatementContext);
        }
    }

    // Process COM_QUERY statement,
    private void handleQuery(MysqlCommand mysqlCommand) throws ConnectionException {
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("handle command {}", command);
        }
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
                handleQuery(command);
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

    private void handleFieldList() throws ConnectionException {
        String tableName = new String(MysqlProto.readNulTerminateString(packetBuf), StandardCharsets.UTF_8);
        handleFieldList(tableName);
    }

    // Process a MySQL request
    public void processOnce() throws IOException {
        // set status of query to OK.
        ctx.getState().reset();
        ctx.setGroupCommit(false);
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
