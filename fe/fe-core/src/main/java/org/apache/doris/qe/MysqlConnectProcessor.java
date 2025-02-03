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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.commands.ExecuteCommand;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
            if (ctx.getSessionVariable().isEnablePreparedStmtAuditLog()) {
                stmtStr = executeStmt.toSql();
                stmtStr = stmtStr + " /*originalSql = " + prepareCommand.getOriginalStmt().originStmt + "*/";
            }
        } catch (Throwable e) {
            // Catch all throwable.
            // If reach here, maybe doris bug.
            LOG.warn("Process one query failed because unknown reason: ", e);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR,
                    e.getClass().getSimpleName() + ", msg: " + e.getMessage());
        }
        if (ctx.getSessionVariable().isEnablePreparedStmtAuditLog()) {
            auditAfterExec(stmtStr, executor.getParsedStmt(), executor.getQueryStatisticsForAuditLog(), true);
        }
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

        ctx.setStartTime();
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

    // Process COM_QUERY statement,
    private void handleQuery() throws ConnectionException {
        // convert statement to Java string
        byte[] bytes = packetBuf.array();
        int ending = packetBuf.limit() - 1;
        while (ending >= 1 && bytes[ending] == '\0') {
            ending--;
        }
        String originStmt = new String(bytes, 1, ending, StandardCharsets.UTF_8);

        handleQuery(originStmt);
    }

    private void dispatch() throws IOException {
        int code = packetBuf.get();
        MysqlCommand command = MysqlCommand.fromCode(code);
        if (command == null) {
            ErrorReport.report(ErrorCode.ERR_UNKNOWN_COM_ERROR);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_COM_ERROR, "Unknown command(" + code + ")");
            LOG.warn("Unknown command({})", code);
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
                handleQuery();
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
            case COM_STATISTICS:
                handleStatistics();
                break;
            case COM_DEBUG:
                handleDebug();
                break;
            case COM_CHANGE_USER:
                handleChangeUser();
                break;
            case COM_STMT_RESET:
                handleStmtReset();
                break;
            case COM_STMT_CLOSE:
                handleStmtClose();
                break;
            case COM_SET_OPTION:
                handleSetOption();
                break;
            case COM_RESET_CONNECTION:
                handleResetConnection();
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

    private void handleChangeUser() throws IOException {
        // Random bytes generated when creating connection.
        byte[] authPluginData = getConnectContext().getAuthPluginData();
        Preconditions.checkNotNull(authPluginData, "Auth plugin data is null.");
        String userName = new String(MysqlProto.readNulTerminateString(packetBuf));
        int passwordLen = MysqlProto.readInt1(packetBuf);
        byte[] password = MysqlProto.readFixedString(packetBuf, passwordLen);
        String db = new String(MysqlProto.readNulTerminateString(packetBuf));
        // Read the character set.
        MysqlProto.readInt2(packetBuf);
        String authPluginName = new String(MysqlProto.readNulTerminateString(packetBuf));

        // Send Protocol::AuthSwitchRequest to client if auth plugin name is not mysql_native_password
        if (!MysqlHandshakePacket.AUTH_PLUGIN_NAME.equals(authPluginName)) {
            MysqlChannel channel = ctx.mysqlChannel;
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1((byte) 0xfe);
            serializer.writeNulTerminateString(MysqlHandshakePacket.AUTH_PLUGIN_NAME);
            serializer.writeBytes(authPluginData);
            serializer.writeInt1(0);
            channel.sendAndFlush(serializer.toByteBuffer());
            // Server receive auth switch response packet from client.
            ByteBuffer authSwitchResponse = channel.fetchOnePacket();
            int length = authSwitchResponse.limit();
            password = new byte[length];
            System.arraycopy(authSwitchResponse.array(), 0, password, 0, length);
        }

        // For safety, not allowed to change to root or admin.
        if (Auth.ROOT_USER.equals(userName) || Auth.ADMIN_USER.equals(userName)) {
            ctx.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR, "Change to root or admin is forbidden");
            return;
        }

        // Check password.
        List<UserIdentity> currentUserIdentity = Lists.newArrayList();
        try {
            Env.getCurrentEnv().getAuth()
                .checkPassword(userName, ctx.remoteIP, password, authPluginData, currentUserIdentity);
        } catch (AuthenticationException e) {
            ctx.getState().setError(ErrorCode.ERR_ACCESS_DENIED_ERROR, "Authentication failed.");
            return;
        }
        ctx.setCurrentUserIdentity(currentUserIdentity.get(0));
        ctx.setQualifiedUser(userName);

        // Change default db if set.
        if (Strings.isNullOrEmpty(db)) {
            ctx.changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        } else {
            String catalogName = null;
            String dbName = null;
            String[] dbNames = db.split("\\.");
            if (dbNames.length == 1) {
                dbName = db;
            } else if (dbNames.length == 2) {
                catalogName = dbNames[0];
                dbName = dbNames[1];
            } else if (dbNames.length > 2) {
                ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "Only one dot can be in the name: " + db);
                return;
            }

            if (Config.isCloudMode()) {
                try {
                    dbName = ((CloudEnv) Env.getCurrentEnv()).analyzeCloudCluster(dbName, ctx);
                } catch (DdlException e) {
                    ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                    return;
                }
            }

            // check catalog and db exists
            if (catalogName != null) {
                CatalogIf catalogIf = ctx.getEnv().getCatalogMgr().getCatalog(catalogName);
                if (catalogIf == null) {
                    ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "No match catalog in doris: " + db);
                    return;
                }
                if (catalogIf.getDbNullable(dbName) == null) {
                    ctx.getState().setError(ErrorCode.ERR_BAD_DB_ERROR, "No match database in doris: " + db);
                    return;
                }
            }
            try {
                if (catalogName != null) {
                    ctx.getEnv().changeCatalog(ctx, catalogName);
                }
                Env.getCurrentEnv().changeDb(ctx, dbName);
            } catch (DdlException e) {
                ctx.getState().setError(e.getMysqlErrorCode(), e.getMessage());
                return;
            }
        }
        ctx.getState().setOk();
    }

    private void handleSetOption() {
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_set_option.html
        int optionOperation = MysqlProto.readInt2(packetBuf);
        LOG.debug("option_operation {}", optionOperation);
        // Do nothing for now.
        // https://dev.mysql.com/doc/c-api/8.0/en/mysql-set-server-option.html
        ctx.getState().setOk();
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
            if (!packetBuf.hasRemaining()) {
                LOG.info("No more data to be read. Close connection. remote={}", channel.getRemoteHostPortString());
                ctx.setKilled();
                return;
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
        ctx.clear();
        executor = null;
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
