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

package org.apache.doris.plsql.executor;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlServerStatusFlag;
import org.apache.doris.plsql.Console;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

// If running from mysql client, first send schema column,
// and then send the ByteBuffer through the mysql channel.
//
// If running from plsql.sh, send the result directly after serialization.
public class PlsqlResult implements ResultListener, Console {

    private static final Logger LOG = LogManager.getLogger(PlsqlResult.class);
    private ConnectProcessor processor;
    private Metadata metadata = null;
    private StringBuilder msg;
    private StringBuilder error;
    private List<ErrorCode> errorCodes;
    private boolean isSendFields;

    public PlsqlResult() {
        this.msg = new StringBuilder();
        this.error = new StringBuilder();
        this.isSendFields = false;
        this.errorCodes = Lists.newArrayList();
    }

    public void reset() {
        processor = null;
        metadata = null;
        isSendFields = false;
        error.delete(0, error.length());
        msg.delete(0, msg.length());
        errorCodes.clear();
    }

    public void setProcessor(ConnectProcessor processor) {
        this.processor = processor;
    }

    public String getMsg() {
        return msg.toString();
    }

    public String getError() {
        return error.toString();
    }

    public List<ErrorCode> getErrorCodes() {
        return errorCodes;
    }

    public ErrorCode getLastErrorCode() {
        return errorCodes.isEmpty() ? ErrorCode.ERR_UNKNOWN_ERROR : errorCodes.get(errorCodes.size() - 1);
    }

    @Override
    public void onMysqlRow(ByteBuffer rows) {
        ConnectContext ctx = processor != null ? processor.getConnectContext() : ConnectContext.get();
        sendData(() -> ctx.getMysqlChannel().sendOnePacket(rows));
    }

    @Override
    public void onRow(Object[] rows) {
        ConnectContext ctx = processor != null ? processor.getConnectContext() : ConnectContext.get();
        sendData(() -> ctx.getMysqlChannel().sendOnePacket(rows));
    }

    @Override
    public void onMetadata(Metadata metadata) {
        this.metadata = metadata;
        isSendFields = false;
    }

    @Override
    public void onEof() {
        ConnectContext ctx = processor != null ? processor.getConnectContext() : ConnectContext.get();
        ctx.getState().setEof();
        try {
            if (metadata != null && !isSendFields) {
                sendFields(metadata, ctx.getMysqlChannel().getSerializer());
                isSendFields = true;
            }
        } catch (IOException e) {
            throw new QueryException(e);
        }
    }

    @Override
    public void onFinalize() {
        // If metadata not null, it means that mysql channel sent query results.
        // If selectByFe, send result in handleQueryStmt.
        if (metadata == null && !processor.isHandleQueryInFe()) {
            return;
        }
        finalizeCommand();
        metadata = null;
    }

    private void sendData(Send send) {
        if (metadata == null) {
            throw new RuntimeException("The metadata has not been set.");
        }

        ConnectContext ctx = processor != null ? processor.getConnectContext() : ConnectContext.get();
        MysqlSerializer serializer = ctx.getMysqlChannel().getSerializer();
        try {
            if (!isSendFields) {
                // For some language driver, getting error packet after fields packet
                // will be recognized as a success result
                // so We need to send fields after first batch arrived
                sendFields(metadata, serializer);
                isSendFields = true;
            }
            serializer.reset();
            send.apply();
        } catch (IOException e) {
            LOG.warn("send data fail.", e);
            throw new RuntimeException(e);
        }
    }

    private void sendFields(Metadata metadata, MysqlSerializer serializer) throws IOException {
        ConnectContext ctx = processor != null ? processor.getConnectContext() : ConnectContext.get();
        serializer.reset();
        serializer.writeVInt(metadata.columnCount());
        ctx.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < metadata.columnCount(); ++i) {
            serializer.reset();
            serializer.writeField(metadata.columnName(i), metadata.dorisType(i));
            ctx.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(ctx.getState());
        eofPacket.writeTo(serializer);
        ctx.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    @Override
    public void print(String msg) {
        this.msg.append(msg);
    }

    @Override
    public void printLine(String msg) {
        this.msg.append(msg).append("\n");
    }

    @Override
    public void printError(String msg) {
        errorCodes.add(ErrorCode.ERR_UNKNOWN_ERROR);
        this.error.append("\n" + errorCodes.size() + ". " + getLastErrorCode().toString() + ", " + msg);
    }

    @Override
    public void printError(ErrorCode errorCode, String msg) {
        errorCodes.add(errorCode != null ? errorCode : ErrorCode.ERR_UNKNOWN_ERROR);
        this.error.append("\n" + errorCodes.size() + ". " + getLastErrorCode().toString() + ", " + msg);
    }

    private void finalizeCommand() {
        if (processor != null) {
            try (AutoCloseConnectContext autoCloseCtx = new AutoCloseConnectContext(processor.getConnectContext())) {
                autoCloseCtx.call();
                QueryState state = processor.getConnectContext().getState();
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_sp.html
                state.serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
                processor.finalizeCommand();
                state.reset();
            } catch (IOException e) {
                throw new QueryException(e);
            }
        }
    }

    @FunctionalInterface
    public interface Send {
        void apply() throws IOException;
    }
}
