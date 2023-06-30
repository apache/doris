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

package org.apache.doris.hplsql.executor;

import org.apache.doris.hplsql.Console;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.MysqlServerStatusFlag;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.QueryState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HplsqlResult implements ResultListener, Console {

    private static final Logger LOG = LogManager.getLogger(HplsqlResult.class);
    private ConnectProcessor processor;
    private Metadata metadata = null;
    private StringBuilder msg;
    private StringBuilder error;
    private boolean isSendFields;

    HplsqlResult(ConnectProcessor processor) {
        this.processor = processor;
        this.msg = new StringBuilder();
        this.error = new StringBuilder();
        this.isSendFields = false;
    }

    public void reset() {
        metadata = null;
        isSendFields = false;
        error.delete(0, error.length());
        msg.delete(0, msg.length());
    }

    public String getMsg() {
        return msg.toString();
    }

    public String getError() {
        return error.toString();
    }

    @Override
    public void onMysqlRow(ByteBuffer rows) {
        sendData(() -> ConnectContext.get().getMysqlChannel().sendOnePacket(rows));
    }

    @Override
    public void onRow(Object[] rows) {
        sendData(() -> ConnectContext.get().getMysqlChannel().sendOnePacket(rows));
    }

    @Override
    public void onMetadata(Metadata metadata) {
        this.metadata = metadata;
        isSendFields = false;
    }

    @Override
    public void onEof() {
        ConnectContext.get().getState().setEof();
        try {
            if (metadata != null && !isSendFields) {
                sendFields(metadata, ConnectContext.get().getMysqlChannel().getSerializer());
                isSendFields = true;
            }
        } catch (IOException e) {
            throw new QueryException(e);
        }
    }

    @Override
    public void onFinalize() {
        if (metadata == null) {
            return;
        }
        finalizeCommand();
        metadata = null;
    }

    private void sendData(Send send) {
        if (metadata == null) {
            throw new RuntimeException("The metadata has not been set.");
        }

        MysqlSerializer serializer = ConnectContext.get().getMysqlChannel().getSerializer();
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
        serializer.reset();
        serializer.writeVInt(metadata.columnCount());
        ConnectContext.get().getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < metadata.columnCount(); ++i) {
            serializer.reset();
            serializer.writeField(metadata.columnName(i), metadata.columnType(i));
            ConnectContext.get().getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(ConnectContext.get().getState());
        eofPacket.writeTo(serializer);
        ConnectContext.get().getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
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
        this.error.append(msg);
    }

    @Override
    public void flushConsole() {
        ConnectContext context = ConnectContext.get();
        boolean needSend = false;
        if (error.length() > 0) {
            context.getState().setError("hplsql exec error, " + error.toString());
            needSend = true;
        } else if (msg.length() > 0) {
            context.getState().setOk(0, 0, msg.toString());
            needSend = true;
        }
        if (needSend) {
            finalizeCommand();
            reset();
        }
    }

    private void finalizeCommand() {
        try {
            QueryState state = ConnectContext.get().getState();
            state.serverStatus |= MysqlServerStatusFlag.SERVER_MORE_RESULTS_EXISTS;
            processor.finalizeCommand();
            state.reset();
        } catch (IOException e) {
            throw new QueryException(e);
        }
    }

    @FunctionalInterface
    public interface Send {
        void apply() throws IOException;
    }
}
