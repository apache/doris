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

import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class HplsqlResultListener implements ResultListener {

    private static final Logger LOG = LogManager.getLogger(HplsqlResultListener.class);

    private Metadata metadata = null;

    private boolean isSendFields = false;

    public boolean isSendFields() {
        return isSendFields;
    }

    public void reset() {
        metadata = null;
        isSendFields = false;
    }

    @Override
    public void onRow(Object[] rows) {
        if (metadata == null) {
            throw new RuntimeException("The metadata has not been set.");
        }

        MysqlSerializer serializer = ConnectContext.get().getMysqlChannel().getSerializer();
        try {
            if (!isSendFields) {
                sendFields(metadata, serializer);
                isSendFields = true;
            }
            serializer.reset();
            ConnectContext.get().getMysqlChannel().sendOnePacket(rows);
        } catch (IOException e) {
            LOG.warn("send data fail.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onMetadata(Metadata metadata) {
        if (this.metadata != null) {
            throw new RuntimeException("The metadata has already been set.");
        }
        this.metadata = metadata;
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
}
