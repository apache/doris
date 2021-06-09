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

package org.apache.doris.mysql;

import org.apache.doris.qe.QueryState;

import com.google.common.base.Strings;

// MySQL protocol OK packet
public class MysqlOkPacket extends MysqlPacket {
    private static final int PACKET_OK_INDICATOR = 0X00;
    // TODO(zhaochun): following are not used in palo
    private static final long LAST_INSERT_ID = 0;
    private final String infoMessage;
    private long affectedRows = 0;
    private int warningRows = 0;
    private int serverStatus = 0;

    public MysqlOkPacket(QueryState state) {
        infoMessage = state.getInfoMessage();
        affectedRows = state.getAffectedRows();
        warningRows = state.getWarningRows();
        serverStatus = state.serverStatus;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // used to check
        MysqlCapability capability = serializer.getCapability();

        serializer.writeInt1(PACKET_OK_INDICATOR);
        serializer.writeVInt(affectedRows);
        serializer.writeVInt(LAST_INSERT_ID);
        if (capability.isProtocol41()) {
            serializer.writeInt2(serverStatus);
            serializer.writeInt2(warningRows);
        } else if (capability.isTransactions()) {
            serializer.writeInt2(serverStatus);
        }

        if (capability.isSessionTrack()) {
            serializer.writeLenEncodedString(infoMessage);
            // TODO(zhaochun): STATUS_FLAGS
            // if ((STATUS_FLAGS & MysqlStatusFlag.SERVER_SESSION_STATE_CHANGED) != 0) {
            // }
        } else {
            if (!Strings.isNullOrEmpty(infoMessage)) {
                // NOTE: in datasheet, use EOF string, but in the code, mysql use length encoded string
                serializer.writeLenEncodedString(infoMessage);
            }
        }
    }
}
