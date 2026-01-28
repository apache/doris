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

package org.apache.doris.protocol.mysql;

import com.google.common.base.Strings;

/**
 * MySQL OK packet.
 * 
 * <p>This packet is sent by the server to indicate successful completion of a command.
 * It contains information about affected rows, last insert id, server status, and warnings.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
 * 
 * @since 2.0.0
 */
public class MysqlOkPacket extends MysqlPacket {
    
    /** OK packet indicator (0x00) */
    private static final int PACKET_OK_INDICATOR = 0X00;
    
    /** Last insert ID (not used in Doris) */
    private static final long LAST_INSERT_ID = 0;
    
    private final String infoMessage;
    private final long affectedRows;
    private final int warningRows;
    private final int serverStatus;
    
    /**
     * Creates an OK packet with specified values.
     * 
     * @param affectedRows number of affected rows
     * @param warningRows number of warnings
     * @param serverStatus server status flags
     * @param infoMessage info message
     */
    public MysqlOkPacket(long affectedRows, int warningRows, int serverStatus, String infoMessage) {
        this.affectedRows = affectedRows;
        this.warningRows = warningRows;
        this.serverStatus = serverStatus;
        this.infoMessage = infoMessage;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
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
            serializer.writeLenEncodedString(infoMessage != null ? infoMessage : "");
        } else {
            if (!Strings.isNullOrEmpty(infoMessage)) {
                serializer.writeLenEncodedString(infoMessage);
            }
        }
    }
    
    public long getAffectedRows() {
        return affectedRows;
    }
    
    public int getWarningRows() {
        return warningRows;
    }
    
    public int getServerStatus() {
        return serverStatus;
    }
    
    public String getInfoMessage() {
        return infoMessage;
    }
}
