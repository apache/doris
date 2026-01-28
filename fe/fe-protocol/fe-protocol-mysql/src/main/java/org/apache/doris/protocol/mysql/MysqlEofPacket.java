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

/**
 * MySQL EOF packet.
 * 
 * <p>This packet is sent by the server to indicate the end of a result set
 * or other data stream. It contains warnings count and server status.
 * 
 * <p>Note: In MySQL 5.7.5+, if CLIENT_DEPRECATE_EOF is set, the EOF packet
 * is replaced by an OK packet.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
 * 
 * @since 2.0.0
 */
public class MysqlEofPacket extends MysqlPacket {
    
    /** EOF packet indicator (0xFE) */
    private static final int EOF_INDICATOR = 0XFE;
    
    /** Default warnings count */
    private static final int DEFAULT_WARNINGS = 0;
    
    private final int warnings;
    private final int serverStatus;
    
    /**
     * Creates an EOF packet with specified server status.
     * 
     * @param serverStatus server status flags
     */
    public MysqlEofPacket(int serverStatus) {
        this(DEFAULT_WARNINGS, serverStatus);
    }
    
    /**
     * Creates an EOF packet with specified values.
     * 
     * @param warnings number of warnings
     * @param serverStatus server status flags
     */
    public MysqlEofPacket(int warnings, int serverStatus) {
        this.warnings = warnings;
        this.serverStatus = serverStatus;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        MysqlCapability capability = serializer.getCapability();

        serializer.writeInt1(EOF_INDICATOR);
        if (capability.isProtocol41()) {
            serializer.writeInt2(warnings);
            serializer.writeInt2(serverStatus);
        }
    }
    
    public int getWarnings() {
        return warnings;
    }
    
    public int getServerStatus() {
        return serverStatus;
    }
}
