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
 * MySQL ERR packet.
 * 
 * <p>This packet is sent by the server to indicate an error occurred.
 * It contains an error code, SQL state, and error message.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
 * 
 * @since 2.0.0
 */
public class MysqlErrPacket extends MysqlPacket {
    
    /** ERR packet indicator (0xFF) */
    private static final int ERROR_PACKET_INDICATOR = 0XFF;
    
    /** Default SQL state (HY000 - general error) */
    private static final byte[] DEFAULT_SQL_STATE = {'H', 'Y', '0', '0', '0'};
    
    /** Default error code (1064 - syntax error) */
    private static final int DEFAULT_ERROR_CODE = 1064;
    
    private final byte[] sqlState;
    private final int errorCode;
    private final String errorMessage;
    
    /**
     * Creates an ERR packet with specified values.
     * 
     * @param errorCode MySQL error code
     * @param sqlState SQL state (5 characters)
     * @param errorMessage error message
     */
    public MysqlErrPacket(int errorCode, byte[] sqlState, String errorMessage) {
        this.errorCode = errorCode;
        this.sqlState = sqlState != null ? sqlState : DEFAULT_SQL_STATE;
        this.errorMessage = errorMessage;
    }
    
    /**
     * Creates an ERR packet with error code and message.
     * 
     * @param errorCode MySQL error code
     * @param errorMessage error message
     */
    public MysqlErrPacket(int errorCode, String errorMessage) {
        this(errorCode, DEFAULT_SQL_STATE, errorMessage);
    }
    
    /**
     * Creates an ERR packet with default error code.
     * 
     * @param errorMessage error message
     */
    public MysqlErrPacket(String errorMessage) {
        this(DEFAULT_ERROR_CODE, DEFAULT_SQL_STATE, errorMessage);
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        MysqlCapability capability = serializer.getCapability();

        serializer.writeInt1(ERROR_PACKET_INDICATOR);
        serializer.writeInt2(errorCode);
        
        if (capability.isProtocol41()) {
            serializer.writeByte((byte) '#');
            serializer.writeBytes(sqlState, 0, 5);
        }
        
        if (errorMessage == null || errorMessage.isEmpty()) {
            // NOTICE: if write "" or "\0", the client will show "Query OK"
            // SO we need write non-empty string
            serializer.writeEofString("Unknown error");
        } else {
            serializer.writeEofString(errorMessage);
        }
    }
    
    public int getErrorCode() {
        return errorCode;
    }
    
    public byte[] getSqlState() {
        return sqlState;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
}
