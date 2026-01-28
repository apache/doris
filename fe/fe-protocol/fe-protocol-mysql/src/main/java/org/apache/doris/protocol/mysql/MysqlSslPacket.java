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

import java.nio.ByteBuffer;

/**
 * MySQL SSL Connection Request packet.
 * 
 * <p>This packet is sent by the client when it wants to establish an SSL connection.
 * It contains capability flags, max packet size, and character set, but not
 * the full authentication data (that comes after SSL handshake).
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html
 * 
 * @since 2.0.0
 */
public class MysqlSslPacket extends MysqlPacket {

    private int maxPacketSize;
    private int characterSet;
    private byte[] randomString;
    private MysqlCapability capability;
    
    // Optional: prefix for proxy authentication
    private String proxyAuthMagicPrefix = null;
    
    public int getMaxPacketSize() {
        return maxPacketSize;
    }
    
    public int getCharacterSet() {
        return characterSet;
    }
    
    public byte[] getRandomString() {
        return randomString;
    }
    
    public MysqlCapability getCapability() {
        return capability;
    }
    
    /**
     * Sets the proxy authentication magic prefix.
     * 
     * @param prefix the 3-character prefix to check
     */
    public void setProxyAuthMagicPrefix(String prefix) {
        this.proxyAuthMagicPrefix = prefix;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        // read capability four bytes, CLIENT_PROTOCOL_41 must be set
        capability = new MysqlCapability(MysqlProto.readInt4(buffer));
        if (!capability.isProtocol41()) {
            return false;
        }
        // max packet size
        maxPacketSize = MysqlProto.readInt4(buffer);
        // character set (only support 33 = utf-8)
        characterSet = MysqlProto.readInt1(buffer);
        
        // reserved 23 bytes
        byte[] reserved3 = MysqlProto.readFixedString(buffer, 3);
        if (proxyAuthMagicPrefix != null && new String(reserved3).equals(proxyAuthMagicPrefix)) {
            randomString = new byte[MysqlPassword.SCRAMBLE_LENGTH];
            buffer.get(randomString);
        } else {
            // Skip remaining 20 reserved bytes
            buffer.position(buffer.position() + 20);
        }
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // SSL packets are only read from client, not written
    }
}
