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

import com.google.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * MySQL Handshake Response (Authentication) packet.
 * 
 * <p>This packet is sent by the client in response to the server's handshake packet.
 * It contains the client's capability flags, authentication data, and optionally
 * the database name and connection attributes.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
 * 
 * @since 2.0.0
 */
public class MysqlAuthPacket extends MysqlPacket {
    
    private int maxPacketSize;
    private int characterSet;
    private String userName;
    private byte[] authResponse;
    private String database;
    private String pluginName;
    private MysqlCapability capability;
    private Map<String, String> connectAttributes;
    private byte[] randomString;
    
    // Optional: prefix for proxy authentication
    private String proxyAuthMagicPrefix = null;

    public String getUser() {
        return userName;
    }

    public byte[] getAuthResponse() {
        return authResponse;
    }

    public void setAuthResponse(byte[] bytes) {
        authResponse = bytes;
    }

    public String getDb() {
        return database;
    }

    public byte[] getRandomString() {
        return randomString;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public String getPluginName() {
        return pluginName;
    }
    
    public int getMaxPacketSize() {
        return maxPacketSize;
    }
    
    public int getCharacterSet() {
        return characterSet;
    }
    
    public Map<String, String> getConnectAttributes() {
        return connectAttributes;
    }
    
    /**
     * Sets the proxy authentication magic prefix.
     * If set, the reserved bytes will be checked for this prefix.
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
        // Check for proxy auth magic prefix in first 3 bytes
        byte[] reserved3 = MysqlProto.readFixedString(buffer, 3);
        if (proxyAuthMagicPrefix != null && new String(reserved3).equals(proxyAuthMagicPrefix)) {
            randomString = new byte[MysqlPassword.SCRAMBLE_LENGTH];
            buffer.get(randomString);
        } else {
            // Skip remaining 20 reserved bytes
            buffer.position(buffer.position() + 20);
        }
        
        // user name (NULL-terminated)
        userName = new String(MysqlProto.readNulTerminateString(buffer));
        
        // auth response
        if (capability.isPluginAuthDataLengthEncoded()) {
            authResponse = MysqlProto.readLenEncodedString(buffer);
        } else if (capability.isSecureConnection()) {
            int len = MysqlProto.readInt1(buffer);
            authResponse = MysqlProto.readFixedString(buffer, len);
        } else {
            authResponse = MysqlProto.readNulTerminateString(buffer);
        }
        
        // database (if CLIENT_CONNECT_WITH_DB is set)
        if (buffer.remaining() > 0 && capability.isConnectedWithDb()) {
            database = new String(MysqlProto.readNulTerminateString(buffer));
        }
        
        // plugin name (if CLIENT_PLUGIN_AUTH is set)
        if (buffer.remaining() > 0 && capability.isPluginAuth()) {
            pluginName = new String(MysqlProto.readNulTerminateString(buffer));
        }
        
        // connection attributes (if CLIENT_CONNECT_ATTRS is set)
        if (buffer.remaining() > 0 && capability.isConnectAttrs()) {
            connectAttributes = Maps.newHashMap();
            long attrsLength = MysqlProto.readVInt(buffer);
            long initialPosition = buffer.position();
            while (buffer.position() - initialPosition < attrsLength) {
                String key = new String(MysqlProto.readLenEncodedString(buffer));
                String value = new String(MysqlProto.readLenEncodedString(buffer));
                connectAttributes.put(key, value);
            }
        }

        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // Auth packets are only read from client, not written
    }
}
