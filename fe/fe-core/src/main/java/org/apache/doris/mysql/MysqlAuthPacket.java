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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.Util;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

// MySQL protocol handshake response packet, which contain authenticate information.
public class MysqlAuthPacket extends MysqlPacket {
    private static final Logger LOG = LogManager.getLogger(MysqlAuthPacket.class);
    private static final String OIDC_CLIENT_PLUGIN_NAME = "authentication_openid_connect_client";
    private static final int AUTH_RESPONSE_LOG_BYTES = 12;
    private static final int AUTH_RESPONSE_LOG_CHARS = 32;

    private int maxPacketSize;
    private int characterSet;
    private String userName;
    private byte[] authResponse;
    private String database;
    private String pluginName;
    private MysqlCapability capability;
    private Map<String, String> connectAttributes;
    private byte[] randomString;

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

    public Map<String, String> getConnectAttributes() {
        return connectAttributes == null ? Collections.emptyMap() : connectAttributes;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        // read capability four byte, which CLIENT_PROTOCOL_41 must be set
        capability = new MysqlCapability(MysqlProto.readInt4(buffer));
        if (!capability.isProtocol41()) {
            return false;
        }
        // max packet size
        maxPacketSize = MysqlProto.readInt4(buffer);
        // character set. only support 33(utf-8)?
        characterSet = MysqlProto.readInt1(buffer);
        // reserved 23 bytes
        if (new String(MysqlProto.readFixedString(buffer, 3)).equals(Config.proxy_auth_magic_prefix)) {
            randomString = new byte[MysqlPassword.SCRAMBLE_LENGTH];
            buffer.get(randomString);
        } else {
            buffer.position(buffer.position() + 20);
        }
        // user name
        userName = new String(MysqlProto.readNulTerminateString(buffer));
        if (capability.isPluginAuthDataLengthEncoded()) {
            authResponse = MysqlProto.readLenEncodedString(buffer);
        } else if (capability.isSecureConnection()) {
            int len = MysqlProto.readInt1(buffer);
            authResponse = MysqlProto.readFixedString(buffer, len);
        } else {
            authResponse = MysqlProto.readNulTerminateString(buffer);
        }
        // maybe no data anymore
        // DB to use
        if (buffer.remaining() > 0 && capability.isConnectedWithDb()) {
            database = new String(MysqlProto.readNulTerminateString(buffer));
        }
        // plugin name to plugin
        if (buffer.remaining() > 0 && capability.isPluginAuth()) {
            pluginName = new String(MysqlProto.readNulTerminateString(buffer));
        }
        if (OIDC_CLIENT_PLUGIN_NAME.equals(pluginName)) {
            logOidcAuthResponse();
        }
        // attribute map, no use now.
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

        // Commented for JDBC
        // if (buffer.remaining() != 0) {
        //     return false;
        // }
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {

    }

    private void logOidcAuthResponse() {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        LOG.info("decoded OIDC mysql auth response: capabilityLenenc={}, authResponseLength={}, "
                        + "authResponsePrefixHex={}, authResponsePrefixText={}",
                capability.isPluginAuthDataLengthEncoded(),
                authResponse == null ? 0 : authResponse.length,
                prefixHex(authResponse),
                prefixText(authResponse));
    }

    private static String prefixHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        int prefixLength = Math.min(bytes.length, AUTH_RESPONSE_LOG_BYTES);
        byte[] prefix = new byte[prefixLength];
        System.arraycopy(bytes, 0, prefix, 0, prefixLength);
        return Util.bytesToHex(prefix);
    }

    private static String prefixText(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        int prefixLength = Math.min(bytes.length, AUTH_RESPONSE_LOG_CHARS);
        return new String(bytes, 0, prefixLength, StandardCharsets.UTF_8).replace("\n", "\\n");
    }
}
