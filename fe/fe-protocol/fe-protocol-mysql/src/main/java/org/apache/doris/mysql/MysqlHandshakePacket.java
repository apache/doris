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

/**
 * MySQL Handshake packet.
 *
 * <p>This packet is sent by the server to the client after the TCP connection
 * is established. It contains server version, connection ID, authentication
 * challenge data, and capability flags.
 *
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
 */
public class MysqlHandshakePacket extends MysqlPacket {

    /**
     * Protocol version (always 10 for MySQL 3.21.0+)
     */
    private static final int PROTOCOL_VERSION = 10;

    /**
     * Default server version string
     */
    public static final String DEFAULT_SERVER_VERSION = "5.7.99";

    /**
     * Character set: 33 = UTF-8
     */
    private static final int CHARACTER_SET = 33;

    /**
     * Default capability (without SSL)
     */
    private static final MysqlCapability CAPABILITY = MysqlCapability.DEFAULT_CAPABILITY;

    /**
     * SSL capability
     */
    private static final MysqlCapability SSL_CAPABILITY = MysqlCapability.SSL_CAPABILITY;

    /**
     * Status flags (not used in Doris)
     */
    private static final int STATUS_FLAGS = 0;

    /**
     * Default authentication plugin name
     */
    public static final String AUTH_PLUGIN_NAME = "mysql_native_password";

    private final long connectionId;
    private final byte[] authPluginData;
    private String serverVersion = DEFAULT_SERVER_VERSION;
    private boolean useSsl = false;

    /**
     * Creates a handshake packet with the given connection ID.
     *
     * @param connectionId the connection ID
     */
    public MysqlHandshakePacket(long connectionId) {
        this.connectionId = connectionId;
        this.authPluginData = MysqlPassword.createRandomString(MysqlPassword.SCRAMBLE_LENGTH);
    }

    /**
     * Creates a handshake packet with custom parameters.
     *
     * @param connectionId  the connection ID
     * @param serverVersion the server version string
     * @param useSsl        whether to enable SSL capability
     */
    public MysqlHandshakePacket(long connectionId, String serverVersion, boolean useSsl) {
        this.connectionId = connectionId;
        this.authPluginData = MysqlPassword.createRandomString(MysqlPassword.SCRAMBLE_LENGTH);
        this.serverVersion = serverVersion;
        this.useSsl = useSsl;
    }

    /**
     * Gets the authentication plugin data (challenge).
     *
     * @return auth plugin data bytes
     */
    public byte[] getAuthPluginData() {
        return authPluginData;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        MysqlCapability capability = useSsl ? SSL_CAPABILITY : CAPABILITY;

        // protocol version
        serializer.writeInt1(PROTOCOL_VERSION);
        // server version (NULL-terminated)
        serializer.writeNulTerminateString(serverVersion);
        // connection id
        serializer.writeInt4((int) connectionId);
        // first 8 bytes of auth plugin data
        serializer.writeBytes(authPluginData, 0, 8);
        // filler
        serializer.writeInt1(0);
        // lower 2 bytes of capability flags
        serializer.writeInt2(capability.getFlags() & 0XFFFF);
        // character set
        serializer.writeInt1(CHARACTER_SET);
        // status flags
        serializer.writeInt2(STATUS_FLAGS);
        // upper 2 bytes of capability flags
        serializer.writeInt2(capability.getFlags() >> 16);

        if (capability.isPluginAuth()) {
            // length of auth plugin data (1 byte is for '\0')
            serializer.writeInt1(authPluginData.length + 1);
        } else {
            serializer.writeInt1(0);
        }

        // reserved 10 zeros
        serializer.writeBytes(new byte[10]);

        if (capability.isSecureConnection()) {
            // NOTE: MySQL protocol requires writing at least 13 bytes here.
            // write len(max(13, len(auth-plugin-data) - 8))
            serializer.writeBytes(authPluginData, 8, 12);
            // append one byte up to 13
            serializer.writeInt1(0);
        }

        if (capability.isPluginAuth()) {
            serializer.writeNulTerminateString(AUTH_PLUGIN_NAME);
        }
    }

    /**
     * Checks if the client's auth plugin matches Doris's default.
     *
     * @param pluginName the plugin name from client
     * @return true if they match
     */
    public boolean checkAuthPluginSameAsDoris(String pluginName) {
        return AUTH_PLUGIN_NAME.equals(pluginName);
    }

    /**
     * Builds an AuthSwitchRequest packet.
     *
     * <p>If the client's default auth plugin is different from Doris,
     * this method creates an AuthSwitchRequest to switch to native password.
     *
     * @param serializer the serializer to write to
     */
    public void buildAuthSwitchRequest(MysqlSerializer serializer) {
        serializer.writeInt1((byte) 0xfe);
        serializer.writeNulTerminateString(AUTH_PLUGIN_NAME);
        serializer.writeBytes(authPluginData);
        serializer.writeInt1(0);
    }

    public long getConnectionId() {
        return connectionId;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }
}
