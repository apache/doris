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
 * MySQL Auth Switch Request packet.
 * 
 * <p>This packet is sent by the server to request the client to switch
 * to a different authentication method.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
 * 
 * @since 2.0.0
 */
public class MysqlAuthSwitchPacket extends MysqlPacket {
    
    /** Status indicator for auth switch (0xFE) */
    private static final int STATUS = 0xfe;
    
    /** Default authentication plugin name */
    private static final String DEFAULT_AUTH_PLUGIN_NAME = "mysql_clear_password";
    
    /** Default data (empty) */
    private static final String DEFAULT_DATA = "";
    
    private final String authPluginName;
    private final String data;
    
    /**
     * Creates an auth switch packet with default values.
     */
    public MysqlAuthSwitchPacket() {
        this(DEFAULT_AUTH_PLUGIN_NAME, DEFAULT_DATA);
    }
    
    /**
     * Creates an auth switch packet with specified plugin and data.
     * 
     * @param authPluginName authentication plugin name
     * @param data plugin-specific data
     */
    public MysqlAuthSwitchPacket(String authPluginName, String data) {
        this.authPluginName = authPluginName;
        this.data = data;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        serializer.writeInt1(STATUS);
        serializer.writeNulTerminateString(authPluginName);
        serializer.writeNulTerminateString(data);
    }
    
    /**
     * Gets the authentication plugin name.
     * 
     * @return plugin name
     */
    public String getAuthPluginName() {
        return authPluginName;
    }
    
    /**
     * Gets the plugin-specific data.
     * 
     * @return data string
     */
    public String getData() {
        return data;
    }
}
