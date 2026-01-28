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
 * MySQL Clear Text Password packet.
 * 
 * <p>This packet is sent by the client when using the mysql_clear_password
 * authentication plugin. The password is sent as a NULL-terminated string.
 * 
 * @since 2.0.0
 */
public class MysqlClearTextPacket extends MysqlPacket {

    private String password = "";

    /**
     * Gets the password from the packet.
     * 
     * @return clear text password
     */
    public String getPassword() {
        return password;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        password = new String(MysqlProto.readNulTerminateString(buffer));
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // Not used - this packet is only read from client
    }
}
