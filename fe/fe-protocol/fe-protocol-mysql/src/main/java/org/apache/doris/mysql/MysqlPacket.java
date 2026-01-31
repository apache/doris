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

import java.nio.ByteBuffer;

/**
 * Base class for MySQL protocol packets.
 *
 * <p>All MySQL protocol packets inherit from this class. The packet format
 * consists of a 4-byte header (3 bytes length + 1 byte sequence id) followed
 * by the packet payload.
 *
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html
 */
public abstract class MysqlPacket {

    /**
     * Reads packet content from a ByteBuffer.
     *
     * <p>This method is primarily used to read authentication packets from the client.
     *
     * @param buffer the byte buffer containing packet data
     * @return true if read was successful
     */
    public boolean readFrom(ByteBuffer buffer) {
        // Default implementation - only used to read authenticate packet from client.
        return false;
    }

    /**
     * Writes packet content to a serializer.
     *
     * <p>This method must be implemented by all packet types to serialize
     * the packet content according to the MySQL protocol format.
     *
     * @param serializer the serializer to write packet data to
     */
    public abstract void writeTo(MysqlSerializer serializer);
}
