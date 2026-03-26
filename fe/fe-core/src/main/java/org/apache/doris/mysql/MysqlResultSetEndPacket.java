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

import org.apache.doris.qe.QueryState;

/**
 * MySQL OK packet with 0xFE header, used as the end-of-result-set marker
 * when CLIENT_DEPRECATE_EOF is set.
 *
 * When CLIENT_DEPRECATE_EOF capability is negotiated, the traditional EOF packet (0xFE, ≤5 bytes)
 * is replaced by an OK packet that also uses 0xFE as its header byte but has a payload larger
 * than 5 bytes. This is called a "ResultSet OK" packet in the MySQL protocol documentation.
 *
 * See: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
 */
public class MysqlResultSetEndPacket extends MysqlPacket {
    // 0xFE header to distinguish from regular OK (0x00)
    private static final int RESULTSET_OK_INDICATOR = 0xFE;
    private static final long AFFECTED_ROWS = 0;
    private static final long LAST_INSERT_ID = 0;

    private int serverStatus = 0;
    private int warningCount = 0;

    public MysqlResultSetEndPacket(QueryState state) {
        this.serverStatus = state.serverStatus;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        // header: 0xFE (same as EOF, but the payload length > 5 distinguishes it)
        serializer.writeInt1(RESULTSET_OK_INDICATOR);
        // affected_rows: int<lenenc>
        serializer.writeVInt(AFFECTED_ROWS);
        // last_insert_id: int<lenenc>
        serializer.writeVInt(LAST_INSERT_ID);
        // status_flags: int<2>
        serializer.writeInt2(serverStatus);
        // warnings: int<2>
        serializer.writeInt2(warningCount);
        // info: string<lenenc> (empty string, written as a single 0x00 byte for length 0)
        // The driver's OkPacket.parse() unconditionally reads this field.
        serializer.writeVInt(0);
    }
}
