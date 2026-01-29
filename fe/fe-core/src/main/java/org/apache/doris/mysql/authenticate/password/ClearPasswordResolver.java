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

package org.apache.doris.mysql.authenticate.password;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlAuthSwitchPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlClearTextPacket;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.ConnectContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class ClearPasswordResolver implements PasswordResolver {
    @Override
    public Optional<Password> resolvePassword(ConnectContext context, MysqlChannel channel, MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        // server send authentication switch packet to request password clear text.
        // https://dev.mysql.com/doc/internals/en/authentication-method-change.html
        serializer.reset();
        MysqlAuthSwitchPacket mysqlAuthSwitchPacket = new MysqlAuthSwitchPacket();
        mysqlAuthSwitchPacket.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());

        // Server receive password clear text.
        ByteBuffer authSwitchResponse = channel.fetchOnePacket();
        if (authSwitchResponse == null) {
            return Optional.empty();
        }
        MysqlClearTextPacket clearTextPacket = new MysqlClearTextPacket();
        if (!clearTextPacket.readFrom(authSwitchResponse)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            MysqlProto.sendResponsePacket(context);
            return Optional.empty();
        }
        return Optional.of(new ClearPassword(clearTextPacket.getPassword()));
    }
}
