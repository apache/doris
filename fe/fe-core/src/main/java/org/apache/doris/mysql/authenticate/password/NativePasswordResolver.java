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

import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.ConnectContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class NativePasswordResolver implements PasswordResolver {
    @Override
    public Optional<Password> resolvePassword(ConnectContext context, MysqlChannel channel, MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        // Starting with MySQL 8.0.4, MySQL changed the default authentication plugin for MySQL client
        // from mysql_native_password to caching_sha2_password.
        // ref: https://mysqlserverteam.com/mysql-8-0-4-new-default-authentication-plugin-caching_sha2_password/
        // So, User use mysql client or ODBC Driver after 8.0.4 have problem to connect to Doris
        // with password.
        // So Doris support the Protocol::AuthSwitchRequest to tell client to keep the default password plugin
        // which Doris is using now.
        // Note: Check the authPacket whether support plugin auth firstly,
        // before we check AuthPlugin between doris and client to compatible with older version: like mysql 5.1
        if (authPacket.getCapability().isPluginAuth()
                && !handshakePacket.checkAuthPluginSameAsDoris(authPacket.getPluginName())) {
            // 1. clear the serializer
            serializer.reset();
            // 2. build the auth switch request and send to the client
            handshakePacket.buildAuthSwitchRequest(serializer);
            channel.sendAndFlush(serializer.toByteBuffer());
            // Server receive auth switch response packet from client.
            ByteBuffer authSwitchResponse = channel.fetchOnePacket();
            if (authSwitchResponse == null) {
                // receive response failed.
                return Optional.empty();
            }
            // 3. the client use default password plugin of Doris to dispose
            // password
            authPacket.setAuthResponse(MysqlProto.readEofString(authSwitchResponse));
        }

        // NOTE: when we behind proxy, we need random string sent by proxy.
        byte[] randomString = handshakePacket.getAuthPluginData();
        if (Config.proxy_auth_enable && authPacket.getRandomString() != null) {
            randomString = authPacket.getRandomString();
        }
        return Optional.of(new NativePassword(authPacket.getAuthResponse(), randomString));
    }
}
