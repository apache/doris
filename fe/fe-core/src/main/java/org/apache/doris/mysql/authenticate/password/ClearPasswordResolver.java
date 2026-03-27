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

import org.apache.doris.authentication.CredentialType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlAuthSwitchPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlClearTextPacket;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.qe.ConnectContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClearPasswordResolver implements PasswordResolver {
    private static final Logger LOG = LogManager.getLogger(ClearPasswordResolver.class);
    private static final String OIDC_CLIENT_PLUGIN_NAME = "authentication_openid_connect_client";
    private static final int TOKEN_LOG_BYTES = 12;
    private static final int TOKEN_LOG_CHARS = 32;

    private Optional<Password> resolveOidcTokenFromAuthPacket(MysqlAuthPacket authPacket) {
        if (authPacket == null || !OIDC_CLIENT_PLUGIN_NAME.equals(authPacket.getPluginName())) {
            return Optional.empty();
        }
        byte[] authResponse = authPacket.getAuthResponse();
        if (authResponse == null || authResponse.length == 0) {
            return Optional.empty();
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("bridging OIDC mysql auth response to token: authResponseLength={}, tokenPrefixHex={}, "
                            + "tokenPrefixText={}",
                    authResponse.length,
                    prefixHex(authResponse),
                    prefixText(authResponse));
        }
        return Optional.of(new ClearPassword(new String(authResponse, StandardCharsets.UTF_8)));
    }

    @Override
    public Optional<Password> resolvePassword(ConnectContext context, MysqlChannel channel, MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        Optional<Password> oidcToken = resolveOidcTokenFromAuthPacket(authPacket);
        if (oidcToken.isPresent()) {
            return oidcToken;
        }

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

    @Override
    public Optional<AuthenticateRequest> resolveAuthenticateRequest(String userName, ConnectContext context,
            MysqlChannel channel, MysqlSerializer serializer, MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        Optional<Password> password = resolvePassword(context, channel, serializer, authPacket, handshakePacket);
        if (!password.isPresent()) {
            return Optional.empty();
        }
        ClearPassword clearPassword = (ClearPassword) password.get();
        String credentialType = authPacket != null
                && OIDC_CLIENT_PLUGIN_NAME.equals(authPacket.getPluginName())
                ? CredentialType.OIDC_ID_TOKEN
                : CredentialType.CLEAR_TEXT_PASSWORD;
        return Optional.of(AuthenticateRequest.builder()
                .userName(userName)
                .password(clearPassword)
                .remoteHost(channel.getRemoteIp())
                .clientType("mysql")
                .credentialType(credentialType)
                .credential(clearPassword.getPassword().getBytes(StandardCharsets.UTF_8))
                .build());
    }

    private static String prefixHex(byte[] bytes) {
        int prefixLength = Math.min(bytes.length, TOKEN_LOG_BYTES);
        byte[] prefix = new byte[prefixLength];
        System.arraycopy(bytes, 0, prefix, 0, prefixLength);
        return Util.bytesToHex(prefix);
    }

    private static String prefixText(byte[] bytes) {
        int prefixLength = Math.min(bytes.length, TOKEN_LOG_CHARS);
        return new String(bytes, 0, prefixLength, StandardCharsets.UTF_8).replace("\n", "\\n");
    }
}
