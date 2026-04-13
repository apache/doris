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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.authentication.CredentialType;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.authenticate.password.ClearPassword;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Extracts non-password credentials that are already carried by the initial MySQL auth packet.
 */
public class MysqlAuthPacketCredentialExtractor {
    private static final String OIDC_CLIENT_PLUGIN_NAME = "authentication_openid_connect_client";
    private static final byte JWT_PREFIX_FIRST_CHAR = 'e';
    private static final byte JWT_PREFIX_SECOND_CHAR = 'y';
    private static final byte JWT_PREFIX_THIRD_CHAR = 'J';

    public Optional<AuthenticateRequest> extractAuthenticateRequest(String userName, MysqlChannel channel,
            MysqlAuthPacket authPacket) {
        if (authPacket == null || !OIDC_CLIENT_PLUGIN_NAME.equals(authPacket.getPluginName())) {
            return Optional.empty();
        }

        byte[] authResponse = authPacket.getAuthResponse();
        if (authResponse == null || authResponse.length == 0) {
            return Optional.empty();
        }

        byte[] oidcToken = extractOidcToken(authResponse);
        String token = new String(oidcToken, StandardCharsets.UTF_8);
        return Optional.of(AuthenticateRequest.builder()
                .userName(userName)
                .password(new ClearPassword(token))
                .remoteHost(channel.getRemoteIp())
                .clientType("mysql")
                .credentialType(CredentialType.OAUTH_TOKEN)
                .credential(oidcToken)
                .build());
    }

    private static byte[] extractOidcToken(byte[] authResponse) {
        if (looksLikeJwt(authResponse)) {
            return authResponse;
        }
        if (authResponse.length <= 1) {
            return authResponse;
        }

        byte[] oidcToken;
        ByteBuffer payload = ByteBuffer.wrap(authResponse);
        try {
            MysqlProto.readInt1(payload);
            oidcToken = MysqlProto.readLenEncodedString(payload);
        } catch (RuntimeException e) {
            return authResponse;
        }
        if (oidcToken == null || oidcToken.length == 0 || payload.remaining() != 0 || !looksLikeJwt(oidcToken)) {
            return authResponse;
        }
        return oidcToken;
    }

    private static boolean looksLikeJwt(byte[] bytes) {
        return bytes.length >= 3
                && bytes[0] == JWT_PREFIX_FIRST_CHAR
                && bytes[1] == JWT_PREFIX_SECOND_CHAR
                && bytes[2] == JWT_PREFIX_THIRD_CHAR;
    }
}
