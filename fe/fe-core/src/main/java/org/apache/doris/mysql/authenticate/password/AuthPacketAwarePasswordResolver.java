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

import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.MysqlAuthPacketCredentialExtractor;
import org.apache.doris.qe.ConnectContext;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Decorates a password resolver with support for credentials that are already
 * carried by the initial MySQL auth packet, such as OIDC tokens.
 */
public class AuthPacketAwarePasswordResolver implements PasswordResolver {
    private final PasswordResolver delegate;
    private final MysqlAuthPacketCredentialExtractor credentialExtractor = new MysqlAuthPacketCredentialExtractor();

    public AuthPacketAwarePasswordResolver(PasswordResolver delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Optional<Password> resolvePassword(ConnectContext context, MysqlChannel channel, MysqlSerializer serializer,
            MysqlAuthPacket authPacket, MysqlHandshakePacket handshakePacket) throws IOException {
        return delegate.resolvePassword(context, channel, serializer, authPacket, handshakePacket);
    }

    @Override
    public Optional<AuthenticateRequest> resolveAuthenticateRequest(String userName, ConnectContext context,
            MysqlChannel channel, MysqlSerializer serializer, MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        Optional<AuthenticateRequest> extractedRequest =
                credentialExtractor.extractAuthenticateRequest(userName, channel, authPacket);
        if (extractedRequest.isPresent()) {
            return extractedRequest;
        }
        return delegate.resolveAuthenticateRequest(userName, context, channel, serializer, authPacket,
                handshakePacket);
    }
}
