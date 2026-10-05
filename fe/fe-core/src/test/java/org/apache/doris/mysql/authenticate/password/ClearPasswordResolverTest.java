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
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class ClearPasswordResolverTest {
    private static final String USER_NAME = "alice";
    private static final String REMOTE_IP = "127.0.0.1";

    @Test
    void testResolveAuthenticateRequestDefaultsToClearTextPassword() throws Exception {
        ClearPasswordResolver resolver = new ClearPasswordResolver();
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(channel.fetchOnePacket()).thenReturn(clearTextResponse("secret-token"));

        AuthenticateRequest request = resolver.resolveAuthenticateRequest(
                USER_NAME,
                Mockito.mock(ConnectContext.class),
                channel,
                MysqlSerializer.newInstance(),
                Mockito.mock(MysqlAuthPacket.class),
                Mockito.mock(MysqlHandshakePacket.class))
                .orElseThrow(() -> new AssertionError("request is required"));

        Assertions.assertEquals(CredentialType.CLEAR_TEXT_PASSWORD, request.getCredentialType());
        Assertions.assertArrayEquals("secret-token".getBytes(StandardCharsets.UTF_8), request.getCredential());
        Assertions.assertInstanceOf(ClearPassword.class, request.getPassword());
    }

    private static ByteBuffer clearTextResponse(String password) {
        return ByteBuffer.wrap((password + "\0").getBytes(StandardCharsets.UTF_8));
    }
}
