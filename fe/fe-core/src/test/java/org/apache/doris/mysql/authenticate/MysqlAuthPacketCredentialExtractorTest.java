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
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.password.ClearPassword;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class MysqlAuthPacketCredentialExtractorTest {
    private static final String USER_NAME = "alice";
    private static final String REMOTE_IP = "127.0.0.1";
    private static final String OIDC_CLIENT_PLUGIN = "authentication_openid_connect_client";

    @Test
    void testExtractAuthenticateRequestReturnsEmptyForNonOidcClientPlugin() {
        MysqlAuthPacketCredentialExtractor extractor = new MysqlAuthPacketCredentialExtractor();
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        Mockito.when(authPacket.getPluginName()).thenReturn("mysql_native_password");

        Assertions.assertFalse(extractor.extractAuthenticateRequest(USER_NAME,
                Mockito.mock(MysqlChannel.class), authPacket).isPresent());
    }

    @Test
    void testExtractAuthenticateRequestForOidcClientPlugin() {
        MysqlAuthPacketCredentialExtractor extractor = new MysqlAuthPacketCredentialExtractor();
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(authPacket.getPluginName()).thenReturn(OIDC_CLIENT_PLUGIN);
        Mockito.when(authPacket.getAuthResponse()).thenReturn("token-from-client".getBytes(StandardCharsets.UTF_8));

        AuthenticateRequest request = extractor.extractAuthenticateRequest(USER_NAME, channel, authPacket)
                .orElseThrow(() -> new AssertionError("request is required"));

        Assertions.assertEquals(CredentialType.OAUTH_TOKEN, request.getCredentialType());
        Assertions.assertArrayEquals("token-from-client".getBytes(StandardCharsets.UTF_8), request.getCredential());
        Assertions.assertInstanceOf(ClearPassword.class, request.getPassword());
        Assertions.assertEquals("token-from-client", ((ClearPassword) request.getPassword()).getPassword());
    }

    @Test
    void testExtractAuthenticateRequestForOidcClientPayload() {
        MysqlAuthPacketCredentialExtractor extractor = new MysqlAuthPacketCredentialExtractor();
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        String token = "eyJhbGciOiJSUzI1NiJ9.payload.signature";
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(authPacket.getPluginName()).thenReturn(OIDC_CLIENT_PLUGIN);
        Mockito.when(authPacket.getAuthResponse()).thenReturn(oidcClientPayload(token));

        AuthenticateRequest request = extractor.extractAuthenticateRequest(USER_NAME, channel, authPacket)
                .orElseThrow(() -> new AssertionError("request is required"));

        Assertions.assertEquals(CredentialType.OAUTH_TOKEN, request.getCredentialType());
        Assertions.assertArrayEquals(token.getBytes(StandardCharsets.UTF_8), request.getCredential());
        Assertions.assertInstanceOf(ClearPassword.class, request.getPassword());
        Assertions.assertEquals(token, ((ClearPassword) request.getPassword()).getPassword());
    }

    @Test
    void testExtractAuthenticateRequestForOidcClientPayloadWithPadding() {
        MysqlAuthPacketCredentialExtractor extractor = new MysqlAuthPacketCredentialExtractor();
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        String token = "eyJhbGciOiJSUzI1NiJ9." + Strings.repeat("p", 1024) + ".signature";
        byte[] payload = oidcClientPayload(token);
        // Connector/J reserves nine bytes for the length and leaves six zero bytes after this token.
        byte[] paddedPayload = Arrays.copyOf(payload, payload.length + 6);
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(authPacket.getPluginName()).thenReturn(OIDC_CLIENT_PLUGIN);
        Mockito.when(authPacket.getAuthResponse()).thenReturn(paddedPayload);

        AuthenticateRequest request = extractor.extractAuthenticateRequest(USER_NAME, channel, authPacket)
                .orElseThrow(() -> new AssertionError("request is required"));

        Assertions.assertEquals(CredentialType.OAUTH_TOKEN, request.getCredentialType());
        Assertions.assertArrayEquals(token.getBytes(StandardCharsets.UTF_8), request.getCredential());
        Assertions.assertInstanceOf(ClearPassword.class, request.getPassword());
        Assertions.assertEquals(token, ((ClearPassword) request.getPassword()).getPassword());
    }

    @Test
    void testExtractAuthenticateRequestPreservesOidcClientPayloadWithNonZeroTrailingByte() {
        MysqlAuthPacketCredentialExtractor extractor = new MysqlAuthPacketCredentialExtractor();
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        String token = "eyJhbGciOiJSUzI1NiJ9.payload.signature";
        byte[] payload = oidcClientPayload(token);
        byte[] invalidPayload = Arrays.copyOf(payload, payload.length + 6);
        invalidPayload[invalidPayload.length - 1] = 1;
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(authPacket.getPluginName()).thenReturn(OIDC_CLIENT_PLUGIN);
        Mockito.when(authPacket.getAuthResponse()).thenReturn(invalidPayload);

        AuthenticateRequest request = extractor.extractAuthenticateRequest(USER_NAME, channel, authPacket)
                .orElseThrow(() -> new AssertionError("request is required"));

        Assertions.assertArrayEquals(invalidPayload, request.getCredential());
        Assertions.assertEquals(new String(invalidPayload, StandardCharsets.UTF_8),
                ((ClearPassword) request.getPassword()).getPassword());
    }

    private static byte[] oidcClientPayload(String token) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(1);
        serializer.writeLenEncodedBytes(token.getBytes(StandardCharsets.UTF_8));
        return serializer.toArray();
    }
}
