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

package org.apache.doris.mysql.authenticate.plugin;

import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.handler.AuthenticationPluginManager;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.privilege.Auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AuthenticationPluginAuthenticatorTest {
    private Env env;
    private Auth auth;
    private AuthenticationPluginManager pluginManager;
    private AuthenticationPlugin plugin;
    private MockedStatic<Env> envMockedStatic;
    private String originalAuthenticationPluginsDir;

    private static final String USER_NAME = "alice";
    private static final String REMOTE_IP = "127.0.0.1";
    private static final String OIDC_CLIENT_PLUGIN = "authentication_openid_connect_client";

    @BeforeEach
    void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);
        pluginManager = Mockito.mock(AuthenticationPluginManager.class);
        plugin = Mockito.mock(AuthenticationPlugin.class);
        originalAuthenticationPluginsDir = org.apache.doris.common.Config.authentication_plugins_dir;

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(pluginManager.hasFactory("oidc")).thenReturn(true);
        Mockito.when(pluginManager.createPlugin(Mockito.any())).thenReturn(plugin);
    }

    @Test
    void testAuthenticateJitUserUsesPrincipalName() throws Exception {
        Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
        Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any()))
                .thenReturn(AuthenticationResult.success(BasicPrincipal.builder()
                        .name("external_alice")
                        .authenticator("oidc")
                        .build()));
        Mockito.when(auth.getUserIdentityForExternalAuth("alice", "127.0.0.1"))
                .thenReturn(Collections.emptyList());

        Map<String, String> integrationProperties = new HashMap<>();
        integrationProperties.put("enable_jit_user", "true");
        AuthenticationPluginAuthenticator authenticator = new AuthenticationPluginAuthenticator(
                "oidc", integrationProperties, pluginManager);
        AuthenticateResponse response = authenticator.authenticate(AuthenticateRequest.builder()
                .userName("alice")
                .remoteHost("127.0.0.1")
                .credentialType("bearer")
                .credential("token".getBytes(StandardCharsets.UTF_8))
                .build());

        Assertions.assertTrue(response.isSuccess());
        Assertions.assertTrue(response.isTemp());
        Assertions.assertEquals("'external_alice'@'127.0.0.1'", response.getUserIdentity().toString());
        Assertions.assertNotNull(response.getPrincipal());
        Assertions.assertEquals("external_alice", response.getPrincipal().getName());
    }

    @Test
    void testOidcPluginResolverUsesOidcCredentialFromMysqlAuthPacket() throws Exception {
        Mockito.when(plugin.supports(Mockito.any())).thenReturn(true);
        Mockito.when(plugin.authenticate(Mockito.any(), Mockito.any()))
                .thenReturn(AuthenticationResult.success(BasicPrincipal.builder()
                        .name("external_alice")
                        .authenticator("oidc")
                        .build()));
        Mockito.when(auth.getUserIdentityForExternalAuth(USER_NAME, REMOTE_IP))
                .thenReturn(Collections.emptyList());

        Map<String, String> integrationProperties = new HashMap<>();
        integrationProperties.put("enable_jit_user", "true");
        AuthenticationPluginAuthenticator authenticator = new AuthenticationPluginAuthenticator(
                "oidc", integrationProperties, pluginManager);

        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        MysqlHandshakePacket handshakePacket = Mockito.mock(MysqlHandshakePacket.class);
        Mockito.when(channel.getRemoteIp()).thenReturn(REMOTE_IP);
        Mockito.when(authPacket.getPluginName()).thenReturn(OIDC_CLIENT_PLUGIN);
        Mockito.when(authPacket.getAuthResponse()).thenReturn("token-from-client".getBytes(StandardCharsets.UTF_8));
        Mockito.when(authPacket.getCapability()).thenReturn(
                org.apache.doris.mysql.MysqlCapability.DEFAULT_CAPABILITY);
        Mockito.when(handshakePacket.checkAuthPluginSameAsDoris(Mockito.anyString())).thenReturn(true);

        AuthenticateRequest request = authenticator.getPasswordResolver().resolveAuthenticateRequest(
                USER_NAME,
                Mockito.mock(org.apache.doris.qe.ConnectContext.class),
                channel,
                MysqlSerializer.newInstance(),
                authPacket,
                handshakePacket).orElseThrow(() -> new AssertionError("request is required"));

        Assertions.assertEquals(CredentialType.OAUTH_TOKEN, request.getCredentialType());
        Assertions.assertArrayEquals("token-from-client".getBytes(StandardCharsets.UTF_8), request.getCredential());

        AuthenticateResponse response = authenticator.authenticate(request);
        Assertions.assertTrue(response.isSuccess());

        ArgumentCaptor<AuthenticationRequest> requestCaptor = ArgumentCaptor.forClass(AuthenticationRequest.class);
        Mockito.verify(plugin).authenticate(requestCaptor.capture(), Mockito.any());
        Assertions.assertEquals(CredentialType.OAUTH_TOKEN, requestCaptor.getValue().getCredentialType());
        Assertions.assertArrayEquals("token-from-client".getBytes(StandardCharsets.UTF_8),
                requestCaptor.getValue().getCredential());
    }

    @Test
    void testEnsurePluginFactoryLoadedSupportsMultiplePluginRoots() throws Exception {
        org.apache.doris.common.Config.authentication_plugins_dir = "/tmp/auth-root-a, /tmp/auth-root-b";
        Mockito.when(pluginManager.hasFactory("oidc")).thenReturn(false, true);

        new AuthenticationPluginAuthenticator("oidc", new HashMap<>(), pluginManager);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Path>> pluginRootsCaptor = ArgumentCaptor.forClass((Class) List.class);
        Mockito.verify(pluginManager).loadAll(pluginRootsCaptor.capture(), Mockito.any());
        Assertions.assertEquals(
                Arrays.asList(Paths.get("/tmp/auth-root-a"), Paths.get("/tmp/auth-root-b")),
                pluginRootsCaptor.getValue());
    }

    @AfterEach
    void tearDown() {
        org.apache.doris.common.Config.authentication_plugins_dir = originalAuthenticationPluginsDir;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }
}
