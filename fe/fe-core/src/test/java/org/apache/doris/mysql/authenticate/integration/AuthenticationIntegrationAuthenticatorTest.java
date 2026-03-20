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

package org.apache.doris.mysql.authenticate.integration;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationIntegrationMeta;
import org.apache.doris.authentication.AuthenticationIntegrationMgr;
import org.apache.doris.authentication.AuthenticationIntegrationRuntime;
import org.apache.doris.authentication.handler.AuthenticationOutcome;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.privilege.Auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class AuthenticationIntegrationAuthenticatorTest {
    private static final String CHAIN_CONFIG = "corp_ldap,backup_ldap";
    private static final String CREATE_USER = "creator";

    private Env env;
    private Auth auth;
    private AuthenticationIntegrationRuntime runtime;
    private AuthenticationIntegrationMgr mgr;
    private MockedStatic<Env> envMockedStatic;

    @BeforeEach
    void setUp() {
        env = Mockito.mock(Env.class);
        auth = Mockito.mock(Auth.class);
        runtime = Mockito.mock(AuthenticationIntegrationRuntime.class);
        mgr = new AuthenticationIntegrationMgr();

        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);

        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getAuthenticationIntegrationRuntime()).thenReturn(runtime);
        Mockito.when(env.getAuthenticationIntegrationMgr()).thenReturn(mgr);
    }

    @Test
    void testAuthenticateExistingUser() throws Exception {
        mgr.replayCreateAuthenticationIntegration(meta("corp_ldap", true));
        mgr.replayCreateAuthenticationIntegration(meta("backup_ldap", true));
        Mockito.when(runtime.authenticate(Mockito.anyList(), Mockito.any()))
                .thenReturn(AuthenticationOutcome.of(integration("corp_ldap", true), success()));
        Mockito.when(auth.getUserIdentityForExternalAuth("alice", "127.0.0.1"))
                .thenReturn(Collections.singletonList(new UserIdentity("alice", "127.0.0.1")));

        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator(CHAIN_CONFIG, "authentication_chain");
        AuthenticateResponse response = authenticator.authenticate(
                new AuthenticateRequest("alice", new ClearPassword("secret"), "127.0.0.1"));

        Assertions.assertTrue(response.isSuccess());
        Assertions.assertFalse(response.isTemp());
        Assertions.assertEquals("'alice'@'127.0.0.1'", response.getUserIdentity().toString());

        ArgumentCaptor<List<AuthenticationIntegrationMeta>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(runtime).authenticate(captor.capture(), Mockito.any());
        Assertions.assertEquals(2, captor.getValue().size());
        Assertions.assertEquals("corp_ldap", captor.getValue().get(0).getName());
        Assertions.assertEquals("backup_ldap", captor.getValue().get(1).getName());
    }

    @Test
    void testAuthenticateJitUser() throws Exception {
        mgr.replayCreateAuthenticationIntegration(meta("corp_ldap", true));
        mgr.replayCreateAuthenticationIntegration(meta("backup_ldap", true));
        Mockito.when(runtime.authenticate(Mockito.anyList(), Mockito.any()))
                .thenReturn(AuthenticationOutcome.of(integration("corp_ldap", true), success()));
        Mockito.when(auth.getUserIdentityForExternalAuth("alice", "127.0.0.1"))
                .thenReturn(Collections.emptyList());

        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator(CHAIN_CONFIG, "authentication_chain");
        AuthenticateResponse response = authenticator.authenticate(
                new AuthenticateRequest("alice", new ClearPassword("secret"), "127.0.0.1"));

        Assertions.assertTrue(response.isSuccess());
        Assertions.assertTrue(response.isTemp());
        Assertions.assertEquals("'alice'@'127.0.0.1'", response.getUserIdentity().toString());
    }

    @Test
    void testAuthenticateFailsWhenJitDisabled() throws Exception {
        mgr.replayCreateAuthenticationIntegration(meta("corp_ldap", false));
        mgr.replayCreateAuthenticationIntegration(meta("backup_ldap", true));
        Mockito.when(runtime.authenticate(Mockito.anyList(), Mockito.any()))
                .thenReturn(AuthenticationOutcome.of(integration("corp_ldap", false), success()));
        Mockito.when(auth.getUserIdentityForExternalAuth("alice", "127.0.0.1"))
                .thenReturn(Collections.emptyList());

        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator(CHAIN_CONFIG, "authentication_chain");
        AuthenticateResponse response = authenticator.authenticate(
                new AuthenticateRequest("alice", new ClearPassword("secret"), "127.0.0.1"));

        Assertions.assertFalse(response.isSuccess());
    }

    @Test
    void testCanDeal() {
        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator(CHAIN_CONFIG, "authentication_chain");

        Assertions.assertFalse(authenticator.canDeal(Auth.ROOT_USER));
        Assertions.assertFalse(authenticator.canDeal(Auth.ADMIN_USER));
        Assertions.assertTrue(authenticator.canDeal("ordinary_user"));
    }

    @AfterEach
    void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    private static AuthenticationIntegrationMeta meta(String name, boolean jitEnabled) throws Exception {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "ldap");
        properties.put("enable_jit_user", String.valueOf(jitEnabled));
        return AuthenticationIntegrationMeta.fromCreateSql(name, properties, null, CREATE_USER);
    }

    private static AuthenticationIntegration integration(String name, boolean jitEnabled) {
        return AuthenticationIntegration.builder()
                .name(name)
                .type("ldap")
                .property("enable_jit_user", String.valueOf(jitEnabled))
                .build();
    }

    private static org.apache.doris.authentication.AuthenticationResult success() {
        return org.apache.doris.authentication.AuthenticationResult.success(
                org.apache.doris.authentication.BasicPrincipal.builder()
                        .name("alice")
                        .authenticator("corp_ldap")
                        .build());
    }
}
