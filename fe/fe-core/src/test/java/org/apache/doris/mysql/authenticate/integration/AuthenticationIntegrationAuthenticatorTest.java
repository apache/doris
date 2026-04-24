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
import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationFailureType;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationIntegrationMeta;
import org.apache.doris.authentication.AuthenticationIntegrationMgr;
import org.apache.doris.authentication.AuthenticationIntegrationRuntime;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.handler.AuthenticationOutcome;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.AuthenticationFailureSummary;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.privilege.Auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

class AuthenticationIntegrationAuthenticatorTest {
    private static final String CHAIN_CONFIG = "corp_ldap,backup_ldap";
    private static final String CREATE_USER = "creator";
    private static final String OIDC_JIT_DISABLED_MESSAGE =
            "OIDC authentication succeeded but no matching Doris user exists and JIT user is disabled";

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
        Assertions.assertNotNull(response.getPrincipal());
        Assertions.assertEquals("external_alice", response.getPrincipal().getName());
        Assertions.assertEquals(expectedAuthenticatedRoles(), response.getAuthenticatedRoles());

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
        Assertions.assertEquals("'external_alice'@'127.0.0.1'", response.getUserIdentity().toString());
        Assertions.assertNotNull(response.getPrincipal());
        Assertions.assertEquals("external_alice", response.getPrincipal().getName());
        Assertions.assertEquals(expectedAuthenticatedRoles(), response.getAuthenticatedRoles());
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
        Assertions.assertNull(response.getFailureSummary());
    }

    @Test
    void testAuthenticateExposesOidcJitDisabledFailure() throws Exception {
        mgr.replayCreateAuthenticationIntegration(meta("corp_oidc", "oidc", false));
        Mockito.when(runtime.authenticate(Mockito.anyList(), Mockito.any()))
                .thenReturn(AuthenticationOutcome.of(integration("corp_oidc", "oidc", false), success()));
        Mockito.when(auth.getUserIdentityForExternalAuth("alice", "127.0.0.1"))
                .thenReturn(Collections.emptyList());

        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator("corp_oidc", "authentication_chain");
        AuthenticateResponse response = authenticator.authenticate(
                new AuthenticateRequest("alice", new ClearPassword("secret"), "127.0.0.1"));

        Assertions.assertFalse(response.isSuccess());
        AuthenticationFailureSummary failureSummary = response.getFailureSummary();
        Assertions.assertNotNull(failureSummary);
        Assertions.assertEquals(AuthenticationFailureType.ACCESS_DENIED, failureSummary.getFailureType());
        Assertions.assertEquals(OIDC_JIT_DISABLED_MESSAGE, failureSummary.getDetailMessage());
        Assertions.assertTrue(failureSummary.hasClientVisibleMessage());
        Assertions.assertEquals(OIDC_JIT_DISABLED_MESSAGE, failureSummary.getClientVisibleMessage());
    }

    @Test
    void testAuthenticateExposesOidcIdTokenRejectedFailure() throws Exception {
        mgr.replayCreateAuthenticationIntegration(meta("corp_oidc", "oidc", true));
        AuthenticationException exception = new AuthenticationException(
                AuthenticationIntegrationRuntime.OIDC_ID_TOKEN_REJECTED_MESSAGE,
                AuthenticationFailureType.BAD_CREDENTIAL);
        Mockito.when(runtime.authenticate(Mockito.anyList(), Mockito.any()))
                .thenReturn(AuthenticationOutcome.of(
                        integration("corp_oidc", "oidc", true),
                        AuthenticationResult.failure(exception)));

        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator("corp_oidc", "authentication_chain");
        AuthenticateResponse response = authenticator.authenticate(AuthenticateRequest.builder()
                .userName("alice")
                .password(new ClearPassword("id-token"))
                .remoteHost("127.0.0.1")
                .credentialType(CredentialType.OIDC_ID_TOKEN)
                .credential("id-token".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .build());

        Assertions.assertFalse(response.isSuccess());
        AuthenticationFailureSummary failureSummary = response.getFailureSummary();
        Assertions.assertNotNull(failureSummary);
        Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL, failureSummary.getFailureType());
        Assertions.assertEquals(AuthenticationIntegrationRuntime.OIDC_ID_TOKEN_REJECTED_MESSAGE,
                failureSummary.getDetailMessage());
        Assertions.assertTrue(failureSummary.hasClientVisibleMessage());
        Assertions.assertEquals(AuthenticationIntegrationRuntime.OIDC_ID_TOKEN_REJECTED_MESSAGE,
                failureSummary.getClientVisibleMessage());
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
        return meta(name, "ldap", jitEnabled);
    }

    private static AuthenticationIntegrationMeta meta(String name, String type, boolean jitEnabled) throws Exception {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", type);
        properties.put("enable_jit_user", String.valueOf(jitEnabled));
        return AuthenticationIntegrationMeta.fromCreateSql(name, properties, null, CREATE_USER);
    }

    private static AuthenticationIntegration integration(String name, boolean jitEnabled) {
        return integration(name, "ldap", jitEnabled);
    }

    private static AuthenticationIntegration integration(String name, String type, boolean jitEnabled) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("enable_jit_user", String.valueOf(jitEnabled));
        return AuthenticationIntegration.builder()
                .name(name)
                .type(type)
                .properties(properties)
                .build();
    }

    private static org.apache.doris.authentication.AuthenticationResult success() {
        return org.apache.doris.authentication.AuthenticationResult.success(
                org.apache.doris.authentication.BasicPrincipal.builder()
                        .name("external_alice")
                        .authenticator("corp_ldap")
                        .externalGroups(Collections.singleton("oncall"))
                        .multiValueAttributes(Collections.singletonMap(
                                "scope", Collections.singleton("logs:write")))
                        .build(),
                expectedAuthenticatedRoles());
    }

    private static LinkedHashSet<String> expectedAuthenticatedRoles() {
        return new LinkedHashSet<>(Arrays.asList("plugin_reader", "mapped_reader"));
    }
}
