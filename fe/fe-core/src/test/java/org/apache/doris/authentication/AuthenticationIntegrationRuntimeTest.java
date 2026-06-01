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

package org.apache.doris.authentication;

import org.apache.doris.authentication.handler.AuthenticationOutcome;
import org.apache.doris.authentication.handler.AuthenticationPluginManager;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;
import org.apache.doris.catalog.Env;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AuthenticationIntegrationRuntimeTest {
    private static final String CREATE_USER = "creator";
    private MockedStatic<Env> envMockedStatic;
    private String originalAuthenticationPluginsDir;

    @BeforeEach
    void setUp() {
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(null);
        originalAuthenticationPluginsDir = org.apache.doris.common.Config.authentication_plugins_dir;
    }

    @AfterEach
    void tearDown() {
        org.apache.doris.common.Config.authentication_plugins_dir = originalAuthenticationPluginsDir;
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    @Test
    void testAuthenticateContinuesOnUserNotFound() throws Exception {
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new ChainTestPluginFactory());
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);

        AuthenticationIntegrationMeta first = meta("first", "chain_test",
                map("result", "USER_NOT_FOUND"));
        AuthenticationIntegrationMeta second = meta("second", "chain_test",
                map("result", "SUCCESS"));

        AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration preparedFirst =
                runtime.prepareAuthenticationIntegration(first);
        runtime.activatePreparedAuthenticationIntegration(preparedFirst);
        AuthenticationIntegrationRuntime.PreparedAuthenticationIntegration preparedSecond =
                runtime.prepareAuthenticationIntegration(second);
        runtime.activatePreparedAuthenticationIntegration(preparedSecond);

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("secret".getBytes(StandardCharsets.UTF_8))
                .remoteHost("127.0.0.1")
                .clientType("mysql")
                .build();
        AuthenticationOutcome outcome = runtime.authenticate(Arrays.asList(first, second), request);

        Assertions.assertTrue(outcome.isSuccess());
        Assertions.assertEquals("second", outcome.getIntegration().getName());
    }

    @Test
    void testAuthenticateStopsOnBadCredential() throws Exception {
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new ChainTestPluginFactory());
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);

        AuthenticationIntegrationMeta first = meta("first", "chain_test",
                map("result", "BAD_CREDENTIAL"));
        AuthenticationIntegrationMeta second = meta("second", "chain_test",
                map("result", "SUCCESS"));

        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(first));
        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(second));

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("secret".getBytes(StandardCharsets.UTF_8))
                .remoteHost("127.0.0.1")
                .clientType("mysql")
                .build();
        AuthenticationOutcome outcome = runtime.authenticate(Arrays.asList(first, second), request);

        Assertions.assertTrue(outcome.isFailure());
        Assertions.assertEquals("first", outcome.getIntegration().getName());
        Assertions.assertEquals(AuthenticationFailureType.BAD_CREDENTIAL,
                outcome.getAuthResult().getException().getFailureType());
    }

    @Test
    void testAuthenticatePreservesPluginGrantedRolesOnSuccess() throws Exception {
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new ChainTestPluginFactory());
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);

        AuthenticationIntegrationMeta integration = meta("mapped", "chain_test",
                map("result", "SUCCESS", "granted_role", "plugin_reader"));
        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(integration));

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("secret".getBytes(StandardCharsets.UTF_8))
                .remoteHost("127.0.0.1")
                .clientType("mysql")
                .build();
        AuthenticationOutcome outcome = runtime.authenticate(Collections.singletonList(integration), request);

        Assertions.assertTrue(outcome.isSuccess());
        Assertions.assertEquals(Collections.singleton("plugin_reader"), outcome.getGrantedRoles());
    }

    @Test
    void testAuthenticateIgnoresInlineRoleMappingProperties() throws Exception {
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new ChainTestPluginFactory());
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);

        AuthenticationIntegrationMeta integration = meta("inline", "chain_test", map(
                "result", "SUCCESS",
                "granted_role", "plugin_reader",
                "role_mapping.rule.oncall.condition", "has_group(\"oncall\") && has_scope(\"logs:write\")",
                "role_mapping.rule.oncall.roles", "mapped_reader"));
        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(integration));

        AuthenticationOutcome outcome = runtime.authenticate(Collections.singletonList(integration), request());

        Assertions.assertTrue(outcome.isSuccess());
        Assertions.assertEquals(Collections.singleton("plugin_reader"), outcome.getGrantedRoles());
    }

    @Test
    void testAuthenticateMergesMetadataBackedRoleMappingRoles() throws Exception {
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new ChainTestPluginFactory());
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);

        AuthenticationIntegrationMeta integration = meta("corp", "chain_test",
                map("result", "SUCCESS", "granted_role", "plugin_reader"));
        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(integration));

        Env env = Mockito.mock(Env.class);
        RoleMappingMgr roleMappingMgr = Mockito.mock(RoleMappingMgr.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getRoleMappingMgr()).thenReturn(roleMappingMgr);
        Mockito.when(roleMappingMgr.getRoleMappingByIntegration("corp")).thenReturn(new RoleMappingMeta(
                "corp_mapping",
                "corp",
                Collections.singletonList(new RoleMappingMeta.RuleMeta(
                        "has_group(\"oncall\") && has_scope(\"logs:write\")",
                        set("mapped_reader"))),
                null,
                CREATE_USER,
                1L,
                CREATE_USER,
                1L));

        AuthenticationOutcome outcome = runtime.authenticate(Collections.singletonList(integration), request());

        Assertions.assertTrue(outcome.isSuccess());
        Assertions.assertEquals(set("plugin_reader", "mapped_reader"), outcome.getGrantedRoles());
        Assertions.assertEquals(set("plugin_reader", "mapped_reader"), outcome.getAuthResult().getGrantedRoles());
    }

    @Test
    void testAuthenticateFailsWhenRoleMappingMetadataIsInvalid() throws Exception {
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new ChainTestPluginFactory());
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);
        AuthenticationIntegrationMeta integration = meta("corp", "chain_test", map("result", "SUCCESS"));
        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(integration));

        Env env = Mockito.mock(Env.class);
        RoleMappingMgr roleMappingMgr = Mockito.mock(RoleMappingMgr.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getRoleMappingMgr()).thenReturn(roleMappingMgr);
        Mockito.when(roleMappingMgr.getRoleMappingByIntegration("corp")).thenReturn(new RoleMappingMeta(
                "broken_mapping",
                "corp",
                Collections.singletonList(new RoleMappingMeta.RuleMeta("unknown_helper()", set("analyst"))),
                null,
                CREATE_USER,
                1L,
                CREATE_USER,
                1L));

        AuthenticationOutcome outcome = runtime.authenticate(Collections.singletonList(integration), request());

        Assertions.assertTrue(outcome.isFailure());
        Assertions.assertEquals(AuthenticationFailureType.MISCONFIGURED,
                outcome.getAuthResult().getException().getFailureType());
    }

    @Test
    void testRebuildKeepsPluginInstancesLazy() throws Exception {
        List<String> initializedMarkers = new ArrayList<>();
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new RecordingPluginFactory(initializedMarkers));
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);
        AuthenticationIntegrationMeta meta = meta("broken", "recording", map("marker", "rebuild"));

        Map<String, AuthenticationIntegrationMeta> snapshot = new LinkedHashMap<>();
        snapshot.put(meta.getName(), meta);
        runtime.rebuildAuthenticationIntegrations(snapshot);

        Assertions.assertTrue(initializedMarkers.isEmpty());
        Assertions.assertNull(runtime.getRuntimeState("broken"));
        Assertions.assertNull(runtime.getBrokenReason("broken"));
    }

    @Test
    void testAuthenticateRefreshesDirtyIntegrationUsingLatestMetadata() throws Exception {
        List<String> initializedMarkers = new ArrayList<>();
        AuthenticationPluginManager pluginManager = new AuthenticationPluginManager();
        pluginManager.registerFactory(new RecordingPluginFactory(initializedMarkers));
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);

        AuthenticationIntegrationMeta oldMeta = meta("corp", "recording", map("marker", "old"));
        AuthenticationIntegrationMeta newMeta = meta("corp", "recording", map("marker", "new"));
        runtime.activatePreparedAuthenticationIntegration(runtime.prepareAuthenticationIntegration(oldMeta));
        runtime.markAuthenticationIntegrationDirty("corp");

        Env env = Mockito.mock(Env.class);
        AuthenticationIntegrationMgr mgr = Mockito.mock(AuthenticationIntegrationMgr.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuthenticationIntegrationMgr()).thenReturn(mgr);
        Mockito.when(mgr.getAuthenticationIntegration("corp")).thenReturn(newMeta);

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("secret".getBytes(StandardCharsets.UTF_8))
                .remoteHost("127.0.0.1")
                .clientType("mysql")
                .build();

        AuthenticationOutcome outcome = runtime.authenticate(Arrays.asList(oldMeta), request);

        Assertions.assertTrue(outcome.isSuccess());
        Assertions.assertEquals(Arrays.asList("old", "new"), initializedMarkers);
        Assertions.assertEquals(AuthenticationIntegrationRuntime.RuntimeState.AVAILABLE,
                runtime.getRuntimeState("corp"));
    }

    @Test
    void testPrepareAuthenticationIntegrationLoadsPluginFactoriesFromMultipleRoots() throws Exception {
        org.apache.doris.common.Config.authentication_plugins_dir = "/tmp/auth-root-a, /tmp/auth-root-b";
        AuthenticationPluginManager pluginManager = Mockito.mock(AuthenticationPluginManager.class);
        AuthenticationPlugin plugin = Mockito.mock(AuthenticationPlugin.class);
        Mockito.when(pluginManager.hasFactory("multi-root")).thenReturn(false, true);
        Mockito.when(pluginManager.createPlugin(Mockito.any())).thenReturn(plugin);

        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(pluginManager);
        runtime.prepareAuthenticationIntegration(meta("corp", "multi-root", Collections.emptyMap()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Path>> pluginRootsCaptor = ArgumentCaptor.forClass((Class) List.class);
        Mockito.verify(pluginManager).loadAll(pluginRootsCaptor.capture(), Mockito.any());
        Assertions.assertEquals(
                Arrays.asList(Paths.get("/tmp/auth-root-a"), Paths.get("/tmp/auth-root-b")),
                pluginRootsCaptor.getValue());
    }

    private static AuthenticationIntegrationMeta meta(String name, String type, Map<String, String> properties)
            throws Exception {
        Map<String, String> createProperties = new LinkedHashMap<>();
        createProperties.put("type", type);
        createProperties.putAll(properties);
        return AuthenticationIntegrationMeta.fromCreateSql(name, createProperties, null, CREATE_USER);
    }

    private static Map<String, String> map(String... kvs) {
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            result.put(kvs[i], kvs[i + 1]);
        }
        return result;
    }

    private static Set<String> set(String... roles) {
        return new LinkedHashSet<>(Arrays.asList(roles));
    }

    private static AuthenticationRequest request() {
        return AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("secret".getBytes(StandardCharsets.UTF_8))
                .remoteHost("127.0.0.1")
                .clientType("mysql")
                .build();
    }

    private static class ChainTestPluginFactory implements AuthenticationPluginFactory {
        @Override
        public String name() {
            return "chain_test";
        }

        @Override
        public AuthenticationPlugin create() {
            return new ChainTestPlugin();
        }
    }

    private static class ChainTestPlugin implements AuthenticationPlugin {
        @Override
        public String name() {
            return "chain_test";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return true;
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationIntegration integration)
                throws AuthenticationException {
            String result = integration.getProperty("result", "SUCCESS");
            switch (result) {
                case "USER_NOT_FOUND":
                    return AuthenticationResult.failure(
                            AuthenticationFailureType.USER_NOT_FOUND, "User not found");
                case "BAD_CREDENTIAL":
                    return AuthenticationResult.failure(
                            AuthenticationFailureType.BAD_CREDENTIAL, "Bad credential");
                default:
                    BasicPrincipal principal = BasicPrincipal.builder()
                            .name(request.getUsername())
                            .authenticator(integration.getName())
                            .externalGroups(Collections.singleton("oncall"))
                            .multiValueAttributes(Collections.singletonMap(
                                    "scope", Collections.singleton("logs:write")))
                            .build();
                    String grantedRole = integration.getProperty("granted_role", "");
                    if (grantedRole.isEmpty()) {
                        return AuthenticationResult.success(principal);
                    }
                    return AuthenticationResult.success(principal, Collections.singleton(grantedRole));
            }
        }
    }

    private static class RecordingPluginFactory implements AuthenticationPluginFactory {
        private final List<String> initializedMarkers;

        private RecordingPluginFactory(List<String> initializedMarkers) {
            this.initializedMarkers = initializedMarkers;
        }

        @Override
        public String name() {
            return "recording";
        }

        @Override
        public AuthenticationPlugin create() {
            return new RecordingPlugin(initializedMarkers);
        }
    }

    private static class RecordingPlugin implements AuthenticationPlugin {
        private final List<String> initializedMarkers;
        private String marker;

        private RecordingPlugin(List<String> initializedMarkers) {
            this.initializedMarkers = initializedMarkers;
        }

        @Override
        public void initialize(AuthenticationIntegration integration) {
            marker = integration.getProperty("marker", "missing");
            initializedMarkers.add(marker);
        }

        @Override
        public String name() {
            return "recording";
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return true;
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request, AuthenticationIntegration integration)
                throws AuthenticationException {
            return AuthenticationResult.success(BasicPrincipal.builder()
                    .name(request.getUsername())
                    .authenticator(marker)
                    .build());
        }
    }
}
