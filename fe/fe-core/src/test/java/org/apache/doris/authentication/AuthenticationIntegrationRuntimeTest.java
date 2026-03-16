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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

class AuthenticationIntegrationRuntimeTest {
    private static final String CREATE_USER = "creator";

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
    void testRebuildMarksBrokenWhenFactoryMissing() throws Exception {
        AuthenticationIntegrationRuntime runtime = new AuthenticationIntegrationRuntime(new AuthenticationPluginManager());
        AuthenticationIntegrationMeta broken = meta("broken", "missing_type", map("result", "SUCCESS"));

        Map<String, AuthenticationIntegrationMeta> snapshot = new LinkedHashMap<>();
        snapshot.put(broken.getName(), broken);
        runtime.rebuildAuthenticationIntegrations(snapshot);

        Assertions.assertEquals(AuthenticationIntegrationRuntime.RuntimeState.BROKEN,
                runtime.getRuntimeState("broken"));
        Assertions.assertTrue(runtime.getBrokenReason("broken").contains("missing_type"));
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
                    return AuthenticationResult.success(BasicPrincipal.builder()
                            .name(request.getUsername())
                            .authenticator(integration.getName())
                            .build());
            }
        }
    }
}
