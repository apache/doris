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

package org.apache.doris.authentication.plugin.ldap;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Comprehensive unit tests for LdapAuthenticationPlugin.
 *
 * <p>Test coverage:
 * <ul>
 *   <li>Plugin metadata (name, description, capabilities)</li>
 *   <li>Configuration validation</li>
 *   <li>Authentication request validation</li>
 *   <li>Lifecycle management (initialize, reload, close)</li>
 *   <li>Multi-instance support</li>
 *   <li>Error handling</li>
 * </ul>
 *
 * <p>Note: These are unit tests that don't require a real LDAP server.
 * Integration tests with a real LDAP server are in LdapAuthenticationPluginIntegrationTest.
 */
@DisplayName("LDAP Authentication Plugin Tests")
class LdapAuthenticationPluginTest {

    private LdapAuthenticationPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = new TestableLdapAuthenticationPlugin();
    }

    @AfterEach
    void tearDown() {
        if (plugin != null) {
            plugin.close();
        }
    }

    // ==================== Plugin Metadata Tests ====================

    @Nested
    @DisplayName("Plugin Metadata")
    class PluginMetadataTests {

        @Test
        @DisplayName("Should return correct plugin name")
        void testPluginName() {
            Assertions.assertEquals("ldap", plugin.name());
            Assertions.assertEquals(LdapAuthenticationPlugin.PLUGIN_NAME, plugin.name());
        }

        @Test
        @DisplayName("Should have non-empty description")
        void testPluginDescription() {
            Assertions.assertNotNull(plugin.description());
            Assertions.assertFalse(plugin.description().isEmpty());
            Assertions.assertTrue(plugin.description().toLowerCase().contains("ldap"));
        }

        @Test
        @DisplayName("Should require clear password")
        void testRequiresClearPassword() {
            Assertions.assertTrue(plugin.requiresClearPassword(),
                    "LDAP authentication requires clear text password");
        }

        @Test
        @DisplayName("Should not support multi-step authentication")
        void testDoesNotSupportMultiStep() {
            Assertions.assertFalse(plugin.supportsMultiStep(),
                    "LDAP is single-step authentication");
        }
    }

    // ==================== Credential Type Support Tests ====================

    @Nested
    @DisplayName("Credential Type Support")
    class CredentialTypeSupportTests {

        @Test
        @DisplayName("Should support CLEAR_TEXT_PASSWORD")
        void testSupportsClearTextPassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("test")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("password".getBytes(StandardCharsets.UTF_8))
                    .build();

            Assertions.assertTrue(plugin.supports(request));
        }

        @Test
        @DisplayName("Should not support MYSQL_NATIVE_PASSWORD")
        void testDoesNotSupportMysqlNativePassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("test")
                    .credentialType(CredentialType.MYSQL_NATIVE_PASSWORD)
                    .credential(new byte[20])
                    .build();

            Assertions.assertFalse(plugin.supports(request));
        }

        @Test
        @DisplayName("Should not support KERBEROS_TOKEN")
        void testDoesNotSupportKerberosToken() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("test")
                    .credentialType(CredentialType.KERBEROS_TOKEN)
                    .credential(new byte[128])
                    .build();

            Assertions.assertFalse(plugin.supports(request));
        }

        @Test
        @DisplayName("Should not support OAUTH_TOKEN")
        void testDoesNotSupportOauthToken() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("test")
                    .credentialType(CredentialType.OAUTH_TOKEN)
                    .credential("token".getBytes(StandardCharsets.UTF_8))
                    .build();

            Assertions.assertFalse(plugin.supports(request));
        }

        @Test
        @DisplayName("Should not support X509_CERTIFICATE")
        void testDoesNotSupportX509Certificate() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("test")
                    .credentialType(CredentialType.X509_CERTIFICATE)
                    .credential(new byte[256])
                    .build();

            Assertions.assertFalse(plugin.supports(request));
        }
    }

    // ==================== Configuration Validation Tests ====================

    @Nested
    @DisplayName("Configuration Validation")
    class ConfigurationValidationTests {

        @Test
        @DisplayName("Should reject configuration without server")
        void testValidateRejectsEmptyServer() {
            Map<String, String> config = new HashMap<>();
            config.put("base_dn", "dc=example,dc=com");

            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test_ldap")
                    .type("ldap")
                    .properties(config)
                    .build();

            AuthenticationException exception = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> plugin.validate(integration)
            );

            Assertions.assertTrue(exception.getMessage().toLowerCase().contains("server"));
        }

        @Test
        @DisplayName("Should reject configuration without base_dn")
        void testValidateRejectsEmptyBaseDn() {
            Map<String, String> config = new HashMap<>();
            config.put("server", "ldap://localhost:389");

            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test_ldap")
                    .type("ldap")
                    .properties(config)
                    .build();

            AuthenticationException exception = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> plugin.validate(integration)
            );

            Assertions.assertTrue(exception.getMessage().toLowerCase().contains("base_dn"));
        }

        @Test
        @DisplayName("Should accept minimal valid configuration")
        void testValidateAcceptsMinimalConfig() {
            Map<String, String> config = new HashMap<>();
            config.put("server", "ldap://localhost:389");
            config.put("base_dn", "dc=example,dc=com");

            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test_ldap")
                    .type("ldap")
                    .properties(config)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("Should accept full configuration")
        void testValidateAcceptsFullConfig() {
            Map<String, String> config = createFullLdapConfig();

            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test_ldap")
                    .type("ldap")
                    .properties(config)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("Should warn when bind_dn set without bind_password")
        void testValidateWarnsOnMissingBindPassword() {
            Map<String, String> config = new HashMap<>();
            config.put("server", "ldap://localhost:389");
            config.put("base_dn", "dc=example,dc=com");
            config.put("bind_dn", "cn=admin,dc=example,dc=com");
            // bind_password is missing

            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test_ldap")
                    .type("ldap")
                    .properties(config)
                    .build();

            // Should not throw, but will log warning
            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }
    }

    // ==================== Authentication Request Validation Tests ====================

    @Nested
    @DisplayName("Authentication Request Validation")
    class AuthenticationRequestValidationTests {

        private AuthenticationIntegration integration;

        @BeforeEach
        void setUp() {
            integration = createTestIntegration("test_ldap");
        }

        @Test
        @DisplayName("Should reject null username")
        void testAuthenticateRejectsNullUsername() {
            // AuthenticationRequest builder throws NPE on null username
            Assertions.assertThrows(NullPointerException.class, () ->
                    AuthenticationRequest.builder()
                    .username(null)
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("password".getBytes(StandardCharsets.UTF_8))
                    .build()
            );
        }

        @Test
        @DisplayName("Should reject empty username")
        void testAuthenticateRejectsEmptyUsername() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("password".getBytes(StandardCharsets.UTF_8))
                    .build();

            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );

            Assertions.assertTrue(result.isFailure());
            Assertions.assertTrue(result.getException().getMessage().toLowerCase().contains("username"));
        }

        @Test
        @DisplayName("Should reject null password")
        void testAuthenticateRejectsNullPassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential(null)
                    .build();

            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );

            Assertions.assertTrue(result.isFailure());
            Assertions.assertTrue(result.getException().getMessage().toLowerCase().contains("password"));
        }

        @Test
        @DisplayName("Should reject empty password")
        void testAuthenticateRejectsEmptyPassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential(new byte[0])
                    .build();

            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );

            Assertions.assertTrue(result.isFailure());
            Assertions.assertTrue(result.getException().getMessage().toLowerCase().contains("password"));
        }

        @Test
        @DisplayName("Should handle special characters in username")
        void testAuthenticateHandlesSpecialCharactersInUsername() {
            String[] specialUsernames = {
                    "user@example.com",
                    "user.name",
                    "user-name",
                    "user_name",
                    "user123"
            };

            for (String username : specialUsernames) {
                AuthenticationRequest request = AuthenticationRequest.builder()
                        .username(username)
                        .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                        .credential("password".getBytes(StandardCharsets.UTF_8))
                        .build();

                AuthenticationResult result = Assertions.assertDoesNotThrow(
                        () -> plugin.authenticate(request, integration),
                        "Should handle username: " + username
                );

                // Should succeed with stub client
                Assertions.assertTrue(result.isSuccess(), "Should succeed for username: " + username);
            }
        }
    }

    // ==================== Lifecycle Management Tests ====================

    @Nested
    @DisplayName("Lifecycle Management")
    class LifecycleManagementTests {

        @Test
        @DisplayName("Should initialize successfully with valid config")
        void testInitializeWithValidConfig() {
            AuthenticationIntegration integration = createTestIntegration("test_init");

            // Note: Will fail to connect to LDAP server, but should not throw during init
            // Real validation happens during authenticate()
            Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));
        }

        @Test
        @DisplayName("Should handle multiple initialize calls")
        void testMultipleInitializeCalls() {
            AuthenticationIntegration integration = createTestIntegration("test_multi_init");

            Assertions.assertDoesNotThrow(() -> {
                plugin.initialize(integration);
                plugin.initialize(integration);  // Should replace existing
            });
        }

        @Test
        @DisplayName("Should reload configuration")
        void testReloadConfiguration() {
            AuthenticationIntegration oldIntegration = createTestIntegration("test_reload");
            Assertions.assertDoesNotThrow(() -> plugin.initialize(oldIntegration));

            // Create new config with different server
            Map<String, String> newConfig = new HashMap<>();
            newConfig.put("server", "ldap://new-server:389");
            newConfig.put("base_dn", "dc=new,dc=com");

            AuthenticationIntegration newIntegration = AuthenticationIntegration.builder()
                    .name("test_reload")
                    .type("ldap")
                    .properties(newConfig)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.reload(newIntegration));
        }

        @Test
        @DisplayName("Should close cleanly")
        void testClose() {
            AuthenticationIntegration integration = createTestIntegration("test_close");
            Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));

            Assertions.assertDoesNotThrow(() -> plugin.close());
        }

        @Test
        @DisplayName("Should handle multiple close calls")
        void testMultipleCloseCalls() {
            Assertions.assertDoesNotThrow(() -> {
                plugin.close();
                plugin.close();  // Should be idempotent
            });
        }
    }

    // ==================== Multi-Instance Support Tests ====================

    @Nested
    @DisplayName("Multi-Instance Support")
    class MultiInstanceSupportTests {

        @Test
        @DisplayName("Should support multiple integrations")
        void testMultipleIntegrations() {
            AuthenticationIntegration corp = createTestIntegration("corp_ldap");
            AuthenticationIntegration partner = createTestIntegration("partner_ldap");

            Assertions.assertDoesNotThrow(() -> {
                plugin.initialize(corp);
                plugin.initialize(partner);
            });
        }

        @Test
        @DisplayName("Should isolate integrations")
        void testIntegrationIsolation() {
            AuthenticationIntegration integration1 = createTestIntegration("ldap1");
            AuthenticationIntegration integration2 = createTestIntegration("ldap2");

            Assertions.assertDoesNotThrow(() -> {
                plugin.initialize(integration1);
                plugin.initialize(integration2);

                // Reload one should not affect the other
                plugin.reload(integration1);
            });
        }

        @Test
        @DisplayName("Should handle concurrent access")
        void testConcurrentAccess() {
            AuthenticationIntegration integration = createTestIntegration("concurrent_test");
            Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));

            // Simulate concurrent authentication attempts
            Thread[] threads = new Thread[10];
            for (int i = 0; i < threads.length; i++) {
                final int index = i;
                threads[i] = new Thread(() -> {
                    AuthenticationRequest request = AuthenticationRequest.builder()
                            .username("user" + index)
                            .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                            .credential("password".getBytes(StandardCharsets.UTF_8))
                            .build();

                    Assertions.assertDoesNotThrow(() -> plugin.authenticate(request, integration));
                });
            }

            // Start all threads
            for (Thread thread : threads) {
                thread.start();
            }

            // Wait for all threads to complete
            for (Thread thread : threads) {
                Assertions.assertDoesNotThrow(() -> thread.join(5000));
            }
        }
    }

    // ==================== Edge Cases Tests ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle very long username")
        void testVeryLongUsername() {
            // Java 8 compatible: use StringBuilder instead of String.repeat()
            StringBuilder sb = new StringBuilder(1000);
            for (int i = 0; i < 1000; i++) {
                sb.append('a');
            }
            String longUsername = sb.toString();

            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username(longUsername)
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("password".getBytes(StandardCharsets.UTF_8))
                    .build();

            AuthenticationIntegration integration = createTestIntegration("edge_test");

            Assertions.assertDoesNotThrow(() -> plugin.authenticate(request, integration));
        }

        @Test
        @DisplayName("Should handle very long password")
        void testVeryLongPassword() {
            // Java 8 compatible: use StringBuilder instead of String.repeat()
            StringBuilder sb = new StringBuilder(10000);
            for (int i = 0; i < 10000; i++) {
                sb.append('p');
            }
            String longPassword = sb.toString();

            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential(longPassword.getBytes(StandardCharsets.UTF_8))
                    .build();

            AuthenticationIntegration integration = createTestIntegration("edge_test");

            Assertions.assertDoesNotThrow(() -> plugin.authenticate(request, integration));
        }

        @Test
        @DisplayName("Should handle unicode characters in credentials")
        void testUnicodeCharacters() {
            String[] unicodeUsernames = {
                    "用户",           // Chinese
                    "ユーザー",       // Japanese
                    "사용자",         // Korean
                    "utilisateur",   // French
                    "пользователь"   // Russian
            };

            for (String username : unicodeUsernames) {
                AuthenticationRequest request = AuthenticationRequest.builder()
                        .username(username)
                        .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                        .credential("密码".getBytes(StandardCharsets.UTF_8))
                        .build();

                AuthenticationIntegration integration = createTestIntegration("unicode_test");

                Assertions.assertDoesNotThrow(() -> plugin.authenticate(request, integration),
                        "Should handle unicode username: " + username);
            }
        }

        @Test
        @DisplayName("Should handle SQL injection attempts in username")
        void testSqlInjectionInUsername() {
            String[] sqlInjectionAttempts = {
                    "admin' OR '1'='1",
                    "admin'; DROP TABLE users--",
                    "admin' UNION SELECT * FROM passwords--"
            };

            for (String maliciousUsername : sqlInjectionAttempts) {
                AuthenticationRequest request = AuthenticationRequest.builder()
                        .username(maliciousUsername)
                        .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                        .credential("password".getBytes(StandardCharsets.UTF_8))
                        .build();

                AuthenticationIntegration integration = createTestIntegration("sql_injection_test");

                Assertions.assertDoesNotThrow(() -> plugin.authenticate(request, integration),
                        "Should safely handle SQL injection attempt: " + maliciousUsername);
            }
        }

        @Test
        @DisplayName("Should handle LDAP injection attempts")
        void testLdapInjectionInUsername() {
            String[] ldapInjectionAttempts = {
                    "admin)(uid=*",
                    "*)(uid=*)(|(uid=*",
                    "admin)(&(uid=*"
            };

            for (String maliciousUsername : ldapInjectionAttempts) {
                AuthenticationRequest request = AuthenticationRequest.builder()
                        .username(maliciousUsername)
                        .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                        .credential("password".getBytes(StandardCharsets.UTF_8))
                        .build();

                AuthenticationIntegration integration = createTestIntegration("ldap_injection_test");

                Assertions.assertDoesNotThrow(() -> plugin.authenticate(request, integration),
                        "Should safely handle LDAP injection attempt: " + maliciousUsername);
            }
        }
    }

    // ==================== Helper Methods ====================

    private AuthenticationIntegration createTestIntegration(String name) {
        Map<String, String> config = new HashMap<>();
        config.put("server", "ldap://localhost:389");
        config.put("base_dn", "dc=example,dc=com");
        config.put("user_base_dn", "ou=users");
        config.put("user_filter", "(uid={login})");
        config.put("group_base_dn", "ou=groups");

        return AuthenticationIntegration.builder()
                .name(name)
                .type("ldap")
                .properties(config)
                .build();
    }

    private Map<String, String> createFullLdapConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("server", "ldap://ldap.example.com:389");
        config.put("base_dn", "dc=example,dc=com");
        config.put("user_base_dn", "ou=users");
        config.put("user_filter", "(uid={login})");
        config.put("group_base_dn", "ou=groups");
        config.put("group_filter", "(memberUid={login})");
        config.put("bind_dn", "cn=admin,dc=example,dc=com");
        config.put("bind_password", "admin_password");
        return config;
    }

    private static class StubLdapClient extends LdapClient {
        StubLdapClient(Map<String, String> config) {
            super(config);
        }

        @Override
        public String getUserDn(String username) {
            return "uid=" + username + ",ou=users,dc=example,dc=com";
        }

        @Override
        public boolean checkPassword(String username, String password) {
            return true;
        }

        @Override
        public java.util.List<String> getGroups(String username) {
            return java.util.Collections.emptyList();
        }
    }

    private static class TestableLdapAuthenticationPlugin extends LdapAuthenticationPlugin {
        @Override
        protected LdapClient createClient(Map<String, String> config) {
            return new StubLdapClient(config);
        }
    }
}
