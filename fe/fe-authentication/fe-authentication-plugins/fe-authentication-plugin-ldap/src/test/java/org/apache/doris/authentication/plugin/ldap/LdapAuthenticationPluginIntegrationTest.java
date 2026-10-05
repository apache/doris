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

import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Principal;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldif.LDIFException;
import com.unboundid.ldif.LDIFReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Integration tests for LdapAuthenticationPlugin using embedded LDAP server.
 *
 * <p>This test uses UnboundID LDAP SDK's in-memory directory server
 * to test real LDAP authentication without requiring an external LDAP server.
 *
 * <p>Test data structure:
 * <pre>
 * dc=example,dc=com
 * ├── ou=users
 * │   ├── uid=alice (password: alice123, groups: developers, employees)
 * │   ├── uid=bob (password: bob123, groups: analysts, employees)
 * │   └── uid=charlie (password: charlie123, groups: employees)
 * └── ou=groups
 *     ├── cn=developers (members: alice)
 *     ├── cn=analysts (members: bob)
 *     └── cn=employees (members: alice, bob, charlie)
 * </pre>
 */
@DisplayName("LDAP Plugin Integration Tests (Embedded LDAP Server)")
class LdapAuthenticationPluginIntegrationTest {

    private static InMemoryDirectoryServer ldapServer;
    private static int ldapPort;

    private LdapAuthenticationPlugin plugin;
    private AuthenticationIntegration integration;

    @BeforeAll
    static void startLdapServer() throws Exception {
        // Configure in-memory LDAP server
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.addAdditionalBindCredentials("cn=admin,dc=example,dc=com", "admin123");

        // Configure listener (use random available port)
        InMemoryListenerConfig listenerConfig = InMemoryListenerConfig.createLDAPConfig(
                "default"
        // null = use random port
        );
        config.setListenerConfigs(listenerConfig);

        // Create and start server
        ldapServer = new InMemoryDirectoryServer(config);

        // Load test data from LDIF file
        try (InputStream ldifStream = LdapAuthenticationPluginIntegrationTest.class
                .getResourceAsStream("/test-ldap-data.ldif")) {
            if (ldifStream != null) {
                ldapServer.importFromLDIF(true, new LDIFReader(ldifStream));
            } else {
                // Fallback: add test data programmatically
                addTestDataProgrammatically();
            }
        }

        ldapServer.startListening();
        ldapPort = ldapServer.getListenPort();

        System.out.println("✅ Embedded LDAP server started on port: " + ldapPort);
    }

    @AfterAll
    static void stopLdapServer() {
        if (ldapServer != null) {
            ldapServer.shutDown(true);
            System.out.println("✅ Embedded LDAP server stopped");
        }
    }

    @BeforeEach
    void setUp() {
        plugin = new LdapAuthenticationPlugin();

        // Create integration configuration pointing to embedded LDAP
        Map<String, String> config = new HashMap<>();
        config.put("server", "ldap://localhost:" + ldapPort);
        config.put("base_dn", "dc=example,dc=com");
        // Use RELATIVE paths (Spring LDAP will append base_dn automatically)
        config.put("user_base_dn", "ou=users");
        config.put("user_filter", "(uid={login})");
        config.put("group_base_dn", "ou=groups");
        config.put("bind_dn", "cn=admin,dc=example,dc=com");
        config.put("bind_password", "admin123");

        integration = AuthenticationIntegration.builder()
                .name("test_ldap")
                .type("ldap")
                .properties(config)
                .build();

        // Initialize plugin
        Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));
    }

    // ==================== Successful Authentication Tests ====================

    @Test
    @DisplayName("Should authenticate alice with correct password")
    void testAuthenticateAliceSuccess() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("alice123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess(), "Authentication should succeed");
        Assertions.assertNotNull(result.getPrincipal());

        Principal principal = result.getPrincipal();
        Assertions.assertEquals("alice", principal.getName());
        Assertions.assertEquals("ldap", principal.getAuthenticator());
    }

    @Test
    @DisplayName("Should authenticate bob with correct password")
    void testAuthenticateBobSuccess() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("bob")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("bob123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertEquals("bob", result.getPrincipal().getName());
    }

    @Test
    @DisplayName("Should authenticate charlie with correct password")
    void testAuthenticateCharlieSuccess() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("charlie")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("charlie123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertEquals("charlie", result.getPrincipal().getName());
    }

    // ==================== Failed Authentication Tests ====================

    @Test
    @DisplayName("Should reject alice with wrong password")
    void testAuthenticateAliceWrongPassword() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("wrongpassword".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isFailure(), "Authentication should fail with wrong password");
        Assertions.assertNotNull(result.getException());
    }

    @Test
    @DisplayName("Should reject non-existent user")
    void testAuthenticateNonExistentUser() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("nonexistent")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isFailure(), "Authentication should fail for non-existent user");
        Assertions.assertTrue(result.getException().getMessage().contains("not found"));
    }

    @Test
    @DisplayName("Should reject empty password")
    void testAuthenticateEmptyPassword() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isFailure());
    }

    // ==================== Group Extraction Tests ====================

    @Test
    @DisplayName("Should extract alice's groups correctly")
    void testExtractAliceGroups() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("alice123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess());
        Principal principal = result.getPrincipal();
        Set<String> groups = principal.getExternalGroups();

        Assertions.assertNotNull(groups, "Groups should not be null");
        Assertions.assertFalse(groups.isEmpty(), "Alice should have groups");
        Assertions.assertTrue(groups.contains("developers"), "Alice should be in developers group");
        Assertions.assertTrue(groups.contains("employees"), "Alice should be in employees group");
    }

    @Test
    @DisplayName("Should extract bob's groups correctly")
    void testExtractBobGroups() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("bob")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("bob123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess());
        Set<String> groups = result.getPrincipal().getExternalGroups();

        Assertions.assertTrue(groups.contains("analysts"), "Bob should be in analysts group");
        Assertions.assertTrue(groups.contains("employees"), "Bob should be in employees group");
        Assertions.assertFalse(groups.contains("developers"), "Bob should not be in developers group");
    }

    @Test
    @DisplayName("Should extract charlie's groups correctly")
    void testExtractCharlieGroups() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("charlie")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("charlie123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess());
        Set<String> groups = result.getPrincipal().getExternalGroups();

        Assertions.assertTrue(groups.contains("employees"), "Charlie should be in employees group");
        Assertions.assertFalse(groups.contains("developers"), "Charlie should not be in developers group");
        Assertions.assertFalse(groups.contains("analysts"), "Charlie should not be in analysts group");
    }

    // ==================== User DN Extraction Tests ====================

    @Test
    @DisplayName("Should extract alice's DN correctly")
    void testExtractAliceDn() throws Exception {
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("alice123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = plugin.authenticate(request, integration);

        Assertions.assertTrue(result.isSuccess());
        Principal principal = result.getPrincipal();

        Assertions.assertTrue(principal.getExternalPrincipal().isPresent());
        String dn = principal.getExternalPrincipal().get();
        Assertions.assertTrue(dn.toLowerCase().contains("uid=alice"),
                "DN should contain uid=alice, got: " + dn);
    }

    // ==================== Configuration Tests ====================

    @Test
    @DisplayName("Should work without bind credentials (anonymous bind)")
    void testAnonymousBind() throws Exception {
        // Create config without bind credentials
        Map<String, String> config = new HashMap<>();
        config.put("server", "ldap://localhost:" + ldapPort);
        config.put("base_dn", "dc=example,dc=com");
        config.put("user_base_dn", "ou=users");
        config.put("user_filter", "(uid={login})");

        AuthenticationIntegration anonIntegration = AuthenticationIntegration.builder()
                .name("anon_ldap")
                .type("ldap")
                .properties(config)
                .build();

        LdapAuthenticationPlugin anonPlugin = new LdapAuthenticationPlugin();
        Assertions.assertDoesNotThrow(() -> anonPlugin.initialize(anonIntegration));

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("alice123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = anonPlugin.authenticate(request, anonIntegration);

        // Should succeed for password validation, but group extraction might fail without bind
        Assertions.assertTrue(result.isSuccess());
    }

    @Test
    @DisplayName("Should handle custom user filter")
    void testCustomUserFilter() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("server", "ldap://localhost:" + ldapPort);
        config.put("base_dn", "dc=example,dc=com");
        config.put("user_base_dn", "ou=users");
        config.put("user_filter", "(&(uid={login})(objectClass=inetOrgPerson))");
        config.put("group_base_dn", "ou=groups");
        config.put("bind_dn", "cn=admin,dc=example,dc=com");
        config.put("bind_password", "admin123");

        AuthenticationIntegration customIntegration = AuthenticationIntegration.builder()
                .name("custom_ldap")
                .type("ldap")
                .properties(config)
                .build();

        LdapAuthenticationPlugin customPlugin = new LdapAuthenticationPlugin();
        customPlugin.initialize(customIntegration);

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("alice123".getBytes(StandardCharsets.UTF_8))
                .build();

        AuthenticationResult result = customPlugin.authenticate(request, customIntegration);

        Assertions.assertTrue(result.isSuccess());
    }

    // ==================== Concurrent Authentication Tests ====================

    @Test
    @DisplayName("Should handle concurrent authentication requests")
    void testConcurrentAuthentication() throws Exception {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        boolean[] results = new boolean[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            final String username = (i % 3 == 0) ? "alice" : (i % 3 == 1) ? "bob" : "charlie";
            final String password = username + "123";

            threads[i] = new Thread(() -> {
                try {
                    AuthenticationRequest request = AuthenticationRequest.builder()
                            .username(username)
                            .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                            .credential(password.getBytes(StandardCharsets.UTF_8))
                            .build();

                    AuthenticationResult result = plugin.authenticate(request, integration);
                    results[index] = result.isSuccess();
                } catch (Exception e) {
                    results[index] = false;
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000);
        }

        // All authentications should succeed
        for (int i = 0; i < threadCount; i++) {
            Assertions.assertTrue(results[i], "Thread " + i + " authentication should succeed");
        }
    }

    // ==================== Helper Methods ====================

    /**
     * Fallback method to add test data programmatically if LDIF file is not found.
     */
    private static void addTestDataProgrammatically() throws LDAPException, LDIFException {
        // Add base DN
        ldapServer.add(
                "dn: dc=example,dc=com",
                "objectClass: top",
                "objectClass: domain",
                "dc: example");

        // Add OUs
        ldapServer.add(
                "dn: ou=users,dc=example,dc=com",
                "objectClass: top",
                "objectClass: organizationalUnit",
                "ou: users");

        ldapServer.add(
                "dn: ou=groups,dc=example,dc=com",
                "objectClass: top",
                "objectClass: organizationalUnit",
                "ou: groups");

        // Add users
        ldapServer.add(
                "dn: uid=alice,ou=users,dc=example,dc=com",
                "objectClass: top",
                "objectClass: person",
                "objectClass: organizationalPerson",
                "objectClass: inetOrgPerson",
                "uid: alice",
                "cn: Alice Smith",
                "sn: Smith",
                "userPassword: alice123");

        ldapServer.add(
                "dn: uid=bob,ou=users,dc=example,dc=com",
                "objectClass: top",
                "objectClass: person",
                "objectClass: organizationalPerson",
                "objectClass: inetOrgPerson",
                "uid: bob",
                "cn: Bob Jones",
                "sn: Jones",
                "userPassword: bob123");

        ldapServer.add(
                "dn: uid=charlie,ou=users,dc=example,dc=com",
                "objectClass: top",
                "objectClass: person",
                "objectClass: organizationalPerson",
                "objectClass: inetOrgPerson",
                "uid: charlie",
                "cn: Charlie Brown",
                "sn: Brown",
                "userPassword: charlie123");

        // Add groups
        ldapServer.add(
                "dn: cn=developers,ou=groups,dc=example,dc=com",
                "objectClass: top",
                "objectClass: groupOfNames",
                "cn: developers",
                "member: uid=alice,ou=users,dc=example,dc=com");

        ldapServer.add(
                "dn: cn=analysts,ou=groups,dc=example,dc=com",
                "objectClass: top",
                "objectClass: groupOfNames",
                "cn: analysts",
                "member: uid=bob,ou=users,dc=example,dc=com");

        ldapServer.add(
                "dn: cn=employees,ou=groups,dc=example,dc=com",
                "objectClass: top",
                "objectClass: groupOfNames",
                "cn: employees",
                "member: uid=alice,ou=users,dc=example,dc=com",
                "member: uid=bob,ou=users,dc=example,dc=com",
                "member: uid=charlie,ou=users,dc=example,dc=com");

        System.out.println("✅ Test LDAP data added programmatically");
    }
}
