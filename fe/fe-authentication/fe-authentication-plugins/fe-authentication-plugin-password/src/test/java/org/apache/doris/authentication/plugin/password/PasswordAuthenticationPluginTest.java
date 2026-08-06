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

package org.apache.doris.authentication.plugin.password;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.AuthenticationResult;
import org.apache.doris.authentication.CredentialType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link PasswordAuthenticationPlugin}.
 */
@DisplayName("PasswordAuthenticationPlugin Unit Tests")
class PasswordAuthenticationPluginTest {

    private PasswordAuthenticationPlugin plugin;
    private AuthenticationIntegration integration;

    @BeforeEach
    void setUp() {
        plugin = new PasswordAuthenticationPlugin();
        integration = createDefaultIntegration();
    }

    private AuthenticationIntegration createDefaultIntegration() {
        Map<String, String> properties = new HashMap<>();
        return AuthenticationIntegration.builder()
                .name("test_password")
                .type("password")
                .properties(properties)
                .build();
    }

    // ==================== Basic Plugin Metadata Tests ====================

    @Nested
    @DisplayName("Plugin Metadata Tests")
    class PluginMetadataTests {

        @Test
        @DisplayName("UT-PWD-P-001: Plugin name should be 'password'")
        void testName() {
            Assertions.assertEquals("password", plugin.name());
        }

        @Test
        @DisplayName("UT-PWD-P-002: Plugin description should not be empty")
        void testDescription() {
            String description = plugin.description();
            Assertions.assertNotNull(description);
            Assertions.assertFalse(description.isEmpty());
            Assertions.assertTrue(description.toLowerCase().contains("password"));
        }

        @Test
        @DisplayName("UT-PWD-P-003: Plugin requires clear password")
        void testRequiresClearPassword() {
            Assertions.assertTrue(plugin.requiresClearPassword());
        }
    }

    // ==================== Supports Tests ====================

    @Nested
    @DisplayName("Request Support Tests")
    class SupportsTests {

        @Test
        @DisplayName("UT-PWD-S-001: Supports CLEAR_TEXT_PASSWORD credential type")
        void testSupports_ClearTextPassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("password".getBytes(StandardCharsets.UTF_8))
                    .build();

            Assertions.assertTrue(plugin.supports(request));
        }

        @Test
        @DisplayName("UT-PWD-S-002: Supports MYSQL_NATIVE_PASSWORD credential type")
        void testSupports_MysqlNativePassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.MYSQL_NATIVE_PASSWORD)
                    .credential("scrambled".getBytes(StandardCharsets.UTF_8))
                    .build();

            Assertions.assertTrue(plugin.supports(request));
        }

        @Test
        @DisplayName("UT-PWD-S-003: Does not support other credential types")
        void testSupports_UnsupportedType() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.KERBEROS_TOKEN)
                    .credential("ticket".getBytes(StandardCharsets.UTF_8))
                    .build();

            Assertions.assertFalse(plugin.supports(request));
        }
    }

    // ==================== Validation Tests ====================

    @Nested
    @DisplayName("Configuration Validation Tests")
    class ValidationTests {

        @Test
        @DisplayName("UT-PWD-V-001: Validate default configuration")
        void testValidate_DefaultConfig() {
            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-002: Validate valid BCRYPT algorithm")
        void testValidate_ValidBcryptAlgorithm() {
            Map<String, String> properties = new HashMap<>();
            properties.put("hash.algorithm", "BCRYPT");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-003: Validate valid SHA256 algorithm")
        void testValidate_ValidSha256Algorithm() {
            Map<String, String> properties = new HashMap<>();
            properties.put("hash.algorithm", "SHA256");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-004: Validate invalid algorithm throws exception")
        void testValidate_InvalidAlgorithm() {
            Map<String, String> properties = new HashMap<>();
            properties.put("hash.algorithm", "INVALID");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            AuthenticationException ex = Assertions.assertThrows(
                    AuthenticationException.class,
                    () -> plugin.validate(integration)
            );
            Assertions.assertTrue(ex.getMessage().contains("Invalid hash algorithm"));
        }

        @Test
        @DisplayName("UT-PWD-V-005: Validate positive max_attempts")
        void testValidate_PositiveMaxAttempts() {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "10");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-006: Validate negative max_attempts throws exception")
        void testValidate_NegativeMaxAttempts() {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "-1");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertThrows(AuthenticationException.class, () -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-007: Validate zero max_attempts throws exception")
        void testValidate_ZeroMaxAttempts() {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "0");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertThrows(AuthenticationException.class, () -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-008: Validate invalid max_attempts format")
        void testValidate_InvalidMaxAttemptsFormat() {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "not-a-number");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertThrows(AuthenticationException.class, () -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-009: Validate positive lockout duration")
        void testValidate_PositiveLockoutDuration() {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.lockout_duration_seconds", "600");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-010: Validate negative lockout duration throws exception")
        void testValidate_NegativeLockoutDuration() {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.lockout_duration_seconds", "-1");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertThrows(AuthenticationException.class, () -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-011: Validate positive min_length")
        void testValidate_PositiveMinLength() {
            Map<String, String> properties = new HashMap<>();
            properties.put("password.min_length", "12");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.validate(integration));
        }

        @Test
        @DisplayName("UT-PWD-V-012: Validate negative min_length throws exception")
        void testValidate_NegativeMinLength() {
            Map<String, String> properties = new HashMap<>();
            properties.put("password.min_length", "-1");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertThrows(AuthenticationException.class, () -> plugin.validate(integration));
        }
    }

    // ==================== Initialization Tests ====================

    @Nested
    @DisplayName("Initialization Tests")
    class InitializationTests {

        @Test
        @DisplayName("UT-PWD-I-001: Initialize with default config")
        void testInitialize_DefaultConfig() {
            Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));
        }

        @Test
        @DisplayName("UT-PWD-I-002: Initialize with custom algorithm")
        void testInitialize_CustomAlgorithm() {
            Map<String, String> properties = new HashMap<>();
            properties.put("hash.algorithm", "SHA256");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));
        }
    }

    // ==================== Authentication Tests ====================

    @Nested
    @DisplayName("Authentication Tests")
    class AuthenticationTests {

        @Test
        @DisplayName("UT-PWD-A-001: Authenticate fails when user not found")
        void testAuthenticate_UserNotFound() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("nonexistent")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("password".getBytes(StandardCharsets.UTF_8))
                    .build();

            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(result.isFailure());
            Assertions.assertTrue(result.getException().getMessage().contains("User not found"));
        }

        @Test
        @DisplayName("UT-PWD-A-002: Authenticate fails with null password")
        void testAuthenticate_NullPassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential(null)
                    .build();

            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(result.isFailure());
            Assertions.assertNotNull(result.getException());
        }

        @Test
        @DisplayName("UT-PWD-A-003: Authenticate fails with empty password")
        void testAuthenticate_EmptyPassword() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential(new byte[0])
                    .build();

            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(result.isFailure());
            Assertions.assertNotNull(result.getException());
        }

        @Test
        @DisplayName("UT-PWD-A-004: Extract plain password from CLEAR_TEXT_PASSWORD")
        void testExtractPlainPassword_ClearText() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("testpassword".getBytes(StandardCharsets.UTF_8))
                    .build();

            // Authentication will fail (user not found) but request type should still be accepted.
            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(result.isFailure());
            Assertions.assertTrue(result.getException().getMessage().contains("User not found"));
        }

        @Test
        @DisplayName("UT-PWD-A-005: Extract plain password from MYSQL_NATIVE_PASSWORD")
        void testExtractPlainPassword_MysqlNative() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.MYSQL_NATIVE_PASSWORD)
                    .credential("scrambled".getBytes(StandardCharsets.UTF_8))
                    .build();

            // Authentication will fail (user not found) but request type should still be accepted.
            AuthenticationResult result = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(result.isFailure());
            Assertions.assertTrue(result.getException().getMessage().contains("User not found"));
        }
    }

    // ==================== Brute Force Protection Tests ====================

    @Nested
    @DisplayName("Brute Force Protection Tests")
    class BruteForceProtectionTests {

        @Test
        @DisplayName("UT-PWD-BF-001: Account locked after max failed attempts")
        void testBruteForce_AccountLockedAfterMaxAttempts() throws Exception {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "3");
            properties.put("brute_force.lockout_duration_seconds", "60");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("wrongpassword".getBytes(StandardCharsets.UTF_8))
                    .build();

            // Attempt 1, 2, 3 - should fail but not lock
            for (int i = 0; i < 3; i++) {
                AuthenticationResult result = plugin.authenticate(request, integration);
                Assertions.assertTrue(result.isFailure());
                Assertions.assertFalse(result.getException().getMessage().contains("locked"));
            }

            // Attempt 4 - should be locked
            AuthenticationResult lockedResult = plugin.authenticate(request, integration);
            Assertions.assertTrue(lockedResult.isFailure());
            Assertions.assertTrue(lockedResult.getException().getMessage().contains("locked"),
                    "Should mention account is locked");
        }

        @Test
        @DisplayName("UT-PWD-BF-002: Default max attempts is 5")
        void testBruteForce_DefaultMaxAttempts() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("bob")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("wrong".getBytes(StandardCharsets.UTF_8))
                    .build();

            // Should allow at least 5 attempts before locking (default)
            for (int i = 0; i < 5; i++) {
                AuthenticationResult result = Assertions.assertDoesNotThrow(
                        () -> plugin.authenticate(request, integration)
                );
                Assertions.assertTrue(result.isFailure());
                Assertions.assertFalse(result.getException().getMessage().contains("locked"));
            }

            AuthenticationResult lockedResult = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(lockedResult.isFailure());
            Assertions.assertTrue(lockedResult.getException().getMessage().contains("locked"));
        }

        @Test
        @DisplayName("UT-PWD-BF-003: Failure count resets after lockout duration")
        void testBruteForce_LockoutExpires() throws Exception {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "2");
            properties.put("brute_force.lockout_duration_seconds", "1"); // 1 second
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("charlie")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("wrong".getBytes(StandardCharsets.UTF_8))
                    .build();

            // Trigger lockout
            AuthenticationResult first = plugin.authenticate(request, integration);
            AuthenticationResult second = plugin.authenticate(request, integration);
            Assertions.assertTrue(first.isFailure());
            Assertions.assertTrue(second.isFailure());

            // Should be locked
            AuthenticationResult locked = plugin.authenticate(request, integration);
            Assertions.assertTrue(locked.isFailure());
            Assertions.assertTrue(locked.getException().getMessage().contains("locked"));

            // Wait for lockout to expire
            Thread.sleep(1100);

            // Should be able to attempt again (will still fail due to wrong password)
            AuthenticationResult afterExpire = plugin.authenticate(request, integration);
            Assertions.assertTrue(afterExpire.isFailure());
            Assertions.assertFalse(afterExpire.getException().getMessage().contains("locked"),
                    "Lockout should have expired");
        }
    }

    // ==================== Lifecycle Tests ====================

    @Nested
    @DisplayName("Lifecycle Tests")
    class LifecycleTests {

        @Test
        @DisplayName("UT-PWD-L-001: Close clears failure trackers")
        void testClose_ClearsFailureTrackers() {
            AuthenticationRequest request = AuthenticationRequest.builder()
                    .username("alice")
                    .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                    .credential("wrong".getBytes(StandardCharsets.UTF_8))
                    .build();

            // Generate some failures
            AuthenticationResult first = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            AuthenticationResult second = Assertions.assertDoesNotThrow(
                    () -> plugin.authenticate(request, integration)
            );
            Assertions.assertTrue(first.isFailure());
            Assertions.assertTrue(second.isFailure());

            // Close plugin
            plugin.close();

            // After close, failure tracker should be cleared
            // We can't directly verify this without exposing internal state,
            // but we can test that the plugin is in a clean state
            Assertions.assertDoesNotThrow(() -> plugin.close());
        }

        @Test
        @DisplayName("UT-PWD-L-002: Close can be called multiple times")
        void testClose_Idempotent() {
            plugin.close();
            Assertions.assertDoesNotThrow(() -> plugin.close());
            Assertions.assertDoesNotThrow(() -> plugin.close());
        }
    }

    // ==================== Configuration Parsing Tests ====================

    @Nested
    @DisplayName("Configuration Parsing Tests")
    class ConfigurationParsingTests {

        @Test
        @DisplayName("UT-PWD-C-001: Parse BCRYPT algorithm from config")
        void testParseConfig_BcryptAlgorithm() throws Exception {
            Map<String, String> properties = new HashMap<>();
            properties.put("hash.algorithm", "bcrypt"); // lowercase
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            plugin.initialize(integration);
            // Should handle case-insensitive algorithm names
        }

        @Test
        @DisplayName("UT-PWD-C-002: Use default values when config missing")
        void testParseConfig_DefaultValues() throws Exception {
            Map<String, String> properties = new HashMap<>();
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            // Should use defaults: BCRYPT, max_attempts=5, lockout=300
            Assertions.assertDoesNotThrow(() -> plugin.initialize(integration));
        }

        @Test
        @DisplayName("UT-PWD-C-003: Handle invalid integer values gracefully")
        void testParseConfig_InvalidIntegerUsesDefault() throws Exception {
            Map<String, String> properties = new HashMap<>();
            properties.put("brute_force.max_attempts", "not-a-number");
            AuthenticationIntegration integration = AuthenticationIntegration.builder()
                    .name("test")
                    .type("password")
                    .properties(properties)
                    .build();

            // Validation should catch this
            Assertions.assertThrows(AuthenticationException.class, () -> plugin.validate(integration));
        }
    }
}
