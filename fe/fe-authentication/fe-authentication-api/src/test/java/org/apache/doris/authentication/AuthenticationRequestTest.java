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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link AuthenticationRequest}.
 */
@DisplayName("AuthenticationRequest Unit Tests")
class AuthenticationRequestTest {

    @Test
    @DisplayName("UT-API-R-001: Create AuthenticationRequest with required fields")
    void testCreateRequest_RequiredFields() {
        // Given
        String username = "alice";
        String credentialType = CredentialType.CLEAR_TEXT_PASSWORD;
        byte[] credential = "password123".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username(username)
                .credentialType(credentialType)
                .credential(credential)
                .build();

        // Then
        Assertions.assertNotNull(request);
        Assertions.assertEquals(username, request.getUsername());
        Assertions.assertEquals(credentialType, request.getCredentialType());
        Assertions.assertArrayEquals(credential, request.getCredential());
    }

    @Test
    @DisplayName("UT-API-R-002: Create AuthenticationRequest with all fields")
    void testCreateRequest_AllFields() {
        // Given
        String username = "bob";
        String credentialType = CredentialType.MYSQL_NATIVE_PASSWORD;
        byte[] credential = new byte[]{1, 2, 3, 4};
        String remoteHost = "192.168.1.100";
        int remotePort = 54321;
        String clientType = "jdbc";
        Map<String, Object> properties = Map.of("key1", "value1", "key2", 123);

        // When
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username(username)
                .credentialType(credentialType)
                .credential(credential)
                .remoteHost(remoteHost)
                .remotePort(remotePort)
                .clientType(clientType)
                .properties(properties)
                .build();

        // Then
        Assertions.assertEquals(username, request.getUsername());
        Assertions.assertEquals(credentialType, request.getCredentialType());
        Assertions.assertArrayEquals(credential, request.getCredential());
        Assertions.assertEquals(remoteHost, request.getRemoteHost());
        Assertions.assertEquals(remotePort, request.getRemotePort());
        Assertions.assertEquals(clientType, request.getClientType());
        Assertions.assertEquals(properties, request.getProperties());
    }

    @Test
    @DisplayName("UT-API-R-003: Create AuthenticationRequest with null username should throw exception")
    void testCreateRequest_NullUsername() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationRequest.builder()
                        .username(null)
                        .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                        .credential("password".getBytes())
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-R-004: Create AuthenticationRequest with null credentialType should throw exception")
    void testCreateRequest_NullCredentialType() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationRequest.builder()
                        .username("alice")
                        .credentialType(null)
                        .credential("password".getBytes())
                        .build()
        );
    }

    @Test
    @DisplayName("UT-API-R-005: AuthenticationRequest properties should be immutable")
    void testRequest_PropertiesImmutability() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .property("key1", "value1")
                .build();

        // When & Then
        Map<String, Object> properties = request.getProperties();
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                properties.put("key2", "value2")
        );
    }

    @Test
    @DisplayName("UT-API-R-006: Get property value - property exists")
    void testRequest_GetProperty_Exists() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .property("email", "alice@example.com")
                .build();

        // When
        Optional<Object> propertyValue = request.getProperty("email");

        // Then
        Assertions.assertTrue(propertyValue.isPresent());
        Assertions.assertEquals("alice@example.com", propertyValue.get());
    }

    @Test
    @DisplayName("UT-API-R-007: Get property value - property does not exist")
    void testRequest_GetProperty_NotExists() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .build();

        // When
        Optional<Object> propertyValue = request.getProperty("nonexistent");

        // Then
        Assertions.assertFalse(propertyValue.isPresent());
    }

    @Test
    @DisplayName("UT-API-R-008: Get string property - valid string")
    void testRequest_GetStringProperty_ValidString() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .property("clientVersion", "1.2.3")
                .build();

        // When
        Optional<String> stringValue = request.getStringProperty("clientVersion");

        // Then
        Assertions.assertTrue(stringValue.isPresent());
        Assertions.assertEquals("1.2.3", stringValue.get());
    }

    @Test
    @DisplayName("UT-API-R-009: Get string property - non-string value")
    void testRequest_GetStringProperty_NonString() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .property("timeout", 3600)
                .build();

        // When
        Optional<String> stringValue = request.getStringProperty("timeout");

        // Then
        Assertions.assertFalse(stringValue.isPresent(), "Non-string property should return empty Optional");
    }

    @Test
    @DisplayName("UT-API-R-010: AuthenticationRequest toString should not expose credentials")
    void testRequest_ToString_NoCredentials() {
        // Given
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("secretPassword123".getBytes(StandardCharsets.UTF_8))
                .remoteHost("192.168.1.100")
                .clientType("jdbc")
                .build();

        // When
        String toString = request.toString();

        // Then
        Assertions.assertTrue(toString.contains("alice"));
        Assertions.assertTrue(toString.contains("CLEAR_TEXT_PASSWORD"));
        Assertions.assertTrue(toString.contains("192.168.1.100"));
        Assertions.assertFalse(toString.contains("secretPassword123"), "toString should not expose credential data");
    }

    @Test
    @DisplayName("UT-API-R-011: AuthenticationRequest builder - chained property calls")
    void testRequest_BuilderChainedProperties() {
        // When
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes())
                .property("prop1", "value1")
                .property("prop2", 123)
                .property("prop3", true)
                .build();

        // Then
        Assertions.assertEquals(3, request.getProperties().size());
        Assertions.assertEquals("value1", request.getProperty("prop1").get());
        Assertions.assertEquals(123, request.getProperty("prop2").get());
        Assertions.assertEquals(true, request.getProperty("prop3").get());
    }

    @Test
    @DisplayName("UT-API-R-012: AuthenticationRequest with null credential")
    void testRequest_NullCredential() {
        // When
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.X509_CERTIFICATE)
                .credential(null)
                .build();

        // Then
        Assertions.assertNull(request.getCredential());
    }
}
