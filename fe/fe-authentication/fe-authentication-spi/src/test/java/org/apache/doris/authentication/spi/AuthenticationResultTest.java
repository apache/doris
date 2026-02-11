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

package org.apache.doris.authentication.spi;

import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.Principal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuthenticationResult}.
 */
@DisplayName("AuthenticationResult Unit Tests")
class AuthenticationResultTest {

    @Test
    @DisplayName("UT-SPI-AR-001: Create successful result with principal")
    void testCreateSuccess() {
        // Given
        Principal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("password")
                .build();

        // When
        AuthenticationResult result = AuthenticationResult.success(principal);

        // Then
        assertNotNull(result);
        assertEquals(AuthenticationResult.Status.SUCCESS, result.getStatus());
        assertTrue(result.isSuccess());
        assertFalse(result.isFailure());
        assertFalse(result.isContinue());
        assertNotNull(result.getPrincipal());
        assertEquals(principal, result.getPrincipal());
        assertTrue(result.principal().isPresent());
        assertNull(result.getException());
    }

    @Test
    @DisplayName("UT-SPI-AR-002: Create failure result with message")
    void testCreateFailure_WithMessage() {
        // Given
        String errorMessage = "Invalid credentials";

        // When
        AuthenticationResult result = AuthenticationResult.failure(errorMessage);

        // Then
        assertNotNull(result);
        assertEquals(AuthenticationResult.Status.FAILURE, result.getStatus());
        assertFalse(result.isSuccess());
        assertTrue(result.isFailure());
        assertFalse(result.isContinue());
        assertNull(result.getPrincipal());
        assertNotNull(result.getException());
        assertEquals(errorMessage, result.getException().getMessage());
    }

    @Test
    @DisplayName("UT-SPI-AR-004: Create continue result")
    void testCreateContinue() {
        // Given
        Object state = new Object();
        byte[] challenge = "challenge".getBytes();

        // When
        AuthenticationResult result = AuthenticationResult.continueWith(state, challenge);

        // Then
        assertNotNull(result);
        assertEquals(AuthenticationResult.Status.CONTINUE, result.getStatus());
        assertFalse(result.isSuccess());
        assertFalse(result.isFailure());
        assertTrue(result.isContinue());
        assertNull(result.getPrincipal());
        assertNull(result.getException());
        assertEquals(state, result.getNextState());
        assertArrayEquals(challenge, result.getChallengeData());
    }

    @Test
    @DisplayName("UT-SPI-AR-006: Success result with null principal should throw exception")
    void testCreateSuccess_NullPrincipal() {
        // When & Then
        assertThrows(NullPointerException.class, () ->
                AuthenticationResult.success((Principal) null)
        );
    }

    @Test
    @DisplayName("UT-SPI-AR-007: AuthenticationResult toString contains key information")
    void testToString() {
        // Given
        Principal principal = BasicPrincipal.builder()
                .name("bob")
                .authenticator("ldap")
                .build();

        // When
        String successToString = AuthenticationResult.success(principal).toString();
        String failureToString = AuthenticationResult.failure("Auth failed").toString();
        String continueToString = AuthenticationResult.continueWith(null, null).toString();

        // Then
        assertTrue(successToString.contains("SUCCESS"));
        assertTrue(successToString.contains("bob"));

        assertTrue(failureToString.contains("FAILURE"));
        assertTrue(failureToString.contains("Auth failed"));

        assertTrue(continueToString.contains("CONTINUE"));
    }

    @Test
    @DisplayName("UT-SPI-AR-008: AuthenticationResult status enum values")
    void testStatusEnum() {
        // When & Then
        assertEquals(3, AuthenticationResult.Status.values().length);
        assertNotNull(AuthenticationResult.Status.valueOf("SUCCESS"));
        assertNotNull(AuthenticationResult.Status.valueOf("FAILURE"));
        assertNotNull(AuthenticationResult.Status.valueOf("CONTINUE"));
    }

    @Test
    @DisplayName("UT-SPI-AR-010: Failure result with exception details")
    void testCreateFailure_WithException() {
        // Given
        AuthenticationException cause = new AuthenticationException("Connection timeout");

        // When
        AuthenticationResult result = AuthenticationResult.failure(cause);

        // Then
        assertTrue(result.isFailure());
        assertNotNull(result.getException());
        assertTrue(result.getException().getMessage().contains("Connection timeout"));
    }
}
