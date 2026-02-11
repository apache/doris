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

/**
 * Unit tests for {@link AuthenticationResult}.
 */
@DisplayName("AuthenticationResult Unit Tests")
class AuthenticationResultTest {

    @Test
    @DisplayName("UT-API-AR-001: Create successful result with principal")
    void testCreateSuccess() {
        // Given
        Principal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("password")
                .build();

        // When
        AuthenticationResult result = AuthenticationResult.success(principal);

        // Then
        Assertions.assertNotNull(result);
        Assertions.assertEquals(AuthenticationResult.Status.SUCCESS, result.getStatus());
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(result.isFailure());
        Assertions.assertFalse(result.isContinue());
        Assertions.assertNotNull(result.getPrincipal());
        Assertions.assertEquals(principal, result.getPrincipal());
        Assertions.assertTrue(result.principal().isPresent());
        Assertions.assertNull(result.getException());
    }

    @Test
    @DisplayName("UT-API-AR-002: Create failure result with message")
    void testCreateFailure_WithMessage() {
        // Given
        String errorMessage = "Invalid credentials";

        // When
        AuthenticationResult result = AuthenticationResult.failure(errorMessage);

        // Then
        Assertions.assertNotNull(result);
        Assertions.assertEquals(AuthenticationResult.Status.FAILURE, result.getStatus());
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertTrue(result.isFailure());
        Assertions.assertFalse(result.isContinue());
        Assertions.assertNull(result.getPrincipal());
        Assertions.assertNotNull(result.getException());
        Assertions.assertEquals(errorMessage, result.getException().getMessage());
    }

    @Test
    @DisplayName("UT-API-AR-004: Create continue result")
    void testCreateContinue() {
        // Given
        Object state = new Object();
        byte[] challenge = "challenge".getBytes();

        // When
        AuthenticationResult result = AuthenticationResult.continueWith(state, challenge);

        // Then
        Assertions.assertNotNull(result);
        Assertions.assertEquals(AuthenticationResult.Status.CONTINUE, result.getStatus());
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertFalse(result.isFailure());
        Assertions.assertTrue(result.isContinue());
        Assertions.assertNull(result.getPrincipal());
        Assertions.assertNull(result.getException());
        Assertions.assertEquals(state, result.getNextState());
        Assertions.assertArrayEquals(challenge, result.getChallengeData());
    }

    @Test
    @DisplayName("UT-API-AR-006: Success result with null principal should throw exception")
    void testCreateSuccess_NullPrincipal() {
        // When & Then
        Assertions.assertThrows(NullPointerException.class, () ->
                AuthenticationResult.success((Principal) null)
        );
    }

    @Test
    @DisplayName("UT-API-AR-007: AuthenticationResult toString contains key information")
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
        Assertions.assertTrue(successToString.contains("SUCCESS"));
        Assertions.assertTrue(successToString.contains("bob"));

        Assertions.assertTrue(failureToString.contains("FAILURE"));
        Assertions.assertTrue(failureToString.contains("Auth failed"));

        Assertions.assertTrue(continueToString.contains("CONTINUE"));
    }

    @Test
    @DisplayName("UT-API-AR-008: AuthenticationResult status enum values")
    void testStatusEnum() {
        // When & Then
        Assertions.assertEquals(3, AuthenticationResult.Status.values().length);
        Assertions.assertNotNull(AuthenticationResult.Status.valueOf("SUCCESS"));
        Assertions.assertNotNull(AuthenticationResult.Status.valueOf("FAILURE"));
        Assertions.assertNotNull(AuthenticationResult.Status.valueOf("CONTINUE"));
    }

    @Test
    @DisplayName("UT-API-AR-010: Failure result with exception details")
    void testCreateFailure_WithException() {
        // Given
        AuthenticationException cause = new AuthenticationException("Connection timeout");

        // When
        AuthenticationResult result = AuthenticationResult.failure(cause);

        // Then
        Assertions.assertTrue(result.isFailure());
        Assertions.assertNotNull(result.getException());
        Assertions.assertTrue(result.getException().getMessage().contains("Connection timeout"));
    }
}
