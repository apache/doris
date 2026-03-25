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
 * Unit tests for {@link AuthenticationException}.
 */
@DisplayName("AuthenticationException Unit Tests")
class AuthenticationExceptionTest {

    @Test
    @DisplayName("UT-API-AE-001: Create exception with message")
    void testCreateException_WithMessage() {
        // Given
        String message = "Authentication failed";

        // When
        AuthenticationException exception = new AuthenticationException(message);

        // Then
        Assertions.assertNotNull(exception);
        Assertions.assertEquals(message, exception.getMessage());
        Assertions.assertNull(exception.getCause());
    }

    @Test
    @DisplayName("UT-API-AE-002: Create exception with message and cause")
    void testCreateException_WithMessageAndCause() {
        // Given
        String message = "LDAP authentication failed";
        Throwable cause = new RuntimeException("Connection timeout");

        // When
        AuthenticationException exception = new AuthenticationException(message, cause);

        // Then
        Assertions.assertNotNull(exception);
        Assertions.assertEquals(message, exception.getMessage());
        Assertions.assertNotNull(exception.getCause());
        Assertions.assertEquals(cause, exception.getCause());
        Assertions.assertEquals("Connection timeout", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("UT-API-AE-003: Create exception with cause only")
    void testCreateException_WithCause() {
        // Given
        Throwable cause = new IllegalArgumentException("Invalid credentials");

        // When
        AuthenticationException exception = new AuthenticationException(cause);

        // Then
        Assertions.assertNotNull(exception);
        Assertions.assertNotNull(exception.getCause());
        Assertions.assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("UT-API-AE-004: Exception is instance of Exception")
    void testExceptionHierarchy() {
        // Given
        AuthenticationException exception = new AuthenticationException("Test");

        // When & Then
        Assertions.assertInstanceOf(Exception.class, exception);
    }

    @Test
    @DisplayName("UT-API-AE-005: Exception supports stack trace")
    void testExceptionStackTrace() {
        // Given
        AuthenticationException exception = new AuthenticationException("Test exception");

        // When
        StackTraceElement[] stackTrace = exception.getStackTrace();

        // Then
        Assertions.assertNotNull(stackTrace);
        Assertions.assertTrue(stackTrace.length > 0);
        Assertions.assertTrue(exception.toString().contains("AuthenticationException"));
        Assertions.assertTrue(exception.toString().contains("Test exception"));
    }
}
