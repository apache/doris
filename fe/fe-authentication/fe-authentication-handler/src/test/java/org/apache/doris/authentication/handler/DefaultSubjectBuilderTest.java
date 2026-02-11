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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.Subject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit tests for {@link DefaultSubjectBuilder}.
 */
@DisplayName("DefaultSubjectBuilder Unit Tests")
class DefaultSubjectBuilderTest {

    private DefaultSubjectBuilder subjectBuilder;
    private BasicPrincipal testPrincipal;
    private AuthenticationRequest testRequest;

    @BeforeEach
    void setUp() {
        subjectBuilder = new DefaultSubjectBuilder();

        testPrincipal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("test_integration")
                .build();

        testRequest = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .remoteHost("192.168.1.100")
                .build();
    }

    @Test
    @DisplayName("UT-DSB-001: Build subject with all fields")
    void testBuild_AllFields() {
        Set<String> roles = new HashSet<>(Arrays.asList("admin", "developer"));

        Subject subject = subjectBuilder.build(testPrincipal, roles, testRequest);

        Assertions.assertNotNull(subject);
        Assertions.assertEquals(testPrincipal, subject.getPrincipal());
        Assertions.assertEquals(2, subject.getAvailableRoles().size());
        Assertions.assertTrue(subject.getAvailableRoles().contains("admin"));
        Assertions.assertTrue(subject.getAvailableRoles().contains("developer"));
        Assertions.assertEquals(2, subject.getActiveRoles().size());
        Assertions.assertEquals("192.168.1.100", subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-DSB-002: Build subject with null roles")
    void testBuild_NullRoles() {
        Subject subject = subjectBuilder.build(testPrincipal, null, testRequest);

        Assertions.assertNotNull(subject);
        Assertions.assertEquals(testPrincipal, subject.getPrincipal());
        Assertions.assertTrue(subject.getAvailableRoles().isEmpty());
        Assertions.assertTrue(subject.getActiveRoles().isEmpty());
        Assertions.assertEquals("192.168.1.100", subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-DSB-003: Build subject with empty roles")
    void testBuild_EmptyRoles() {
        Subject subject = subjectBuilder.build(testPrincipal, Collections.emptySet(), testRequest);

        Assertions.assertNotNull(subject);
        Assertions.assertTrue(subject.getAvailableRoles().isEmpty());
        Assertions.assertTrue(subject.getActiveRoles().isEmpty());
    }

    @Test
    @DisplayName("UT-DSB-004: Build subject with null request")
    void testBuild_NullRequest() {
        Set<String> roles = new HashSet<>(Collections.singletonList("user"));

        Subject subject = subjectBuilder.build(testPrincipal, roles, null);

        Assertions.assertNotNull(subject);
        Assertions.assertEquals(testPrincipal, subject.getPrincipal());
        Assertions.assertEquals(1, subject.getAvailableRoles().size());
        Assertions.assertNull(subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-DSB-005: Build subject with null remoteHost in request")
    void testBuild_NullRemoteHost() {
        AuthenticationRequest requestWithoutHost = AuthenticationRequest.builder()
                .username("bob")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .build();

        Subject subject = subjectBuilder.build(testPrincipal, null, requestWithoutHost);

        Assertions.assertNotNull(subject);
        Assertions.assertNull(subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-DSB-006: Available roles and active roles are equal")
    void testBuild_RolesAreEqual() {
        Set<String> roles = new HashSet<>(Arrays.asList("role1", "role2", "role3"));

        Subject subject = subjectBuilder.build(testPrincipal, roles, testRequest);

        Assertions.assertEquals(subject.getAvailableRoles(), subject.getActiveRoles());
    }

    @Test
    @DisplayName("UT-DSB-007: Roles are copied (not same instance)")
    void testBuild_RolesCopied() {
        Set<String> roles = new HashSet<>(Arrays.asList("admin"));

        Subject subject = subjectBuilder.build(testPrincipal, roles, testRequest);

        // Modifying original set should not affect subject
        roles.add("hacker");

        Assertions.assertEquals(1, subject.getAvailableRoles().size());
        Assertions.assertFalse(subject.getAvailableRoles().contains("hacker"));
    }

    @Test
    @DisplayName("UT-DSB-008: Build multiple subjects with same input")
    void testBuild_MultipleCalls() {
        Set<String> roles = new HashSet<>(Collections.singletonList("user"));

        Subject subject1 = subjectBuilder.build(testPrincipal, roles, testRequest);
        Subject subject2 = subjectBuilder.build(testPrincipal, roles, testRequest);

        Assertions.assertNotNull(subject1);
        Assertions.assertNotNull(subject2);
        Assertions.assertNotSame(subject1, subject2); // Different instances
        Assertions.assertEquals(subject1.getPrincipal(), subject2.getPrincipal());
    }

    @Test
    @DisplayName("UT-DSB-009: Build subject with IPv6 address")
    void testBuild_IPv6Address() {
        AuthenticationRequest ipv6Request = AuthenticationRequest.builder()
                .username("charlie")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .remoteHost("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
                .build();

        Subject subject = subjectBuilder.build(testPrincipal, null, ipv6Request);

        Assertions.assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-DSB-010: Build subject with hostname")
    void testBuild_Hostname() {
        AuthenticationRequest hostnameRequest = AuthenticationRequest.builder()
                .username("dave")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .credential("password".getBytes(StandardCharsets.UTF_8))
                .remoteHost("workstation.example.com")
                .build();

        Subject subject = subjectBuilder.build(testPrincipal, null, hostnameRequest);

        Assertions.assertEquals("workstation.example.com", subject.getSourceIp());
    }

    @Test
    @DisplayName("UT-DSB-011: Build subject with single role")
    void testBuild_SingleRole() {
        Set<String> roles = Collections.singleton("admin");

        Subject subject = subjectBuilder.build(testPrincipal, roles, testRequest);

        Assertions.assertEquals(1, subject.getAvailableRoles().size());
        Assertions.assertEquals(1, subject.getActiveRoles().size());
        Assertions.assertTrue(subject.getAvailableRoles().contains("admin"));
    }

    @Test
    @DisplayName("UT-DSB-012: Build subject with many roles")
    void testBuild_ManyRoles() {
        Set<String> roles = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            roles.add("role_" + i);
        }

        Subject subject = subjectBuilder.build(testPrincipal, roles, testRequest);

        Assertions.assertEquals(100, subject.getAvailableRoles().size());
        Assertions.assertEquals(100, subject.getActiveRoles().size());
    }
}
