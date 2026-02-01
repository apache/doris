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

import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.Identity;
import org.apache.doris.authentication.spi.AuthenticationResult;

import org.junit.Assert;
import org.junit.Test;

public class AuthenticationOutcomeTest {

    @Test
    public void testPrincipalAndIdentityAccessors() {
        AuthenticationProfile profile = AuthenticationProfile.builder()
                .name("p1")
                .pluginType(AuthenticationPluginType.PASSWORD)
                .build();

        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("password")
                .authenticatorType(AuthenticationPluginType.PASSWORD)
                .build();

        AuthenticationResult result = AuthenticationResult.success(identity);
        AuthenticationOutcome outcome = AuthenticationOutcome.of(profile, result, null);

        Assert.assertTrue(outcome.getPrincipal().isPresent());
        Assert.assertTrue(outcome.getIdentity().isPresent());
        Assert.assertFalse(outcome.getResolvedUser().isPresent());
        Assert.assertEquals("alice", outcome.getPrincipal().get().getName());
        Assert.assertEquals("alice", outcome.getIdentity().get().getUsername());
    }

    @Test
    public void testFailureHasNoIdentity() {
        AuthenticationProfile profile = AuthenticationProfile.builder()
                .name("p1")
                .pluginType(AuthenticationPluginType.PASSWORD)
                .build();

        AuthenticationResult result = AuthenticationResult.failure("invalid");
        AuthenticationOutcome outcome = AuthenticationOutcome.of(profile, result, null);

        Assert.assertFalse(outcome.getPrincipal().isPresent());
        Assert.assertFalse(outcome.getIdentity().isPresent());
        Assert.assertFalse(outcome.getResolvedUser().isPresent());
    }

    @Test
    public void testResolvedUserPresent() {
        AuthenticationProfile profile = AuthenticationProfile.builder()
                .name("p1")
                .pluginType(AuthenticationPluginType.PASSWORD)
                .build();

        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("password")
                .authenticatorType(AuthenticationPluginType.PASSWORD)
                .build();

        AuthenticationResult result = AuthenticationResult.success(identity);
        Object resolvedUser = "user-alice";
        AuthenticationOutcome outcome = AuthenticationOutcome.of(profile, result, null, resolvedUser);

        Assert.assertTrue(outcome.getResolvedUser().isPresent());
        Assert.assertEquals(resolvedUser, outcome.getResolvedUser().get());
    }
}
